"""
Phase 5 — KPI Aggregation Engine
==================================
Computes 5 defensible operational KPIs from PostgreSQL core tables
and writes the evidence table to output/kpi_summary_dashboard.csv.

KPI definitions are also executed as SQL VIEWs (sql/03_kpis.sql).
This module provides the Python bridge for loading KPI results.

Semantic constraint honoured throughout:
  KPI 1, 5: calendar unavailability is an OCCUPANCY PROXY, not a booking.
  KPI 2   : listed price volatility, not accepted price.
  KPI 3   : review velocity is a LOWER-BOUND proxy for completed stays.
  KPI 4   : correlation analysis; causation is explicitly disclaimed.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import text

from pipeline.db import engine_from_config
from pipeline.logger import StepTimer, get_logger

log = get_logger("kpi")


def _engine(cfg: dict):
    return engine_from_config(cfg)


def _exec_sql_file(engine, sql_path: Path):
    sql = sql_path.read_text()
    with engine.begin() as conn:
        conn.execute(text(sql))
    log.info(f"Executed SQL file: {sql_path}", extra={"step": "kpi.sql"})


def _query(engine, sql: str) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn)


# ---------------------------------------------------------------------------
# KPI 1: Adjusted Listing Unavailability Rate (Occupancy Proxy)
# ---------------------------------------------------------------------------
KPI1_SQL = """
SELECT
    l.neighbourhood,
    DATE_TRUNC('month', c.date)::date                          AS month,
    COUNT(*)                                                    AS total_days,
    SUM(CASE WHEN c.is_unavailable THEN 1 ELSE 0 END)          AS unavailable_days,
    ROUND(
        100.0 * SUM(CASE WHEN c.is_unavailable THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
        2
    )                                                           AS unavailability_rate_pct,
    COUNT(DISTINCT c.listing_id)                               AS active_listings
FROM stg_calendar c
JOIN stg_listings l ON l.id::text = c.listing_id::text
WHERE c.date IS NOT NULL
GROUP BY l.neighbourhood, DATE_TRUNC('month', c.date)
HAVING COUNT(*) >= 30   -- Exclude listings with < 30 days calendar coverage
ORDER BY l.neighbourhood, month;
"""

# ---------------------------------------------------------------------------
# KPI 2: Listed Price Volatility Index (CoV per listing per month)
# ---------------------------------------------------------------------------
KPI2_SQL = """
SELECT
    l.neighbourhood,
    l.room_type,
    DATE_TRUNC('month', c.date)::date          AS month,
    COUNT(DISTINCT c.listing_id)               AS listings_included,
    ROUND(AVG(c.price_usd)::numeric, 2)        AS avg_listed_price,
    ROUND(STDDEV(c.price_usd)::numeric, 2)     AS stddev_price,
    ROUND(
        CASE WHEN AVG(c.price_usd) > 0
             THEN 100.0 * STDDEV(c.price_usd) / AVG(c.price_usd)
             ELSE NULL
        END::numeric,
        2
    )                                          AS price_volatility_cov_pct
FROM stg_calendar c
JOIN stg_listings l ON l.id::text = c.listing_id::text
WHERE c.price_usd IS NOT NULL
  AND c.date IS NOT NULL
GROUP BY l.neighbourhood, l.room_type, DATE_TRUNC('month', c.date)
HAVING COUNT(c.price_usd) >= 15   -- At least 15 priced days in the month
ORDER BY l.neighbourhood, l.room_type, month;
"""

# ---------------------------------------------------------------------------
# KPI 2B: Fallback listing price baseline (uses listing-level prices only)
# ---------------------------------------------------------------------------
KPI2B_SQL = """
SELECT
    l.neighbourhood,
    l.room_type,
    COUNT(*) AS listings_with_price,
    ROUND(AVG(l.price_usd)::numeric, 2) AS avg_listing_price_usd,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY l.price_usd)::numeric, 2) AS median_listing_price_usd,
    ROUND(PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY l.price_usd)::numeric, 2) AS p90_listing_price_usd
FROM stg_listings l
WHERE l.price_usd IS NOT NULL
GROUP BY l.neighbourhood, l.room_type
ORDER BY l.neighbourhood, l.room_type;
"""

# ---------------------------------------------------------------------------
# KPI 3: Guest Review Velocity (Stay Proxy)
# ---------------------------------------------------------------------------
KPI3_SQL = """
SELECT
    l.neighbourhood,
    DATE_TRUNC('month', r.date)::date          AS month,
    COUNT(r.id)                                AS review_count,
    COUNT(DISTINCT r.listing_id)               AS listings_with_reviews,
    ROUND(COUNT(r.id)::numeric
          / NULLIF(COUNT(DISTINCT r.listing_id), 0), 2)
                                               AS avg_reviews_per_listing
FROM stg_reviews r
JOIN stg_listings l ON l.id::text = r.listing_id::text
WHERE r.orphaned = FALSE
  AND r.date IS NOT NULL
GROUP BY l.neighbourhood, DATE_TRUNC('month', r.date)
ORDER BY l.neighbourhood, month;
"""

# ---------------------------------------------------------------------------
# KPI 4: Weather Sensitivity Index
# ---------------------------------------------------------------------------
KPI4_SQL = """
WITH daily_unavail AS (
    SELECT
        date::date                                              AS obs_date,
        ROUND(100.0 * SUM(CASE WHEN is_unavailable THEN 1 ELSE 0 END)
              / NULLIF(COUNT(*), 0), 4)                        AS unavailability_rate_pct
    FROM stg_calendar
    WHERE date IS NOT NULL
    GROUP BY date::date
),
weather AS (
    SELECT
        date::date          AS obs_date,
        precipitation_mm,
        wind_speed_max_kmh,
        temp_max_c
    FROM stg_weather
    WHERE date IS NOT NULL
)
SELECT
    d.obs_date,
    d.unavailability_rate_pct,
    w.precipitation_mm,
    w.wind_speed_max_kmh,
    w.temp_max_c
FROM daily_unavail d
JOIN weather w ON w.obs_date = d.obs_date
ORDER BY d.obs_date;
"""

# ---------------------------------------------------------------------------
# KPI 5: Estimated Revenue Potential Proxy
# ---------------------------------------------------------------------------
KPI5_SQL = """
WITH proxy_pricing AS (
    SELECT
        c.listing_id,
        c.date,
        l.room_type,
        l.neighbourhood,
        CASE
            WHEN c.is_unavailable = TRUE AND c.price_usd IS NOT NULL THEN c.price_usd
            WHEN c.is_unavailable = TRUE AND c.price_usd IS NULL AND l.price_usd IS NOT NULL THEN l.price_usd
            ELSE NULL
        END AS proxy_price_usd,
        CASE
            WHEN c.is_unavailable = TRUE AND c.price_usd IS NOT NULL THEN 'calendar_price'
            WHEN c.is_unavailable = TRUE AND l.price_usd IS NOT NULL THEN 'listing_price_fallback'
            ELSE 'unpriced'
        END AS price_basis,
        c.is_unavailable
    FROM stg_calendar c
    JOIN stg_listings l ON l.id::text = c.listing_id::text
    WHERE c.date IS NOT NULL
)
SELECT
    room_type,
    neighbourhood,
    DATE_TRUNC('month', date)::date                           AS month,
    COUNT(CASE WHEN is_unavailable THEN 1 END)                AS unavailable_days,
    ROUND(SUM(proxy_price_usd)::numeric, 2)                   AS rev_proxy_usd,
    COUNT(*) FILTER (WHERE price_basis = 'calendar_price')   AS calendar_priced_unavailable_days,
    COUNT(*) FILTER (WHERE price_basis = 'listing_price_fallback') AS fallback_priced_unavailable_days,
    COUNT(*) FILTER (WHERE price_basis = 'unpriced')         AS unpriced_unavailable_days,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE price_basis <> 'unpriced')
        / NULLIF(COUNT(*), 0), 2
    )                                                         AS price_coverage_pct,
    CASE
        WHEN COUNT(*) FILTER (WHERE price_basis = 'unpriced') = COUNT(*) THEN 'unpriced'
        WHEN COUNT(*) FILTER (WHERE price_basis = 'unpriced') > 0 THEN 'mixed'
        WHEN COUNT(*) FILTER (WHERE price_basis = 'listing_price_fallback') > 0 THEN 'listing_price_fallback'
        ELSE 'calendar_price'
    END                                                       AS price_basis,
    COUNT(DISTINCT listing_id)                                AS listings_included
FROM proxy_pricing
WHERE is_unavailable = TRUE
GROUP BY room_type, neighbourhood, DATE_TRUNC('month', date)
ORDER BY room_type, neighbourhood, month;
"""


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run_kpi(cfg: dict, sql_dir: Path, output_dir: Path) -> dict[str, pd.DataFrame]:
    """Execute KPI SQL aggregations and export evidence tables."""
    log.info("=== Phase 5: KPI Aggregation START ===", extra={"step": "kpi"})
    engine = _engine(cfg)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Execute KPI view definitions
    with StepTimer(log, "kpi.views"):
        _exec_sql_file(engine, sql_dir / "03_kpis.sql")

    kpis: dict[str, pd.DataFrame] = {}

    with StepTimer(log, "kpi.compute"):
        kpis["kpi1_unavailability_rate"]       = _query(engine, KPI1_SQL)
        kpis["kpi2_price_volatility"]          = _query(engine, KPI2_SQL)
        kpis["kpi2b_listing_price_baseline"]   = _query(engine, KPI2B_SQL)
        kpis["kpi3_review_velocity"]           = _query(engine, KPI3_SQL)
        kpis["kpi4_weather_sensitivity"]       = _query(engine, KPI4_SQL)
        kpis["kpi5_revenue_proxy"]             = _query(engine, KPI5_SQL)

    # Compute correlation for KPI 4 summary
    kpi4 = kpis["kpi4_weather_sensitivity"]
    if len(kpi4) > 10:
        corr_precip = kpi4["unavailability_rate_pct"].corr(kpi4["precipitation_mm"])
        corr_wind   = kpi4["unavailability_rate_pct"].corr(kpi4["wind_speed_max_kmh"])
        corr_temp   = kpi4["unavailability_rate_pct"].corr(kpi4["temp_max_c"])
        kpis["kpi4_weather_correlations"] = pd.DataFrame([{
            "metric": "unavailability_rate_pct",
            "corr_with_precipitation_mm":   round(float(corr_precip), 4),
            "corr_with_wind_speed_max_kmh": round(float(corr_wind), 4),
            "corr_with_temp_max_c":         round(float(corr_temp), 4),
            "n_observations":               len(kpi4),
            "note": "Correlation does not imply causation. External factors (tourism events, school holidays) not controlled.",
        }])
        log.info(
            f"KPI4 correlations — precip: {corr_precip:.4f}  wind: {corr_wind:.4f}  temp: {corr_temp:.4f}",
            extra={"step": "kpi.kpi4"},
        )

    # Write individual CSVs
    for name, df in kpis.items():
        csv_path = output_dir / f"{name}.csv"
        df.to_csv(csv_path, index=False)
        log.info(
            f"Saved {name}: {len(df):,} rows → {csv_path}",
            extra={"step": "kpi.export", "meta": {"rows": len(df), "path": str(csv_path)}},
        )

    # Write consolidated summary dashboard
    summary_rows = []
    for name, df in kpis.items():
        summary_rows.append({
            "kpi_name": name,
            "n_rows": len(df),
            "columns": ", ".join(df.columns.tolist()),
        })
    summary_df = pd.DataFrame(summary_rows)
    summary_df.to_csv(output_dir / "kpi_summary_dashboard.csv", index=False)

    log.info("=== Phase 5: KPI Aggregation COMPLETE ===", extra={"step": "kpi"})
    return kpis
