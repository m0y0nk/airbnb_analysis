"""
Phase 3 — Data Profiling & Business-Oriented Validation
=========================================================
Principle of Non-Silent Handling:
  - Fatal violations  → pipeline halts (status=ERROR)
  - Warning violations → records flagged; pipeline continues with annotation

Validation rules implemented:
  [Structural]
    V01: Duplicate listing IDs
    V02: Invalid latitude/longitude bounds
    V03: Missing price on available calendar days
    V04: Composite key uniqueness (listing_id, date) in calendar

  [Lifecycle / Temporal]
    V05: Calendar dates prior to minimum temporal bound (2020-01-01)
    V06: Review dates after today

  [Cross-Data Referential Integrity]
    V07: Orphaned reviews (listing_id not in listings.id)
    V08: Calendar rows referencing unknown listing IDs

  [Weather]
    V09: Missing daily weather records for expected date range
"""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from pipeline.logger import StepTimer, get_logger

log = get_logger("validate")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_price(series: pd.Series) -> pd.Series:
    """Strip '$', ',' and cast to float; return NaN for unparseable values."""
    return (
        series.astype(str)
        .str.replace(r"[\$,]", "", regex=True)
        .str.strip()
        .replace("", np.nan)
        .replace("nan", np.nan)
        .astype(float)
    )


def _load_listings(path: Path) -> pd.DataFrame:
    cols = ["id", "host_id", "neighbourhood_cleansed", "latitude", "longitude",
            "room_type", "price", "last_scraped"]
    df = pd.read_csv(path, compression="infer", usecols=lambda c: c in cols, low_memory=False)
    if "price" in df.columns:
        df["price_numeric"] = _parse_price(df["price"])
    return df


def _load_calendar(path: Path) -> pd.DataFrame:
    cols = ["listing_id", "date", "available", "price", "adjusted_price", "minimum_nights"]
    df = pd.read_csv(path, compression="infer", usecols=lambda c: c in cols, low_memory=False)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    if "price" in df.columns:
        df["price_numeric"] = _parse_price(df["price"])
    return df


def _load_reviews(path: Path) -> pd.DataFrame:
    cols = ["id", "listing_id", "date", "reviewer_id"]
    df = pd.read_csv(path, compression="infer", usecols=lambda c: c in cols, low_memory=False)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df


def _write_rejects(output_dir: Path | None, source: str, frame: pd.DataFrame, mask: pd.Series, reason: str) -> int:
    """Persist rejected source rows with an explicit reason for auditability."""
    rejected = frame.loc[mask].copy()
    if rejected.empty:
        return 0
    rejected.insert(0, "source_name", source)
    rejected.insert(1, "reject_reason", reason)
    if output_dir is not None:
        reject_dir = output_dir / "rejected"
        reject_dir.mkdir(parents=True, exist_ok=True)
        rejected.to_csv(reject_dir / f"{source}_rejected.csv", index=False)
    return len(rejected)


# ---------------------------------------------------------------------------
# Core validation engine
# ---------------------------------------------------------------------------

def validate_sources(
    listings_path: Path,
    calendar_path: Path,
    reviews_path: Path,
    weather_payload: dict[str, Any],
    cfg: dict,
    output_dir: Path | None = None,
) -> dict[str, Any]:
    """
    Run all validation rules and return a structured Validation Report.
    Raises SystemExit on FATAL errors.
    """
    report: dict[str, Any] = {
        "run_ts": datetime.utcnow().isoformat() + "Z",
        "status": "PASS",
        "errors": [],
        "warnings": [],
        "metrics": {},
        "profiles": {},
        "rejected_rows": {},
    }

    max_dup_rate = cfg["validation"]["max_allowed_duplicate_rate"]
    max_orphan_rate = cfg["validation"]["max_allowed_orphan_reviews"]
    min_cal_date = pd.Timestamp(cfg["validation"]["min_calendar_date"])
    today = pd.Timestamp(date.today())

    # -----------------------------------------------------------------------
    # Load sources
    # -----------------------------------------------------------------------
    log.info("Loading sources for profiling …", extra={"step": "validate.load"})
    listings = _load_listings(listings_path)
    calendar = _load_calendar(calendar_path)
    reviews  = _load_reviews(reviews_path)

    report["metrics"]["n_listings_raw"] = len(listings)
    report["metrics"]["n_calendar_raw"] = len(calendar)
    report["metrics"]["n_reviews_raw"]  = len(reviews)
    log.info(
        f"Loaded: listings={len(listings):,}  calendar={len(calendar):,}  reviews={len(reviews):,}",
        extra={"step": "validate.load"},
    )

    # -----------------------------------------------------------------------
    # PROFILES — exploratory summaries (non-blocking)
    # -----------------------------------------------------------------------
    report["profiles"]["listings"] = {
        "columns": list(listings.columns),
        "n_rows": len(listings),
        "price_null_pct": round(listings["price_numeric"].isna().mean() * 100, 2) if "price_numeric" in listings.columns else None,
        "lat_range": [float(listings["latitude"].min()), float(listings["latitude"].max())] if "latitude" in listings.columns else None,
        "lon_range": [float(listings["longitude"].min()), float(listings["longitude"].max())] if "longitude" in listings.columns else None,
    }
    report["profiles"]["calendar"] = {
        "n_rows": len(calendar),
        "date_min": str(calendar["date"].min().date()) if not calendar["date"].isna().all() else None,
        "date_max": str(calendar["date"].max().date()) if not calendar["date"].isna().all() else None,
        "availability_pct_f": round((calendar["available"] == "f").mean() * 100, 2) if "available" in calendar.columns else None,
        "price_null_pct": round(calendar["price_numeric"].isna().mean() * 100, 2) if "price_numeric" in calendar.columns else None,
    }
    report["profiles"]["reviews"] = {
        "n_rows": len(reviews),
        "date_min": str(reviews["date"].min().date()) if not reviews["date"].isna().all() else None,
        "date_max": str(reviews["date"].max().date()) if not reviews["date"].isna().all() else None,
        "unique_listings_with_reviews": int(reviews["listing_id"].nunique()),
    }

    listing_rejects = listings["id"].isna() | listings["id"].duplicated(keep="first")
    calendar_rejects = calendar["date"].isna() | calendar.duplicated(subset=["listing_id", "date"], keep="first")
    review_rejects = reviews["date"].isna() | reviews["listing_id"].isna()
    report["rejected_rows"]["listings"] = _write_rejects(
        output_dir, "listings", listings, listing_rejects, "missing_or_duplicate_listing_id"
    )
    report["rejected_rows"]["calendar"] = _write_rejects(
        output_dir, "calendar", calendar, calendar_rejects, "invalid_date_or_duplicate_listing_date"
    )
    report["rejected_rows"]["reviews"] = _write_rejects(
        output_dir, "reviews", reviews, review_rejects, "missing_review_date_or_listing_id"
    )
    log.info(
        f"Quarantined rejected rows — listings={report['rejected_rows']['listings']:,}, "
        f"calendar={report['rejected_rows']['calendar']:,}, reviews={report['rejected_rows']['reviews']:,}",
        extra={"step": "validate.rejects", "meta": report["rejected_rows"]},
    )

    # -----------------------------------------------------------------------
    # V01 — Duplicate listing IDs  (FATAL)
    # -----------------------------------------------------------------------
    dup_count = int(listings["id"].duplicated().sum())
    dup_rate  = dup_count / max(len(listings), 1)
    report["metrics"]["v01_duplicate_listings"] = dup_count
    if dup_rate > max_dup_rate:
        msg = f"V01 FATAL: {dup_count} duplicate listing IDs (rate {dup_rate:.4%} > threshold {max_dup_rate:.4%})."
        log.error(msg, extra={"step": "validate.v01"})
        report["errors"].append(msg)
        report["status"] = "ERROR"
    else:
        log.info(f"V01 PASS: {dup_count} duplicate listing IDs (within threshold).", extra={"step": "validate.v01"})

    # -----------------------------------------------------------------------
    # V02 — Invalid lat/lon bounds  (FATAL)
    # -----------------------------------------------------------------------
    if "latitude" in listings.columns and "longitude" in listings.columns:
        invalid_coords = listings[
            ~listings["latitude"].between(-90, 90) | ~listings["longitude"].between(-180, 180)
        ]
        n_invalid = len(invalid_coords)
        report["metrics"]["v02_invalid_coords"] = n_invalid
        if n_invalid > 0:
            msg = f"V02 FATAL: {n_invalid} listings with coordinates outside valid bounds."
            log.error(msg, extra={"step": "validate.v02"})
            report["errors"].append(msg)
            report["status"] = "ERROR"
        else:
            log.info("V02 PASS: All listing coordinates within valid bounds.", extra={"step": "validate.v02"})

    # -----------------------------------------------------------------------
    # V03 — Missing price on available calendar days  (WARNING)
    # -----------------------------------------------------------------------
    if "available" in calendar.columns and "price_numeric" in calendar.columns:
        available_rows = calendar[calendar["available"] == "t"]
        missing_price_on_avail = available_rows["price_numeric"].isna().sum()
        report["metrics"]["v03_missing_price_on_available"] = int(missing_price_on_avail)
        if missing_price_on_avail > 0:
            msg = f"V03 WARNING: {missing_price_on_avail:,} available calendar days have no listed price."
            log.warning(msg, extra={"step": "validate.v03"})
            report["warnings"].append(msg)
            if report["status"] == "PASS":
                report["status"] = "WARNING"
        else:
            log.info("V03 PASS: No missing prices on available calendar days.", extra={"step": "validate.v03"})

    # -----------------------------------------------------------------------
    # V04 — Composite key uniqueness (listing_id, date)  (WARNING)
    # -----------------------------------------------------------------------
    dup_cal = int(calendar.duplicated(subset=["listing_id", "date"]).sum())
    report["metrics"]["v04_duplicate_calendar_keys"] = dup_cal
    if dup_cal > 0:
        msg = f"V04 WARNING: {dup_cal:,} duplicate (listing_id, date) rows in calendar."
        log.warning(msg, extra={"step": "validate.v04"})
        report["warnings"].append(msg)
        if report["status"] == "PASS":
            report["status"] = "WARNING"
    else:
        log.info("V04 PASS: Calendar composite key is unique.", extra={"step": "validate.v04"})

    # -----------------------------------------------------------------------
    # V05 — Dates before minimum temporal bound  (WARNING)
    # -----------------------------------------------------------------------
    early_cal = int((calendar["date"] < min_cal_date).sum())
    report["metrics"]["v05_calendar_pre2020"] = early_cal
    if early_cal > 0:
        msg = f"V05 WARNING: {early_cal:,} calendar rows before {cfg['validation']['min_calendar_date']}."
        log.warning(msg, extra={"step": "validate.v05"})
        report["warnings"].append(msg)
        if report["status"] == "PASS":
            report["status"] = "WARNING"
    else:
        log.info("V05 PASS: All calendar dates within temporal bounds.", extra={"step": "validate.v05"})

    # -----------------------------------------------------------------------
    # V06 — Review dates in the future  (WARNING)
    # -----------------------------------------------------------------------
    future_reviews = int((reviews["date"] > today).sum())
    report["metrics"]["v06_future_review_dates"] = future_reviews
    if future_reviews > 0:
        msg = f"V06 WARNING: {future_reviews:,} review records with future timestamps."
        log.warning(msg, extra={"step": "validate.v06"})
        report["warnings"].append(msg)
        if report["status"] == "PASS":
            report["status"] = "WARNING"
    else:
        log.info("V06 PASS: No future review dates.", extra={"step": "validate.v06"})

    # -----------------------------------------------------------------------
    # V07 — Orphaned reviews  (WARNING if below threshold, ERROR if above)
    # -----------------------------------------------------------------------
    valid_listing_ids = set(listings["id"].dropna().astype(str))
    orphaned_mask = ~reviews["listing_id"].astype(str).isin(valid_listing_ids)
    orphaned_count = int(orphaned_mask.sum())
    orphaned_rate  = orphaned_count / max(len(reviews), 1)
    report["metrics"]["v07_orphaned_reviews"] = orphaned_count
    report["metrics"]["v07_orphaned_reviews_rate"] = round(orphaned_rate, 4)
    if orphaned_rate > max_orphan_rate:
        msg = f"V07 ERROR: {orphaned_count:,} orphaned review records (rate {orphaned_rate:.2%} > threshold {max_orphan_rate:.2%})."
        log.error(msg, extra={"step": "validate.v07"})
        report["errors"].append(msg)
        report["status"] = "ERROR"
    elif orphaned_count > 0:
        msg = f"V07 WARNING: {orphaned_count:,} orphaned review records (within tolerance, rate {orphaned_rate:.2%})."
        log.warning(msg, extra={"step": "validate.v07"})
        report["warnings"].append(msg)
        if report["status"] == "PASS":
            report["status"] = "WARNING"
    else:
        log.info("V07 PASS: All review listing IDs match known listings.", extra={"step": "validate.v07"})

    # -----------------------------------------------------------------------
    # V08 — Calendar rows referencing unknown listings  (WARNING)
    # -----------------------------------------------------------------------
    unknown_cal = int(~calendar["listing_id"].astype(str).isin(valid_listing_ids).sum() if len(calendar) > 0 else 0)
    # Correct computation:
    unknown_cal = int((~calendar["listing_id"].astype(str).isin(valid_listing_ids)).sum())
    report["metrics"]["v08_calendar_unknown_listings"] = unknown_cal
    if unknown_cal > 0:
        msg = f"V08 WARNING: {unknown_cal:,} calendar rows reference listing IDs not in listings file."
        log.warning(msg, extra={"step": "validate.v08"})
        report["warnings"].append(msg)
        if report["status"] == "PASS":
            report["status"] = "WARNING"
    else:
        log.info("V08 PASS: All calendar listing IDs resolved.", extra={"step": "validate.v08"})

    # -----------------------------------------------------------------------
    # V09 — Weather record completeness
    # -----------------------------------------------------------------------
    weather_dates = weather_payload.get("daily", {}).get("time", [])
    n_weather_days = len(weather_dates)
    report["metrics"]["v09_weather_days_fetched"] = n_weather_days
    if n_weather_days < 30:
        msg = f"V09 WARNING: Only {n_weather_days} weather days fetched — less than 30-day minimum for meaningful analysis."
        log.warning(msg, extra={"step": "validate.v09"})
        report["warnings"].append(msg)
        if report["status"] == "PASS":
            report["status"] = "WARNING"
    else:
        log.info(f"V09 PASS: {n_weather_days} weather days fetched.", extra={"step": "validate.v09"})

    # -----------------------------------------------------------------------
    # V10 — Future calendar dates beyond the source snapshot window
    # -----------------------------------------------------------------------
    future_calendar_days = int((calendar["date"] > today).sum()) if "date" in calendar.columns else 0
    report["metrics"]["v10_future_calendar_dates"] = future_calendar_days
    if future_calendar_days > 0:
        msg = (
            f"V10 WARNING: {future_calendar_days:,} calendar rows fall after the analysis date "
            f"({today.date()}); source is forward-looking and should be interpreted as a horizon, not a snapshot."
        )
        log.warning(msg, extra={"step": "validate.v10"})
        report["warnings"].append(msg)
        if report["status"] == "PASS":
            report["status"] = "WARNING"
    else:
        log.info("V10 PASS: No future calendar dates beyond the current analysis date.", extra={"step": "validate.v10"})

    # -----------------------------------------------------------------------
    # V11 — Missing price field on calendar rows (used by price-based KPIs)
    # -----------------------------------------------------------------------
    has_calendar_price = "price_numeric" in calendar.columns or "price_usd" in calendar.columns
    report["metrics"]["v11_calendar_price_available"] = bool(has_calendar_price)
    if not has_calendar_price:
        msg = (
            "V11 WARNING: Calendar pricing is unavailable in the source file; price-based KPIs "
            "must be treated as unsupported or derived from listing-level price data instead."
        )
        log.warning(msg, extra={"step": "validate.v11"})
        report["warnings"].append(msg)
        if report["status"] == "PASS":
            report["status"] = "WARNING"
    else:
        log.info("V11 PASS: Calendar pricing column is available for price-based KPI calculations.", extra={"step": "validate.v11"})

    # -----------------------------------------------------------------------
    # V12 — Listing-level price availability as fallback basis for price metrics
    # -----------------------------------------------------------------------
    listing_price_count = int(listings["price_numeric"].notna().sum()) if "price_numeric" in listings.columns else 0
    report["metrics"]["v12_listing_price_available"] = listing_price_count
    if listing_price_count == 0:
        msg = (
            "V12 WARNING: No listing-level prices are available; the listing-price baseline fallback "
            "cannot be computed from this source."
        )
        log.warning(msg, extra={"step": "validate.v12"})
        report["warnings"].append(msg)
        if report["status"] == "PASS":
            report["status"] = "WARNING"
    else:
        log.info(f"V12 PASS: {listing_price_count:,} listing prices available for fallback price baselines.", extra={"step": "validate.v12"})

    # -----------------------------------------------------------------------
    # Final report summary
    # -----------------------------------------------------------------------
    log.info(
        f"Validation complete — status={report['status']}  errors={len(report['errors'])}  warnings={len(report['warnings'])}",
        extra={
            "step": "validate.summary",
            "meta": {"status": report["status"], "n_errors": len(report["errors"]), "n_warnings": len(report["warnings"])},
        },
    )
    return report


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run_validate(
    file_paths: dict[str, Path],
    weather_payload: dict[str, Any],
    cfg: dict,
    output_dir: Path,
) -> dict[str, Any]:
    """Run validation and write report to output_dir/validation_report.json."""
    log.info("=== Phase 3: Data Validation START ===", extra={"step": "validate"})

    with StepTimer(log, "validate"):
        report = validate_sources(
            listings_path=file_paths["listings"],
            calendar_path=file_paths["calendar"],
            reviews_path=file_paths["reviews"],
            weather_payload=weather_payload,
            cfg=cfg,
            output_dir=output_dir,
        )

    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / "validation_report.json"
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    log.info(
        f"Validation report saved to {report_path}",
        extra={"step": "validate", "meta": {"path": str(report_path)}},
    )

    if report["status"] == "ERROR":
        import sys
        log.error(
            "Pipeline halted: FATAL validation errors detected. Fix source data before rerunning.",
            extra={"step": "validate"},
        )
        sys.exit(1)

    log.info("=== Phase 3: Data Validation COMPLETE ===", extra={"step": "validate"})
    return report
