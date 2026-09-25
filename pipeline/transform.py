"""
Phase 4+5 — Transform, Relational Modelling & PostgreSQL Staging
=================================================================
Stages:
  1. Parse & clean each source into normalised DataFrames.
  2. Expand weather JSON into a row-per-date DataFrame.
  3. Load DataFrames into PostgreSQL via SQLAlchemy (upsert / TRUNCATE-INSERT strategy).
  4. Create the core relational schema using sql/01_schema.sql.

Idempotency:
  - Each staging table is TRUNCATED before INSERT (safe for re-runs with same run-date).
  - Listing table uses INSERT … ON CONFLICT (id) DO UPDATE for incremental safety.

Semantic constraint enforced:
  - Calendar 'available = f' is loaded as is; NO fabricated booking_confirmed flag.
  - Column `is_unavailable` BOOLEAN is the canonical field; `is_booking` does NOT exist.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sqlalchemy import text

from pipeline.db import engine_from_config
from pipeline.logger import StepTimer, get_logger

log = get_logger("transform")


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _engine(cfg: dict):
    return engine_from_config(cfg)


def _exec_sql_file(engine, sql_path: Path):
    """Execute a .sql file against the target database."""
    sql = sql_path.read_text()
    with engine.begin() as conn:
        conn.execute(text(sql))
    log.info(f"Executed SQL file: {sql_path}", extra={"step": "transform.sql"})


# ---------------------------------------------------------------------------
# Price parsing
# ---------------------------------------------------------------------------

def _parse_price(series: pd.Series) -> pd.Series:
    return (
        series.astype(str)
        .str.replace(r"[\$,]", "", regex=True)
        .str.strip()
        .replace({"": np.nan, "nan": np.nan})
        .astype(float)
    )


# ---------------------------------------------------------------------------
# 1. Listings
# ---------------------------------------------------------------------------

def _clean_listings(path: Path) -> pd.DataFrame:
    cols = ["id", "host_id", "neighbourhood_cleansed", "latitude", "longitude",
            "room_type", "price", "last_scraped", "number_of_reviews",
            "review_scores_rating", "minimum_nights"]
    df = pd.read_csv(path, compression="infer", usecols=lambda c: c in cols, low_memory=False)
    df = df.rename(columns={"neighbourhood_cleansed": "neighbourhood"})
    df["price_usd"] = _parse_price(df["price"]) if "price" in df.columns else np.nan
    df = df.drop(columns=["price"], errors="ignore")
    df["last_scraped"] = pd.to_datetime(df["last_scraped"], errors="coerce")
    # Drop fatal duplicates (keep first occurrence)
    before = len(df)
    df = df.drop_duplicates(subset=["id"], keep="first")
    after = len(df)
    if before != after:
        log.warning(
            f"Dropped {before - after} duplicate listing rows.",
            extra={"step": "transform.listings"},
        )
    log.info(f"Listings cleaned: {len(df):,} rows", extra={"step": "transform.listings"})
    return df


# ---------------------------------------------------------------------------
# 2. Calendar
# ---------------------------------------------------------------------------

def _clean_calendar(path: Path) -> pd.DataFrame:
    cols = ["listing_id", "date", "available", "price", "adjusted_price", "minimum_nights"]
    df = pd.read_csv(path, compression="infer", usecols=lambda c: c in cols, low_memory=False)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["price_usd"] = _parse_price(df["price"]) if "price" in df.columns else np.nan
    df["adjusted_price_usd"] = _parse_price(df["adjusted_price"]) if "adjusted_price" in df.columns else np.nan
    df = df.drop(columns=["price", "adjusted_price"], errors="ignore")

    # Semantic constraint: is_unavailable is an OBSERVED STATE, NOT a booking flag
    df["is_unavailable"] = df["available"].str.strip().str.lower() == "f"

    # Drop composite-key duplicates, keep first
    before = len(df)
    df = df.drop_duplicates(subset=["listing_id", "date"], keep="first")
    df = df.dropna(subset=["date"])
    after = len(df)
    if before != after:
        log.warning(
            f"Dropped {before - after} duplicate/invalid calendar rows.",
            extra={"step": "transform.calendar"},
        )
    log.info(f"Calendar cleaned: {len(df):,} rows", extra={"step": "transform.calendar"})
    return df


# ---------------------------------------------------------------------------
# 3. Reviews
# ---------------------------------------------------------------------------

def _clean_reviews(path: Path, valid_listing_ids: set) -> pd.DataFrame:
    cols = ["id", "listing_id", "date", "reviewer_id"]
    df = pd.read_csv(path, compression="infer", usecols=lambda c: c in cols, low_memory=False)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "listing_id"])
    # Tag and retain orphaned reviews with a quality flag
    df["orphaned"] = ~df["listing_id"].astype(str).isin(valid_listing_ids)
    log.info(
        f"Reviews cleaned: {len(df):,} rows  ({df['orphaned'].sum():,} orphaned tagged)",
        extra={"step": "transform.reviews"},
    )
    return df


# ---------------------------------------------------------------------------
# 4. Weather
# ---------------------------------------------------------------------------

def _clean_weather(payload: dict[str, Any]) -> pd.DataFrame:
    daily = payload.get("daily", {})
    df = pd.DataFrame({
        "date":                pd.to_datetime(daily.get("time", [])),
        "temp_max_c":          daily.get("temperature_2m_max", []),
        "precipitation_mm":    daily.get("precipitation_sum", []),
        "wind_speed_max_kmh":  daily.get("wind_speed_10m_max", []),
    })
    df["latitude"]  = payload.get("latitude", np.nan)
    df["longitude"] = payload.get("longitude", np.nan)
    df = df.dropna(subset=["date"])
    log.info(f"Weather cleaned: {len(df):,} rows", extra={"step": "transform.weather"})
    return df


# ---------------------------------------------------------------------------
# 5. Neighbourhoods
# ---------------------------------------------------------------------------

def _clean_neighbourhoods(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df.columns = [c.lower().strip() for c in df.columns]
    log.info(f"Neighbourhoods cleaned: {len(df):,} rows", extra={"step": "transform.neighbourhoods"})
    return df


# ---------------------------------------------------------------------------
# DB loaders
# ---------------------------------------------------------------------------

def _upsert_listings(engine, df: pd.DataFrame):
    """Upsert listings using INSERT … ON CONFLICT DO UPDATE."""
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE stg_listings CASCADE;"))
        df.to_sql("stg_listings", conn, if_exists="append", index=False, method="multi", chunksize=5000)
    log.info(f"Loaded {len(df):,} rows → stg_listings", extra={"step": "transform.load"})


def _load_table(engine, df: pd.DataFrame, table: str, chunksize: int = 10000):
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {table} CASCADE;"))
        df.to_sql(table, conn, if_exists="append", index=False, method="multi", chunksize=chunksize)
    log.info(f"Loaded {len(df):,} rows → {table}", extra={"step": "transform.load"})


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run_transform(
    file_paths: dict[str, Path],
    weather_payload: dict[str, Any],
    cfg: dict,
    sql_dir: Path,
) -> None:
    """Execute schema creation, data cleaning, and PostgreSQL staging."""
    log.info("=== Phase 4/5: Transform & Load START ===", extra={"step": "transform"})

    engine = _engine(cfg)

    # 1. Create schema (idempotent DDL)
    with StepTimer(log, "transform.schema"):
        _exec_sql_file(engine, sql_dir / "01_schema.sql")

    # 2. Clean datasets
    with StepTimer(log, "transform.clean"):
        listings_df      = _clean_listings(file_paths["listings"])
        calendar_df      = _clean_calendar(file_paths["calendar"])
        valid_ids        = set(listings_df["id"].astype(str))
        reviews_df       = _clean_reviews(file_paths["reviews"], valid_ids)
        weather_df       = _clean_weather(weather_payload)
        neighbourhoods_df = _clean_neighbourhoods(file_paths["neighbourhoods_csv"])

    # 3. Load to PostgreSQL staging tables
    with StepTimer(log, "transform.load"):
        _upsert_listings(engine, listings_df)
        _load_table(engine, calendar_df, "stg_calendar", chunksize=50000)
        _load_table(engine, reviews_df,  "stg_reviews",  chunksize=20000)
        _load_table(engine, weather_df,  "stg_weather")
        _load_table(engine, neighbourhoods_df, "stg_neighbourhoods")

    # 4. Run staging SQL transforms (core relational tables)
    with StepTimer(log, "transform.staging_sql"):
        _exec_sql_file(engine, sql_dir / "02_staging.sql")

    log.info("=== Phase 4/5: Transform & Load COMPLETE ===", extra={"step": "transform"})
