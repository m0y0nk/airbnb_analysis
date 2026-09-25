"""
Tests for business validation rules (validate.py).
Uses synthetic DataFrames to exercise each validation rule independently.
"""

import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from pipeline.kpi import KPI5_SQL

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


# ---------------------------------------------------------------------------
# Minimal validate function wrappers (avoid full file I/O in unit tests)
# ---------------------------------------------------------------------------

def _make_listings(n=100, n_dups=0, bad_coords=0):
    ids = list(range(1, n + 1))
    if n_dups:
        ids[-n_dups:] = ids[:n_dups]  # inject duplicates at end
    df = pd.DataFrame({
        "id":        ids,
        "host_id":   range(1, n + 1),
        "neighbourhood": ["Eixample"] * n,
        "latitude":  [41.38] * n,
        "longitude": [2.17] * n,
        "room_type": ["Entire home/apt"] * n,
        "price":     ["$100.00"] * n,
        "last_scraped": ["2026-06-24"] * n,
        "number_of_reviews": [10] * n,
        "review_scores_rating": [4.5] * n,
        "minimum_nights": [2] * n,
    })
    if bad_coords:
        df.loc[:bad_coords - 1, "latitude"] = 999  # invalid
    df["price_numeric"] = 100.0
    return df


def _make_calendar(n=500, n_early=0, orphans=0, listing_ids=None):
    if listing_ids is None:
        listing_ids = list(range(1, 11)) * (n // 10)
    df = pd.DataFrame({
        "listing_id": listing_ids[:n],
        "date":       pd.date_range("2025-01-01", periods=n, freq="D"),
        "available":  (["t", "f"] * (n // 2 + 1))[:n],
        "price_usd":  [100.0] * n,
    })
    df["price_numeric"] = df["price_usd"]
    df["is_unavailable"] = df["available"] == "f"
    if n_early:
        df.loc[:n_early - 1, "date"] = pd.Timestamp("2019-01-01")
    return df


def _make_reviews(n=200, n_orphans=0, listing_ids=None):
    if listing_ids is None:
        listing_ids = list(range(1, 11)) * (n // 10)
    df = pd.DataFrame({
        "id":          range(1, n + 1),
        "listing_id":  listing_ids[:n],
        "date":        pd.date_range("2025-01-01", periods=n, freq="D"),
        "reviewer_id": range(1001, n + 1001),
    })
    if n_orphans:
        df.loc[:n_orphans - 1, "listing_id"] = 99999  # non-existent
    return df


# 60 daily records — satisfies V09's ≥30-day requirement
VALID_WEATHER = {
    "daily": {
        "time": [
            (datetime(2025, 1, 1) + timedelta(days=i)).strftime("%Y-%m-%d")
            for i in range(60)
        ],
        "temperature_2m_max": [20.0 + i * 0.05 for i in range(60)],
        "precipitation_sum":  [0.5]  * 60,
        "wind_speed_10m_max": [10.0] * 60,
    }
}

MINIMAL_CFG = {
    "validation": {
        "max_allowed_duplicate_rate": 0.001,
        "max_allowed_orphan_reviews": 0.05,
        "min_calendar_date": "2020-01-01",
        "min_calendar_days_per_listing": 30,
    }
}


# ---------------------------------------------------------------------------
# Helper: run validation rules directly against DataFrames (not file paths)
# ---------------------------------------------------------------------------

def _run_validation_core(listings, calendar, reviews, weather, cfg=MINIMAL_CFG):
    """
    Inline extraction of the validation logic from validate.py for unit testing.
    We import and test the internal validation function logic directly.
    """
    from pipeline.validate import validate_sources
    import tempfile, os

    # Write DataFrames to temp files so validate_sources can load them
    with tempfile.TemporaryDirectory() as td:
        p = Path(td)
        listings_path = p / "listings.csv"
        calendar_path = p / "calendar.csv"
        reviews_path  = p / "reviews.csv"
        listings.to_csv(listings_path, index=False)
        calendar.to_csv(calendar_path, index=False)
        reviews.to_csv(reviews_path,   index=False)

        report = validate_sources(
            listings_path=listings_path,
            calendar_path=calendar_path,
            reviews_path=reviews_path,
            weather_payload=weather,
            cfg=cfg,
        )
    return report


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestV01DuplicateListings:
    def test_no_duplicates_passes(self):
        report = _run_validation_core(_make_listings(50), _make_calendar(50), _make_reviews(50), VALID_WEATHER)
        assert report["metrics"]["v01_duplicate_listings"] == 0
        assert not any("V01" in e for e in report["errors"])

    def test_duplicate_above_threshold_errors(self):
        # 5 dups in 50 rows = 10% >> 0.1% threshold → ERROR
        report = _run_validation_core(_make_listings(50, n_dups=5), _make_calendar(10), _make_reviews(10), VALID_WEATHER)
        assert any("V01" in e and "FATAL" in e for e in report["errors"])
        assert report["status"] == "ERROR"


class TestV02InvalidCoords:
    def test_invalid_coords_errors(self):
        report = _run_validation_core(_make_listings(20, bad_coords=2), _make_calendar(20), _make_reviews(20), VALID_WEATHER)
        assert any("V02" in e and "FATAL" in e for e in report["errors"])
        assert report["status"] == "ERROR"

    def test_valid_coords_passes(self):
        report = _run_validation_core(_make_listings(20), _make_calendar(20), _make_reviews(20), VALID_WEATHER)
        assert not any("V02" in e for e in report["errors"])


class TestV05TemporalBound:
    def test_early_dates_warning(self):
        cal = _make_calendar(50, n_early=3)
        report = _run_validation_core(_make_listings(20), cal, _make_reviews(20), VALID_WEATHER)
        assert report["metrics"]["v05_calendar_pre2020"] == 3
        assert any("V05" in w for w in report["warnings"])


class TestV07OrphanedReviews:
    def test_orphans_within_tolerance_warns(self):
        # 3% orphans < 5% threshold → WARNING, not ERROR
        rev = _make_reviews(100, n_orphans=3)
        report = _run_validation_core(_make_listings(20), _make_calendar(50), rev, VALID_WEATHER)
        assert any("V07" in w for w in report["warnings"])
        assert not any("V07" in e for e in report["errors"])

    def test_orphans_above_threshold_errors(self):
        # 10 orphans in 50 = 20% >> 5% threshold → ERROR
        rev = _make_reviews(50, n_orphans=10)
        report = _run_validation_core(_make_listings(20), _make_calendar(50), rev, VALID_WEATHER)
        assert any("V07" in e and "ERROR" in e for e in report["errors"])
        assert report["status"] == "ERROR"


class TestV09WeatherCompleteness:
    def test_insufficient_weather_warns(self):
        sparse_weather = {
            "daily": {
                "time": ["2025-01-01", "2025-01-02"],  # Only 2 days
                "temperature_2m_max": [20.0, 21.0],
                "precipitation_sum":  [0.0, 0.5],
                "wind_speed_10m_max": [10.0, 12.0],
            }
        }
        report = _run_validation_core(_make_listings(20), _make_calendar(20), _make_reviews(20), sparse_weather)
        assert any("V09" in w for w in report["warnings"])

    def test_sufficient_weather_passes(self):
        report = _run_validation_core(_make_listings(20), _make_calendar(20), _make_reviews(20), VALID_WEATHER)
        assert not any("V09" in w for w in report["warnings"])


class TestV10FutureCalendarDates:
    def test_future_dates_warn(self):
        cal = _make_calendar(20)
        cal.loc[0, "date"] = pd.Timestamp(datetime.utcnow() + timedelta(days=365))
        report = _run_validation_core(_make_listings(20), cal, _make_reviews(20), VALID_WEATHER)
        assert any("V10" in w for w in report["warnings"])


class TestV11MissingCalendarPrice:
    def test_missing_calendar_price_warns(self):
        cal = _make_calendar(20).drop(columns=["price_usd", "price_numeric"])
        report = _run_validation_core(_make_listings(20), cal, _make_reviews(20), VALID_WEATHER)
        assert any("V11" in w for w in report["warnings"])


class TestV12ListingPriceFallback:
    def test_listing_price_fallback_available(self):
        report = _run_validation_core(_make_listings(20), _make_calendar(20), _make_reviews(20), VALID_WEATHER)
        assert any("V12" in w for w in report["warnings"]) is False


class TestKPI5RevenueProxyFallback:
    def test_kpi5_uses_listing_price_baseline_when_calendar_prices_missing(self):
        sql_lower = KPI5_SQL.lower()
        assert "listing_price_usd" in sql_lower or "l.price_usd" in sql_lower
        assert "proxy_price_usd" in sql_lower
        assert "unpriced_unavailable_days" in sql_lower
        assert "price_coverage_pct" in sql_lower
        assert "listing_price_fallback" in sql_lower
        assert "else null" in sql_lower


class TestValidationReportStructure:
    def test_report_has_required_keys(self):
        report = _run_validation_core(_make_listings(20), _make_calendar(20), _make_reviews(20), VALID_WEATHER)
        for key in ("status", "errors", "warnings", "metrics", "profiles"):
            assert key in report, f"Missing key: {key}"

    def test_pass_status_on_clean_data(self):
        report = _run_validation_core(_make_listings(50), _make_calendar(50), _make_reviews(50), VALID_WEATHER)
        assert report["status"] in ("PASS", "WARNING")  # Minor warnings OK on synthetic data


class TestRejectedRowQuarantine:
    def test_rejected_rows_are_counted_and_written(self, tmp_path):
        listings = _make_listings(4)
        listings.loc[3, "id"] = listings.loc[0, "id"]
        calendar = _make_calendar(4, listing_ids=[1, 2, 3, 4])
        calendar.loc[3, "date"] = pd.NaT
        reviews = _make_reviews(4, listing_ids=[1, 2, 3, 4])
        reviews.loc[3, "listing_id"] = np.nan

        from pipeline.validate import validate_sources

        listings_path = tmp_path / "listings.csv"
        calendar_path = tmp_path / "calendar.csv"
        reviews_path = tmp_path / "reviews.csv"
        listings.to_csv(listings_path, index=False)
        calendar.to_csv(calendar_path, index=False)
        reviews.to_csv(reviews_path, index=False)

        report = validate_sources(
            listings_path=listings_path,
            calendar_path=calendar_path,
            reviews_path=reviews_path,
            weather_payload=VALID_WEATHER,
            cfg=MINIMAL_CFG,
            output_dir=tmp_path / "output",
        )

        assert report["rejected_rows"]["listings"] == 1
        assert report["rejected_rows"]["calendar"] == 1
        assert report["rejected_rows"]["reviews"] == 1
        reject_file = tmp_path / "output" / "rejected" / "calendar_rejected.csv"
        assert reject_file.exists()
        rejected = pd.read_csv(reject_file)
        assert rejected.loc[0, "source_name"] == "calendar"
        assert rejected.loc[0, "reject_reason"] == "invalid_date_or_duplicate_listing_date"
