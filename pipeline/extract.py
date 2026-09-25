"""
Phase 2 — Multi-Modal Data Retrieval & Ingestion
=================================================
Retrieval modes demonstrated:
  1. File retrieval  : Downloads compressed CSVs and GeoJSON from Inside Airbnb CDN.
  2. REST API        : Queries Open-Meteo historical weather archive (JSON).
  3. Raw preservation: Every downloaded artefact is saved to a date-partitioned raw layer.

Failure handling:
  - Transient HTTP errors (429, 500, 502, 503, 504) → exponential backoff with jitter.
  - Permanent errors (401, 404, 400)               → immediate failure, no retry.
  - File size check (> 0 bytes) and JSON key check after every download.
"""

from __future__ import annotations

import gzip
import json
import os
import random
import shutil
import time
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import requests
import pandas as pd
import yaml

from pipeline.logger import StepTimer, get_logger

log = get_logger("extract")

# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

def load_config(config_path: str = "config/config.yaml") -> dict:
    with open(config_path) as f:
        return yaml.safe_load(f)


def raw_dir(cfg: dict, run_date: str) -> Path:
    base = Path(cfg["storage"]["raw_dir"]) / run_date
    base.mkdir(parents=True, exist_ok=True)
    return base


# ---------------------------------------------------------------------------
# 1. File Retrieval: Inside Airbnb CSVs + GeoJSON
# ---------------------------------------------------------------------------

_AIRBNB_FILES = {
    "listings":           "data/listings.csv.gz",
    "calendar":          "data/calendar.csv.gz",
    "reviews":           "data/reviews.csv.gz",
    "neighbourhoods_csv": "visualisations/neighbourhoods.csv",
    "neighbourhoods_geo": "visualisations/neighbourhoods.geojson",
}

_REQUIRED_COLUMNS = {
    "listings": {"id", "latitude", "longitude", "room_type", "last_scraped"},
    "calendar": {"listing_id", "date", "available"},
    "reviews": {"id", "listing_id", "date"},
    "neighbourhoods_csv": {"neighbourhood"},
}


def _download_file(
    url: str,
    dest: Path,
    label: str,
    max_retries: int = 4,
    timeout_seconds: int = 120,
) -> Path:
    """Download a single file with retry and byte-size validation."""
    log.info(f"Downloading [{label}] from {url}", extra={"step": "extract.download"})
    for attempt in range(1, max_retries + 1):
        try:
            with requests.get(url, stream=True, timeout=timeout_seconds) as resp:
                if resp.status_code in (429, 500, 502, 503, 504):
                    wait = min(2 ** (attempt - 1) + random.uniform(0, 1), 16)
                    log.warning(
                        f"[{label}] HTTP {resp.status_code} — retrying in {wait:.1f}s (attempt {attempt})",
                        extra={"step": "extract.download"},
                    )
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                with open(dest, "wb") as fh:
                    for chunk in resp.iter_content(chunk_size=1 << 20):
                        fh.write(chunk)
            # Completeness check: file must be non-empty
            size = dest.stat().st_size
            if size == 0:
                raise RuntimeError(f"[{label}] downloaded file is 0 bytes — treating as failure.")
            log.info(
                f"[{label}] saved to {dest} ({size:,} bytes)",
                extra={"step": "extract.download", "meta": {"bytes": size, "dest": str(dest)}},
            )
            return dest
        except requests.exceptions.HTTPError as exc:
            if exc.response is not None and exc.response.status_code in (400, 401, 403, 404):
                raise RuntimeError(f"[{label}] permanent HTTP error {exc.response.status_code}: {exc}") from exc
            if attempt == max_retries:
                raise
            wait = min(2 ** attempt + random.uniform(0, 1), 16)
            time.sleep(wait)
        except requests.exceptions.RequestException as exc:
            if attempt == max_retries:
                raise RuntimeError(f"[{label}] network error after {max_retries} attempts: {exc}") from exc
            time.sleep(min(2 ** attempt, 16))
    raise RuntimeError(f"[{label}] exhausted all retry attempts.")


def _validate_downloaded_file(path: Path, label: str) -> None:
    """Validate required columns or GeoJSON structure before transformation."""
    if label == "neighbourhoods_geo":
        with open(path, encoding="utf-8") as fh:
            payload = json.load(fh)
        features = payload.get("features")
        if not isinstance(features, list) or not features:
            raise RuntimeError(f"[{label}] GeoJSON contains no features.")
        invalid = [
            feature for feature in features
            if feature.get("type") != "Feature"
            or not feature.get("geometry")
            or not feature.get("properties", {}).get("neighbourhood")
        ]
        if invalid:
            raise RuntimeError(f"[{label}] GeoJSON has {len(invalid)} invalid features.")
        return

    required = _REQUIRED_COLUMNS.get(label)
    if not required:
        return
    actual = set(pd.read_csv(path, compression="infer", nrows=0).columns)
    missing = required - actual
    if missing:
        raise RuntimeError(f"[{label}] missing required columns: {sorted(missing)}")


def ingest_airbnb_files(cfg: dict, run_date: str) -> dict[str, Path]:
    """Download all Inside Airbnb source files to the raw layer."""
    rd = raw_dir(cfg, run_date)
    base_url = cfg["source"]["base_url"]
    paths: dict[str, Path] = {}

    with StepTimer(log, "extract.airbnb_files"):
        for key, rel_path in _AIRBNB_FILES.items():
            url = f"{base_url}/{rel_path}"
            filename = Path(rel_path).name
            dest = rd / filename
            if dest.exists() and dest.stat().st_size > 0:
                log.info(
                    f"[{key}] already exists at {dest}, skipping download (idempotent).",
                    extra={"step": "extract.airbnb_files"},
                )
                paths[key] = dest
            else:
                paths[key] = _download_file(
                    url,
                    dest,
                    key,
                    max_retries=cfg.get("retrieval", {}).get("max_retries", 4),
                    timeout_seconds=cfg.get("retrieval", {}).get("request_timeout_seconds", 120),
                )
            _validate_downloaded_file(paths[key], key)

    return paths


# ---------------------------------------------------------------------------
# 2. REST API Retrieval: Open-Meteo Historical Weather
# ---------------------------------------------------------------------------

def _auto_detect_weather_date_range(calendar_path: Path, run_date: str) -> tuple[str, str]:
    """
    Use the calendar's first date through the run date, capped at today so the
    historical weather API is never asked for future observations.
    """
    try:
        import pandas as pd  # only needed here for auto-detect
        dates = pd.read_csv(calendar_path, usecols=["date"])["date"]
        calendar_dates = pd.to_datetime(dates, errors="coerce").dropna()
        if calendar_dates.empty:
            raise ValueError("calendar has no valid dates")
        start_date = calendar_dates.min().date()
        requested_end = min(pd.Timestamp(run_date).date(), datetime.utcnow().date())
        end_date = min(calendar_dates.max().date(), requested_end)
        if end_date < start_date:
            raise ValueError("calendar date range does not overlap the run date")
        return start_date.isoformat(), end_date.isoformat()
    except (OSError, KeyError, ValueError, pd.errors.ParserError) as exc:
        today = datetime.utcnow().date()
        log.warning(
            f"Could not derive weather range from calendar ({exc}); falling back to the previous 365 days.",
            extra={"step": "extract.weather"},
        )
        return (today - timedelta(days=365)).isoformat(), today.isoformat()


def fetch_open_meteo_weather(
    lat: float,
    lon: float,
    start_date: str,
    end_date: str,
    api_url: str,
    daily_vars: list[str],
    timezone: str = "UTC",
    max_retries: int = 4,
    timeout_seconds: int = 30,
) -> dict[str, Any]:
    """
    Query Open-Meteo historical archive API.
    Returns parsed JSON payload and preserves it to raw_path if provided.
    """
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "daily": ",".join(daily_vars),
        "timezone": timezone,
    }
    log.info(
        f"Fetching weather from Open-Meteo: {start_date} → {end_date}",
        extra={"step": "extract.weather", "meta": {"params": params}},
    )
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(api_url, params=params, timeout=timeout_seconds)
            if resp.status_code in (429, 500, 502, 503, 504):
                wait = min(2 ** (attempt - 1) + random.uniform(0, 1), 16)
                log.warning(
                    f"Open-Meteo HTTP {resp.status_code} — retrying in {wait:.1f}s (attempt {attempt}/{max_retries})",
                    extra={"step": "extract.weather"},
                )
                time.sleep(wait)
                continue
            resp.raise_for_status()
            payload = resp.json()

            # Completeness check: must contain 'daily' and 'time' keys
            if "daily" not in payload or "time" not in payload.get("daily", {}):
                raise RuntimeError(
                    f"Open-Meteo response missing expected keys: {list(payload.keys())}"
                )
            n_days = len(payload["daily"]["time"])
            expected_dates = pd.date_range(start_date, end_date, freq="D")
            actual_dates = pd.to_datetime(payload["daily"]["time"], errors="coerce")
            if actual_dates.isna().any() or not actual_dates.equals(expected_dates):
                raise RuntimeError(
                    f"Open-Meteo date coverage mismatch: expected {len(expected_dates)} "
                    f"contiguous days, received {n_days}."
                )
            for variable in daily_vars:
                if len(payload["daily"].get(variable, [])) != n_days:
                    raise RuntimeError(
                        f"Open-Meteo variable {variable} has inconsistent array length."
                    )
            log.info(
                f"Weather data retrieved: {n_days} daily records ({start_date} → {end_date})",
                extra={"step": "extract.weather", "meta": {"n_days": n_days}},
            )
            return payload
        except requests.exceptions.HTTPError as exc:
            code = exc.response.status_code if exc.response is not None else "?"
            if code in (400, 401, 403, 404):
                raise RuntimeError(f"Permanent Open-Meteo error HTTP {code}: {exc}") from exc
            if attempt == max_retries:
                raise RuntimeError(f"Open-Meteo failed after {max_retries} attempts: {exc}") from exc
            time.sleep(min(2 ** attempt, 16))
        except requests.exceptions.RequestException as exc:
            if attempt == max_retries:
                raise RuntimeError(f"Open-Meteo network error: {exc}") from exc
            time.sleep(min(2 ** attempt, 16))

    raise RuntimeError("Open-Meteo: exhausted all retry attempts.")


def ingest_weather(cfg: dict, run_date: str, calendar_path: Path) -> dict[str, Any]:
    """Fetch weather data and persist raw JSON to raw layer."""
    rd = raw_dir(cfg, run_date)
    raw_path = rd / "weather_barcelona.json"

    start_date, end_date = _auto_detect_weather_date_range(calendar_path, run_date)

    if raw_path.exists() and raw_path.stat().st_size > 0:
        with open(raw_path) as f:
            cached_payload = json.load(f)
        cached_dates = cached_payload.get("daily", {}).get("time", [])
        if cached_dates and min(cached_dates) <= start_date and max(cached_dates) >= end_date:
            log.info(
                f"Weather raw JSON already covers {start_date} → {end_date}; loading from cache (idempotent).",
                extra={"step": "extract.weather"},
            )
            return cached_payload
        log.info(
            f"Cached weather coverage is insufficient for {start_date} → {end_date}; refreshing.",
            extra={"step": "extract.weather"},
        )

    with StepTimer(log, "extract.weather"):
        payload = fetch_open_meteo_weather(
            lat=cfg["weather"]["latitude"],
            lon=cfg["weather"]["longitude"],
            start_date=start_date,
            end_date=end_date,
            api_url=cfg["weather"]["api_url"],
            daily_vars=cfg["weather"]["daily_variables"],
            timezone=cfg["weather"]["timezone"],
            max_retries=cfg["weather"]["max_retries"],
            timeout_seconds=cfg["weather"].get("request_timeout_seconds", 30),
        )

    # Preserve raw JSON
    with open(raw_path, "w") as f:
        json.dump(payload, f, indent=2)
    log.info(
        f"Raw weather JSON saved to {raw_path}",
        extra={"step": "extract.weather", "meta": {"path": str(raw_path)}},
    )
    return payload


# ---------------------------------------------------------------------------
# Main ingestion entry-point
# ---------------------------------------------------------------------------

def run_extract(cfg: dict, run_date: str) -> dict[str, Any]:
    """
    Execute full multi-modal ingestion.
    Returns a dict with:
      'file_paths'   : {key → Path} for each downloaded Airbnb file
      'weather_raw'  : raw Open-Meteo JSON payload
    """
    log.info("=== Phase 2: Multi-Modal Ingestion START ===", extra={"step": "extract"})

    file_paths = ingest_airbnb_files(cfg, run_date)
    weather_raw = ingest_weather(cfg, run_date, file_paths["calendar"])

    log.info("=== Phase 2: Multi-Modal Ingestion COMPLETE ===", extra={"step": "extract"})
    return {"file_paths": file_paths, "weather_raw": weather_raw}
