"""
Tests for retrieval pipeline (extract.py).
Tests use httpretty/responses to mock HTTP calls — no real network required.
"""

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pipeline.extract import fetch_open_meteo_weather, _auto_detect_weather_date_range


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

VALID_WEATHER_PAYLOAD = {
    "latitude": 41.3851,
    "longitude": 2.1734,
    "daily": {
           "time": [f"2025-01-{day:02d}" for day in range(1, 10)],
        "temperature_2m_max": [20.0] * 9,
        "precipitation_sum":  [0.5]  * 9,
        "wind_speed_10m_max": [12.0] * 9,
    },
}


# ---------------------------------------------------------------------------
# fetch_open_meteo_weather
# ---------------------------------------------------------------------------

def test_weather_fetch_success():
    """Happy path: valid response returns correct payload."""
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = VALID_WEATHER_PAYLOAD
    mock_resp.raise_for_status = MagicMock()

    with patch("pipeline.extract.requests.get", return_value=mock_resp):
        result = fetch_open_meteo_weather(
            lat=41.3851,
            lon=2.1734,
            start_date="2025-01-01",
                end_date="2025-01-09",
            api_url="https://archive-api.open-meteo.com/v1/archive",
            daily_vars=["temperature_2m_max", "precipitation_sum", "wind_speed_10m_max"],
        )

    assert "daily" in result
    assert "time" in result["daily"]
    assert len(result["daily"]["time"]) == 9


def test_weather_fetch_missing_keys_raises():
    """Response missing 'daily' key must raise RuntimeError."""
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"error": "unknown location"}
    mock_resp.raise_for_status = MagicMock()

    with patch("pipeline.extract.requests.get", return_value=mock_resp):
        with pytest.raises(RuntimeError, match="missing expected keys"):
            fetch_open_meteo_weather(
                lat=0.0, lon=0.0,
                start_date="2025-01-01", end_date="2025-01-31",
                api_url="https://archive-api.open-meteo.com/v1/archive",
                daily_vars=["temperature_2m_max"],
            )


def test_weather_fetch_transient_500_retries():
    """HTTP 500 should trigger retry; succeeds on second attempt."""
    fail_resp = MagicMock()
    fail_resp.status_code = 500
    fail_resp.raise_for_status = MagicMock()

    ok_resp = MagicMock()
    ok_resp.status_code = 200
    ok_resp.json.return_value = VALID_WEATHER_PAYLOAD
    ok_resp.raise_for_status = MagicMock()

    call_count = {"n": 0}

    def side_effect(*args, **kwargs):
        call_count["n"] += 1
        if call_count["n"] == 1:
            return fail_resp
        return ok_resp

    with patch("pipeline.extract.requests.get", side_effect=side_effect):
        with patch("pipeline.extract.time.sleep"):  # Don't actually sleep in tests
            result = fetch_open_meteo_weather(
                lat=41.3851, lon=2.1734,
                start_date="2025-01-01", end_date="2025-01-09",
                api_url="https://archive-api.open-meteo.com/v1/archive",
                daily_vars=["temperature_2m_max"],
                max_retries=4,
            )
    assert "daily" in result
    assert call_count["n"] == 2


def test_weather_fetch_exhausted_retries_raises():
    """All attempts returning HTTP 503 must raise RuntimeError."""
    fail_resp = MagicMock()
    fail_resp.status_code = 503
    fail_resp.raise_for_status = MagicMock()

    with patch("pipeline.extract.requests.get", return_value=fail_resp):
        with patch("pipeline.extract.time.sleep"):
            with pytest.raises(RuntimeError):
                fetch_open_meteo_weather(
                    lat=41.3851, lon=2.1734,
                    start_date="2025-01-01", end_date="2025-01-31",
                    api_url="https://archive-api.open-meteo.com/v1/archive",
                    daily_vars=["temperature_2m_max"],
                    max_retries=2,
                )


def test_file_zero_bytes_raises(tmp_path):
    """A downloaded file of 0 bytes must raise RuntimeError."""
    from pipeline.extract import _download_file

    dest = tmp_path / "test_file.csv.gz"
    dest.write_bytes(b"")  # 0-byte file pre-exists

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.iter_content.return_value = [b""]  # empty content
    mock_resp.__enter__ = lambda s: s
    mock_resp.__exit__ = MagicMock(return_value=False)

    with patch("pipeline.extract.requests.get", return_value=mock_resp):
        with pytest.raises(RuntimeError, match="0 bytes"):
            _download_file("http://example.com/file.gz", dest, "test")
