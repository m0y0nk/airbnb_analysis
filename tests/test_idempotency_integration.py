"""Opt-in integration test for same-run-date idempotency."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
from pathlib import Path

import pytest
import yaml
from sqlalchemy import create_engine, text

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RUN_DATE = "2026-09-24"


def _output_hashes() -> dict[str, str]:
    hashes = {}
    for path in sorted((PROJECT_ROOT / "workspace" / "out").glob("*.csv")):
        hashes[path.name] = hashlib.sha256(path.read_bytes()).hexdigest()
    report = PROJECT_ROOT / "workspace" / "out" / "validation_report.json"
    report_payload = json.loads(report.read_text())
    report_payload.pop("run_ts", None)
    normalized_report = json.dumps(report_payload, sort_keys=True).encode()
    hashes[report.name] = hashlib.sha256(normalized_report).hexdigest()
    return hashes


def _database_snapshot() -> dict[str, int]:
    cfg = yaml.safe_load((PROJECT_ROOT / "config" / "config.yaml").read_text())
    db = cfg["database"]
    password = f":{db['password']}" if db.get("password") else ""
    url = f"postgresql+psycopg2://{db['user']}{password}@{db['host']}:{db['port']}/{db['dbname']}"
    engine = create_engine(url, future=True)
    queries = {
        "listings": "SELECT COUNT(*) FROM stg_listings",
        "calendar": "SELECT COUNT(*) FROM stg_calendar",
        "reviews": "SELECT COUNT(*) FROM stg_reviews",
        "weather": "SELECT COUNT(*) FROM stg_weather",
        "listing_duplicate_ids": "SELECT COUNT(*) - COUNT(DISTINCT id) FROM stg_listings",
        "calendar_duplicate_keys": "SELECT COUNT(*) - COUNT(DISTINCT (listing_id, date)) FROM stg_calendar",
    }
    with engine.connect() as conn:
        return {name: int(conn.execute(text(query)).scalar_one()) for name, query in queries.items()}


@pytest.mark.integration
def test_same_run_date_is_idempotent():
    if os.environ.get("RUN_INTEGRATION") != "1":
        pytest.skip("Set RUN_INTEGRATION=1 to run the PostgreSQL integration test.")

    command = [
        "python3",
        "pipeline/run_pipeline.py",
        "--run-date",
        RUN_DATE,
        "--skip-extract",
    ]
    subprocess.run(command, cwd=PROJECT_ROOT, check=True)
    first_hashes = _output_hashes()
    first_db = _database_snapshot()

    subprocess.run(command, cwd=PROJECT_ROOT, check=True)
    second_hashes = _output_hashes()
    second_db = _database_snapshot()

    assert first_hashes == second_hashes
    assert first_db == second_db
    assert second_db["listing_duplicate_ids"] == 0
    assert second_db["calendar_duplicate_keys"] == 0
