"""
Pipeline Orchestrator — FDE Ground Truth (Barcelona Airbnb)
============================================================
Usage:
    python pipeline/run_pipeline.py [--run-date YYYY-MM-DD] [--config config/config.yaml]

Flags:
    --run-date   Override the run date (default: today's date).
    --config     Path to config YAML (default: config/config.yaml).
    --skip-extract  Skip downloading files (use cached raw layer).
    --skip-kpi      Run extract+validate+transform only.

Idempotency:
    Running with the same --run-date twice yields identical DB state and outputs.
    Raw files are cached; database tables are TRUNCATE-and-reload.

Exit codes:
    0 — success
    1 — fatal validation or database error
    2 — configuration error
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date
from pathlib import Path

import yaml

# Ensure project root is on path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pipeline.extract   import _validate_downloaded_file, run_extract
from pipeline.kpi       import run_kpi
from pipeline.logger    import StepTimer, get_logger
from pipeline.transform import run_transform
from pipeline.validate  import run_validate


def load_config(config_path: str) -> dict:
    try:
        with open(config_path) as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        print(f"[ERROR] Config file not found: {config_path}", file=sys.stderr)
        sys.exit(2)
    except yaml.YAMLError as exc:
        print(f"[ERROR] Invalid YAML in config: {exc}", file=sys.stderr)
        sys.exit(2)


def main():
    parser = argparse.ArgumentParser(
        description="FDE Ground Truth Pipeline — Barcelona Airbnb Analysis"
    )
    parser.add_argument(
        "--run-date",
        default=date.today().isoformat(),
        help="Pipeline run date in YYYY-MM-DD format (default: today)",
    )
    parser.add_argument(
        "--config",
        default="config/config.yaml",
        help="Path to pipeline configuration file",
    )
    parser.add_argument(
        "--skip-extract",
        action="store_true",
        help="Skip file downloads (use cached raw layer)",
    )
    parser.add_argument(
        "--skip-kpi",
        action="store_true",
        help="Skip KPI aggregation (run extract+validate+transform only)",
    )
    args = parser.parse_args()

    cfg = load_config(args.config)
    cfg["pipeline"]["run_date"] = args.run_date

    log = get_logger("orchestrator", level=cfg.get("logging", {}).get("level", "INFO"))

    log.info(
        f"=== FDE Ground Truth Pipeline v{cfg['pipeline']['version']} | run-date={args.run_date} ===",
        extra={"step": "orchestrator", "meta": {"run_date": args.run_date, "city": cfg["pipeline"]["city"]}},
    )

    pipeline_start = time.monotonic()

    # -------------------------------------------------------------------
    # Phase 2: Multi-Modal Ingestion
    # -------------------------------------------------------------------
    if args.skip_extract:
        log.info("--skip-extract flag set; loading file paths from raw layer.", extra={"step": "orchestrator"})
        raw_dir = Path(cfg["storage"]["raw_dir"]) / args.run_date
        # Find cached files
        import os
        file_paths = {}
        name_map = {
            "listings.csv.gz":        "listings",
            "calendar.csv.gz":        "calendar",
            "reviews.csv.gz":         "reviews",
            "neighbourhoods.csv":     "neighbourhoods_csv",
            "neighbourhoods.geojson": "neighbourhoods_geo",
        }
        for fname, key in name_map.items():
            p = raw_dir / fname
            if p.exists():
                _validate_downloaded_file(p, key)
                file_paths[key] = p
            else:
                log.error(f"Cached file not found: {p}", extra={"step": "orchestrator"})
                sys.exit(1)
        weather_path = raw_dir / "weather_barcelona.json"
        if not weather_path.exists():
            log.error(f"Cached weather JSON not found: {weather_path}", extra={"step": "orchestrator"})
            sys.exit(1)
        with open(weather_path) as f:
            weather_raw = json.load(f)
        ingestion_result = {"file_paths": file_paths, "weather_raw": weather_raw}
    else:
        with StepTimer(log, "phase2.extract"):
            ingestion_result = run_extract(cfg, args.run_date)

    file_paths  = ingestion_result["file_paths"]
    weather_raw = ingestion_result["weather_raw"]

    # -------------------------------------------------------------------
    # Phase 3: Validation
    # -------------------------------------------------------------------
    output_dir = Path(cfg["storage"]["output_dir"])
    with StepTimer(log, "phase3.validate"):
        validation_report = run_validate(
            file_paths=file_paths,
            weather_payload=weather_raw,
            cfg=cfg,
            output_dir=output_dir,
        )
    log.info(
        f"Validation status: {validation_report['status']} "
        f"| errors={len(validation_report['errors'])} "
        f"| warnings={len(validation_report['warnings'])}",
        extra={"step": "orchestrator"},
    )

    # -------------------------------------------------------------------
    # Phase 4/5: Transform & Load
    # -------------------------------------------------------------------
    sql_dir = PROJECT_ROOT / "sql"
    with StepTimer(log, "phase4.transform"):
        run_transform(
            file_paths=file_paths,
            weather_payload=weather_raw,
            cfg=cfg,
            sql_dir=sql_dir,
        )

    # -------------------------------------------------------------------
    # Phase 5: KPI Aggregation
    # -------------------------------------------------------------------
    if not args.skip_kpi:
        with StepTimer(log, "phase5.kpi"):
            kpi_results = run_kpi(cfg=cfg, sql_dir=sql_dir, output_dir=output_dir)

        log.info(
            f"KPI evidence tables written to {output_dir}",
            extra={
                "step": "orchestrator",
                "meta": {k: len(v) for k, v in kpi_results.items()},
            },
        )

    # -------------------------------------------------------------------
    # Pipeline summary
    # -------------------------------------------------------------------
    elapsed = round(time.monotonic() - pipeline_start, 1)
    log.info(
        f"=== Pipeline COMPLETE in {elapsed}s | run-date={args.run_date} ===",
        extra={"step": "orchestrator", "meta": {"elapsed_s": elapsed, "status": "SUCCESS"}},
    )
    sys.exit(0)


if __name__ == "__main__":
    main()
