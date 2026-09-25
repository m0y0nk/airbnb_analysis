"""
Structured JSON logger for the FDE pipeline.
Every step emits: timestamp, level, step, message, and optional metadata.
"""

import json
import logging
import sys
import time
from datetime import datetime, timezone
from typing import Any


class JsonFormatter(logging.Formatter):
    """Emit log records as single-line JSON objects."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "step": getattr(record, "step", "pipeline"),
            "msg": record.getMessage(),
        }
        if hasattr(record, "meta"):
            payload["meta"] = record.meta
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload)


def get_logger(name: str = "pipeline", level: str = "INFO") -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    return logger


class StepTimer:
    """Context manager that logs step duration."""

    def __init__(self, logger: logging.Logger, step_name: str):
        self.logger = logger
        self.step = step_name
        self.start: float = 0.0

    def __enter__(self):
        self.start = time.monotonic()
        self.logger.info(
            f"Step [{self.step}] started",
            extra={"step": self.step, "meta": {"event": "start"}},
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        elapsed = round(time.monotonic() - self.start, 3)
        if exc_type:
            self.logger.error(
                f"Step [{self.step}] FAILED after {elapsed}s — {exc_val}",
                extra={"step": self.step, "meta": {"event": "error", "elapsed_s": elapsed}},
            )
        else:
            self.logger.info(
                f"Step [{self.step}] completed in {elapsed}s",
                extra={"step": self.step, "meta": {"event": "end", "elapsed_s": elapsed}},
            )
        return False  # Do not suppress exceptions
