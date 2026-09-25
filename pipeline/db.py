"""Shared PostgreSQL connection helpers."""

from __future__ import annotations

from sqlalchemy import create_engine


def engine_from_config(cfg: dict):
    """Build the PostgreSQL engine from external configuration."""
    db = cfg["database"]
    password = f":{db["password"]}" if db.get("password") else ""
    url = (
        f"postgresql+psycopg2://{db['user']}{password}"
        f"@{db['host']}:{db['port']}/{db['dbname']}"
    )
    return create_engine(url, future=True, pool_pre_ping=True)
