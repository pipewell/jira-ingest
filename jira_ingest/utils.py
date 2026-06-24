"""Shared utility functions."""

from __future__ import annotations

import hashlib
import logging
from typing import Any


def configure_logging(log_level: str = "INFO") -> logging.Logger:
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s  %(name)s  %(levelname)s  %(message)s",
    )
    return logging.getLogger("jira_ingest")


def hash_pii(*fields: str) -> str:
    """SHA-256 hash of one or more PII fields concatenated together."""
    combined = "".join(fields)
    return hashlib.sha256(combined.encode("utf-8")).hexdigest()


def safe_str(value: Any) -> str:
    return str(value) if value is not None else ""


def safe_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def safe_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        if value.lower() in ("true", "1", "yes"):
            return True
        if value.lower() in ("false", "0", "no"):
            return False
    return None


def clean_whitespace(data: Any) -> Any:
    """Recursively strip string values in dicts and lists."""
    if isinstance(data, dict):
        return {k: clean_whitespace(v) for k, v in data.items()}
    if isinstance(data, list):
        return [clean_whitespace(item) for item in data]
    if isinstance(data, str):
        return data.strip()
    return data
