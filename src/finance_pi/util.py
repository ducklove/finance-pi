from __future__ import annotations

import json
from contextlib import suppress
from datetime import date
from pathlib import Path
from typing import Any


def iso_or_none(value: Any) -> str | None:
    return value.isoformat() if hasattr(value, "isoformat") else None


def parse_iso_date(value: Any) -> date | None:
    if isinstance(value, date):
        return value
    if not value:
        return None
    try:
        return date.fromisoformat(str(value))
    except ValueError:
        return None


def coerce_date_or_none(value: Any) -> date | None:
    """Coerce a value to a date, returning None on any failure (including None input)."""
    if isinstance(value, date):
        return value
    if value is None:
        return None
    with suppress(ValueError):
        return date.fromisoformat(str(value))
    return None


def parse_iso_date_strict(value: object) -> date:
    """Coerce a value to a date, raising ValueError if it cannot be parsed."""
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def int_or_none(value: Any) -> int | None:
    if value in (None, "", " ", "-"):
        return None
    try:
        return int(float(str(value).replace(",", "").strip()))
    except ValueError:
        return None


def duckdb_path(path: Path) -> str:
    return path.as_posix().replace("'", "''")


def sql_placeholders(values: list[str]) -> str:
    return ", ".join("?" for _ in values)


def backfill_marker_path(data_root: Path, year: int) -> Path:
    return data_root / "_state" / "backfill" / "yearly" / f"{year}.json"


def read_backfill_marker_status(marker: Path) -> str | None:
    if not marker.exists():
        return None
    try:
        data = json.loads(marker.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return "marker_invalid"
    status = data.get("status")
    return str(status) if status else "complete"
