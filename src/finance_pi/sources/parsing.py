from __future__ import annotations

from datetime import date
from typing import Any

from finance_pi.config import parse_yyyymmdd


def value_for(row: dict[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        if key in row and row[key] not in (None, ""):
            return row[key]
    return default


def parse_int(value: Any, *, default: int | None = None) -> int | None:
    if value in (None, ""):
        return default
    if isinstance(value, int):
        return value
    text = str(value).replace(",", "").strip()
    if text in {"", "-", "N/A"}:
        return default
    return int(float(text))


def parse_float(value: Any, *, default: float | None = None) -> float | None:
    if value in (None, ""):
        return default
    if isinstance(value, int | float):
        return float(value)
    text = str(value).replace(",", "").strip()
    if text in {"", "-", "N/A"}:
        return default
    return float(text)


def parse_date(value: Any) -> date:
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if len(text) == 8 and text.isdigit():
        return parse_yyyymmdd(text)
    return date.fromisoformat(text)
