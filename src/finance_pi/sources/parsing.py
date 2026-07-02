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


def share_class_from_stock_kind(stock_kind: str | None) -> str | None:
    """Classify a DART stock-kind string ("주식의 종류") into common/preferred.

    DART reports use strings such as "보통주", "우선주", "의결권 있는 주식",
    "의결권 없는 주식", and various "종류주식" labels for share classes with
    special rights. Anything containing "종류" without "보통" is treated as
    a non-common (preferred-like) class.
    """
    if not stock_kind:
        return None
    if "보통" in stock_kind or "의결권 있는" in stock_kind:
        return "common"
    if "우선" in stock_kind or "종류" in stock_kind or "의결권 없는" in stock_kind:
        return "preferred"
    return None
