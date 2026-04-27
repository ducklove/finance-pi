from __future__ import annotations

from datetime import date

from pydantic import BaseModel, ConfigDict, Field


class KrxDailyPriceRow(BaseModel):
    """Canonical KRX daily-price row after source-level field normalization."""

    model_config = ConfigDict(extra="forbid")

    date: date
    ticker: str = Field(pattern=r"^\d{6}$")
    isin: str | None = None
    name: str
    market: str
    open: float
    high: float
    low: float
    close: float
    volume: int = Field(ge=0)
    trading_value: int = Field(ge=0)
    market_cap: int | None = Field(default=None, ge=0)
    listed_shares: int | None = Field(default=None, ge=0)
