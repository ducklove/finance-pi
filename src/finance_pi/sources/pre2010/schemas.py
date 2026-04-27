from __future__ import annotations

from datetime import date

from pydantic import BaseModel, ConfigDict, Field


class Pre2010PriceRow(BaseModel):
    model_config = ConfigDict(extra="forbid")

    date: date
    ticker: str = Field(pattern=r"^\d{6}$")
    source: str
    open: float | None = None
    high: float | None = None
    low: float | None = None
    close: float
    volume: int | None = Field(default=None, ge=0)
    confidence: float = Field(default=1.0, ge=0.0, le=1.0)
