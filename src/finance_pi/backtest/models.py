from __future__ import annotations

from datetime import date

import polars as pl
from pydantic import BaseModel, ConfigDict, Field, model_validator


class BacktestConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    start: date
    end: date
    initial_nav: float = Field(default=1.0, gt=0)
    top_fraction: float = Field(default=0.1, gt=0, le=1)
    signal_lag_days: int = Field(default=1, ge=0)
    entry_lag_days: int = Field(default=1, ge=0)
    max_position_weight: float = Field(default=0.2, gt=0, le=1)

    @model_validator(mode="after")
    def ordered_dates(self) -> BacktestConfig:
        if self.end < self.start:
            raise ValueError("end must be on or after start")
        return self


class BacktestResult(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    nav: pl.DataFrame
    positions: pl.DataFrame
    ledger: pl.DataFrame
