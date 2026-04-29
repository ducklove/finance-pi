from __future__ import annotations

import polars as pl

PRICE_SCHEMA: dict[str, pl.DataType] = {
    "date": pl.Date,
    "ticker": pl.String,
    "isin": pl.String,
    "name": pl.String,
    "market": pl.String,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Int64,
    "trading_value": pl.Int64,
    "market_cap": pl.Int64,
    "listed_shares": pl.Int64,
}
