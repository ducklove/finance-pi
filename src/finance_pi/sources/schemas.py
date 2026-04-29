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

NAVER_SUMMARY_SCHEMA: dict[str, pl.DataType] = {
    "snapshot_dt": pl.Date,
    "ticker": pl.String,
    "name": pl.String,
    "market": pl.String,
    "close": pl.Int64,
    "change_abs": pl.Int64,
    "change_rate_pct": pl.Float64,
    "par_value": pl.Int64,
    "market_cap": pl.Int64,
    "listed_shares": pl.Int64,
    "foreign_ownership_pct": pl.Float64,
    "volume": pl.Int64,
    "per": pl.Float64,
    "roe": pl.Float64,
}
