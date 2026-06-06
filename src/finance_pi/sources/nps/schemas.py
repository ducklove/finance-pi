from __future__ import annotations

import polars as pl

NPS_HOLDINGS_SCHEMA: dict[str, pl.DataType] = {
    "date": pl.Date,
    "stock_code": pl.String,
    "stock_name": pl.String,
    "shares": pl.Int64,
    "ownership_pct": pl.Float64,
    "price": pl.Float64,
    "market_value": pl.Float64,
    "change_pct": pl.Float64,
    "rank": pl.Int64,
    "source": pl.String,
    "source_date": pl.Date,
    "source_market_value": pl.Float64,
    "source_weight_pct": pl.Float64,
    "shares_source": pl.String,
    "price_date": pl.Date,
    "is_exact_price": pl.Boolean,
}
