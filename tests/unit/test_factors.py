from __future__ import annotations

from datetime import date, timedelta

import polars as pl

from finance_pi.factors import InMemoryFactorContext, factor_registry


def test_builtin_factors_are_registered() -> None:
    assert {"momentum_12_1", "quality_roa", "value_earnings_yield"} <= set(factor_registry.names())


def test_momentum_factor_returns_lazy_scores() -> None:
    start = date(2024, 1, 1)
    rows = [
        {"date": start + timedelta(days=i), "security_id": "S001", "close_adj": float(100 + i)}
        for i in range(260)
    ]
    ctx = InMemoryFactorContext({"gold.daily_prices_adj": pl.DataFrame(rows)})
    factor = factor_registry.get("momentum_12_1")()

    result = factor.compute(ctx).collect()

    assert result.select("score").drop_nulls().height > 0
