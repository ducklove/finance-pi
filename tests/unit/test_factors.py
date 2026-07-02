from __future__ import annotations

from datetime import date, timedelta

import polars as pl
import pytest

from finance_pi.factors import InMemoryFactorContext, factor_registry

AS_OF = date(2024, 6, 28)


def _fundamentals_row(
    security_id: str,
    account_id: str,
    amount: float,
    *,
    report_type: str = "11011",
) -> dict:
    return {
        "as_of_date": AS_OF,
        "security_id": security_id,
        "account_id": account_id,
        "amount": amount,
        "report_type": report_type,
    }


def _price_row(security_id: str, market_cap: float) -> dict:
    return {"date": AS_OF, "security_id": security_id, "market_cap": market_cap}


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


def test_value_earnings_yield_scores_known_inputs() -> None:
    ctx = InMemoryFactorContext(
        {
            "gold.fundamentals_pit": pl.DataFrame(
                [
                    # Both accounts reported: owners-of-parent (90) must win, not 190.
                    _fundamentals_row(
                        "S001", "ifrs-full_ProfitLossAttributableToOwnersOfParent", 90.0
                    ),
                    _fundamentals_row("S001", "ifrs-full_ProfitLoss", 100.0),
                    _fundamentals_row("S002", "ifrs-full_ProfitLoss", 50.0),
                    _fundamentals_row("S003", "net_income", 30.0),
                ]
            ),
            "gold.daily_prices_adj": pl.DataFrame(
                [
                    _price_row("S001", 1000.0),
                    _price_row("S002", 500.0),
                    _price_row("S003", 200.0),
                ]
            ),
        }
    )
    factor = factor_registry.get("value_earnings_yield")()

    result = factor.compute(ctx).collect()

    scores = {row["security_id"]: row["score"] for row in result.iter_rows(named=True)}
    assert scores["S001"] == pytest.approx(90.0 / 1000.0)
    assert scores["S002"] == pytest.approx(50.0 / 500.0)
    assert scores["S003"] == pytest.approx(30.0 / 200.0)


def test_value_earnings_yield_ignores_non_annual_reports() -> None:
    ctx = InMemoryFactorContext(
        {
            "gold.fundamentals_pit": pl.DataFrame(
                [
                    _fundamentals_row("S001", "ifrs-full_ProfitLoss", 100.0),
                    _fundamentals_row(
                        "S002", "ifrs-full_ProfitLoss", 400.0, report_type="11013"
                    ),
                ]
            ),
            "gold.daily_prices_adj": pl.DataFrame(
                [_price_row("S001", 1000.0), _price_row("S002", 1000.0)]
            ),
        }
    )
    factor = factor_registry.get("value_earnings_yield")()

    result = factor.compute(ctx).collect()

    assert result["security_id"].to_list() == ["S001"]
    assert result["score"].item() == pytest.approx(0.1)


def test_quality_roa_scores_known_inputs() -> None:
    ctx = InMemoryFactorContext(
        {
            "gold.fundamentals_pit": pl.DataFrame(
                [
                    # Owners-of-parent (90) wins over ProfitLoss (100); no summing.
                    _fundamentals_row(
                        "S001", "ifrs-full_ProfitLossAttributableToOwnersOfParent", 90.0
                    ),
                    _fundamentals_row("S001", "ifrs-full_ProfitLoss", 100.0),
                    # ifrs-full_Assets (1000) wins over the ad-hoc alias (1234).
                    _fundamentals_row("S001", "ifrs-full_Assets", 1000.0),
                    _fundamentals_row("S001", "assets", 1234.0),
                    # Non-annual rows must be ignored entirely.
                    _fundamentals_row(
                        "S001", "ifrs-full_ProfitLoss", 999.0, report_type="11013"
                    ),
                    _fundamentals_row("S002", "profit_loss", 20.0),
                    _fundamentals_row("S002", "total_assets", 200.0),
                ]
            ),
        }
    )
    factor = factor_registry.get("quality_roa")()

    result = factor.compute(ctx).collect()

    scores = {row["security_id"]: row["score"] for row in result.iter_rows(named=True)}
    assert scores["S001"] == pytest.approx(90.0 / 1000.0)
    assert scores["S002"] == pytest.approx(20.0 / 200.0)
