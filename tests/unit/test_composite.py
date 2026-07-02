from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from finance_pi.factors import CompositeFactor, InMemoryFactorContext, factor_registry
from finance_pi.factors.base import Factor

D1 = date(2024, 6, 28)
SQRT_HALF = 0.5**0.5


@factor_registry.register("test_comp_a")
class _CompA(Factor):
    requires = ["comp_a"]

    def compute(self, ctx):
        return ctx.scan("comp_a")


@factor_registry.register("test_comp_b")
class _CompB(Factor):
    requires = ["comp_b"]

    def compute(self, ctx):
        return ctx.scan("comp_b")


@factor_registry.register("test_comp_b_inverse")
class _CompBInverse(_CompB):
    direction = -1


def _scores(values: dict[str, float]) -> pl.DataFrame:
    return pl.DataFrame(
        [{"date": D1, "security_id": sid, "score": value} for sid, value in values.items()]
    )


def _ctx(comp_a: dict[str, float], comp_b: dict[str, float]) -> InMemoryFactorContext:
    return InMemoryFactorContext({"comp_a": _scores(comp_a), "comp_b": _scores(comp_b)})


def _by_security(result: pl.DataFrame) -> dict[str, float]:
    return {row["security_id"]: row["score"] for row in result.iter_rows(named=True)}


def test_composite_equal_weight_hand_computed() -> None:
    # comp_a z = [-1, 0, 1]; comp_b z = [1, -1, 0]; equal-weight average.
    ctx = _ctx({"A": 1.0, "B": 2.0, "C": 3.0}, {"A": 30.0, "B": 10.0, "C": 20.0})
    factor = CompositeFactor("blend", ["test_comp_a", "test_comp_b"])

    scores = _by_security(factor.compute(ctx).collect())

    assert scores["A"] == pytest.approx(0.0)
    assert scores["B"] == pytest.approx(-0.5)
    assert scores["C"] == pytest.approx(0.5)


def test_composite_custom_weights() -> None:
    ctx = _ctx({"A": 1.0, "B": 2.0, "C": 3.0}, {"A": 30.0, "B": 10.0, "C": 20.0})
    factor = CompositeFactor("blend", ["test_comp_a", "test_comp_b"], [3.0, 1.0])

    scores = _by_security(factor.compute(ctx).collect())

    assert scores["A"] == pytest.approx((3 * -1.0 + 1 * 1.0) / 4)
    assert scores["B"] == pytest.approx((3 * 0.0 + 1 * -1.0) / 4)
    assert scores["C"] == pytest.approx((3 * 1.0 + 1 * 0.0) / 4)


def test_composite_flips_negative_direction_components() -> None:
    # comp_b_inverse z is flipped: [-1, 1, 0] over raw [30, 10, 20].
    ctx = _ctx({"A": 1.0, "B": 2.0, "C": 3.0}, {"A": 30.0, "B": 10.0, "C": 20.0})
    factor = CompositeFactor("blend", ["test_comp_a", "test_comp_b_inverse"])

    scores = _by_security(factor.compute(ctx).collect())

    assert scores["A"] == pytest.approx(-1.0)
    assert scores["B"] == pytest.approx(0.5)
    assert scores["C"] == pytest.approx(0.5)


def test_composite_renormalizes_over_missing_components() -> None:
    # comp_b lacks C, so its z-scores span {A, B} only: [sqrt(.5), -sqrt(.5)].
    ctx = _ctx({"A": 1.0, "B": 2.0, "C": 3.0}, {"A": 30.0, "B": 10.0})
    factor = CompositeFactor("blend", ["test_comp_a", "test_comp_b"])

    scores = _by_security(factor.compute(ctx).collect())

    assert scores["A"] == pytest.approx((-1.0 + SQRT_HALF) / 2)
    assert scores["B"] == pytest.approx((0.0 - SQRT_HALF) / 2)
    assert scores["C"] == pytest.approx(1.0)  # only comp_a contributes


def test_composite_treats_zero_variance_component_as_missing() -> None:
    ctx = _ctx({"A": 5.0, "B": 5.0}, {"A": 1.0, "B": 2.0})
    factor = CompositeFactor("blend", ["test_comp_a", "test_comp_b"])

    scores = _by_security(factor.compute(ctx).collect())

    assert scores["A"] == pytest.approx(-SQRT_HALF)
    assert scores["B"] == pytest.approx(SQRT_HALF)


def test_composite_requires_union_and_validation() -> None:
    factor = CompositeFactor("blend", ["test_comp_a", "test_comp_b"])
    assert factor.requires == ["comp_a", "comp_b"]
    assert factor.direction == 1

    with pytest.raises(ValueError, match="at least one component"):
        CompositeFactor("empty", [])
    with pytest.raises(ValueError, match="one-to-one"):
        CompositeFactor("bad", ["test_comp_a"], [1.0, 2.0])
    with pytest.raises(ValueError, match="positive"):
        CompositeFactor("bad", ["test_comp_a", "test_comp_b"], [1.0, -1.0])
    with pytest.raises(KeyError, match="unknown factor"):
        CompositeFactor("bad", ["not_registered"])


def test_composite_value_quality_registered_and_computes() -> None:
    cls = factor_registry.get("composite_value_quality")
    assert cls.components == ("value_book_to_price", "value_earnings_yield", "quality_roa")

    def fundamentals_row(sid: str, account_id: str, amount: float) -> dict:
        return {
            "as_of_date": D1,
            "security_id": sid,
            "account_id": account_id,
            "amount": amount,
            "report_type": "11011",
        }

    ctx = InMemoryFactorContext(
        {
            "gold.fundamentals_pit": pl.DataFrame(
                [
                    # S001: B/P 0.8, E/P 0.09, ROA 0.09; S002: B/P 0.2, E/P 0.04, ROA 0.1.
                    fundamentals_row("S001", "ifrs-full_Equity", 800.0),
                    fundamentals_row("S001", "ifrs-full_ProfitLoss", 90.0),
                    fundamentals_row("S001", "ifrs-full_Assets", 1000.0),
                    fundamentals_row("S002", "ifrs-full_Equity", 200.0),
                    fundamentals_row("S002", "ifrs-full_ProfitLoss", 40.0),
                    fundamentals_row("S002", "ifrs-full_Assets", 400.0),
                ]
            ),
            "gold.daily_prices_adj": pl.DataFrame(
                [
                    {"date": D1, "security_id": "S001", "market_cap": 1000.0},
                    {"date": D1, "security_id": "S002", "market_cap": 1000.0},
                ]
            ),
        }
    )

    scores = _by_security(cls().compute(ctx).collect())

    # Two-name z-scores are +/- sqrt(1/2); ROA favors S002, the value legs favor S001.
    assert scores["S001"] == pytest.approx((SQRT_HALF + SQRT_HALF - SQRT_HALF) / 3)
    assert scores["S002"] == pytest.approx((-SQRT_HALF - SQRT_HALF + SQRT_HALF) / 3)
