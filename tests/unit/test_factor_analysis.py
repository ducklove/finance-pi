from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from finance_pi.factors.analysis import (
    factor_turnover,
    forward_returns,
    information_coefficient,
    quantile_returns,
)

D1 = date(2024, 1, 31)
D2 = date(2024, 2, 29)


def _scores(rows: dict[tuple[date, str], float]) -> pl.DataFrame:
    return pl.DataFrame(
        [{"date": d, "security_id": sid, "score": value} for (d, sid), value in rows.items()]
    )


def _returns(rows: dict[tuple[date, str], float]) -> pl.DataFrame:
    return pl.DataFrame(
        [
            {"date": d, "security_id": sid, "forward_return": value}
            for (d, sid), value in rows.items()
        ]
    )


def test_forward_returns_hand_computed() -> None:
    days = [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)]
    prices = pl.LazyFrame(
        {
            "date": days,
            "security_id": ["S001"] * 3,
            "close_adj": [100.0, 110.0, 121.0],
        }
    )

    one_day = forward_returns(prices, 1).collect().sort("date")
    assert one_day.get_column("forward_return").to_list() == pytest.approx([0.1, 0.1, None])

    two_day = forward_returns(prices, 2).collect().sort("date")
    assert two_day.get_column("forward_return").to_list()[0] == pytest.approx(0.21)
    assert two_day.get_column("forward_return").to_list()[1:] == [None, None]

    with pytest.raises(ValueError, match="horizon_days"):
        forward_returns(prices, 0)


def test_information_coefficient_spearman_hand_computed() -> None:
    scores = _scores({(D1, s): v for s, v in [("A", 1.0), ("B", 2.0), ("C", 3.0), ("D", 4.0)]})
    fwd = _returns({(D1, s): v for s, v in [("A", 0.01), ("B", 0.03), ("C", 0.02), ("D", 0.04)]})

    result = information_coefficient(scores, fwd)

    row = result.row(0, named=True)
    # Rank pairs (1,1),(2,3),(3,2),(4,4): rho = 1 - 6*2/(4*15) = 0.8.
    assert row["ic"] == pytest.approx(0.8)
    assert row["n"] == 4


def test_information_coefficient_pearson_and_filtering() -> None:
    scores = _scores(
        {
            (D1, "A"): 1.0,
            (D1, "B"): 2.0,
            (D1, "C"): 3.0,
            (D1, "D"): float("nan"),  # dropped
        }
    )
    fwd = _returns({(D1, "A"): 0.02, (D1, "B"): 0.04, (D1, "C"): 0.06, (D1, "D"): 0.5})

    result = information_coefficient(scores, fwd, method="pearson")

    row = result.row(0, named=True)
    assert row["ic"] == pytest.approx(1.0)
    assert row["n"] == 3

    with pytest.raises(ValueError, match="method"):
        information_coefficient(scores, fwd, method="kendall")


def test_quantile_returns_hand_computed() -> None:
    scores = _scores({(D1, s): v for s, v in [("A", 1.0), ("B", 2.0), ("C", 3.0), ("D", 4.0)]})
    fwd = _returns({(D1, s): v for s, v in [("A", 0.01), ("B", 0.02), ("C", 0.03), ("D", 0.05)]})

    result = quantile_returns(scores, fwd, quantiles=2).sort("quantile")

    assert result.get_column("quantile").to_list() == [1, 2]
    assert result.get_column("mean_forward_return").to_list() == pytest.approx([0.015, 0.04])
    assert result.get_column("n").to_list() == [2, 2]

    with pytest.raises(ValueError, match="quantiles"):
        quantile_returns(scores, fwd, quantiles=1)


def test_factor_turnover_overlap_ratio() -> None:
    scores = _scores(
        {
            (D1, "A"): 4.0,
            (D1, "B"): 3.0,
            (D1, "C"): 2.0,
            (D1, "D"): 1.0,
            (D2, "B"): 4.0,
            (D2, "C"): 3.0,
            (D2, "A"): 2.0,
            (D2, "D"): float("nan"),  # excluded from the cross-section
        }
    )

    result = factor_turnover(scores, top_fraction=0.5)

    assert result.get_column("date").to_list() == [D1, D2]
    assert result.get_column("top_count").to_list() == [2, 2]
    overlap = result.get_column("overlap").to_list()
    assert overlap[0] is None
    assert overlap[1] == pytest.approx(0.5)  # {A,B} -> {B,C}

    with pytest.raises(ValueError, match="top_fraction"):
        factor_turnover(scores, top_fraction=0.0)
