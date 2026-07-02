from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from finance_pi.factors.cross_section import (
    neutralize_by,
    quantile_bucket,
    rank_pct,
    size_bucket,
    winsorize,
    zscore,
)

D1 = date(2024, 1, 31)
D2 = date(2024, 2, 29)


def _frame(values: dict[date, list[float | None]]) -> pl.DataFrame:
    rows = [{"date": day, "x": value} for day, day_values in values.items() for value in day_values]
    return pl.DataFrame(rows, schema={"date": pl.Date, "x": pl.Float64})


def test_winsorize_clips_to_per_date_quantiles() -> None:
    frame = _frame({D1: [1.0, 2.0, 3.0, 4.0, 100.0]})
    # Linear-interpolated q25 of [1..4, 100] is 2, q75 is 4.
    out = frame.select(winsorize(pl.col("x"), lower=0.25, upper=0.75)).to_series().to_list()
    assert out == [2.0, 2.0, 3.0, 4.0, 4.0]


def test_winsorize_is_per_date() -> None:
    frame = _frame({D1: [1.0, 2.0, 3.0], D2: [100.0, 200.0, 300.0]})
    out = frame.select(winsorize(pl.col("x"), lower=0.0, upper=0.5)).to_series().to_list()
    # Upper quantile 0.5 clips each date to its own median.
    assert out == [1.0, 2.0, 2.0, 100.0, 200.0, 200.0]


def test_zscore_hand_computed_per_date() -> None:
    frame = _frame({D1: [1.0, 2.0, 3.0], D2: [10.0, 20.0]})
    out = frame.select(zscore(pl.col("x"))).to_series().to_list()
    assert out[:3] == pytest.approx([-1.0, 0.0, 1.0])  # std ddof=1 of [1,2,3] is 1
    assert out[3:] == pytest.approx([-(0.5**0.5), 0.5**0.5])


def test_rank_pct_average_ties_and_nulls() -> None:
    frame = _frame({D1: [10.0, 20.0, 30.0, 40.0], D2: [10.0, 10.0, 20.0, None]})
    out = frame.select(rank_pct(pl.col("x"))).to_series().to_list()
    assert out[:4] == pytest.approx([0.25, 0.5, 0.75, 1.0])
    assert out[4:6] == pytest.approx([0.5, 0.5])  # tied average rank 1.5 over 3 non-null
    assert out[6] == pytest.approx(1.0)
    assert out[7] is None


def test_neutralize_by_demeans_within_date_and_group() -> None:
    frame = pl.DataFrame(
        {
            "date": [D1, D1, D1, D2],
            "market": ["KOSPI", "KOSPI", "KOSDAQ", "KOSPI"],
            "x": [1.0, 3.0, 10.0, 7.0],
        }
    )
    out = frame.select(neutralize_by(pl.col("x"), ["market"])).to_series().to_list()
    assert out == pytest.approx([-1.0, 1.0, 0.0, 0.0])


def test_size_bucket_quintiles_per_date() -> None:
    frame = _frame({D1: [float(i) for i in range(1, 11)], D2: [5.0, 15.0, 25.0]})
    out = frame.select(size_bucket(pl.col("x"), n=5)).to_series().to_list()
    assert out[:10] == [0, 0, 1, 1, 2, 2, 3, 3, 4, 4]
    assert out[10:] == [0, 1, 3]  # ranks 1,2,3 of 3 -> floor((r-1)*5/3)


def test_quantile_bucket_handles_ties() -> None:
    frame = _frame({D1: [5.0, 5.0, 7.0]})
    out = frame.select(quantile_bucket(pl.col("x"), 2)).to_series().to_list()
    assert out == [0, 0, 1]  # tied average rank 1.5 -> floor(0.5 * 2 / 3) = 0
