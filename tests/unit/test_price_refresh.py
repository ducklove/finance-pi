from datetime import date

import polars as pl
import pytest
from test_transforms import _silver_price_row, _write_silver_prices

from finance_pi.transforms.builders import _adjusted_price_frame
from finance_pi.transforms.price_refresh import (
    apply_refreshed_price_history,
    refresh_action_price_history,
)


class Quotes:
    def __init__(self, rows):
        self.rows = rows

    def fetch_daily_prices(self, ticker, since, until):
        return self.rows


def test_refresh_replaces_old_adjusted_basis_without_double_adjustment(tmp_path):
    rows = [
        _silver_price_row(date(2026, 8, 13), "019680", close=1000, price_source="naver"),
        _silver_price_row(date(2026, 9, 9), "019680", close=1000, price_source="kis"),
        _silver_price_row(date(2026, 9, 10), "019680", close=1874, price_source="kis"),
    ]
    _write_silver_prices(tmp_path, rows)
    fresh = [
        {**row, **dict.fromkeys(["open", "high", "low", "close"], price)}
        for row, price in zip(rows, [2000, 2000, 1874], strict=True)
    ]
    actions = pl.DataFrame(
        [{"security_id": "S019680", "effective_date": date(2026, 9, 10), "adjustment_factor": 2.0}]
    )
    for _ in range(2):
        assert refresh_action_price_history(tmp_path, {"S019680"}, client=Quotes(fresh)) == 1
        prices = apply_refreshed_price_history(tmp_path, pl.DataFrame(rows))
        adjusted = _adjusted_price_frame(prices, actions)
        assert adjusted["close_adj"].to_list() == [2000, 2000, 1874]
    assert len(list((tmp_path / "bronze/action_price_refresh/019680").glob("*.parquet"))) == 1
    # 원본 가격은 변경하지 않는다.
    assert (
        pl.read_parquet(tmp_path / "silver/prices/dt=2026-08-13/part.parquet")["close"][0] == 1000
    )


def test_partial_refresh_fails_before_writing_snapshot(tmp_path):
    rows = [
        _silver_price_row(date(2026, 8, day), "019680", close=1000, price_source="naver")
        for day in [12, 13]
    ]
    _write_silver_prices(tmp_path, rows)
    with pytest.raises(ValueError, match="재조회 누락"):
        refresh_action_price_history(tmp_path, {"S019680"}, client=Quotes(rows[1:]))
    assert not (tmp_path / "silver/action_price_refresh/019680.parquet").exists()


def test_unrelated_security_and_new_dates_remain_on_original_basis(tmp_path):
    rows = [_silver_price_row(date(2026, 8, 13), "019680", close=1000, price_source="naver")]
    _write_silver_prices(tmp_path, rows)
    refresh_action_price_history(tmp_path, {"S019680"}, client=Quotes(rows))
    other = _silver_price_row(date(2026, 9, 11), "019680", close=1819, price_source="kis")
    unrelated = _silver_price_row(date(2026, 8, 13), "005930", close=50000)
    result = apply_refreshed_price_history(tmp_path, pl.DataFrame([other, unrelated]))
    assert result["close"].to_list() == [1819, 50000]
    assert result["price_basis"].to_list() == ["raw", "raw"]


def test_source_that_has_not_applied_reverse_split_is_rejected(tmp_path):
    rows = [
        _silver_price_row(date(2026, 9, 9), "019680", close=1000, price_source="naver"),
        _silver_price_row(date(2026, 9, 10), "019680", close=1874, price_source="naver"),
    ]
    _write_silver_prices(tmp_path, rows)
    path = tmp_path / "silver/corporate_actions/dt=2026-09-10/part.parquet"
    path.parent.mkdir(parents=True)
    pl.DataFrame(
        [{"security_id": "S019680", "effective_date": date(2026, 9, 10), "adjustment_factor": 2.0}]
    ).write_parquet(path)
    with pytest.raises(ValueError, match="큰 단절"):
        refresh_action_price_history(tmp_path, {"S019680"}, client=Quotes(rows))
    assert not (tmp_path / "silver/action_price_refresh/019680.parquet").exists()
