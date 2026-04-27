from __future__ import annotations

from datetime import date

import polars as pl

from finance_pi.backtest import BacktestConfig, BacktestEngine
from finance_pi.calendar import TradingCalendar
from finance_pi.factors import InMemoryFactorContext
from finance_pi.factors.base import Factor


class StaticScoreFactor(Factor):
    requires = ["scores"]

    def compute(self, ctx):
        return ctx.scan("scores")


def test_backtest_separates_signal_and_entry_dates() -> None:
    calendar = TradingCalendar.weekdays(date(2024, 1, 1), date(2024, 2, 29))
    dates = list(calendar.dates)
    scores = pl.DataFrame(
        [{"date": d, "security_id": "S001", "score": 1.0} for d in dates]
        + [{"date": d, "security_id": "S002", "score": 0.5} for d in dates]
    )
    universe = pl.DataFrame(
        [
            {
                "date": d,
                "security_id": sid,
                "listing_id": f"L{sid[-1]}",
                "market": "KOSPI",
                "is_active": True,
                "share_class": "common",
                "security_type": "equity",
                "is_spac_pre": False,
                "is_halted": False,
                "is_designated": False,
                "is_liquidation_window": False,
            }
            for d in dates
            for sid in ["S001", "S002"]
        ]
    )
    prices = pl.DataFrame(
        [
            {
                "date": d,
                "security_id": sid,
                "listing_id": f"L{sid[-1]}",
                "close_adj": 100.0,
                "return_1d": 0.001,
                "market_cap": 1_000_000,
            }
            for d in dates
            for sid in ["S001", "S002"]
        ]
    )
    ctx = InMemoryFactorContext(
        {
            "scores": scores,
            "gold.universe_history": universe,
            "gold.daily_prices_adj": prices,
        }
    )

    result = BacktestEngine(calendar).run(
        StaticScoreFactor(),
        ctx,
        BacktestConfig(start=date(2024, 1, 1), end=date(2024, 2, 29), top_fraction=0.5),
    )

    assert not result.ledger.is_empty()
    first = result.ledger.row(0, named=True)
    assert first["entry_date"] > first["signal_date"]
    assert result.nav.select("nav").drop_nulls().height > 0
