from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from math import ceil

import polars as pl

from finance_pi.backtest.costs import FixedBpsCostModel
from finance_pi.backtest.models import BacktestConfig, BacktestResult
from finance_pi.calendar import TradingCalendar
from finance_pi.factors import Factor, FactorContext


@dataclass(frozen=True)
class BacktestEngine:
    calendar: TradingCalendar
    cost_model: FixedBpsCostModel = FixedBpsCostModel()

    def run(self, factor: Factor, ctx: FactorContext, config: BacktestConfig) -> BacktestResult:
        scores = factor.compute(ctx).collect()
        universe = ctx.scan("gold.universe_history").collect()
        prices = ctx.scan("gold.daily_prices_adj").collect()

        rebalance_dates = self.calendar.month_end_dates(config.start, config.end)
        schedules = self._build_schedules(rebalance_dates, config)
        position_rows: list[dict[str, object]] = []
        ledger_rows: list[dict[str, object]] = []
        previous_weights: dict[str, float] = {}

        for index, schedule in enumerate(schedules):
            signal_date = schedule["signal_date"]
            entry_date = schedule["entry_date"]
            if index + 1 < len(schedules):
                exit_date = self.calendar.previous(schedules[index + 1]["entry_date"], offset=1)
            else:
                exit_date = config.end
            active = universe.filter(
                (pl.col("date") == signal_date)
                & pl.col("is_active")
                & (pl.col("share_class") == "common")
                & (pl.col("is_spac_pre").fill_null(False).not_())
            ).select("security_id")
            candidates = (
                scores.filter((pl.col("date") == signal_date) & pl.col("score").is_not_null())
                .join(active, on="security_id", how="inner")
                .sort("score", descending=True)
            )
            count = candidates.height
            selected_count = min(count, max(1, ceil(count * config.top_fraction))) if count else 0
            selected = candidates.head(selected_count)
            target_weight = min(
                config.max_position_weight,
                1.0 / selected_count if selected_count else 0.0,
            )
            current_weights = {
                row["security_id"]: target_weight
                for row in selected.select("security_id").to_dicts()
            }
            turnover = _turnover(previous_weights, current_weights)
            entry_cost = self.cost_model.cost_for_turnover(turnover)
            ledger_rows.append(
                {
                    "signal_date": signal_date,
                    "entry_date": entry_date,
                    "exit_date": exit_date,
                    "selected_count": selected_count,
                    "turnover": turnover,
                    "entry_cost": entry_cost,
                }
            )
            for security_id, weight in current_weights.items():
                for trading_date in self.calendar.between(entry_date, exit_date):
                    position_rows.append(
                        {
                            "date": trading_date,
                            "security_id": security_id,
                            "weight": weight,
                            "signal_date": signal_date,
                            "entry_date": entry_date,
                        }
                    )
            previous_weights = current_weights

        positions = pl.DataFrame(position_rows) if position_rows else _empty_positions()
        ledger = pl.DataFrame(ledger_rows) if ledger_rows else _empty_ledger()
        nav = self._compute_nav(positions, prices, ledger, config)
        return BacktestResult(nav=nav, positions=positions, ledger=ledger)

    def _build_schedules(
        self,
        rebalance_dates: tuple[date, ...],
        config: BacktestConfig,
    ) -> list[dict[str, date]]:
        schedules: list[dict[str, date]] = []
        for rebalance_date in rebalance_dates:
            signal_date = self.calendar.previous(rebalance_date, offset=config.signal_lag_days)
            entry_date = self.calendar.next(signal_date, offset=config.entry_lag_days)
            if config.start <= entry_date <= config.end:
                schedules.append(
                    {
                        "rebalance_date": rebalance_date,
                        "signal_date": signal_date,
                        "entry_date": entry_date,
                    }
                )
        return schedules

    def _compute_nav(
        self,
        positions: pl.DataFrame,
        prices: pl.DataFrame,
        ledger: pl.DataFrame,
        config: BacktestConfig,
    ) -> pl.DataFrame:
        if positions.is_empty():
            return pl.DataFrame(
                {"date": [], "gross_return": [], "cost": [], "return": [], "nav": []},
                schema={
                    "date": pl.Date,
                    "gross_return": pl.Float64,
                    "cost": pl.Float64,
                    "return": pl.Float64,
                    "nav": pl.Float64,
                },
            )
        daily_returns = (
            positions.join(
                prices.select(["date", "security_id", "return_1d"]),
                on=["date", "security_id"],
                how="left",
            )
            .with_columns(
                (pl.col("weight") * pl.col("return_1d").fill_null(0.0)).alias("contribution")
            )
            .group_by("date")
            .agg(pl.col("contribution").sum().alias("gross_return"))
        )
        costs = ledger.select(
            pl.col("entry_date").alias("date"),
            pl.col("entry_cost").alias("cost"),
        ).group_by("date").agg(pl.col("cost").sum())
        return (
            daily_returns.join(costs, on="date", how="left")
            .with_columns(pl.col("cost").fill_null(0.0))
            .with_columns((pl.col("gross_return") - pl.col("cost")).alias("return"))
            .sort("date")
            .with_columns(((1 + pl.col("return")).cum_prod() * config.initial_nav).alias("nav"))
        )


def _turnover(previous: dict[str, float], current: dict[str, float]) -> float:
    keys = set(previous) | set(current)
    return sum(abs(current.get(key, 0.0) - previous.get(key, 0.0)) for key in keys)


def _empty_positions() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "date": [],
            "security_id": [],
            "weight": [],
            "signal_date": [],
            "entry_date": [],
        },
        schema={
            "date": pl.Date,
            "security_id": pl.String,
            "weight": pl.Float64,
            "signal_date": pl.Date,
            "entry_date": pl.Date,
        },
    )


def _empty_ledger() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "signal_date": [],
            "entry_date": [],
            "exit_date": [],
            "selected_count": [],
            "turnover": [],
            "entry_cost": [],
        },
        schema={
            "signal_date": pl.Date,
            "entry_date": pl.Date,
            "exit_date": pl.Date,
            "selected_count": pl.Int64,
            "turnover": pl.Float64,
            "entry_cost": pl.Float64,
        },
    )
