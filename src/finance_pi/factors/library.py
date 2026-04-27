from __future__ import annotations

import polars as pl

from finance_pi.factors.base import Factor, factor_registry


@factor_registry.register("momentum_12_1")
class Momentum12_1(Factor):
    """12-month momentum excluding the most recent 1-month return."""

    requires = ["gold.daily_prices_adj"]
    rebalance = "monthly"

    def compute(self, ctx) -> pl.LazyFrame:
        prices = ctx.scan("gold.daily_prices_adj")
        return (
            prices.sort(["security_id", "date"])
            .with_columns(
                pl.col("close_adj").pct_change(252).over("security_id").alias("ret_12m"),
                pl.col("close_adj").pct_change(21).over("security_id").alias("ret_1m"),
            )
            .with_columns(((1 + pl.col("ret_12m")) / (1 + pl.col("ret_1m")) - 1).alias("score"))
            .select(["date", "security_id", "score"])
        )


@factor_registry.register("value_earnings_yield")
class ValueEarningsYield(Factor):
    """Net income divided by market capitalization, PIT by construction."""

    requires = ["gold.fundamentals_pit", "gold.daily_prices_adj"]
    rebalance = "monthly"
    net_income_accounts = {
        "ifrs-full_ProfitLoss",
        "ifrs-full_ProfitLossAttributableToOwnersOfParent",
        "profit_loss",
        "net_income",
    }

    def compute(self, ctx) -> pl.LazyFrame:
        fundamentals = (
            ctx.scan("gold.fundamentals_pit")
            .filter(pl.col("account_id").is_in(self.net_income_accounts))
            .group_by(["as_of_date", "security_id"])
            .agg(pl.col("amount").sum().alias("net_income"))
            .rename({"as_of_date": "date"})
        )
        prices = ctx.scan("gold.daily_prices_adj").select(["date", "security_id", "market_cap"])
        return (
            fundamentals.join(prices, on=["date", "security_id"], how="inner")
            .with_columns((pl.col("net_income") / pl.col("market_cap")).alias("score"))
            .select(["date", "security_id", "score"])
        )


@factor_registry.register("quality_roa")
class QualityRoa(Factor):
    """Return on assets from PIT fundamentals."""

    requires = ["gold.fundamentals_pit"]
    rebalance = "monthly"
    net_income_accounts = {
        "ifrs-full_ProfitLoss",
        "ifrs-full_ProfitLossAttributableToOwnersOfParent",
        "profit_loss",
        "net_income",
    }
    asset_accounts = {
        "ifrs-full_Assets",
        "assets",
        "total_assets",
    }

    def compute(self, ctx) -> pl.LazyFrame:
        fundamentals = ctx.scan("gold.fundamentals_pit")
        return (
            fundamentals.group_by(["as_of_date", "security_id"])
            .agg(
                pl.when(pl.col("account_id").is_in(self.net_income_accounts))
                .then(pl.col("amount"))
                .otherwise(None)
                .sum()
                .alias("net_income"),
                pl.when(pl.col("account_id").is_in(self.asset_accounts))
                .then(pl.col("amount"))
                .otherwise(None)
                .sum()
                .alias("assets"),
            )
            .with_columns((pl.col("net_income") / pl.col("assets")).alias("score"))
            .rename({"as_of_date": "date"})
            .select(["date", "security_id", "score"])
        )
