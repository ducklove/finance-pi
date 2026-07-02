"""Data-frame level factor analytics: IC, quantile returns, turnover, forward returns.

Scores frames carry (date, security_id, score); forward-return frames carry
(date, security_id, forward_return). Non-finite scores and returns are
excluded everywhere, matching the backtest engine's selection filter.
"""

from __future__ import annotations

from math import ceil

import polars as pl

from finance_pi.factors.cross_section import quantile_bucket

IC_METHODS = ("spearman", "pearson")


def _lazy(frame: pl.DataFrame | pl.LazyFrame) -> pl.LazyFrame:
    return frame.lazy() if isinstance(frame, pl.DataFrame) else frame


def forward_returns(prices_lf: pl.LazyFrame, horizon_days: int) -> pl.LazyFrame:
    """Per-security forward return over ``horizon_days``: close_adj[t+h] / close_adj[t] - 1."""
    if horizon_days < 1:
        raise ValueError("horizon_days must be >= 1")
    return (
        _lazy(prices_lf)
        .sort(["security_id", "date"])
        .with_columns(
            (
                pl.col("close_adj").shift(-horizon_days).over("security_id") / pl.col("close_adj")
                - 1
            ).alias("forward_return")
        )
        .select(["date", "security_id", "forward_return"])
    )


def _joined_finite(
    scores: pl.DataFrame | pl.LazyFrame,
    fwd_returns: pl.DataFrame | pl.LazyFrame,
) -> pl.LazyFrame:
    return (
        _lazy(scores)
        .join(_lazy(fwd_returns), on=["date", "security_id"], how="inner")
        .filter(pl.col("score").is_finite() & pl.col("forward_return").is_finite())
    )


def information_coefficient(
    scores: pl.DataFrame | pl.LazyFrame,
    fwd_returns: pl.DataFrame | pl.LazyFrame,
    method: str = "spearman",
) -> pl.DataFrame:
    """Per-date correlation between scores and forward returns: (date, ic, n)."""
    if method not in IC_METHODS:
        raise ValueError(f"method must be one of {IC_METHODS}, got {method!r}")
    return (
        _joined_finite(scores, fwd_returns)
        .group_by("date")
        .agg(
            pl.corr("score", "forward_return", method=method).alias("ic"),
            pl.len().alias("n"),
        )
        .sort("date")
        .collect()
    )


def quantile_returns(
    scores: pl.DataFrame | pl.LazyFrame,
    fwd_returns: pl.DataFrame | pl.LazyFrame,
    quantiles: int = 5,
) -> pl.DataFrame:
    """Per-date mean forward return by score quantile (1 = lowest scores)."""
    if quantiles < 2:
        raise ValueError("quantiles must be >= 2")
    return (
        _joined_finite(scores, fwd_returns)
        .with_columns((quantile_bucket(pl.col("score"), quantiles) + 1).alias("quantile"))
        .group_by(["date", "quantile"])
        .agg(
            pl.col("forward_return").mean().alias("mean_forward_return"),
            pl.len().alias("n"),
        )
        .sort(["date", "quantile"])
        .collect()
    )


def factor_turnover(
    scores: pl.DataFrame | pl.LazyFrame,
    top_fraction: float,
) -> pl.DataFrame:
    """Per-date overlap of the top-``top_fraction`` set with the previous date's set.

    Top sets mirror engine selection: highest finite scores, at least one name.
    Pass ``score * direction`` for direction=-1 factors. ``overlap`` is
    |previous ∩ current| / |current| and null on the first date.
    """
    if not 0 < top_fraction <= 1:
        raise ValueError("top_fraction must be in (0, 1]")
    frame = _lazy(scores).filter(pl.col("score").is_finite()).sort("date").collect()
    rows: list[dict[str, object]] = []
    previous: set[str] | None = None
    for day_frame in frame.partition_by("date", maintain_order=True):
        count = day_frame.height
        take = min(count, max(1, ceil(count * top_fraction)))
        top = set(
            day_frame.sort("score", descending=True).head(take).get_column("security_id").to_list()
        )
        rows.append(
            {
                "date": day_frame.get_column("date").first(),
                "top_count": take,
                "overlap": None if previous is None else len(top & previous) / len(top),
            }
        )
        previous = top
    return pl.DataFrame(
        rows, schema={"date": pl.Date, "top_count": pl.Int64, "overlap": pl.Float64}
    )
