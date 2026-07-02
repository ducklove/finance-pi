"""Cross-sectional expression helpers.

Every function returns a ``pl.Expr`` windowed over ``date`` so it can be used
inside ``with_columns``/``select`` on any frame holding one row per
(date, security_id).
"""

from __future__ import annotations

import polars as pl


def winsorize(expr: pl.Expr, lower: float = 0.01, upper: float = 0.99) -> pl.Expr:
    """Clip to the per-date cross-sectional [lower, upper] quantiles (linear interpolation)."""
    return expr.clip(
        expr.quantile(lower, interpolation="linear").over("date"),
        expr.quantile(upper, interpolation="linear").over("date"),
    )


def zscore(expr: pl.Expr) -> pl.Expr:
    """Per-date cross-sectional z-score: (x - mean) / std with ddof=1.

    A date whose cross-section has zero variance yields NaN; a single-name
    date yields null (std undefined). Callers that cannot tolerate either
    should filter with ``is_finite`` afterwards.
    """
    return (expr - expr.mean().over("date")) / expr.std().over("date")


def rank_pct(expr: pl.Expr) -> pl.Expr:
    """Per-date percentile rank in (0, 1]: average rank / non-null count. Nulls stay null."""
    return expr.rank(method="average").over("date") / expr.count().over("date")


def neutralize_by(expr: pl.Expr, group_cols: list[str]) -> pl.Expr:
    """Demean within (date, *group_cols).

    Sector classifications are not ingested yet, so the available groupings
    are market (``market``: KOSPI/KOSDAQ from ``gold.universe_history``) and
    size buckets (join a ``size_bucket`` column first, then neutralize on it).
    """
    return expr - expr.mean().over(["date", *group_cols])


def quantile_bucket(expr: pl.Expr, n: int) -> pl.Expr:
    """Per-date quantile bucket in [0, n-1] by ascending value; ties share average rank."""
    rank = expr.rank(method="average").over("date")
    count = expr.count().over("date")
    return ((rank - 1) * n / count).floor().cast(pl.Int32)


def size_bucket(market_cap_expr: pl.Expr, n: int = 5) -> pl.Expr:
    """Per-date market-cap quintile (0 = smallest, n-1 = largest by default)."""
    return quantile_bucket(market_cap_expr, n)
