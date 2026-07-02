"""Composite factors: weighted blends of z-scored registered component factors."""

from __future__ import annotations

from typing import ClassVar

import polars as pl

from finance_pi.factors.base import Factor, FactorContext, factor_registry
from finance_pi.factors.cross_section import zscore


class CompositeFactor(Factor):
    """Weighted average of per-date z-scored component factor scores.

    Each component is computed from the registry, restricted to finite raw
    scores, z-scored per date, then multiplied by its class ``direction`` so
    every normalized component is higher-is-better. NaN policy: non-finite raw
    scores are dropped before normalization; a zero-variance date z-scores to
    NaN and is treated as missing. A (date, security_id) needs at least one
    valid component score — weights are renormalized over the components
    present, and rows only exist where some component scored at all.
    """

    components: ClassVar[tuple[str, ...]] = ()
    weights: ClassVar[tuple[float, ...] | None] = None

    def __init__(
        self,
        name: str | None = None,
        components: list[str] | tuple[str, ...] | None = None,
        weights: list[float] | tuple[float, ...] | None = None,
    ) -> None:
        if components is not None:
            self.components = tuple(components)
        if weights is not None:
            self.weights = tuple(weights)
        if not self.components:
            raise ValueError("CompositeFactor needs at least one component")
        if self.weights is None:
            self.weights = tuple(1.0 for _ in self.components)
        if len(self.weights) != len(self.components):
            raise ValueError("weights must match components one-to-one")
        if any(weight <= 0 for weight in self.weights):
            raise ValueError("weights must be positive")
        self.name = name or type(self).__name__
        component_classes = [factor_registry.get(component) for component in self.components]
        self.requires = sorted({need for cls in component_classes for need in cls.requires})

    def compute(self, ctx: FactorContext) -> pl.LazyFrame:
        combined: pl.LazyFrame | None = None
        for component in self.components:
            cls = factor_registry.get(component)
            normalized = zscore(pl.col("score")) * cls.direction
            frame = (
                cls()
                .compute(ctx)
                .filter(pl.col("score").is_finite())
                .select(
                    "date",
                    "security_id",
                    # Zero-variance dates z-score to NaN; treat them as missing.
                    pl.when(normalized.is_finite()).then(normalized).alias(f"z_{component}"),
                )
            )
            if combined is None:
                combined = frame
            else:
                combined = combined.join(
                    frame, on=["date", "security_id"], how="full", coalesce=True
                )
        assert combined is not None
        weighted = pl.sum_horizontal(
            pl.col(f"z_{component}").fill_null(0.0) * weight
            for component, weight in zip(self.components, self.weights, strict=True)
        )
        available = pl.sum_horizontal(
            pl.when(pl.col(f"z_{component}").is_not_null()).then(weight).otherwise(0.0)
            for component, weight in zip(self.components, self.weights, strict=True)
        )
        score = pl.when(available > 0).then(weighted / available)
        return combined.with_columns(score.alias("score")).select(["date", "security_id", "score"])


@factor_registry.register("composite_value_quality")
class CompositeValueQuality(CompositeFactor):
    """Equal-weight blend of value (book-to-price, earnings yield) and quality (ROA)."""

    components = (
        "value_book_to_price",
        "value_earnings_yield",
        "quality_roa",
    )
    requires = ["gold.daily_prices_adj", "gold.fundamentals_pit"]

    def __init__(self) -> None:
        super().__init__("composite_value_quality")
