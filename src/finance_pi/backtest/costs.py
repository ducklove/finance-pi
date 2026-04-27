from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FixedBpsCostModel:
    bps: float = 10.0

    def cost_for_turnover(self, turnover: float) -> float:
        return turnover * self.bps / 10_000
