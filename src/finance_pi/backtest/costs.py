from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


class CostModel(Protocol):
    """Minimal cost interface.

    Models may additionally expose ``cost_for_trades(buys, sells)``; the
    engine uses that richer API when available.
    """

    def cost_for_turnover(self, turnover: float) -> float: ...


@dataclass(frozen=True)
class FixedBpsCostModel:
    bps: float = 10.0

    def cost_for_turnover(self, turnover: float) -> float:
        return turnover * self.bps / 10_000


@dataclass(frozen=True)
class KoreanTradingCostModel:
    """Commission on both sides plus the securities transaction tax on sells.

    The sell-side transaction tax dominates Korean trading costs
    (0.15%~0.30% depending on era); set ``sell_tax_bps`` to the rate of the
    backtest period.
    """

    commission_bps: float = 5.0
    sell_tax_bps: float = 15.0

    def cost_for_trades(self, buys: float, sells: float) -> float:
        commission = (buys + sells) * self.commission_bps / 10_000
        return commission + sells * self.sell_tax_bps / 10_000

    def cost_for_turnover(self, turnover: float) -> float:
        # Assume symmetric flows when only total turnover is known.
        half = turnover / 2
        return self.cost_for_trades(half, half)
