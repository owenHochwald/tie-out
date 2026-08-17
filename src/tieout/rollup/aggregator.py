from dataclasses import dataclass, field
from decimal import Decimal

from tieout.domain import TradeEvent


@dataclass
class SymbolRollup:
    symbol: str
    total_quantity: Decimal = Decimal(0)
    total_notional: Decimal = Decimal(0)
    trade_ids: set[str] = field(default_factory=set)  # idempotency at the rollup level

    def apply(self, trade: TradeEvent) -> None:
        if trade.trade_id in self.trade_ids:
            return  # already applied — this is what makes it order-independent AND duplicate-safe
        self.trade_ids.add(trade.trade_id)
        self.total_quantity += trade.quantity
        self.total_notional += trade.quantity * trade.price


class RollupAggregator:
    def __init__(self) -> None:
        self._rollups: dict[str, SymbolRollup] = {}

    def apply(self, trade: TradeEvent) -> None:
        rollup = self._rollups.setdefault(trade.symbol, SymbolRollup(symbol=trade.symbol))
        rollup.apply(trade)

    def get(self, symbol: str) -> SymbolRollup | None:
        return self._rollups.get(symbol)

    def all_rollups(self) -> dict[str, SymbolRollup]:
        return dict(self._rollups)
