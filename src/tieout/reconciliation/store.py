from collections import OrderedDict
from datetime import datetime

from tieout.domain import Break, BreakType, EventSource, TradeEvent
from tieout.reconciliation.engine import classify


class ReconciliationStore:
    def __init__(self) -> None:
        # Insertion order == arrival order
        self._pending: OrderedDict[str, tuple[TradeEvent, datetime]] = OrderedDict()

    def apply(self, event: TradeEvent, arrived_at: datetime) -> Break | None:
        if event.trade_id not in self._pending:
            self._pending[event.trade_id] = (event, arrived_at)
            return None

        other, _ = self._pending.pop(event.trade_id)
        return classify(event, other)

    def sweep_stale(self, now: datetime, timeout_s: float) -> list[Break]:
        breaks: list[Break] = []
        while self._pending:
            trade_id, (event, arrived_at) = next(iter(self._pending.items()))
            if (now - arrived_at).total_seconds() <= timeout_s:
                break
            self._pending.pop(trade_id)
            breaks.append(
                Break(
                    trade_id=trade_id,
                    break_type=BreakType.MISSING_TRADE,
                    missing_side=(
                        EventSource.MARKET if event.source is EventSource.BOOK else EventSource.BOOK
                    ),
                    present_event=event,
                )
            )
        return breaks

    def __len__(self) -> int:
        return len(self._pending)
