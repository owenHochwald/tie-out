from dataclasses import dataclass, field
from datetime import datetime

from tieout.domain import Break, EventSource, TradeEvent
from tieout.reconciliation.store import ReconciliationStore
from tieout.rollup.aggregator import RollupAggregator


@dataclass
class Processor:
    store: ReconciliationStore = field(default_factory=ReconciliationStore)
    rollup: RollupAggregator = field(default_factory=RollupAggregator)
    break_log: list[Break] = field(default_factory=list)
    seen: set[tuple[str, EventSource]] = field(default_factory=set)

    def process(self, event: TradeEvent, arrived_at: datetime) -> Break | None:
        key = (event.trade_id, event.source)
        if key in self.seen:
            return None
        self.seen.add(key)

        maybe_break = self.store.apply(event, arrived_at)
        self.rollup.apply(event)

        if maybe_break:
            self.break_log.append(maybe_break)
        return maybe_break

    def sweep_stale(self, now: datetime, timeout_s: float) -> list[Break]:
        breaks = self.store.sweep_stale(now, timeout_s)
        self.break_log.extend(breaks)
        return breaks
