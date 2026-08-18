import logging
from dataclasses import dataclass, field
from datetime import datetime

from tieout.domain import Break, EventSource, TradeEvent
from tieout.reconciliation.store import ReconciliationStore
from tieout.rollup.aggregator import RollupAggregator

logger = logging.getLogger(__name__)


def _break_symbol(b: Break) -> str:
    event = b.present_event or b.book_trade or b.market_trade
    assert event is not None, "every Break variant carries at least one TradeEvent"
    return event.symbol


def _log_break(b: Break) -> None:
    # One line per break detected — the audit trail CLAUDE.md's Reporting
    # section asks for, logged at the single chokepoint both apply() matches
    # and sweep_stale() timeouts pass through (break_log.append).
    logger.info("break detected: trade_id=%s type=%s symbol=%s", b.trade_id, b.break_type.name, _break_symbol(b))


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
            _log_break(maybe_break)
        return maybe_break

    def sweep_stale(self, now: datetime, timeout_s: float) -> list[Break]:
        breaks = self.store.sweep_stale(now, timeout_s)
        self.break_log.extend(breaks)
        for b in breaks:
            _log_break(b)
        return breaks
