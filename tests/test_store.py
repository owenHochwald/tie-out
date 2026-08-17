"""Tests for reconciliation/store.py's ReconciliationStore.

Covers: half-open matching (apply/apply), arrival-time staleness (not
event.timestamp — see store.py's module docstring for why that distinction is
load-bearing, not stylistic), and sweep_stale's early-exit correctness.
"""

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from tieout.domain import BreakType, EventSource, Side, TradeEvent
from tieout.reconciliation.engine import ReconciliationInvariantError
from tieout.reconciliation.store import ReconciliationStore

TS = datetime(2026, 1, 1, tzinfo=timezone.utc)


def make(trade_id: str, source: EventSource, **overrides) -> TradeEvent:
    base = dict(
        trade_id=trade_id,
        symbol="AAPL",
        quantity=Decimal(100),
        price=Decimal("150.00"),
        side=Side.BUY,
        timestamp=TS,
        source=source,
    )
    return TradeEvent(**{**base, **overrides})


def test_first_side_returns_none_and_is_pending():
    store = ReconciliationStore()
    result = store.apply(make("T1", EventSource.BOOK), arrived_at=TS)
    assert result is None
    assert len(store) == 1


def test_second_side_matches_and_clears_pending():
    store = ReconciliationStore()
    store.apply(make("T1", EventSource.BOOK), arrived_at=TS)
    result = store.apply(make("T1", EventSource.MARKET), arrived_at=TS)
    assert result is None  # clean match
    assert len(store) == 0


def test_second_side_with_mismatch_returns_break():
    store = ReconciliationStore()
    store.apply(make("T1", EventSource.BOOK, quantity=Decimal(100)), arrived_at=TS)
    result = store.apply(make("T1", EventSource.MARKET, quantity=Decimal(90)), arrived_at=TS)
    assert result is not None
    assert result.break_type is BreakType.QUANTITY_MISMATCH
    assert len(store) == 0  # resolved either way — matched, not left pending


def test_order_of_arrival_does_not_matter():
    a = ReconciliationStore()
    a.apply(make("T1", EventSource.BOOK), arrived_at=TS)
    r1 = a.apply(make("T1", EventSource.MARKET), arrived_at=TS)

    b = ReconciliationStore()
    b.apply(make("T1", EventSource.MARKET), arrived_at=TS)
    r2 = b.apply(make("T1", EventSource.BOOK), arrived_at=TS)

    assert r1 is None and r2 is None


def test_invariant_violation_propagates_and_still_clears_pending():
    store = ReconciliationStore()
    store.apply(make("T1", EventSource.BOOK, symbol="AAPL"), arrived_at=TS)
    with pytest.raises(ReconciliationInvariantError):
        store.apply(make("T1", EventSource.MARKET, symbol="MSFT"), arrived_at=TS)
    # The bad pair was popped before classify() raised — it shouldn't be
    # sitting in _pending waiting to be re-matched.
    assert len(store) == 0


def test_independent_trade_ids_do_not_interfere():
    store = ReconciliationStore()
    store.apply(make("T1", EventSource.BOOK), arrived_at=TS)
    store.apply(make("T2", EventSource.BOOK), arrived_at=TS)
    assert len(store) == 2
    store.apply(make("T1", EventSource.MARKET), arrived_at=TS)
    assert len(store) == 1


# --------------------------------------------------------------------------
# sweep_stale
# --------------------------------------------------------------------------


def test_sweep_stale_ignores_fresh_entries():
    store = ReconciliationStore()
    store.apply(make("T1", EventSource.BOOK), arrived_at=TS)
    breaks = store.sweep_stale(now=TS + timedelta(seconds=10), timeout_s=300)
    assert breaks == []
    assert len(store) == 1


def test_sweep_stale_declares_missing_trade_past_timeout():
    store = ReconciliationStore()
    store.apply(make("T1", EventSource.BOOK), arrived_at=TS)
    breaks = store.sweep_stale(now=TS + timedelta(seconds=301), timeout_s=300)
    assert len(breaks) == 1
    assert breaks[0].break_type is BreakType.MISSING_TRADE
    assert breaks[0].trade_id == "T1"
    assert breaks[0].missing_side is EventSource.MARKET  # book was present
    assert breaks[0].present_event.source is EventSource.BOOK
    assert len(store) == 0


def test_sweep_stale_records_correct_missing_side_for_market_only():
    store = ReconciliationStore()
    store.apply(make("T1", EventSource.MARKET), arrived_at=TS)
    breaks = store.sweep_stale(now=TS + timedelta(seconds=301), timeout_s=300)
    assert breaks[0].missing_side is EventSource.BOOK


def test_sweep_stale_exactly_at_boundary_is_not_stale():
    """Strictly greater-than the timeout, matching classify()'s tolerance
    convention (rules doc: exactly-at-boundary is not a break)."""
    store = ReconciliationStore()
    store.apply(make("T1", EventSource.BOOK), arrived_at=TS)
    breaks = store.sweep_stale(now=TS + timedelta(seconds=300), timeout_s=300)
    assert breaks == []


def test_sweep_stale_uses_arrival_time_not_event_timestamp():
    """The load-bearing property: an event whose own `timestamp` is already
    old when it arrives must NOT be immediately declared stale — the clock
    starts at arrival, not at the event's origin. This is what distinguishes
    "genuinely missing" from "delayed by upstream backlog."
    """
    store = ReconciliationStore()
    old_event_timestamp = TS - timedelta(seconds=10_000)  # ancient by event.timestamp
    event = make("T1", EventSource.BOOK, timestamp=old_event_timestamp)
    store.apply(event, arrived_at=TS)  # but it just arrived NOW

    breaks = store.sweep_stale(now=TS + timedelta(seconds=10), timeout_s=300)
    assert breaks == [], "arrival is recent — must not be stale despite an old event.timestamp"


def test_sweep_stale_processes_multiple_stale_entries_in_arrival_order():
    store = ReconciliationStore()
    store.apply(make("T1", EventSource.BOOK), arrived_at=TS)
    store.apply(make("T2", EventSource.BOOK), arrived_at=TS + timedelta(seconds=1))
    store.apply(make("T3", EventSource.BOOK), arrived_at=TS + timedelta(seconds=500))  # still fresh

    breaks = store.sweep_stale(now=TS + timedelta(seconds=400), timeout_s=300)
    assert [b.trade_id for b in breaks] == ["T1", "T2"]
    assert len(store) == 1  # T3 remains, still within its own timeout


def test_sweep_stale_early_exit_is_sound_regardless_of_middle_pops():
    """apply()-driven pops from the middle of _pending must not corrupt the
    front-to-back arrival ordering sweep_stale relies on."""
    store = ReconciliationStore()
    store.apply(make("T1", EventSource.BOOK), arrived_at=TS)
    store.apply(make("T2", EventSource.BOOK), arrived_at=TS + timedelta(seconds=1))
    store.apply(make("T3", EventSource.BOOK), arrived_at=TS + timedelta(seconds=2))

    # Resolve T2 (the middle entry) via its counterpart, out of arrival order.
    store.apply(make("T2", EventSource.MARKET), arrived_at=TS + timedelta(seconds=3))
    assert len(store) == 2  # T1, T3 remain

    breaks = store.sweep_stale(now=TS + timedelta(seconds=400), timeout_s=300)
    assert {b.trade_id for b in breaks} == {"T1", "T3"}


def test_sweep_stale_empty_store_returns_empty_list():
    store = ReconciliationStore()
    assert store.sweep_stale(now=TS, timeout_s=300) == []
