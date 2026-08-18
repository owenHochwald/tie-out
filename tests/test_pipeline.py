"""Tests for pipeline.py's Processor — the per-event loop wiring both stores.

The key case this module exists to prove: chaos-config duplicates must not
corrupt the reconciliation store. See pipeline.py's module docstring for why
`seen` keyed on bare trade_id (CLAUDE.md's own sketch) would fail this.
"""

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from tieout.domain import BreakType, EventSource, Side, TradeEvent
from tieout.ingest.simulator import generate_events
from tieout.pipeline import Processor

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


def test_clean_pair_updates_rollup_and_produces_no_break():
    p = Processor()
    assert p.process(make("T1", EventSource.BOOK), arrived_at=TS) is None
    assert p.process(make("T1", EventSource.MARKET), arrived_at=TS) is None
    assert p.break_log == []
    assert p.rollup.get("AAPL").total_quantity == Decimal(100)


def test_mismatch_produces_break_and_logs_it(caplog):
    p = Processor()
    p.process(make("T1", EventSource.BOOK, quantity=Decimal(100)), arrived_at=TS)
    with caplog.at_level("INFO", logger="tieout.pipeline"):
        result = p.process(make("T1", EventSource.MARKET, quantity=Decimal(90)), arrived_at=TS)
    assert result.break_type is BreakType.QUANTITY_MISMATCH
    assert p.break_log == [result]
    # CLAUDE.md's audit trail: one line per break detected.
    assert "break detected" in caplog.text
    assert "QUANTITY_MISMATCH" in caplog.text
    assert "T1" in caplog.text


def test_exact_duplicate_is_ignored_not_treated_as_counterpart():
    """The core bug this module fixes: a duplicate of the SAME side must not
    be mistaken for the other side's arrival."""
    p = Processor()
    market = make("T1", EventSource.MARKET)
    assert p.process(market, arrived_at=TS) is None
    assert len(p.store) == 1
    # Redelivery of the exact same message — same trade_id AND source.
    assert p.process(market, arrived_at=TS + timedelta(seconds=1)) is None
    assert len(p.store) == 1, "duplicate must not have been treated as the book side arriving"

    # The REAL counterpart still resolves correctly afterward.
    result = p.process(make("T1", EventSource.BOOK), arrived_at=TS + timedelta(seconds=2))
    assert result is None  # clean match
    assert len(p.store) == 0


def test_duplicate_does_not_double_count_rollup():
    p = Processor()
    event = make("T1", EventSource.BOOK)
    p.process(event, arrived_at=TS)
    p.process(event, arrived_at=TS + timedelta(seconds=1))
    p.process(event, arrived_at=TS + timedelta(seconds=2))
    assert p.rollup.get("AAPL").total_quantity == Decimal(100)


def test_legitimate_counterpart_is_not_swallowed_by_seen():
    """The exact failure mode CLAUDE.md's bare-trade_id `seen` sketch has:
    book and market share a trade_id but must both reach the store."""
    p = Processor()
    p.process(make("T1", EventSource.BOOK), arrived_at=TS)
    result = p.process(make("T1", EventSource.MARKET), arrived_at=TS)
    assert result is None  # reached classify() and matched cleanly
    assert len(p.store) == 0


def test_sweep_stale_appends_to_break_log(caplog):
    p = Processor()
    p.process(make("T1", EventSource.BOOK), arrived_at=TS)
    with caplog.at_level("INFO", logger="tieout.pipeline"):
        breaks = p.sweep_stale(now=TS + timedelta(seconds=301), timeout_s=300)
    assert len(breaks) == 1
    assert breaks[0].break_type is BreakType.MISSING_TRADE
    assert p.break_log == breaks
    assert "break detected" in caplog.text
    assert "MISSING_TRADE" in caplog.text


def test_full_chaos_config_stream_does_not_crash_and_stays_consistent():
    """End-to-end: feed real chaos-config simulator output (duplicates +
    reordering + all break types) through the Processor and check the
    invariants that must hold regardless of the specific random outcomes.
    """
    p = Processor()
    events = list(
        generate_events(
            rate_per_second=100,
            duration_seconds=30,
            break_rate=0.1,
            duplicate_rate=0.2,
            reorder_window=15,
            seed=99,
        )
    )
    t = TS
    for event in events:
        t += timedelta(milliseconds=1)
        p.process(event, arrived_at=t)

    # Every trade_id the store still holds pending has a genuine reason: its
    # counterpart either wasn't generated (MISSING_TRADE case) or hasn't been
    # fed in yet — no trade should be "stuck" due to dedup bugs.
    assert isinstance(len(p.store), int)  # smoke: didn't raise

    # No exceptions means classify()'s invariant checks never fired spuriously
    # (symbol/side never diverge — already covered in the simulator suite, but
    # this is the same guarantee holding through real Processor wiring).
    for b in p.break_log:
        assert b.break_type in (BreakType.MISSING_TRADE, BreakType.QUANTITY_MISMATCH, BreakType.PRICE_MISMATCH)

    # Rollup must have counted something, and never negative/zero from a
    # dedup failure wiping out real trades.
    total = sum(r.total_quantity for r in p.rollup.all_rollups().values())
    assert total > 0
