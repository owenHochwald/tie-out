"""Tests for rollup/aggregator.py.

The key proof CLAUDE.md calls out explicitly: feed the same set of trades to
two RollupAggregator instances in different orders, assert identical final
state. That's the actual evidence for "survives out-of-order arrival," not
just a claim.
"""

import random
from datetime import datetime, timezone
from decimal import Decimal

from tieout.domain import EventSource, Side, TradeEvent
from tieout.rollup.aggregator import RollupAggregator, SymbolRollup

TS = datetime(2026, 1, 1, tzinfo=timezone.utc)


def make(trade_id: str, symbol: str, quantity, price, source=EventSource.BOOK, **overrides) -> TradeEvent:
    base = dict(
        trade_id=trade_id,
        symbol=symbol,
        quantity=Decimal(quantity),
        price=Decimal(price),
        side=Side.BUY,
        timestamp=TS,
        source=source,
    )
    return TradeEvent(**{**base, **overrides})


def test_single_trade_updates_totals():
    agg = RollupAggregator()
    agg.apply(make("T1", "AAPL", 100, "150.00"))
    rollup = agg.get("AAPL")
    assert rollup.total_quantity == Decimal(100)
    assert rollup.total_notional == Decimal("15000.00")


def test_accumulates_across_multiple_trades_same_symbol():
    agg = RollupAggregator()
    agg.apply(make("T1", "AAPL", 100, "150.00"))
    agg.apply(make("T2", "AAPL", 50, "151.00"))
    rollup = agg.get("AAPL")
    assert rollup.total_quantity == Decimal(150)
    assert rollup.total_notional == Decimal("15000.00") + Decimal("7550.00")


def test_symbols_tracked_independently():
    agg = RollupAggregator()
    agg.apply(make("T1", "AAPL", 100, "150.00"))
    agg.apply(make("T2", "MSFT", 10, "310.00"))
    assert agg.get("AAPL").total_quantity == Decimal(100)
    assert agg.get("MSFT").total_quantity == Decimal(10)
    assert set(agg.all_rollups()) == {"AAPL", "MSFT"}


def test_unknown_symbol_returns_none():
    agg = RollupAggregator()
    assert agg.get("GOOG") is None


def test_duplicate_trade_id_is_not_double_counted():
    """Same trade delivered twice (at-least-once redelivery) must not inflate
    the total — this is the rollup-level idempotency CLAUDE.md calls out."""
    agg = RollupAggregator()
    event = make("T1", "AAPL", 100, "150.00")
    agg.apply(event)
    agg.apply(event)
    agg.apply(event)
    rollup = agg.get("AAPL")
    assert rollup.total_quantity == Decimal(100)
    assert rollup.total_notional == Decimal("15000.00")


def test_book_and_market_sides_of_a_clean_trade_count_once():
    """A trade_id has two occurrences (book + market); a clean trade must
    still only contribute once to the total, not twice."""
    agg = RollupAggregator()
    agg.apply(make("T1", "AAPL", 100, "150.00", source=EventSource.BOOK))
    agg.apply(make("T1", "AAPL", 100, "150.00", source=EventSource.MARKET))
    rollup = agg.get("AAPL")
    assert rollup.total_quantity == Decimal(100)  # not 200
    assert rollup.total_notional == Decimal("15000.00")  # not 30000


def test_decimal_used_throughout_no_float():
    agg = RollupAggregator()
    agg.apply(make("T1", "AAPL", 3, "10.10"))
    rollup = agg.get("AAPL")
    assert isinstance(rollup.total_quantity, Decimal)
    assert isinstance(rollup.total_notional, Decimal)
    # Exact — no float drift possible: 3 * 10.10 must be exactly 30.30.
    assert rollup.total_notional == Decimal("30.30")


def test_order_independence_random_shuffles_converge_to_identical_state():
    trades = [
        make(f"T{i}", random.choice(["AAPL", "MSFT", "GOOG"]), 10 + i, "100.00")
        for i in range(200)
    ]

    rng = random.Random(1)
    order_a = trades[:]
    order_b = trades[:]
    rng.shuffle(order_a)
    rng.shuffle(order_b)
    assert order_a != order_b  # sanity: the shuffles actually differ

    agg_a, agg_b = RollupAggregator(), RollupAggregator()
    for t in order_a:
        agg_a.apply(t)
    for t in order_b:
        agg_b.apply(t)

    a_state = {s: (r.total_quantity, r.total_notional) for s, r in agg_a.all_rollups().items()}
    b_state = {s: (r.total_quantity, r.total_notional) for s, r in agg_b.all_rollups().items()}
    assert a_state == b_state


def test_order_independence_with_duplicates_interspersed():
    """Same property, but with each trade appearing 1-3 times at random
    positions — proving duplicate-safety AND order-independence together, as
    they'd actually co-occur under at-least-once delivery."""
    base_trades = [make(f"T{i}", "AAPL", 10 + i, "100.00") for i in range(50)]

    rng = random.Random(2)

    def build_stream(seed: int) -> list[TradeEvent]:
        r = random.Random(seed)
        stream = []
        for t in base_trades:
            stream.extend([t] * r.randint(1, 3))
        r.shuffle(stream)
        return stream

    agg_a, agg_b = RollupAggregator(), RollupAggregator()
    for t in build_stream(10):
        agg_a.apply(t)
    for t in build_stream(20):
        agg_b.apply(t)

    expected_quantity = sum(t.quantity for t in base_trades)
    assert agg_a.get("AAPL").total_quantity == expected_quantity
    assert agg_b.get("AAPL").total_quantity == expected_quantity


def test_symbol_rollup_apply_directly():
    rollup = SymbolRollup(symbol="AAPL")
    rollup.apply(make("T1", "AAPL", 10, "100.00"))
    rollup.apply(make("T1", "AAPL", 10, "100.00"))  # duplicate, ignored
    assert rollup.total_quantity == Decimal(10)
    assert rollup.trade_ids == {"T1"}
