"""Tests for reconciliation/engine.py's classify()."""

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from tieout.domain import BreakType, EventSource, Side, TradeEvent
from tieout.reconciliation.engine import ReconciliationInvariantError, classify
from tieout.reconciliation.rules import PRICE_TOLERANCE, QUANTITY_TOLERANCE

TS = datetime(2026, 1, 1, tzinfo=timezone.utc)


def make(source: EventSource, **overrides) -> TradeEvent:
    base = dict(
        trade_id="T1",
        symbol="AAPL",
        quantity=Decimal(100),
        price=Decimal("150.00"),
        side=Side.BUY,
        timestamp=TS,
        source=source,
    )
    return TradeEvent(**{**base, **overrides})


def test_clean_match_returns_none():
    book = make(EventSource.BOOK)
    market = make(EventSource.MARKET)
    assert classify(book, market) is None
    assert classify(market, book) is None  # order must not matter


def test_quantity_mismatch_beyond_tolerance():
    book = make(EventSource.BOOK, quantity=Decimal(100))
    market = make(EventSource.MARKET, quantity=Decimal(95))
    result = classify(book, market)
    assert result.break_type is BreakType.QUANTITY_MISMATCH
    assert result.trade_id == "T1"
    assert result.book_trade is book
    assert result.market_trade is market


def test_price_mismatch_beyond_tolerance():
    book = make(EventSource.BOOK, price=Decimal("150.00"))
    market = make(EventSource.MARKET, price=Decimal("150.05"))
    result = classify(book, market)
    assert result.break_type is BreakType.PRICE_MISMATCH


def test_quantity_wins_over_price_when_both_mismatch():
    """Rules doc §2: no precedence that hides a dimension — both values are
    carried on the Break regardless — but the LABEL follows the more severe
    dimension when both are wrong."""
    book = make(EventSource.BOOK, quantity=Decimal(100), price=Decimal("150.00"))
    market = make(EventSource.MARKET, quantity=Decimal(90), price=Decimal("151.00"))
    result = classify(book, market)
    assert result.break_type is BreakType.QUANTITY_MISMATCH
    # Both sides' actual values are still fully present as data.
    assert result.book_trade.price == Decimal("150.00")
    assert result.market_trade.price == Decimal("151.00")


@pytest.mark.parametrize("delta", [Decimal("0.001"), Decimal("0.005"), Decimal("0.009")])
def test_price_within_tolerance_is_not_a_break(delta):
    book = make(EventSource.BOOK, price=Decimal("150.00"))
    market = make(EventSource.MARKET, price=Decimal("150.00") + delta)
    assert abs(delta) <= PRICE_TOLERANCE
    assert classify(book, market) is None


def test_price_exactly_at_tolerance_boundary_is_not_a_break():
    book = make(EventSource.BOOK, price=Decimal("150.00"))
    market = make(EventSource.MARKET, price=Decimal("150.00") + PRICE_TOLERANCE)
    assert classify(book, market) is None  # strictly greater-than, not >=


def test_quantity_tolerance_is_zero_any_difference_is_a_break():
    assert QUANTITY_TOLERANCE == Decimal("0")
    book = make(EventSource.BOOK, quantity=Decimal(100))
    market = make(EventSource.MARKET, quantity=Decimal("100.0001"))
    result = classify(book, market)
    assert result.break_type is BreakType.QUANTITY_MISMATCH


def test_mismatched_trade_id_raises_value_error():
    book = make(EventSource.BOOK, trade_id="T1")
    market = make(EventSource.MARKET, trade_id="T2")
    with pytest.raises(ValueError):
        classify(book, market)


def test_same_source_twice_raises_value_error():
    a = make(EventSource.MARKET)
    b = make(EventSource.MARKET)
    with pytest.raises(ValueError):
        classify(a, b)


def test_mismatched_symbol_raises_invariant_error_not_a_break():
    book = make(EventSource.BOOK, symbol="AAPL")
    market = make(EventSource.MARKET, symbol="MSFT")
    with pytest.raises(ReconciliationInvariantError):
        classify(book, market)


def test_mismatched_side_raises_invariant_error_not_a_break():
    book = make(EventSource.BOOK, side=Side.BUY)
    market = make(EventSource.MARKET, side=Side.SELL)
    with pytest.raises(ReconciliationInvariantError):
        classify(book, market)


def test_timestamp_difference_alone_is_never_a_break():
    """Timing is operational-only (sweep_stale's job), never a comparison
    dimension — rules doc §3."""
    from datetime import timedelta

    book = make(EventSource.BOOK, timestamp=TS)
    market = make(EventSource.MARKET, timestamp=TS + timedelta(hours=1))
    assert classify(book, market) is None
