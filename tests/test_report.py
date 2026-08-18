from datetime import datetime, timezone
from decimal import Decimal

from tieout.domain import BreakType, EventSource, Side, TradeEvent, Break
from tieout.reporting.report import break_report, position_report
from tieout.rollup.aggregator import RollupAggregator

TS = datetime(2026, 1, 1, tzinfo=timezone.utc)


def make(trade_id, symbol, source=EventSource.BOOK) -> TradeEvent:
    return TradeEvent(
        trade_id=trade_id, symbol=symbol, quantity=Decimal(100), price=Decimal("150.00"),
        side=Side.BUY, timestamp=TS, source=source,
    )


def test_break_report_empty():
    df = break_report([])
    assert df.empty
    assert list(df.columns) == ["trade_id", "break_type", "symbol"]


def test_break_report_sorted_by_severity():
    breaks = [
        Break(trade_id="T1", break_type=BreakType.PRICE_MISMATCH, book_trade=make("T1", "AAPL"), market_trade=make("T1", "AAPL", EventSource.MARKET)),
        Break(trade_id="T2", break_type=BreakType.MISSING_TRADE, present_event=make("T2", "MSFT")),
        Break(trade_id="T3", break_type=BreakType.QUANTITY_MISMATCH, book_trade=make("T3", "GOOG"), market_trade=make("T3", "GOOG", EventSource.MARKET)),
    ]
    df = break_report(breaks)
    assert list(df["break_type"]) == ["MISSING_TRADE", "QUANTITY_MISMATCH", "PRICE_MISMATCH"]
    assert list(df["symbol"]) == ["MSFT", "GOOG", "AAPL"]


def test_position_report_reflects_rollup():
    rollup = RollupAggregator()
    rollup.apply(make("T1", "AAPL"))
    rollup.apply(make("T2", "MSFT"))
    df = position_report(rollup)
    assert list(df["symbol"]) == ["AAPL", "MSFT"]
    assert df.loc[df["symbol"] == "AAPL", "total_quantity"].iloc[0] == Decimal(100)
