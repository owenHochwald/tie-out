from tieout.domain import Break, BreakType, EventSource, TradeEvent
from tieout.reconciliation.rules import PRICE_TOLERANCE, QUANTITY_TOLERANCE


class ReconciliationInvariantError(Exception):
    pass


def classify(a: TradeEvent, b: TradeEvent) -> Break | None:
    """Compare the two sides of one trade_id. Argument order doesn't matter."""
    if a.trade_id != b.trade_id:
        raise ValueError(
            f"classify() requires both events to share a trade_id, got "
            f"{a.trade_id!r} and {b.trade_id!r}"
        )
    if a.source is b.source:
        raise ValueError(
            f"classify() requires one BOOK and one MARKET event, got two "
            f"{a.source.name} events for trade_id {a.trade_id!r}"
        )

    if a.symbol != b.symbol or a.side != b.side:
        raise ReconciliationInvariantError(
            f"trade_id {a.trade_id!r} matched across mismatched symbol/side "
            f"({a.symbol!r}/{a.side!r} vs {b.symbol!r}/{b.side!r}) — this "
            f"indicates data corruption, not a reconciliation break"
        )

    book, market = (a, b) if a.source is EventSource.BOOK else (b, a)

    qty_mismatch = abs(book.quantity - market.quantity) > QUANTITY_TOLERANCE
    price_mismatch = abs(book.price - market.price) > PRICE_TOLERANCE

    if not qty_mismatch and not price_mismatch:
        return None

    # quantity outranks price
    break_type = BreakType.QUANTITY_MISMATCH if qty_mismatch else BreakType.PRICE_MISMATCH
    return Break(
        trade_id=a.trade_id,
        break_type=break_type,
        book_trade=book,
        market_trade=market,
    )
