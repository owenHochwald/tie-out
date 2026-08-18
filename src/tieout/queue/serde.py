from decimal import Decimal

from tieout.domain import EventSource, Side, TradeEvent


def serialize_event(event: TradeEvent) -> dict[str, str]:
    return {
        "trade_id": event.trade_id,
        "symbol": event.symbol,
        "quantity": str(event.quantity),
        "price": str(event.price),
        "side": event.side.name,
        "timestamp": event.timestamp.isoformat(),
        "source": event.source.name,
    }


def deserialize_event(fields: dict[str, str]) -> TradeEvent:
    from datetime import datetime

    return TradeEvent(
        trade_id=fields["trade_id"],
        symbol=fields["symbol"],
        quantity=Decimal(fields["quantity"]),
        price=Decimal(fields["price"]),
        side=Side[fields["side"]],
        timestamp=datetime.fromisoformat(fields["timestamp"]),
        source=EventSource[fields["source"]],
    )
