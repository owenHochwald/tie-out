from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum, auto


class Side(Enum):
    BUY = auto()
    SELL = auto()


class EventSource(Enum):
    BOOK = auto()
    MARKET = auto()

@dataclass(frozen=True, slots=True)
class TradeEvent:
    trade_id: str
    symbol: str
    quantity: Decimal
    price: Decimal
    side: Side
    timestamp: datetime
    source: EventSource