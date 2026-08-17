from enum import Enum, auto
from dataclasses import dataclass
from tieout.domain.events import EventSource, TradeEvent

class BreakType(Enum):
    MISSING_TRADE = auto()
    QUANTITY_MISMATCH = auto()
    PRICE_MISMATCH = auto()

@dataclass(frozen=True, slots=True)
class Break:
    trade_id: str
    break_type: BreakType
    # missing trade
    missing_side: EventSource | None = None
    present_event: TradeEvent | None = None

    # field mismatch
    book_trade: TradeEvent | None = None
    market_trade: TradeEvent | None = None

