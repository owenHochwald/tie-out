from enum import Enum, auto
from dataclasses import dataclass
from tieout.domain.events import EventSource, TradeEvent

class BreakType(Enum):
    MISSING_TRADE = auto()
    QUANTITIY_MISMATCH = auto()
    PRICE_MISMATCH = auto()

@dataclass
class Break:
    break_type: BreakType
    # missing trade
    missing_side: EventSource | None = None
    present_even: TradeEvent | None = None

    # field mismatch
    book_trade: TradeEvent | None = None
    market_trade: TradeEvent | None = None 

