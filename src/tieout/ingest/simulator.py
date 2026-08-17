from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Iterator, NamedTuple
import itertools
import random
import string

from tieout.domain import BreakType, EventSource, Side, TradeEvent
from tieout.reconciliation.rules import PRICE_TOLERANCE, QUANTITY_TOLERANCE


class _Truth(NamedTuple):

    trade_id: str
    symbol: str
    quantity: Decimal
    price: Decimal
    side: Side
    timestamp: datetime


_SIDES = tuple(Side)

_CENTS = Decimal(100)

PRICE_JITTER_CENTS = 50
MIN_QUANTITY = 1
MAX_QUANTITY = 1000

MIN_PRICE_BREAK_CENTS = 2   # PRICE_TOLERANCE is $0.01, so 2c is the smallest real break
MAX_PRICE_BREAK_CENTS = 50
MIN_QUANTITY_BREAK = 1      # QUANTITY_TOLERANCE is 0, so any whole share qualifies
MAX_QUANTITY_BREAK = 25

TICKER_LENGTH = 4            # standard US-equity-style ticker length
DEFAULT_NUM_SYMBOLS = 7      # parity with the old hand-picked universe's size
MIN_BASE_PRICE = Decimal("10.00")    # floor — keeps a fixed-cents jitter (PRICE_JITTER_CENTS)
                                      # from dominating the price of a near-worthless symbol
MAX_BASE_PRICE = Decimal("500.00")   # ceiling — realistic large-cap-equity band

_DEFAULT_BREAK_TYPE_WEIGHTS: dict[BreakType, float] = {
    BreakType.MISSING_TRADE: 1.0,
    BreakType.QUANTITY_MISMATCH: 1.0,
    BreakType.PRICE_MISMATCH: 1.0,
}


def _generate_true_trade(
    rng: random.Random,
    symbols: list[str],
    base_prices: dict[str, Decimal],
    trade_id: str,
    timestamp: datetime,
) -> _Truth:
    symbol = rng.choice(symbols)

    # Integer cents throughout, then one exact division. Never str() or float:
    # Decimal(int) / Decimal(100) is exact, and skipping the string round-trip
    # is the faster path at volume.
    base_cents = int(base_prices[symbol] * _CENTS)
    jitter = rng.randint(-PRICE_JITTER_CENTS, PRICE_JITTER_CENTS)
    price = Decimal(base_cents + jitter) / _CENTS

    return _Truth(
        trade_id=trade_id,
        symbol=symbol,
        quantity=Decimal(rng.randint(MIN_QUANTITY, MAX_QUANTITY)),
        price=price,
        side=rng.choice(_SIDES),
        timestamp=timestamp,
    )


def _generate_ticker(rng: random.Random) -> str:
    return "".join(rng.choices(string.ascii_uppercase, k=TICKER_LENGTH))


def _generate_symbol_universe(
    rng: random.Random,
    num_symbols: int,
    min_price: Decimal,
    max_price: Decimal,
) -> dict[str, Decimal]:
    if num_symbols < 1:
        raise ValueError("num_symbols must be >= 1")
    if min_price <= 0 or max_price < min_price:
        raise ValueError("require 0 < min_price <= max_price")

    # Integer cents throughout, one exact division — same reasoning as the price
    # jitter in _generate_true_trade: skip the float/string round-trip.
    min_cents = int(min_price * _CENTS)
    max_cents = int(max_price * _CENTS)

    universe: dict[str, Decimal] = {}
    while len(universe) < num_symbols:
        ticker = _generate_ticker(rng)
        if ticker in universe:
            continue  # collision — retry; 26**4 possible tickers makes this rare
        universe[ticker] = Decimal(rng.randint(min_cents, max_cents)) / _CENTS
    return universe


def _perturb_price(price: Decimal, rng: random.Random) -> Decimal:
    """Shift a price by more than PRICE_TOLERANCE, in either direction."""
    delta_cents = rng.randint(MIN_PRICE_BREAK_CENTS, MAX_PRICE_BREAK_CENTS)
    if rng.random() < 0.5:
        delta_cents = -delta_cents
    perturbed = price + (Decimal(delta_cents) / _CENTS)
    # Never produce a non-positive price — that's a corrupt event, not a break.
    if perturbed <= 0:
        perturbed = price + (Decimal(abs(delta_cents)) / _CENTS)
    assert abs(perturbed - price) > PRICE_TOLERANCE
    return perturbed


def _perturb_quantity(quantity: Decimal, rng: random.Random) -> Decimal:
    """Shift a quantity by more than QUANTITY_TOLERANCE, in either direction."""
    delta = rng.randint(MIN_QUANTITY_BREAK, MAX_QUANTITY_BREAK)
    if rng.random() < 0.5 and quantity - delta >= MIN_QUANTITY:
        delta = -delta
    perturbed = quantity + Decimal(delta)
    assert abs(perturbed - quantity) > QUANTITY_TOLERANCE
    return perturbed


def _derive_sides(
    truth: _Truth,
    rng: random.Random,
    break_rate: float,
    break_type_weights: dict[BreakType, float] | None = None,
) -> tuple[TradeEvent | None, TradeEvent | None]:
    fields = truth._asdict()
    book = TradeEvent(**fields, source=EventSource.BOOK)
    market = TradeEvent(**fields, source=EventSource.MARKET)

    if rng.random() >= break_rate:
        return book, market   # clean match — identical except `source`

    weights = break_type_weights or _DEFAULT_BREAK_TYPE_WEIGHTS
    population = list(weights)
    break_type = rng.choices(population, weights=[weights[k] for k in population])[0]

    if break_type is BreakType.MISSING_TRADE:
        # One side never recorded the trade. 50/50 which side goes missing:
        # "we booked a trade the market never saw" and "the market saw a trade
        # we never booked" are equally plausible failure modes.
        if rng.random() < 0.5:
            return None, market
        return book, None

    if break_type is BreakType.QUANTITY_MISMATCH:
        fields["quantity"] = _perturb_quantity(truth.quantity, rng)
    elif break_type is BreakType.PRICE_MISMATCH:
        fields["price"] = _perturb_price(truth.price, rng)
    else:
        raise ValueError(f"unhandled BreakType in simulator: {break_type}")

    return book, TradeEvent(**fields, source=EventSource.MARKET)


def _inject_duplicates(
    events: Iterator[TradeEvent],
    rng: random.Random,
    duplicate_rate: float,
) -> Iterator[TradeEvent]:
    """Re-emit some events a second time, simulating at-least-once redelivery.

    Runs BEFORE _reorder on purpose: the shuffle downstream then scatters each
    duplicate away from its original, so "the duplicate arrives much later"
    falls out of pipeline composition without separate delay logic.
    """
    if duplicate_rate <= 0:
        yield from events
        return

    for event in events:
        yield event
        if rng.random() < duplicate_rate:
            yield event   # same object — a true duplicate, same trade_id


def _reorder(
    events: Iterator[TradeEvent],
    rng: random.Random,
    window: int,
) -> Iterator[TradeEvent]:
    if window <= 1:
        yield from events
        return

    buf: list[TradeEvent] = []
    for event in events:
        buf.append(event)
        if len(buf) >= window:
            yield buf.pop(rng.randrange(len(buf)))

    # Drain whatever is still buffered when the input runs dry, or those events
    # would be silently dropped.
    rng.shuffle(buf)
    yield from buf


def _emit_true_events(
    rng: random.Random,
    symbols: list[str],
    base_prices: dict[str, Decimal],
    rate_per_second: float,
    duration_seconds: float,
    break_rate: float,
    break_type_weights: dict[BreakType, float] | None = None,
) -> Iterator[TradeEvent]:
    ids = itertools.count(start=1)
    # Anchored ONCE, not read per-event: wall-clock reads would make timestamps
    # differ between runs even with an identical seed.
    start = datetime.now(timezone.utc)
    end = start + timedelta(seconds=duration_seconds)

    t = start
    while t < end:
        t += timedelta(seconds=rng.expovariate(rate_per_second))
        if t >= end:
            break

        truth = _generate_true_trade(
            rng,
            symbols,
            base_prices,
            trade_id=f"T{next(ids):012d}",
            timestamp=t,
        )
        book, market = _derive_sides(truth, rng, break_rate, break_type_weights)
        if book is not None:
            yield book
        if market is not None:
            yield market


def generate_events(
    rate_per_second: float,
    duration_seconds: float,
    break_rate: float = 0.01,
    break_type_weights: dict[BreakType, float] | None = None,
    duplicate_rate: float = 0.0,
    reorder_window: int = 1,
    num_symbols: int = DEFAULT_NUM_SYMBOLS,
    min_base_price: Decimal = MIN_BASE_PRICE,
    max_base_price: Decimal = MAX_BASE_PRICE,
    seed: int | None = None,
) -> Iterator[TradeEvent]:
    root = random.Random(seed)
    symbol_rng = random.Random(root.getrandbits(64))
    emit_rng = random.Random(root.getrandbits(64))
    duplicate_rng = random.Random(root.getrandbits(64))
    reorder_rng = random.Random(root.getrandbits(64))

    base_prices = _generate_symbol_universe(symbol_rng, num_symbols, min_base_price, max_base_price)
    symbols = list(base_prices)

    stream = _emit_true_events(
        emit_rng, symbols, base_prices, rate_per_second, duration_seconds, break_rate, break_type_weights
    )
    stream = _inject_duplicates(stream, duplicate_rng, duplicate_rate)
    yield from _reorder(stream, reorder_rng, reorder_window)
