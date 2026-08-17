"""Synthetic event simulator — see CLAUDE.md "The synthetic event simulator".

Conceptual model: one ground truth, two possibly-divergent derived recordings.
We generate one true trade, then derive what the book side and the market side
each *recorded* — which may diverge because of an injected break.

Pipeline (each stage is a lazy generator consuming the previous one, so nothing
is ever materialized as a full list):

    _emit_true_events  →  _inject_duplicates  →  _reorder  →  caller

Reproducibility invariant: every stage draws randomness from the SAME
`random.Random(seed)` instance, threaded through as a parameter. Never call the
module-level `random.*` functions here, and never create per-stage Random()
instances — either silently breaks `seed`'s whole purpose (rerunning the exact
failing sequence). For the same reason, nothing stateful that affects output
(counters, clock anchors) may live at module scope: it must be created per call.
"""

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Iterator, NamedTuple
import itertools
import random

from tieout.domain import BreakType, EventSource, Side, TradeEvent
from tieout.reconciliation.rules import PRICE_TOLERANCE, QUANTITY_TOLERANCE


class _Truth(NamedTuple):
    """The ground-truth trade, before either side records it.

    Deliberately NOT a TradeEvent: a TradeEvent has a `source` (BOOK/MARKET),
    and the truth is neither — it's what actually happened. `_derive_sides` is
    where real TradeEvents (each with a real source) get constructed from this.
    """

    trade_id: str
    symbol: str
    quantity: Decimal
    price: Decimal
    side: Side
    timestamp: datetime


BASE_PRICES = {
    "AAPL": Decimal("150.00"),
    "MSFT": Decimal("310.00"),
    "GOOG": Decimal("140.00"),
    "AMZN": Decimal("145.00"),
    "TSLA": Decimal("240.00"),
    "SPCX": Decimal("400.00"),
    "META": Decimal("200.00"),
}

SYMBOLS = list(BASE_PRICES.keys())
_SIDES = tuple(Side)

_CENTS = Decimal(100)

PRICE_JITTER_CENTS = 50
# Trades print within ±$0.50 of the symbol's base price. Uniform rather than
# gaussian: easier to reason about the bounds when asserting in tests.

MIN_QUANTITY = 1
MAX_QUANTITY = 1000

# Perturbation magnitudes for injected breaks. Each must exceed the matching
# tolerance in reconciliation/rules.py — if a perturbation landed *within*
# tolerance, classify() would call the trade clean and the observed break rate
# would silently undershoot the configured break_rate.
MIN_PRICE_BREAK_CENTS = 2   # PRICE_TOLERANCE is $0.01, so 2c is the smallest real break
MAX_PRICE_BREAK_CENTS = 50
MIN_QUANTITY_BREAK = 1      # QUANTITY_TOLERANCE is 0, so any whole share qualifies
MAX_QUANTITY_BREAK = 25

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
    """Generate the content of one true trade: symbol, quantity, price, side.

    `trade_id` and `timestamp` are supplied by the caller (_emit_true_events
    owns the counter and the Poisson clock) — this function only invents the
    trade's *content*.
    """
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
    """Derive (book_event, market_event) from one ground truth.

    Two separate dice rolls:
      1. `rng.random() < break_rate` — does a break happen at all?
      2. only if so — which BreakType, by relative weight?

    The market side is the one perturbed, by convention. Which side "got it
    wrong" is arbitrary from the reconciliation engine's point of view: it
    compares the two and reports a disagreement without attributing blame.
    """
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
    """Shuffle within a sliding buffer of `window` events.

    O(window) memory, O(1) amortized per event — the stream is never fully
    materialized. `window <= 1` is exact passthrough (no reordering), matching
    the documented default of `reorder_window=1`.

    What this guarantees, precisely (worth being exact about, since the obvious
    assumption is wrong):

      - the output is an exact permutation of the input — nothing lost, nothing
        invented;
      - an event can be emitted at most `window - 1` positions EARLIER than it
        arrived (it can't jump ahead of a buffer it hasn't entered);
      - lateness is NOT hard-bounded. Each round pops a random buffer index, so
        an event survives a round with probability (window-1)/window — lateness
        is geometrically distributed, mean ≈ window, with a long tail. At
        window=50, displacements past 150 show up routinely.

    That tail is realistic for simulating out-of-order arrival (most events
    near-ordered, occasional stragglers), but it means a test asserting a hard
    displacement bound will fail intermittently. Assert on the permutation
    property instead, or on displacement percentiles.
    """
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
    rate_per_second: float,
    duration_seconds: float,
    break_rate: float,
    break_type_weights: dict[BreakType, float] | None = None,
) -> Iterator[TradeEvent]:
    """Poisson-paced source stage: generate truths, derive sides, flatten.

    Owns the trade_id counter and the clock. Event count is a BYPRODUCT of
    duration × rate, not an input — a rate is an average, so real counts vary
    run to run. That variance is correct, not a bug.

    Inter-arrival gaps are exponential (a Poisson arrival process) rather than a
    fixed 1/rate interval: real trade prints are bursty and clustered, and
    uniform spacing wouldn't stress backlog or reordering realistically.

    No sleeping here: this runs as fast as Python allows and produces
    well-timestamped objects. Pacing emission against wall-clock time belongs in
    loadtest/, wrapping this generator.
    """
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
            SYMBOLS,
            BASE_PRICES,
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
    seed: int | None = None,
) -> Iterator[TradeEvent]:
    rng = random.Random(seed)

    stream = _emit_true_events(
        rng, rate_per_second, duration_seconds, break_rate, break_type_weights
    )
    stream = _inject_duplicates(stream, rng, duplicate_rate)
    yield from _reorder(stream, rng, reorder_window)
