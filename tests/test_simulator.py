"""Tests for the synthetic event simulator.

The simulator is what every downstream component is tested against, so a silent
bug here would invalidate the reconciliation and rollup test suites too. The
properties worth pinning down are:

  - reproducibility (seed actually works — CLAUDE.md's "rerun the exact failing
    sequence")
  - the clean config really is clean (no injected divergence)
  - injected breaks land OUTSIDE the tolerances in reconciliation/rules.py, so
    classify() will actually flag them
  - the chaos knobs (duplicates, reordering) do what they claim without losing
    or inventing events
"""

import random
from collections import Counter
from decimal import Decimal

import pytest

from tieout.domain import BreakType, EventSource, Side, TradeEvent
from tieout.ingest.simulator import (
    BASE_PRICES,
    MAX_QUANTITY,
    MIN_QUANTITY,
    PRICE_JITTER_CENTS,
    SYMBOLS,
    _Truth,
    _derive_sides,
    _generate_true_trade,
    _inject_duplicates,
    _reorder,
    generate_events,
)
from tieout.reconciliation.rules import PRICE_TOLERANCE, QUANTITY_TOLERANCE

_CENTS = Decimal(100)


def group_by_trade_id(events) -> dict[str, list[TradeEvent]]:
    grouped: dict[str, list[TradeEvent]] = {}
    for event in events:
        grouped.setdefault(event.trade_id, []).append(event)
    return grouped


def identity(events) -> list[tuple]:
    """Comparable projection of a stream — TradeEvent equality plus ordering."""
    return [
        (e.trade_id, e.source, e.symbol, e.quantity, e.price, e.side, e.timestamp)
        for e in events
    ]


# --------------------------------------------------------------------------
# Reproducibility — the whole point of the `seed` parameter
# --------------------------------------------------------------------------


def test_same_seed_produces_identical_stream():
    kwargs = dict(
        rate_per_second=50,
        duration_seconds=5,
        break_rate=0.1,
        duplicate_rate=0.1,
        reorder_window=10,
        seed=42,
    )
    first = list(generate_events(**kwargs))
    second = list(generate_events(**kwargs))

    assert first, "simulator produced no events"
    # Timestamps are anchored per call to datetime.now(), so they legitimately
    # differ between runs; everything else must match exactly.
    strip_ts = lambda events: [t[:-1] for t in identity(events)]
    assert strip_ts(first) == strip_ts(second)


def test_different_seeds_produce_different_streams():
    kwargs = dict(rate_per_second=50, duration_seconds=5, break_rate=0.1, duplicate_rate=0.1, reorder_window=10)
    a = list(generate_events(**kwargs, seed=42))
    b = list(generate_events(**kwargs, seed=7))
    assert [e.trade_id for e in a] != [e.trade_id for e in b]


def test_trade_ids_are_unique_and_sequential():
    events = list(generate_events(rate_per_second=50, duration_seconds=5, break_rate=0.0, seed=3))
    ids = sorted({e.trade_id for e in events})
    assert ids == [f"T{i:012d}" for i in range(1, len(ids) + 1)]


# --------------------------------------------------------------------------
# The clean config really is clean
# --------------------------------------------------------------------------


def test_clean_config_emits_both_sides_in_agreement():
    events = list(generate_events(rate_per_second=20, duration_seconds=10, break_rate=0.0, seed=1))
    grouped = group_by_trade_id(events)

    assert grouped, "no events generated"
    for trade_id, pair in grouped.items():
        assert len(pair) == 2, f"{trade_id} did not get both sides"
        book, market = sorted(pair, key=lambda e: e.source.value)
        assert {book.source, market.source} == {EventSource.BOOK, EventSource.MARKET}
        # Everything except `source` must be identical on a clean trade.
        assert book.quantity == market.quantity
        assert book.price == market.price
        assert book.symbol == market.symbol
        assert book.side == market.side
        assert book.timestamp == market.timestamp


def test_generated_events_respect_domain_invariants():
    events = list(generate_events(rate_per_second=20, duration_seconds=10, break_rate=0.0, seed=2))
    for e in events:
        assert e.timestamp.tzinfo is not None, "timestamp must be tz-aware"
        assert e.price > 0
        assert e.quantity > 0
        assert e.symbol in SYMBOLS
        assert isinstance(e.side, Side)
        # Decimal, never float — float drift would be indistinguishable from a
        # real reconciliation break.
        assert isinstance(e.price, Decimal)
        assert isinstance(e.quantity, Decimal)


def test_prices_stay_within_jitter_band_of_base():
    events = list(generate_events(rate_per_second=20, duration_seconds=10, break_rate=0.0, seed=4))
    band = Decimal(PRICE_JITTER_CENTS) / _CENTS
    for e in events:
        assert abs(e.price - BASE_PRICES[e.symbol]) <= band


def test_timestamps_advance_monotonically_without_reordering():
    events = list(generate_events(rate_per_second=30, duration_seconds=10, break_rate=0.0, reorder_window=1, seed=5))
    stamps = [e.timestamp for e in events]
    assert stamps == sorted(stamps)


# --------------------------------------------------------------------------
# _generate_true_trade — content generation
# --------------------------------------------------------------------------


def test_generate_true_trade_is_deterministic_for_a_given_rng():
    from datetime import datetime, timezone

    ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    a = _generate_true_trade(random.Random(9), SYMBOLS, BASE_PRICES, "T1", ts)
    b = _generate_true_trade(random.Random(9), SYMBOLS, BASE_PRICES, "T1", ts)
    assert a == b
    assert isinstance(a, _Truth)
    assert MIN_QUANTITY <= a.quantity <= MAX_QUANTITY
    assert a.trade_id == "T1" and a.timestamp == ts


# --------------------------------------------------------------------------
# _derive_sides — break injection
# --------------------------------------------------------------------------


def make_truth(**overrides) -> _Truth:
    from datetime import datetime, timezone

    base = dict(
        trade_id="T000000000001",
        symbol="AAPL",
        quantity=Decimal(100),
        price=Decimal("150.00"),
        side=Side.BUY,
        timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    return _Truth(**{**base, **overrides})


def test_derive_sides_without_break_returns_matching_pair():
    book, market = _derive_sides(make_truth(), random.Random(0), break_rate=0.0)
    assert book is not None and market is not None
    assert book.source is EventSource.BOOK
    assert market.source is EventSource.MARKET
    assert book.quantity == market.quantity
    assert book.price == market.price


@pytest.mark.parametrize("seed", range(25))
def test_missing_trade_drops_exactly_one_side(seed):
    book, market = _derive_sides(
        make_truth(),
        random.Random(seed),
        break_rate=1.0,
        break_type_weights={BreakType.MISSING_TRADE: 1.0},
    )
    present = [e for e in (book, market) if e is not None]
    assert len(present) == 1, "MISSING_TRADE must drop exactly one side"


def test_missing_trade_drops_both_sides_across_seeds():
    """Neither side should be the one that always goes missing."""
    dropped = Counter()
    for seed in range(200):
        book, market = _derive_sides(
            make_truth(),
            random.Random(seed),
            break_rate=1.0,
            break_type_weights={BreakType.MISSING_TRADE: 1.0},
        )
        dropped[EventSource.BOOK if book is None else EventSource.MARKET] += 1
    assert dropped[EventSource.BOOK] > 0 and dropped[EventSource.MARKET] > 0


@pytest.mark.parametrize("seed", range(25))
def test_quantity_mismatch_exceeds_tolerance_and_leaves_price_alone(seed):
    truth = make_truth()
    book, market = _derive_sides(
        truth,
        random.Random(seed),
        break_rate=1.0,
        break_type_weights={BreakType.QUANTITY_MISMATCH: 1.0},
    )
    assert book is not None and market is not None
    # Must exceed tolerance, or classify() would call this trade clean and the
    # observed break rate would silently undershoot the configured one.
    assert abs(book.quantity - market.quantity) > QUANTITY_TOLERANCE
    assert book.price == market.price
    assert market.quantity > 0


@pytest.mark.parametrize("seed", range(25))
def test_price_mismatch_exceeds_tolerance_and_leaves_quantity_alone(seed):
    truth = make_truth()
    book, market = _derive_sides(
        truth,
        random.Random(seed),
        break_rate=1.0,
        break_type_weights={BreakType.PRICE_MISMATCH: 1.0},
    )
    assert book is not None and market is not None
    assert abs(book.price - market.price) > PRICE_TOLERANCE
    assert book.quantity == market.quantity
    assert market.price > 0


def test_break_type_weights_are_respected():
    """A zero-weighted break type must never be produced."""
    for excluded in BreakType:
        weights = {bt: 1.0 for bt in BreakType if bt is not excluded}
        seen_missing = seen_qty = seen_price = False
        for seed in range(120):
            book, market = _derive_sides(
                make_truth(), random.Random(seed), break_rate=1.0, break_type_weights=weights
            )
            if book is None or market is None:
                seen_missing = True
            elif book.quantity != market.quantity:
                seen_qty = True
            elif book.price != market.price:
                seen_price = True
        produced = {
            BreakType.MISSING_TRADE: seen_missing,
            BreakType.QUANTITY_MISMATCH: seen_qty,
            BreakType.PRICE_MISMATCH: seen_price,
        }
        assert not produced[excluded], f"{excluded} produced despite zero weight"


def test_symbol_and_side_never_diverge_between_sides():
    """Per docs/RECONCILIATION_RULES.md these are trusted invariants, not break
    dimensions — a divergence here is corruption, so the simulator must never
    manufacture one."""
    events = generate_events(rate_per_second=100, duration_seconds=30, break_rate=0.5, seed=6)
    for pair in group_by_trade_id(events).values():
        if len(pair) == 2:
            assert pair[0].symbol == pair[1].symbol
            assert pair[0].side == pair[1].side


# --------------------------------------------------------------------------
# Observed break rate tracks the configured rate
# --------------------------------------------------------------------------


def classify_pair(pair: list[TradeEvent]) -> str:
    if len(pair) == 1:
        return "missing"
    a, b = pair
    if a.quantity != b.quantity:
        return "quantity"
    if a.price != b.price:
        return "price"
    return "clean"


def test_break_rate_zero_produces_no_breaks():
    grouped = group_by_trade_id(
        generate_events(rate_per_second=100, duration_seconds=30, break_rate=0.0, seed=8)
    )
    assert grouped
    assert all(classify_pair(p) == "clean" for p in grouped.values())


def test_break_rate_one_produces_only_breaks():
    grouped = group_by_trade_id(
        generate_events(rate_per_second=100, duration_seconds=30, break_rate=1.0, seed=9)
    )
    assert grouped
    outcomes = Counter(classify_pair(p) for p in grouped.values())
    assert outcomes["clean"] == 0
    # all three types should appear with even default weights
    assert outcomes["missing"] > 0 and outcomes["quantity"] > 0 and outcomes["price"] > 0


def test_observed_break_rate_approximates_configured_rate():
    configured = 0.10
    grouped = group_by_trade_id(
        generate_events(rate_per_second=100, duration_seconds=120, break_rate=configured, seed=11)
    )
    observed = sum(1 for p in grouped.values() if classify_pair(p) != "clean") / len(grouped)
    assert len(grouped) > 5000, "sample too small to be meaningful"
    assert abs(observed - configured) < 0.02, f"observed {observed:.4f} vs configured {configured}"


# --------------------------------------------------------------------------
# _inject_duplicates
# --------------------------------------------------------------------------


def test_duplicate_rate_zero_is_identity():
    src = list(range(500))
    assert list(_inject_duplicates(iter(src), random.Random(1), 0.0)) == src


def test_duplicates_repeat_existing_events_only():
    src = list(range(2000))
    out = list(_inject_duplicates(iter(src), random.Random(2), 0.25))
    assert len(out) > len(src)
    assert set(out) == set(src), "duplication must not invent new events"
    # every extra occurrence is a repeat of something already in the stream
    counts = Counter(out)
    assert all(v in (1, 2) for v in counts.values())


def test_duplicate_rate_scales_output_size():
    src = list(range(4000))
    sizes = {
        rate: len(list(_inject_duplicates(iter(src), random.Random(3), rate)))
        for rate in (0.0, 0.1, 0.5)
    }
    assert sizes[0.0] < sizes[0.1] < sizes[0.5]
    assert sizes[0.5] == pytest.approx(len(src) * 1.5, rel=0.05)


def test_duplicates_preserve_trade_identity():
    events = list(
        generate_events(
            rate_per_second=100, duration_seconds=20, break_rate=0.0, duplicate_rate=0.3, reorder_window=1, seed=12
        )
    )
    # A duplicated event is the *same* trade — same id and same field values,
    # which is exactly what makes downstream idempotency load-bearing.
    by_key: dict[tuple, list[TradeEvent]] = {}
    for e in events:
        by_key.setdefault((e.trade_id, e.source), []).append(e)
    assert any(len(v) > 1 for v in by_key.values()), "no duplicates produced"
    for occurrences in by_key.values():
        first = occurrences[0]
        assert all(o == first for o in occurrences)


# --------------------------------------------------------------------------
# _reorder
# --------------------------------------------------------------------------


@pytest.mark.parametrize("window", [1, 2, 5, 10, 50])
def test_reorder_is_an_exact_permutation(window):
    src = list(range(1000))
    out = list(_reorder(iter(src), random.Random(window), window))
    assert Counter(out) == Counter(src), "events lost or invented"
    assert len(out) == len(src)


@pytest.mark.parametrize("window", [0, 1])
def test_reorder_window_at_or_below_one_is_passthrough(window):
    src = list(range(200))
    assert list(_reorder(iter(src), random.Random(0), window)) == src


def test_reorder_drains_buffer_when_input_is_shorter_than_window():
    """A stream shorter than the window must still emit everything."""
    src = list(range(5))
    out = list(_reorder(iter(src), random.Random(0), window=50))
    assert Counter(out) == Counter(src)


@pytest.mark.parametrize("window", [2, 5, 20])
def test_reorder_bounds_how_early_an_event_can_appear(window):
    """The one hard displacement guarantee: an event cannot jump ahead of a
    buffer it has not entered yet, so it can move at most `window - 1` positions
    EARLIER. Lateness is NOT bounded — it is geometrically distributed with a
    long tail, so no upper-bound assertion belongs here.
    """
    src = list(range(2000))
    out = list(_reorder(iter(src), random.Random(7), window))
    position = {value: index for index, value in enumerate(out)}
    for input_index, value in enumerate(src):
        assert position[value] >= input_index - (window - 1)


def test_reorder_actually_reorders_at_large_windows():
    src = list(range(1000))
    out = list(_reorder(iter(src), random.Random(5), window=50))
    assert out != src
    displaced = sum(1 for i, v in enumerate(out) if v != i)
    assert displaced > len(src) // 2


def test_larger_window_scrambles_more():
    src = list(range(2000))

    def mean_abs_displacement(window: int) -> float:
        out = list(_reorder(iter(src), random.Random(11), window))
        return sum(abs(out.index(v) - v) for v in src) / len(src)

    assert mean_abs_displacement(2) < mean_abs_displacement(10) < mean_abs_displacement(50)


# --------------------------------------------------------------------------
# End-to-end pipeline composition
# --------------------------------------------------------------------------


def test_chaos_config_preserves_every_generated_event():
    """Duplication and reordering must not lose the underlying trades."""
    ordered = list(
        generate_events(rate_per_second=100, duration_seconds=20, break_rate=0.1, duplicate_rate=0.0, reorder_window=1, seed=13)
    )
    chaotic = list(
        generate_events(rate_per_second=100, duration_seconds=20, break_rate=0.1, duplicate_rate=0.3, reorder_window=25, seed=13)
    )
    # Same seed drives the same underlying trades; chaos only duplicates and
    # shuffles, so the distinct set of (trade_id, source) pairs is unchanged.
    assert {(e.trade_id, e.source) for e in chaotic} == {(e.trade_id, e.source) for e in ordered}
    assert len(chaotic) > len(ordered)


def test_zero_duration_produces_nothing():
    assert list(generate_events(rate_per_second=100, duration_seconds=0, seed=1)) == []


def test_event_count_scales_with_rate_and_duration():
    low = len(list(generate_events(rate_per_second=10, duration_seconds=10, break_rate=0.0, seed=14)))
    high = len(list(generate_events(rate_per_second=100, duration_seconds=10, break_rate=0.0, seed=14)))
    assert high > low * 5, "event volume should scale roughly with rate"
