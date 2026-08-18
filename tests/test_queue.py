"""Integration tests against a real local Redis (docker run -p 6379:6379
redis:7-alpine) — this module is what exercises the actual XADD/XGROUP
CREATE/XREADGROUP/XACK/XAUTOCLAIM mechanics CLAUDE.md asks to be run by hand
before writing code; skips cleanly if Redis isn't reachable.
"""

import asyncio
import contextlib
from datetime import datetime, timezone
from decimal import Decimal

import pytest
import pytest_asyncio
import redis.exceptions

from tieout.domain import EventSource, Side, TradeEvent
from tieout.pipeline import Processor
from tieout.queue.client import GROUP_NAME, STREAM_NAME, make_client
from tieout.queue.producer import push_event
from tieout.queue.reclaim import reclaim_loop
from tieout.queue.serde import deserialize_event
from tieout.queue.setup import ensure_group
from tieout.queue.worker import worker_loop


def make_event(trade_id, source=EventSource.BOOK) -> TradeEvent:
    return TradeEvent(
        trade_id=trade_id,
        symbol="AAPL",
        quantity=Decimal(100),
        price=Decimal("150.00"),
        side=Side.BUY,
        timestamp=datetime.now(timezone.utc),
        source=source,
    )


@pytest_asyncio.fixture
async def client():
    c = make_client()
    try:
        await c.ping()
    except redis.exceptions.ConnectionError:
        pytest.skip("Redis not reachable at REDIS_URL — start it with `docker run -p 6379:6379 redis:7-alpine`")
    await c.delete(STREAM_NAME)
    yield c
    await c.delete(STREAM_NAME)
    await c.aclose()


async def _run_until(condition, *, timeout_s: float = 2.0, interval_s: float = 0.02) -> None:
    """Poll `condition()` (sync or async) until true or timeout — used
    instead of a fixed sleep so tests aren't racing an arbitrary delay
    against the loops under test."""
    async def _wait():
        while True:
            result = condition()
            if asyncio.iscoroutine(result):
                result = await result
            if result:
                return
            await asyncio.sleep(interval_s)

    await asyncio.wait_for(_wait(), timeout=timeout_s)


async def _cancel(task: asyncio.Task) -> None:
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


async def test_ensure_group_is_idempotent(client):
    await ensure_group(client)
    await ensure_group(client)  # BUSYGROUP on the second call must not raise


async def test_push_event_round_trips_through_serde(client):
    event = make_event("T1")
    entry_id = await push_event(client, event)

    entries = await client.xrange(STREAM_NAME, min=entry_id, max=entry_id)
    [(_, fields)] = entries
    assert deserialize_event(fields) == event


async def test_worker_loop_processes_and_acks(client):
    await ensure_group(client)
    await push_event(client, make_event("T1"))

    processor = Processor()
    running = True
    task = asyncio.create_task(
        worker_loop(client, "worker-1", processor, running=lambda: running, block_ms=100)
    )
    try:
        await _run_until(lambda: ("T1", EventSource.BOOK) in processor.seen)
    finally:
        running = False
        await _cancel(task)

    assert processor.rollup.all_rollups()["AAPL"].total_quantity == Decimal(100)
    pending = await client.xpending(STREAM_NAME, GROUP_NAME)
    assert pending["pending"] == 0  # acked, nothing left in the PEL


async def test_worker_loop_bad_message_stays_unacked_for_reclaim(client, monkeypatch):
    await ensure_group(client)
    await push_event(client, make_event("T1"))

    processor = Processor()

    def _boom(event, arrived_at):
        raise ValueError("simulated processing failure")

    monkeypatch.setattr(processor, "process", _boom)

    running = True
    task = asyncio.create_task(
        worker_loop(client, "worker-1", processor, running=lambda: running, block_ms=100)
    )
    try:
        await asyncio.sleep(0.3)  # let the loop take one pass at the bad message
    finally:
        running = False
        await _cancel(task)

    pending = await client.xpending(STREAM_NAME, GROUP_NAME)
    assert pending["pending"] == 1  # process() raised -> xack was skipped


async def test_reclaim_loop_picks_up_message_left_unacked_by_dead_worker(client, monkeypatch, caplog):
    import tieout.queue.reclaim as reclaim_module

    await ensure_group(client)
    event = make_event("T1")
    await push_event(client, event)

    # Simulate a worker that read the message and crashed before XACK: read
    # it under a consumer name that never comes back.
    await client.xreadgroup(GROUP_NAME, "dead-worker", streams={STREAM_NAME: ">"}, count=1)
    pending = await client.xpending(STREAM_NAME, GROUP_NAME)
    assert pending["pending"] == 1

    monkeypatch.setattr(reclaim_module, "IDLE_THRESHOLD_MS", 10)
    await asyncio.sleep(0.05)  # exceed the (patched) idle threshold

    processor = Processor()
    with caplog.at_level("INFO", logger="tieout.queue.reclaim"):
        task = asyncio.create_task(
            reclaim_module.reclaim_loop(client, "reclaimer", processor, interval_s=0.01)
        )
        try:
            await _run_until(lambda: ("T1", EventSource.BOOK) in processor.seen)
        finally:
            await _cancel(task)

    assert processor.rollup.all_rollups()["AAPL"].total_quantity == Decimal(100)
    pending = await client.xpending(STREAM_NAME, GROUP_NAME)
    assert pending["pending"] == 0  # reclaimed message got acked
    # CLAUDE.md's audit trail: one line per stale-sweep reclaim.
    assert "reclaimed and reprocessed" in caplog.text


async def test_reclaimed_message_already_applied_is_a_no_op(client, monkeypatch):
    """The exact interleaving CLAUDE.md calls out: a worker fully applies an
    event to the shared Processor, then dies before XACK. Reclaim redelivers
    it — Processor.seen must make that redelivery a no-op, not a double
    count."""
    import tieout.queue.reclaim as reclaim_module

    await ensure_group(client)
    event = make_event("T1")
    await push_event(client, event)

    await client.xreadgroup(GROUP_NAME, "dead-worker", streams={STREAM_NAME: ">"}, count=1)

    processor = Processor()
    processor.process(event, arrived_at=datetime.now(timezone.utc))  # crash happens right here, pre-XACK
    assert processor.rollup.all_rollups()["AAPL"].total_quantity == Decimal(100)

    monkeypatch.setattr(reclaim_module, "IDLE_THRESHOLD_MS", 10)
    await asyncio.sleep(0.05)

    async def _pending_is_zero() -> bool:
        pending = await client.xpending(STREAM_NAME, GROUP_NAME)
        return pending["pending"] == 0

    task = asyncio.create_task(
        reclaim_module.reclaim_loop(client, "reclaimer", processor, interval_s=0.01)
    )
    try:
        await _run_until(_pending_is_zero)
    finally:
        await _cancel(task)

    # Still 100, not 200 — the redelivered apply was a no-op.
    assert processor.rollup.all_rollups()["AAPL"].total_quantity == Decimal(100)
