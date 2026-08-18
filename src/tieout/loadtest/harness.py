"""run_load_test() — sweep ingestion rates, and for each one push events at
that rate through the real queue (real Redis, real worker_loop coroutines),
watching two things per CLAUDE.md: XLEN over time, and per-event latency
(XADD -> XACK). A rate is a bottleneck if the backlog doesn't drain back to
empty within `drain_timeout_s` after generation stops — that's the concrete,
checkable meaning of "growing unboundedly" used here, not a slope heuristic.

Deliberately doesn't run reclaim_loop or kill workers mid-test: this harness
measures sustained throughput of a healthy system, not crash recovery — that
scenario is already covered directly in test_queue.py.
"""

import asyncio
import logging
import time
from collections.abc import Callable

import redis.asyncio as redis

from tieout.ingest.simulator import generate_events
from tieout.loadtest.report import LoadTestReport, RateResult, WorkerSweepReport
from tieout.pipeline import Processor
from tieout.queue.client import STREAM_NAME, make_client
from tieout.queue.setup import ensure_group
from tieout.queue.producer import push_event
from tieout.queue.worker import worker_loop

logger = logging.getLogger(__name__)

DEFAULT_NUM_WORKERS = 4
# Hard wall-clock cap on the push phase, as a multiple of duration_s. Once a
# requested rate exceeds what one unpipelined connection can actually drive,
# duration_s stops bounding real time — the push loop would otherwise just
# keep awaiting XADD round-trips until the (possibly huge) generated event
# count is exhausted, regardless of how small duration_s/drain_timeout_s
# are. This is what keeps total runtime bounded in that case; 3x is
# generous enough that a merely-slow-but-keeping-up run isn't cut off.
PUSH_OVERRUN_FACTOR = 3.0
# XLEN sampling cadence: fine enough to see a trend within a duration_s of
# tens of seconds, coarse enough not to make the sampler itself a meaningful
# share of Redis traffic.
DEFAULT_SAMPLE_INTERVAL_S = 0.5
# How long to wait, after generation stops, for the backlog to hit 0 before
# calling this rate a bottleneck. Long enough that a brief queue after a
# burst isn't mistaken for falling behind; short enough that the sweep
# doesn't stall for minutes on a rate that's genuinely never catching up.
DEFAULT_DRAIN_TIMEOUT_S = 30.0
# "Realistic chaos" per CLAUDE.md's volume config — moderate, not clean-path
# (lower) and not a stress test of the reconciliation logic itself (higher).
DEFAULT_BREAK_RATE = 0.02
DEFAULT_DUPLICATE_RATE = 0.02
DEFAULT_REORDER_WINDOW = 5


def _make_on_ack(pushed_at: dict[str, float], latencies: list[float]) -> Callable[[str], None]:
    def _on_ack(entry_id: str) -> None:
        start = pushed_at.pop(entry_id, None)
        if start is not None:
            latencies.append(time.monotonic() - start)

    return _on_ack


async def _sample_xlen(
    client: redis.Redis,
    xlen_samples: list[tuple[float, int]],
    backlog_samples: list[tuple[float, int]],
    latencies: list[float],
    start: float,
    interval_s: float,
    stop: asyncio.Event,
) -> None:
    """Samples raw XLEN (informational — never shrinks on XACK) and derives
    outstanding backlog as XLEN minus acks observed so far (len(latencies),
    since every message this harness pushes is tracked through to its ack).
    """
    while not stop.is_set():
        elapsed = time.monotonic() - start
        xlen = await client.xlen(STREAM_NAME)
        xlen_samples.append((elapsed, xlen))
        backlog_samples.append((elapsed, max(0, xlen - len(latencies))))
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval_s)
        except TimeoutError:
            pass  # normal: means the interval elapsed without stop being set


async def _generate_at_rate(
    client: redis.Redis,
    rate: float,
    duration_s: float,
    pushed_at: dict[str, float],
    *,
    break_rate: float,
    duplicate_rate: float,
    reorder_window: int,
    seed: int | None,
) -> bool:
    """Paces real XADDs against wall-clock time, using the inter-arrival
    gaps generate_events() already computed — the simulator itself never
    sleeps (CLAUDE.md/producer.py), pacing is this module's job.

    Returns True if the producer itself couldn't sustain `rate` and had to
    stop early (see PUSH_OVERRUN_FACTOR) — distinct from the queue/worker
    system falling behind, which `drained` in the caller reports separately.
    """
    start = time.monotonic()
    first_ts = None
    push_deadline = start + duration_s * PUSH_OVERRUN_FACTOR
    pushed = 0
    for event in generate_events(
        rate_per_second=rate,
        duration_seconds=duration_s,
        break_rate=break_rate,
        duplicate_rate=duplicate_rate,
        reorder_window=reorder_window,
        seed=seed,
    ):
        if first_ts is None:
            first_ts = event.timestamp
        now = time.monotonic()
        if now > push_deadline:
            logger.warning(
                "producer could not sustain %.0f evt/s: stopped after %d events / %.1fs "
                "(requested duration %.1fs) — this is the producer's own ceiling, not "
                "necessarily the queue/worker system's",
                rate, pushed, now - start, duration_s,
            )
            return True
        target = start + (event.timestamp - first_ts).total_seconds()
        delay = target - now
        if delay > 0:
            await asyncio.sleep(delay)
        entry_id = await push_event(client, event)
        pushed_at[entry_id] = time.monotonic()
        pushed += 1
    return False


async def _run_single_rate(
    client: redis.Redis,
    rate: float,
    duration_s: float,
    num_workers: int,
    sample_interval_s: float,
    drain_timeout_s: float,
    break_rate: float,
    duplicate_rate: float,
    reorder_window: int,
    seed: int | None,
) -> RateResult:
    await client.delete(STREAM_NAME)  # clean slate per rate — each trial is independent
    await ensure_group(client)

    processor = Processor()
    pushed_at: dict[str, float] = {}
    latencies: list[float] = []
    on_ack = _make_on_ack(pushed_at, latencies)

    running = True
    worker_tasks = [
        asyncio.create_task(
            worker_loop(
                client, f"loadtest-{i}", processor,
                running=lambda: running, block_ms=200, on_ack=on_ack,
            )
        )
        for i in range(num_workers)
    ]

    xlen_samples: list[tuple[float, int]] = []
    backlog_samples: list[tuple[float, int]] = []
    stop_sampling = asyncio.Event()
    start = time.monotonic()
    sampler_task = asyncio.create_task(
        _sample_xlen(client, xlen_samples, backlog_samples, latencies, start, sample_interval_s, stop_sampling)
    )

    push_capped = await _generate_at_rate(
        client, rate, duration_s, pushed_at,
        break_rate=break_rate, duplicate_rate=duplicate_rate,
        reorder_window=reorder_window, seed=seed,
    )

    # Drain phase: workers and the sampler are still running — watch the
    # derived backlog (not raw XLEN, which never shrinks on its own) until
    # it hits 0 or we give up.
    drain_start = time.monotonic()
    drained = False
    while time.monotonic() - drain_start < drain_timeout_s:
        backlog = max(0, await client.xlen(STREAM_NAME) - len(latencies))
        if backlog == 0:
            drained = True
            break
        await asyncio.sleep(sample_interval_s)
    drain_elapsed = (time.monotonic() - drain_start) if drained else None
    final_backlog = max(0, await client.xlen(STREAM_NAME) - len(latencies))

    running = False
    stop_sampling.set()
    for task in (*worker_tasks, sampler_task):
        task.cancel()
    await asyncio.gather(*worker_tasks, sampler_task, return_exceptions=True)

    return RateResult(
        rate=rate,
        num_workers=num_workers,
        xlen_samples=xlen_samples,
        backlog_samples=backlog_samples,
        latencies_s=latencies,
        drained=drained,
        drain_elapsed_s=drain_elapsed,
        final_backlog=final_backlog,
        push_capped=push_capped,
        rollup=processor.rollup,
        break_log=processor.break_log,
    )


async def run_load_test(
    rates: list[float],
    duration_s: float,
    num_workers: int = DEFAULT_NUM_WORKERS,
    sample_interval_s: float = DEFAULT_SAMPLE_INTERVAL_S,
    drain_timeout_s: float = DEFAULT_DRAIN_TIMEOUT_S,
    break_rate: float = DEFAULT_BREAK_RATE,
    duplicate_rate: float = DEFAULT_DUPLICATE_RATE,
    reorder_window: int = DEFAULT_REORDER_WINDOW,
    seed: int | None = None,
) -> LoadTestReport:
    """Sweeps `rates` in order, stopping at the first one that doesn't drain
    — matching CLAUDE.md's `for rate in rates: ... if backlog_growing:
    report.bottleneck_rate = rate; break`.
    """
    client = make_client()
    report = LoadTestReport()
    try:
        for rate in rates:
            result = await _run_single_rate(
                client, rate, duration_s, num_workers, sample_interval_s, drain_timeout_s,
                break_rate, duplicate_rate, reorder_window, seed,
            )
            report.results.append(result)
            if not result.drained:
                report.bottleneck_rate = rate
                break
    finally:
        await client.aclose()
    return report


async def sweep_worker_counts(
    rate: float,
    worker_counts: list[int],
    duration_s: float,
    sample_interval_s: float = DEFAULT_SAMPLE_INTERVAL_S,
    drain_timeout_s: float = DEFAULT_DRAIN_TIMEOUT_S,
    break_rate: float = DEFAULT_BREAK_RATE,
    duplicate_rate: float = DEFAULT_DUPLICATE_RATE,
    reorder_window: int = DEFAULT_REORDER_WINDOW,
    seed: int | None = None,
) -> WorkerSweepReport:
    """Fixed rate, worker count varied — demonstrates horizontal scaling
    (CLAUDE.md non-goals: demonstrate, don't auto-provision). More consumer
    group workers should drain the same backlog faster; this measures
    whether that's actually true, not just plausible.
    """
    client = make_client()
    report = WorkerSweepReport(rate=rate)
    try:
        for num_workers in worker_counts:
            result = await _run_single_rate(
                client, rate, duration_s, num_workers, sample_interval_s, drain_timeout_s,
                break_rate, duplicate_rate, reorder_window, seed,
            )
            report.results.append(result)
    finally:
        await client.aclose()
    return report
