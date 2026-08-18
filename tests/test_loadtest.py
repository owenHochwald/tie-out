"""Integration tests against real local Redis — see tests/conftest.py's
redis_client fixture (skips cleanly if Redis isn't reachable). Kept fast:
short durations at rates trivially inside/outside what 0-2 in-process
workers can keep up with.
"""

from decimal import Decimal

from tieout.loadtest import harness
from tieout.loadtest.harness import run_load_test, sweep_worker_counts


async def test_drains_at_a_sustainable_rate(redis_client):
    report = await run_load_test(
        rates=[50.0],
        duration_s=1.0,
        num_workers=2,
        sample_interval_s=0.1,
        drain_timeout_s=5.0,
        break_rate=0.0,
        duplicate_rate=0.0,
        reorder_window=1,
        seed=1,
    )

    assert len(report.results) == 1
    result = report.results[0]
    assert result.drained is True
    assert result.final_backlog == 0
    assert report.bottleneck_rate is None
    assert result.latencies_s  # ack timing was actually observed
    assert result.xlen_samples  # the sampler actually ran
    assert result.backlog_samples
    assert all(l >= 0 for l in result.latencies_s)
    assert result.push_capped is False  # comfortably sustainable rate

    # Rollup/break data flows through for real, via the same reporting
    # module the text report and reconciliation code already use.
    position_df = result.position_report()
    assert not position_df.empty
    assert position_df["total_quantity"].sum() > Decimal(0)
    assert list(result.break_report().columns) == ["trade_id", "break_type", "symbol"]


async def test_flags_bottleneck_when_nothing_consumes(redis_client):
    report = await run_load_test(
        rates=[50.0],
        duration_s=1.0,
        num_workers=0,  # no workers -> backlog can never drain, deterministically
        sample_interval_s=0.1,
        drain_timeout_s=0.5,
        break_rate=0.0,
        duplicate_rate=0.0,
        reorder_window=1,
        seed=1,
    )

    result = report.results[0]
    assert result.drained is False
    assert result.drain_elapsed_s is None
    assert result.final_backlog > 0
    assert result.latencies_s == []  # nothing was ever acked
    assert report.bottleneck_rate == 50.0


async def test_sweep_stops_at_the_first_bottleneck(redis_client):
    report = await run_load_test(
        rates=[50.0, 100.0, 200.0],
        duration_s=1.0,
        num_workers=0,
        sample_interval_s=0.1,
        drain_timeout_s=0.5,
        break_rate=0.0,
        duplicate_rate=0.0,
        reorder_window=1,
        seed=1,
    )

    assert len(report.results) == 1  # 100 and 200 never ran
    assert report.bottleneck_rate == 50.0


async def test_producer_stops_early_when_it_cannot_sustain_the_requested_rate(redis_client, monkeypatch):
    """The exact scenario that motivated PUSH_OVERRUN_FACTOR: an unrealistic
    requested rate must not make total runtime unbounded. Shrinks the factor
    so the test itself stays fast rather than actually waiting out 3x a long
    duration."""
    monkeypatch.setattr(harness, "PUSH_OVERRUN_FACTOR", 0.2)

    report = await run_load_test(
        rates=[1_000_000.0],
        duration_s=1.0,  # requested workload; cap kicks in at 0.2s of real time
        num_workers=4,
        sample_interval_s=0.05,
        drain_timeout_s=2.0,
        break_rate=0.0,
        duplicate_rate=0.0,
        reorder_window=1,
        seed=1,
    )

    result = report.results[0]
    assert result.push_capped is True
    # Fewer events pushed than a full 1M/sec*1s workload would imply.
    assert len(result.latencies_s) < 1_000_000


async def test_report_summary_is_readable(redis_client):
    report = await run_load_test(
        rates=[50.0],
        duration_s=0.5,
        num_workers=1,
        sample_interval_s=0.1,
        drain_timeout_s=3.0,
        break_rate=0.0,
        duplicate_rate=0.0,
        reorder_window=1,
        seed=1,
    )
    text = report.summary()
    assert "50 evt/s" in text
    assert "latency p50=" in text


async def test_worker_sweep_runs_every_count_without_stopping_early(redis_client):
    report = await sweep_worker_counts(
        rate=50.0,
        worker_counts=[0, 2],  # 0 workers guarantees a non-drain; sweep must still run 2 afterward
        duration_s=1.0,
        sample_interval_s=0.1,
        drain_timeout_s=3.0,
        break_rate=0.0,
        duplicate_rate=0.0,
        reorder_window=1,
        seed=1,
    )

    assert report.rate == 50.0
    assert [r.num_workers for r in report.results] == [0, 2]
    assert report.results[0].drained is False
    assert report.results[1].drained is True

    text = report.summary()
    assert "0 workers" in text
    assert "2 workers" in text
