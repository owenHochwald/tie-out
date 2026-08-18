"""Integration tests against real local Redis — see tests/conftest.py's
redis_client fixture (skips cleanly if Redis isn't reachable). Kept fast:
short durations at rates trivially inside/outside what 0-2 in-process
workers can keep up with.
"""

from tieout.loadtest.harness import run_load_test


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
