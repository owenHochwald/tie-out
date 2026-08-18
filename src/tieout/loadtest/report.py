"""Report types for run_load_test(). Two measurements per rate, per CLAUDE.md:
XLEN sampled over time (is the backlog draining or growing?) and per-event
latency (generation -> XACK). Everything else here is derived from those two.
"""

from dataclasses import dataclass, field

from tieout.domain import Break
from tieout.rollup.aggregator import RollupAggregator


def _percentile(sorted_values: list[float], pct: float) -> float:
    if not sorted_values:
        return float("nan")
    idx = min(len(sorted_values) - 1, int(len(sorted_values) * pct))
    return sorted_values[idx]


@dataclass
class RateResult:
    rate: float
    num_workers: int
    xlen_samples: list[tuple[float, int]]     # (elapsed_seconds, raw XLEN) — total stream size over time.
                                               # Redis never shrinks this on XACK (only XTRIM/XDEL do), so
                                               # raw XLEN alone can never signal "caught up" — it's kept here
                                               # as honest, literal data, not as the drain signal.
    backlog_samples: list[tuple[float, int]]  # (elapsed_seconds, XLEN minus acks observed so far) — the
                                               # actual outstanding-work signal "growing unboundedly" is
                                               # judged against.
    latencies_s: list[float]                  # per-event, push (XADD) -> XACK
    drained: bool                             # did backlog_samples return to 0 within drain_timeout_s?
    drain_elapsed_s: float | None             # time to drain, if it did
    final_backlog: int

    def latency_percentiles(self) -> dict[str, float]:
        values = sorted(self.latencies_s)
        return {
            "p50": _percentile(values, 0.50),
            "p95": _percentile(values, 0.95),
            "p99": _percentile(values, 0.99),
            "max": values[-1] if values else float("nan"),
        }

    def peak_backlog(self) -> int:
        return max((backlog for _, backlog in self.backlog_samples), default=0)

    def _metrics_str(self) -> str:
        p = self.latency_percentiles()
        status = "OK, drained" if self.drained else "BOTTLENECK, did not drain"
        return (
            f"{status:<26}  peak backlog={self.peak_backlog():<6}  final backlog={self.final_backlog:<6}  "
            f"latency p50={p['p50']:.3f}s p95={p['p95']:.3f}s p99={p['p99']:.3f}s max={p['max']:.3f}s"
        )

    def summary_line(self) -> str:
        return f"{self.rate:>8g} evt/s  {self._metrics_str()}"


@dataclass
class LoadTestReport:
    """One or more rates, tested ascending, stopping at the first bottleneck."""

    results: list[RateResult] = field(default_factory=list)
    bottleneck_rate: float | None = None  # None means every tested rate kept up

    def summary(self) -> str:
        lines = [r.summary_line() for r in self.results]
        if self.bottleneck_rate is not None:
            lines.append(
                f"\nBottleneck: backlog stopped draining at {self.bottleneck_rate:g} events/sec "
                "— that's the honest ceiling, not a projection."
            )
        else:
            lines.append(
                "\nNo bottleneck found in the tested rates — the system kept up and fully drained "
                "at every rate tried. Re-run with higher rates to find the actual ceiling."
            )
        return "\n".join(lines)


@dataclass
class WorkerSweepReport:
    """Fixed rate, worker count varied — the horizontal-scaling demonstration
    CLAUDE.md names as the natural extension once the rate sweep works. Runs
    every requested worker count (unlike LoadTestReport, it does not stop
    early): an intentionally saturated low-worker-count run is exactly the
    interesting data point here, not a stopping condition.
    """

    rate: float
    results: list[RateResult] = field(default_factory=list)  # one per worker count, as requested

    def summary(self) -> str:
        header = f"Worker-count sweep @ {self.rate:g} events/sec:"
        lines = [f"{r.num_workers:>3} workers  {r._metrics_str()}" for r in self.results]
        return "\n".join([header, *lines])
