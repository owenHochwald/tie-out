"""PNG charts for load test results — the same two measurements the text
report is built from (backlog over time, per-event latency), plus a
scaling view across trials. Object-oriented matplotlib API (Figure +
savefig), not pyplot — see reporting/charts.py for why.
"""

from collections.abc import Callable

from matplotlib.figure import Figure

from tieout.loadtest.report import RateResult

_DEFAULT_LABEL: Callable[[RateResult], str] = lambda r: f"{r.rate:g} evt/s"  # noqa: E731


def plot_backlog_over_time(
    results: list[RateResult],
    path: str,
    label_fn: Callable[[RateResult], str] = _DEFAULT_LABEL,
) -> None:
    """One line per trial. Plots the derived backlog (XLEN minus acks so
    far), not raw XLEN — see report.py for why raw XLEN can't show this."""
    fig = Figure(figsize=(9, 5))
    ax = fig.add_subplot(111)
    for r in results:
        if not r.backlog_samples:
            continue
        xs = [t for t, _ in r.backlog_samples]
        ys = [b for _, b in r.backlog_samples]
        ax.plot(xs, ys, label=label_fn(r), marker=".", markersize=3)
    ax.set_xlabel("Elapsed (s)")
    ax.set_ylabel("Backlog (unacked entries)")
    ax.set_title("Backlog over time")
    ax.axhline(0, color="black", linewidth=0.5)
    ax.legend()
    fig.savefig(path, dpi=150, bbox_inches="tight")


def plot_latency_percentiles(
    results: list[RateResult],
    path: str,
    label_fn: Callable[[RateResult], str] = _DEFAULT_LABEL,
) -> None:
    """Grouped bars: p50/p95/p99 per trial, side by side."""
    labels = [label_fn(r) for r in results]
    percentiles = [r.latency_percentiles() for r in results]
    p50 = [p["p50"] for p in percentiles]
    p95 = [p["p95"] for p in percentiles]
    p99 = [p["p99"] for p in percentiles]

    x = list(range(len(labels)))
    width = 0.25

    fig = Figure(figsize=(9, 5))
    ax = fig.add_subplot(111)
    ax.bar([i - width for i in x], p50, width, label="p50")
    ax.bar(x, p95, width, label="p95")
    ax.bar([i + width for i in x], p99, width, label="p99")
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.set_ylabel("Latency (s)")
    ax.set_title("Latency percentiles (XADD → XACK)")
    ax.legend()
    fig.savefig(path, dpi=150, bbox_inches="tight")
