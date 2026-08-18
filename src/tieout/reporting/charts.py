"""PNG charts from reporting/report.py's DataFrames — same break_log/rollup
inputs as the text report, no separate data path (CLAUDE.md "Reporting").

Uses matplotlib's object-oriented API (Figure + savefig) rather than
pyplot: this runs headless from a CLI/load-test context with no display,
and avoids pyplot's global figure-state entirely rather than remembering
to close it.
"""

import pandas as pd
from matplotlib.figure import Figure

# Same severity order report.py sorts by — MISSING_TRADE is the most severe
# (money potentially unaccounted for), PRICE_MISMATCH the least (CLAUDE.md's
# rough severity intuition: a missing trade beats a rounding-scale price gap).
_SEVERITY_ORDER = ["MISSING_TRADE", "QUANTITY_MISMATCH", "PRICE_MISMATCH"]
_SEVERITY_COLORS = ["#c0392b", "#e67e22", "#f1c40f"]


def plot_position_report(df: pd.DataFrame, path: str) -> None:
    fig = Figure(figsize=(8, 4.5))
    ax = fig.add_subplot(111)
    if df.empty:
        ax.text(0.5, 0.5, "no positions", ha="center", va="center", transform=ax.transAxes)
    else:
        ax.bar(df["symbol"], df["total_notional"].astype(float), color="#2980b9")
        ax.set_ylabel("Total notional ($)")
        ax.set_xlabel("Symbol")
        ax.tick_params(axis="x", rotation=45)
    ax.set_title("Position rollup — total notional by symbol")
    fig.savefig(path, dpi=150, bbox_inches="tight")


def plot_break_report(df: pd.DataFrame, path: str) -> None:
    fig = Figure(figsize=(8, 4.5))
    ax = fig.add_subplot(111)
    if df.empty:
        ax.text(0.5, 0.5, "no breaks", ha="center", va="center", transform=ax.transAxes)
    else:
        counts = df["break_type"].value_counts()
        order = [t for t in _SEVERITY_ORDER if t in counts.index]
        counts = counts.reindex(order)
        colors = [_SEVERITY_COLORS[_SEVERITY_ORDER.index(t)] for t in order]
        ax.bar(counts.index, counts.values, color=colors)
        ax.set_ylabel("Count")
    ax.set_title("Breaks by type (ranked by severity)")
    fig.savefig(path, dpi=150, bbox_inches="tight")
