"""Break log + rollups -> pandas DataFrames. Reads existing state, no separate
reporting database (CLAUDE.md "Reporting")."""

import pandas as pd

from tieout.domain import Break
from tieout.rollup.aggregator import RollupAggregator

_SEVERITY = {"MISSING_TRADE": 0, "QUANTITY_MISMATCH": 1, "PRICE_MISMATCH": 2}


def break_report(break_log: list[Break]) -> pd.DataFrame:
    rows = [
        {
            "trade_id": b.trade_id,
            "break_type": b.break_type.name,
            "symbol": (b.book_trade or b.present_event).symbol,
        }
        for b in break_log
    ]
    df = pd.DataFrame(rows, columns=["trade_id", "break_type", "symbol"])
    if df.empty:
        return df
    df["_severity"] = df["break_type"].map(_SEVERITY)
    return df.sort_values("_severity").drop(columns="_severity").reset_index(drop=True)


def position_report(rollup: RollupAggregator) -> pd.DataFrame:
    rows = [
        {"symbol": r.symbol, "total_quantity": r.total_quantity, "total_notional": r.total_notional}
        for r in rollup.all_rollups().values()
    ]
    return pd.DataFrame(rows, columns=["symbol", "total_quantity", "total_notional"]).sort_values(
        "symbol"
    ).reset_index(drop=True)
