"""Redis client factory + the constants everything else in queue/ shares.

`decode_responses=True` means stream fields come back as `str`, not `bytes` —
serde.py's deserialize_event() is written assuming that, so don't drop this
when constructing a client elsewhere.
"""

import os

import redis.asyncio as redis

STREAM_NAME = os.environ.get("TRADES_STREAM", "trades")
GROUP_NAME = os.environ.get("WORKER_GROUP", "tieout-workers")

IDLE_THRESHOLD_MS = 30_000
# How long a message can sit read-but-unacked before XAUTOCLAIM reassigns it.
# Named constant, same discipline as reconciliation/rules.py's tolerances:
# long enough that a slow-but-alive worker isn't preempted mid-processing,
# short enough that a crashed worker's messages don't sit orphaned for long.
# Tune against real processing latency once measured — this is a placeholder,
# same caveat as MISSING_TRADE_TIMEOUT_S.


def make_client() -> redis.Redis:
    return redis.Redis.from_url(
        os.environ.get("REDIS_URL", "redis://localhost:6379/0"),
        decode_responses=True,
    )
