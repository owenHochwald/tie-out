"""XADD wrapper — pushes a TradeEvent onto the stream.

Small on purpose: this is what you'll use to manually feed your worker_loop
while testing it, and later what loadtest/ wraps with real-time pacing
(CLAUDE.md: no sleeping in the simulator itself, pacing belongs here/loadtest).
"""

import redis.asyncio as redis

from tieout.domain import TradeEvent
from tieout.queue.client import STREAM_NAME
from tieout.queue.serde import serialize_event


async def push_event(client: redis.Redis, event: TradeEvent) -> str:
    """Returns the Redis-assigned stream entry ID."""
    return await client.xadd(STREAM_NAME, serialize_event(event))
