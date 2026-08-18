import logging
from collections.abc import Callable
from datetime import datetime, timezone

import redis.asyncio as redis

from tieout.pipeline import Processor
from tieout.queue.client import GROUP_NAME, STREAM_NAME
from tieout.queue.serde import deserialize_event

logger = logging.getLogger(__name__)


async def worker_loop(
    client: redis.Redis,
    consumer_name: str,
    processor: Processor,
    running: Callable[[], bool],
    block_ms: int = 5_000,
    on_ack: Callable[[str], None] | None = None,
) -> None:
    """`block_ms`: server-side wait via XREADGROUP's BLOCK, not our own
    asyncio.sleep — avoids busy-polling while still rechecking `running()`
    on every timeout, so the loop shuts down within one block interval.

    `on_ack`: optional hook called with the entry_id right after a
    successful XACK. Not needed for normal operation — added for
    loadtest/harness.py, which has no other way to observe per-message ack
    timing without duplicating this loop.
    """
    while running():
        response = await client.xreadgroup(
            GROUP_NAME, consumer_name, streams={STREAM_NAME: ">"}, count=10, block=block_ms
        )
        for _stream_name, messages in response:
            for entry_id, fields in messages:
                # arrived_at is wall-clock now, not entry_id's embedded XADD
                # time — using the latter would leak stream backlog delay
                # into the reconciliation staleness clock (same reasoning
                # ReconciliationStore already rules out for its own timers).
                try:
                    event = deserialize_event(fields)
                    processor.process(event, arrived_at=datetime.now(timezone.utc))
                except Exception:
                    # Leave unacked on purpose — reclaim.py's XAUTOCLAIM
                    # sweep is what picks this back up, not a retry here.
                    logger.exception(
                        "%s: failed processing %s, leaving unacked for reclaim",
                        consumer_name, entry_id,
                    )
                    continue
                await client.xack(STREAM_NAME, GROUP_NAME, entry_id)
                if on_ack is not None:
                    on_ack(entry_id)
