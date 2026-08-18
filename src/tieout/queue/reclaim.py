"""XAUTOCLAIM sweep — the queue-layer analog of ReconciliationStore.sweep_stale,
but a different kind of staleness: this is "message read but never acked" (a
delivery fact, worker died/hung), not "trade's counterpart never showed up"
(a reconciliation fact). Reclaimed messages are re-handed to a live worker
and run through the same process()+xack() path as worker_loop; Processor's
`seen` set is what makes replaying an already-applied message a no-op.
"""

import asyncio
import logging
from datetime import datetime, timezone

import redis.asyncio as redis

from tieout.pipeline import Processor
from tieout.queue.client import GROUP_NAME, IDLE_THRESHOLD_MS, STREAM_NAME
from tieout.queue.serde import deserialize_event

logger = logging.getLogger(__name__)


async def reclaim_loop(
    client: redis.Redis,
    consumer_name: str,
    processor: Processor,
    interval_s: float,
) -> None:
    while True:
        await asyncio.sleep(interval_s)
        _cursor, claimed, _deleted = await client.xautoclaim(
            STREAM_NAME, GROUP_NAME, consumer_name,
            min_idle_time=IDLE_THRESHOLD_MS, start_id="0-0",
        )
        for entry_id, fields in claimed:
            try:
                event = deserialize_event(fields)
                processor.process(event, arrived_at=datetime.now(timezone.utc))
            except Exception:
                logger.exception(
                    "%s: failed reprocessing reclaimed %s, leaving unacked",
                    consumer_name, entry_id,
                )
                continue
            await client.xack(STREAM_NAME, GROUP_NAME, entry_id)
            # One line per stale-sweep reclaim (CLAUDE.md's Reporting section) —
            # this message sat unacked past IDLE_THRESHOLD_MS, was redelivered
            # to a live consumer, and just completed successfully.
            logger.info("%s: reclaimed and reprocessed %s (idle >= %dms)", consumer_name, entry_id, IDLE_THRESHOLD_MS)
