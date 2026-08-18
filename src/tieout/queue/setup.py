"""Idempotent consumer group creation — call once at process startup before
any worker starts.
"""

import redis.asyncio as redis
import redis.exceptions

from tieout.queue.client import GROUP_NAME, STREAM_NAME


async def ensure_group(client: redis.Redis) -> None:
    try:
        await client.xgroup_create(STREAM_NAME, GROUP_NAME, id="$", mkstream=True)
    except redis.exceptions.ResponseError as exc:
        # BUSYGROUP means the group already exists — expected on every restart
        # after the first, not a real failure.
        if "BUSYGROUP" not in str(exc):
            raise
