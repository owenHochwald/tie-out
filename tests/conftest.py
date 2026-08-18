import os

os.environ.setdefault("TRADES_STREAM", "trades-test")
os.environ.setdefault("WORKER_GROUP", "tieout-workers-test")

import pytest
import pytest_asyncio
import redis.exceptions

from tieout.queue.client import STREAM_NAME, make_client


@pytest_asyncio.fixture
async def redis_client():
    """A pinged, clean-stream Redis client — skips the test if Redis isn't
    reachable, rather than failing the whole suite."""
    c = make_client()
    try:
        await c.ping()
    except redis.exceptions.ConnectionError:
        pytest.skip("Redis not reachable at REDIS_URL — start it with `docker run -p 6379:6379 redis:7-alpine`")
    await c.delete(STREAM_NAME)
    yield c
    await c.delete(STREAM_NAME)
    await c.aclose()
