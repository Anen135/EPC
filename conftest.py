import pytest_asyncio

from app import queue


@pytest_asyncio.fixture(scope="session", autouse=True)
async def _redis_pool_lifecycle():
    """app.queue keeps a module-level connection pool singleton, matching
    how the real app runs (one pool for the process lifetime). Tests share
    a session-scoped event loop (see pytest.ini) so that singleton, and
    the SQLAlchemy engine in app.database, stay bound to a single live
    loop across the whole test session instead of being torn down and
    reused across per-test loops."""
    yield
    await queue.close_redis_pool()
