import pytest_asyncio

from app import queue


@pytest_asyncio.fixture(scope="session", autouse=True)
async def _redis_pool_lifecycle():
    yield
    await queue.close_redis_pool()
