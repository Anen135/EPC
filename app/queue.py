import uuid

from arq import create_pool
from arq.connections import ArqRedis, RedisSettings

from .config import settings

_redis_settings = RedisSettings.from_dsn(settings.redis_url)
_pool: ArqRedis | None = None


async def get_redis_pool() -> ArqRedis:
    global _pool
    if _pool is None:
        _pool = await create_pool(_redis_settings)
    return _pool


async def close_redis_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.aclose()
        _pool = None


async def enqueue_process_event(db_id: uuid.UUID) -> None:
    pool = await get_redis_pool()
    await pool.enqueue_job("process_event", str(db_id), _job_id=f"process_event:{db_id}")
