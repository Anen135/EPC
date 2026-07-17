from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from .config import settings


class Base(DeclarativeBase):
    pass


engine = create_async_engine(
    settings.database_url,
    pool_size=settings.db_pool_size,
    max_overflow=settings.db_max_overflow,
    pool_pre_ping=True,
)

async_session_factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


async def get_db():
    """FastAPI dependency yielding a request-scoped session."""
    async with async_session_factory() as session:
        yield session


async def init_db() -> None:
    """Create tables if they don't exist. Fine for small services / demos;
    swap for Alembic migrations in a real production rollout so schema
    changes are versioned and reviewable."""
    from . import models  # noqa: F401  (ensure models are registered on Base.metadata)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
