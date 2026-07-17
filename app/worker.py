import time
import uuid
from datetime import datetime, timezone

from arq.connections import RedisSettings

from .config import settings
from .database import async_session_factory, init_db
from .logging_config import configure_logging, get_logger
from .metrics import (
    EVENT_PROCESSING_DURATION,
    EVENTS_DEAD_LETTER,
    EVENTS_PROCESSED,
    EVENTS_RETRIED,
)
from .models import Event, EventStatus
from .queue import close_redis_pool

logger = get_logger(__name__)


def business_logic(event: Event) -> None:
    if not event.event_type:
        raise ValueError("event_type must not be empty")


async def process_event(ctx, db_id: str) -> None:
    session_factory = ctx["session_factory"]

    async with session_factory() as session:
        event = await session.get(
            Event, uuid.UUID(db_id), with_for_update={"skip_locked": True}
        )
        if event is None:
            logger.warning("event_not_found", db_id=db_id)
            return

        if event.status in (EventStatus.COMPLETED.value, EventStatus.PROCESSING.value):
            logger.info("event_already_handled", db_id=db_id, status=event.status)
            return

        event.status = EventStatus.PROCESSING.value
        event.processing_started_at = datetime.now(timezone.utc)
        await session.commit()

        log = logger.bind(db_id=db_id, event_id=event.event_id, event_type=event.event_type)
        start = time.monotonic()

        try:
            business_logic(event)
        except Exception as exc:
            duration = time.monotonic() - start
            EVENT_PROCESSING_DURATION.labels(event_type=event.event_type).observe(duration)

            event.retry_count += 1
            event.error_message = str(exc)[:2000]

            if event.retry_count <= settings.max_retries:
                event.status = EventStatus.PENDING.value
                await session.commit()

                EVENTS_RETRIED.labels(event_type=event.event_type).inc()
                defer = settings.retry_defer_seconds * event.retry_count
                log.warning(
                    "event_processing_failed_retrying",
                    retry_count=event.retry_count,
                    defer_seconds=defer,
                    error=str(exc),
                )
                await ctx["redis"].enqueue_job(
                    "process_event",
                    db_id,
                    _defer_by=defer,
                    _job_id=f"process_event:{db_id}:{event.retry_count}",
                )
            else:
                event.status = EventStatus.DEAD_LETTER.value
                await session.commit()

                EVENTS_DEAD_LETTER.labels(event_type=event.event_type).inc()
                EVENTS_PROCESSED.labels(event_type=event.event_type, status="dead_letter").inc()
                log.error(
                    "event_processing_dead_lettered",
                    retry_count=event.retry_count,
                    error=str(exc),
                )
            return

        duration = time.monotonic() - start
        EVENT_PROCESSING_DURATION.labels(event_type=event.event_type).observe(duration)

        event.status = EventStatus.COMPLETED.value
        event.processed_at = datetime.now(timezone.utc)
        await session.commit()

        EVENTS_PROCESSED.labels(event_type=event.event_type, status="completed").inc()
        log.info("event_processed", duration_seconds=round(duration, 4))


async def startup(ctx):
    configure_logging()
    await init_db()
    ctx["session_factory"] = async_session_factory
    logger.info("worker_started")


async def shutdown(ctx):
    await close_redis_pool()
    logger.info("worker_stopped")


class WorkerSettings:
    functions = [process_event]
    on_startup = startup
    on_shutdown = shutdown
    redis_settings = RedisSettings.from_dsn(settings.redis_url)
    max_tries = 3
    max_jobs = 20
    job_timeout = 60
