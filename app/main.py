import time
import uuid
from contextlib import asynccontextmanager

import structlog
from fastapi import Depends, FastAPI, HTTPException, Request, Response
from prometheus_fastapi_instrumentator import Instrumentator
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from .config import settings
from .crud import get_event_by_event_id, insert_event, insert_events_batch
from .database import get_db, init_db
from .logging_config import configure_logging, get_logger
from .metrics import EVENT_QUEUE_ENQUEUED, EVENTS_DUPLICATE, EVENTS_RECEIVED
from .queue import close_redis_pool, enqueue_process_event, get_redis_pool
from .schemas import BatchIn, BatchIngestResponse, EventIn, EventOut, IngestResult

configure_logging()
logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    await get_redis_pool()
    logger.info("service_started", app=settings.app_name)
    yield
    await close_redis_pool()
    logger.info("service_stopped")


app = FastAPI(title=settings.app_name, lifespan=lifespan)
Instrumentator().instrument(app).expose(app, include_in_schema=False)


@app.middleware("http")
async def request_context_middleware(request: Request, call_next):
    request_id = request.headers.get("x-request-id", str(uuid.uuid4()))
    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(
        request_id=request_id, path=request.url.path, method=request.method
    )

    start = time.monotonic()
    try:
        response: Response = await call_next(request)
    except Exception:
        logger.exception("unhandled_exception")
        raise
    duration = time.monotonic() - start

    response.headers["x-request-id"] = request_id
    logger.info(
        "request_completed",
        status_code=response.status_code,
        duration_seconds=round(duration, 4),
    )
    return response


@app.get("/health")
async def health(db: AsyncSession = Depends(get_db)):
    try:
        await db.execute(text("SELECT 1"))
        db_ok = True
    except Exception:
        db_ok = False

    try:
        pool = await get_redis_pool()
        await pool.ping()
        redis_ok = True
    except Exception:
        redis_ok = False

    payload = {"status": "ok" if (db_ok and redis_ok) else "degraded", "database": db_ok, "redis": redis_ok}
    if not (db_ok and redis_ok):
        raise HTTPException(status_code=503, detail=payload)
    return payload


@app.post("/events", response_model=IngestResult, status_code=202)
async def ingest_event(event_in: EventIn, db: AsyncSession = Depends(get_db)):
    EVENTS_RECEIVED.labels(event_type=event_in.event_type).inc()

    event, is_new = await insert_event(db, event_in)
    if event is None:
        raise HTTPException(status_code=500, detail="failed to persist event")

    if not is_new:
        EVENTS_DUPLICATE.labels(event_type=event_in.event_type).inc()
        logger.info("duplicate_event_rejected", event_id=event_in.event_id)
        return IngestResult(event_id=event_in.event_id, status="duplicate", id=event.id)

    await enqueue_process_event(event.id)
    EVENT_QUEUE_ENQUEUED.labels(event_type=event_in.event_type).inc()
    logger.info("event_accepted", event_id=event_in.event_id, db_id=str(event.id))
    return IngestResult(event_id=event_in.event_id, status="accepted", id=event.id)


@app.post("/events/batch", response_model=BatchIngestResponse, status_code=202)
async def ingest_events_batch(batch: BatchIn, db: AsyncSession = Depends(get_db)):
    for e in batch.events:
        EVENTS_RECEIVED.labels(event_type=e.event_type).inc()

    inserted, duplicate_ids = await insert_events_batch(db, batch.events)

    for event in inserted:
        await enqueue_process_event(event.id)
        EVENT_QUEUE_ENQUEUED.labels(event_type=event.event_type).inc()

    for dup_id in duplicate_ids:
        dup_event_type = next((e.event_type for e in batch.events if e.event_id == dup_id), "unknown")
        EVENTS_DUPLICATE.labels(event_type=dup_event_type).inc()

    results = [IngestResult(event_id=e.event_id, status="accepted", id=e.id) for e in inserted]
    results += [IngestResult(event_id=eid, status="duplicate", id=None) for eid in duplicate_ids]

    logger.info("batch_ingested", accepted=len(inserted), duplicates=len(duplicate_ids))
    return BatchIngestResponse(accepted=len(inserted), duplicates=len(duplicate_ids), results=results)


@app.get("/events/{event_id}", response_model=EventOut)
async def get_event_status(event_id: str, db: AsyncSession = Depends(get_db)):
    event = await get_event_by_event_id(db, event_id)
    if event is None:
        raise HTTPException(status_code=404, detail="event not found")
    return event
