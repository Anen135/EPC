from typing import Sequence

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from .models import Event, EventStatus
from .schemas import EventIn


async def insert_event(session: AsyncSession, event_in: EventIn) -> tuple[Event | None, bool]:
    """Insert a single event. Returns (event, is_new).

    Deduplication is done with a single atomic INSERT ... ON CONFLICT DO
    NOTHING against the unique index on event_id, rather than a
    check-then-insert. That avoids a race where two concurrent requests
    for the same event_id both pass a SELECT check and both try to insert.
    """
    stmt = (
        pg_insert(Event)
        .values(
            event_id=event_in.event_id,
            event_type=event_in.event_type,
            source=event_in.source,
            payload=event_in.payload,
            occurred_at=event_in.occurred_at,
            status=EventStatus.PENDING.value,
        )
        .on_conflict_do_nothing(index_elements=["event_id"])
        .returning(Event)
    )
    result = await session.execute(stmt)
    row = result.scalar_one_or_none()
    await session.commit()

    if row is not None:
        return row, True

    existing = await session.execute(select(Event).where(Event.event_id == event_in.event_id))
    return existing.scalar_one_or_none(), False


async def insert_events_batch(
    session: AsyncSession, events_in: Sequence[EventIn]
) -> tuple[list[Event], set[str]]:
    """Bulk insert with the same conflict-safe dedup semantics as
    insert_event. Duplicates against existing rows AND repeats of the same
    event_id within the batch itself are both reported as duplicates (only
    the first occurrence of a repeated event_id in the batch is inserted).
    Returns (inserted_events, duplicate_event_ids).
    """
    seen: set[str] = set()
    intra_batch_duplicates: set[str] = set()
    deduped_values = []
    for e in events_in:
        if e.event_id in seen:
            intra_batch_duplicates.add(e.event_id)
            continue
        seen.add(e.event_id)
        deduped_values.append(
            {
                "event_id": e.event_id,
                "event_type": e.event_type,
                "source": e.source,
                "payload": e.payload,
                "occurred_at": e.occurred_at,
                "status": EventStatus.PENDING.value,
            }
        )

    stmt = (
        pg_insert(Event)
        .values(deduped_values)
        .on_conflict_do_nothing(index_elements=["event_id"])
        .returning(Event)
    )
    result = await session.execute(stmt)
    inserted = list(result.scalars().all())
    await session.commit()

    inserted_ids = {e.event_id for e in inserted}
    duplicate_ids = (seen - inserted_ids) | intra_batch_duplicates
    return inserted, duplicate_ids


async def get_event_by_event_id(session: AsyncSession, event_id: str) -> Event | None:
    result = await session.execute(select(Event).where(Event.event_id == event_id))
    return result.scalar_one_or_none()
