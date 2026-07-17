"""Integration tests against a live stack.

These require Postgres + Redis to be reachable (e.g. `docker compose up -d
postgres redis` from the project root) and the app importable with a
correctly configured EVENTSVC_DATABASE_URL / EVENTSVC_REDIS_URL.

Run with:  pytest -m integration
Skip in CI without infra with:  pytest -m "not integration"
"""

import uuid

import pytest
from httpx import ASGITransport, AsyncClient

from app.main import app

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_ingest_and_fetch_event():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        event_id = f"evt-{uuid.uuid4()}"
        resp = await client.post(
            "/events",
            json={
                "event_id": event_id,
                "event_type": "order.created",
                "source": "checkout-api",
                "payload": {"amount": 42},
            },
        )
        assert resp.status_code == 202
        body = resp.json()
        assert body["status"] == "accepted"

        status_resp = await client.get(f"/events/{event_id}")
        assert status_resp.status_code == 200


@pytest.mark.asyncio
async def test_duplicate_event_rejected():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        event_id = f"evt-{uuid.uuid4()}"
        payload = {
            "event_id": event_id,
            "event_type": "order.created",
            "source": "checkout-api",
            "payload": {"amount": 42},
        }
        first = await client.post("/events", json=payload)
        second = await client.post("/events", json=payload)

        assert first.json()["status"] == "accepted"
        assert second.json()["status"] == "duplicate"


@pytest.mark.asyncio
async def test_batch_with_internal_duplicates():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        event_id = f"evt-{uuid.uuid4()}"
        events = [
            {"event_id": event_id, "event_type": "order.created", "source": "checkout-api", "payload": {}},
            {"event_id": event_id, "event_type": "order.created", "source": "checkout-api", "payload": {}},
        ]
        resp = await client.post("/events/batch", json={"events": events})
        body = resp.json()
        assert body["accepted"] == 1
        assert body["duplicates"] == 1
