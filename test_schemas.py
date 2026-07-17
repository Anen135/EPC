import pytest
from pydantic import ValidationError

from app.config import settings
from app.schemas import BatchIn, EventIn


def make_event(**overrides):
    base = dict(event_id="evt-1", event_type="order.created", source="checkout-api", payload={"amount": 10})
    base.update(overrides)
    return base


def test_valid_event_passes():
    event = EventIn(**make_event())
    assert event.event_id == "evt-1"
    assert event.event_type == "order.created"


def test_blank_event_id_rejected():
    with pytest.raises(ValidationError):
        EventIn(**make_event(event_id="   "))


def test_empty_event_type_rejected():
    with pytest.raises(ValidationError):
        EventIn(**make_event(event_type=""))


def test_oversized_payload_rejected():
    big_payload = {"blob": "x" * (settings.max_payload_bytes + 1)}
    with pytest.raises(ValidationError):
        EventIn(**make_event(payload=big_payload))


def test_far_future_occurred_at_rejected():
    from datetime import datetime, timedelta, timezone

    future = datetime.now(timezone.utc) + timedelta(days=30)
    with pytest.raises(ValidationError):
        EventIn(**make_event(occurred_at=future))


def test_batch_requires_at_least_one_event():
    with pytest.raises(ValidationError):
        BatchIn(events=[])


def test_batch_rejects_oversized_batch():
    events = [make_event(event_id=f"evt-{i}") for i in range(settings.max_batch_size + 1)]
    with pytest.raises(ValidationError):
        BatchIn(events=events)


def test_batch_accepts_valid_events():
    events = [make_event(event_id=f"evt-{i}") for i in range(5)]
    batch = BatchIn(events=events)
    assert len(batch.events) == 5
