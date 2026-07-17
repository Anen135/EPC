import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from .config import settings


class EventIn(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    event_id: str = Field(..., min_length=1, max_length=255)
    event_type: str = Field(..., min_length=1, max_length=128)
    source: str = Field(..., min_length=1, max_length=128)
    payload: dict[str, Any] = Field(default_factory=dict)
    occurred_at: datetime | None = None

    @field_validator("event_id")
    @classmethod
    def event_id_no_whitespace(cls, v: str) -> str:
        if not v or v.isspace():
            raise ValueError("event_id must not be blank")
        return v

    @field_validator("payload")
    @classmethod
    def validate_payload_size(cls, v: dict[str, Any]) -> dict[str, Any]:
        size = len(json.dumps(v).encode("utf-8"))
        if size > settings.max_payload_bytes:
            raise ValueError(f"payload exceeds max size of {settings.max_payload_bytes} bytes (got {size})")
        return v

    @field_validator("occurred_at")
    @classmethod
    def validate_occurred_at(cls, v: datetime | None) -> datetime | None:
        if v is None:
            return v
        if v.tzinfo is None:
            v = v.replace(tzinfo=timezone.utc)
        if v > datetime.now(timezone.utc) + timedelta(days=1):
            raise ValueError("occurred_at cannot be more than 1 day in the future")
        return v


class BatchIn(BaseModel):
    events: list[EventIn] = Field(..., min_length=1)

    @field_validator("events")
    @classmethod
    def validate_batch_size(cls, v: list[EventIn]) -> list[EventIn]:
        if len(v) > settings.max_batch_size:
            raise ValueError(f"batch exceeds max size of {settings.max_batch_size} events")
        return v


class IngestResult(BaseModel):
    event_id: str
    status: str
    id: uuid.UUID | None = None


class BatchIngestResponse(BaseModel):
    accepted: int
    duplicates: int
    results: list[IngestResult]


class EventOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    event_id: str
    event_type: str
    source: str
    status: str
    received_at: datetime
    processing_started_at: datetime | None
    processed_at: datetime | None
    retry_count: int
    error_message: str | None
