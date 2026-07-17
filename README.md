# Event Processing Service

An HTTP ingestion service for events that validates, deduplicates, queues,
and asynchronously processes them, with metrics and structured logging
throughout.

**Stack:** FastAPI · PostgreSQL (SQLAlchemy 2.0 async / asyncpg) · Redis +
arq (queue/worker) · Prometheus · structlog

## Architecture

```
                 ┌─────────────┐        ┌───────────┐
  POST /events → │  FastAPI    │──────→ │ PostgreSQL│  (events table,
                 │  (validate, │  write │           │   event_id UNIQUE)
                 │   dedup)    │        └───────────┘
                 └──────┬──────┘
                        │ enqueue job (db id)
                        ▼
                 ┌─────────────┐
                 │ Redis (arq) │
                 └──────┬──────┘
                        │
                        ▼
                 ┌─────────────┐        ┌───────────┐
                 │ arq worker  │──────→ │ PostgreSQL│  (status transitions,
                 │ (business   │        │           │   retry_count)
                 │  logic)     │        └───────────┘
                 └─────────────┘
```

**Why this shape:**

- **Validation** happens at the Pydantic layer before anything touches the
  database or queue — bad event_type, oversized payloads, blank ids, and
  batches over the size limit are all rejected with a 422 before they can
  cause downstream problems.
- **Deduplication** is enforced by a `UNIQUE` constraint on `event_id` and
  an atomic `INSERT ... ON CONFLICT DO NOTHING RETURNING`, not a
  check-then-insert. That makes it race-safe under concurrent requests for
  the same `event_id` — no two requests can both "win" the insert.
- **Async processing** is decoupled from the request path: the API only
  inserts a row and enqueues a job; a separate worker process does the
  actual work, so ingestion stays fast and the API doesn't fall over if
  processing is slow. Workers use `SELECT ... FOR UPDATE SKIP LOCKED` when
  loading the event, so multiple worker replicas can run safely in
  parallel without double-processing.
- **Retries & dead-lettering**: failures are retried with linear backoff
  up to `EVENTSVC_MAX_RETRIES` times (tracked in the `events.retry_count`
  column), then the event is moved to `dead_letter` status for manual
  inspection instead of being retried forever.
- **Metrics** cover both HTTP-level behavior (via
  `prometheus-fastapi-instrumentator`) and business-level behavior
  (events received / duplicated / enqueued / processed / retried /
  dead-lettered, plus a processing-duration histogram), all exposed on
  `/metrics`.
- **Logging** is structured JSON (via `structlog`) with a request ID bound
  to every log line for a given HTTP request, and event/db IDs bound for
  every log line the worker emits about a given event — so you can grep
  one request or one event's whole lifecycle out of aggregated logs.

## Quickstart

```bash
docker compose up --build
```

This starts Postgres, Redis, the API (`:8000`), and one worker replica.
Scale workers with `docker compose up --scale worker=3`.

```bash
# Ingest a single event
curl -X POST http://localhost:8000/events \
  -H "Content-Type: application/json" \
  -d '{
        "event_id": "evt-001",
        "event_type": "order.created",
        "source": "checkout-api",
        "payload": {"amount": 42, "currency": "USD"}
      }'
# -> {"event_id":"evt-001","status":"accepted","id":"..."}

# Same event again -> flagged as a duplicate, not reprocessed
curl -X POST http://localhost:8000/events -H "Content-Type: application/json" \
  -d '{"event_id":"evt-001","event_type":"order.created","source":"checkout-api","payload":{}}'
# -> {"event_id":"evt-001","status":"duplicate","id":"..."}

# Check status once the worker has picked it up
curl http://localhost:8000/events/evt-001

# Batch ingest
curl -X POST http://localhost:8000/events/batch -H "Content-Type: application/json" \
  -d '{"events":[{"event_id":"evt-100","event_type":"user.signup","source":"web","payload":{}}]}'

curl http://localhost:8000/health
curl http://localhost:8000/metrics
```

## Running locally without Docker

```bash
pip install -r requirements.txt
cp .env.example .env   # edit if your Postgres/Redis aren't on localhost defaults

# Terminal 1
uvicorn app.main:app --reload

# Terminal 2
arq app.worker.WorkerSettings
```

Tables are created automatically on startup via `init_db()`. For a real
production rollout, swap that for versioned Alembic migrations.

## API

| Method | Path             | Description                                             |
|--------|------------------|-----------------------------------------------------------|
| POST   | `/events`        | Ingest one event. `202` on accept/duplicate, `422` on invalid input. |
| POST   | `/events/batch`  | Ingest up to `EVENTSVC_MAX_BATCH_SIZE` events at once. |
| GET    | `/events/{event_id}` | Look up an event's current status. `404` if unknown. |
| GET    | `/health`        | Checks DB + Redis connectivity. `503` if either is down. |
| GET    | `/metrics`       | Prometheus exposition format. |
| GET    | `/docs`          | Interactive OpenAPI docs (Swagger UI). |

### Event schema (`POST /events`)

```jsonc
{
  "event_id": "evt-001",       // required, 1-255 chars, your idempotency key
  "event_type": "order.created", // required, 1-128 chars
  "source": "checkout-api",     // required, 1-128 chars
  "payload": {"amount": 42},    // required (default {}), max 256 KB as JSON
  "occurred_at": "2026-07-17T10:00:00Z" // optional, can't be >1 day in the future
}
```

### Event status lifecycle

`pending` → `processing` → `completed`
                        ↘ (failure, retries remain) → back to `pending`
                        ↘ (failure, retries exhausted) → `dead_letter`

## Configuration

All settings are env vars prefixed `EVENTSVC_` (see `.env.example`):

| Variable | Default | Meaning |
|---|---|---|
| `EVENTSVC_DATABASE_URL` | `postgresql+asyncpg://postgres:postgres@localhost:5432/events` | Postgres connection string |
| `EVENTSVC_REDIS_URL` | `redis://localhost:6379/0` | Redis connection string (used as the arq broker) |
| `EVENTSVC_LOG_LEVEL` | `INFO` | Python logging level |
| `EVENTSVC_LOG_JSON` | `true` | JSON logs (prod) vs. pretty console logs (dev) |
| `EVENTSVC_MAX_BATCH_SIZE` | `1000` | Max events per `/events/batch` call |
| `EVENTSVC_MAX_PAYLOAD_BYTES` | `262144` | Max serialized payload size per event |
| `EVENTSVC_MAX_RETRIES` | `3` | Business-logic retries before dead-lettering |
| `EVENTSVC_RETRY_DEFER_SECONDS` | `5` | Backoff unit; deferred by `n * retry_count` seconds |

## Extending it

- **Real processing logic**: replace the placeholder in
  `app/worker.py:business_logic()`. Raise any exception for a retryable
  failure; it'll be retried with backoff and eventually dead-lettered.
- **Migrations**: add Alembic once the schema needs to evolve without
  `create_all`'s limitations (no column alters/drops).
- **Auth**: none is included by default — add an API key or JWT
  dependency on the ingestion routes before exposing this publicly.
- **Dead-letter recovery**: query `WHERE status = 'dead_letter'` and
  build an admin endpoint or script to requeue after fixing the root
  cause.

## Testing

```bash
# Pure validation logic, no infra required
pytest -m "not integration"

# Full flow against a live stack
docker compose up -d postgres redis
pytest -m integration
```
