# Event Processing Service

Small FastAPI service for ingesting events, storing them in Postgres, and
processing them asynchronously through Redis/arq.

## Stack

- FastAPI
- PostgreSQL with SQLAlchemy async and asyncpg
- Redis + arq
- Prometheus metrics
- structlog

## Layout

```text
app/                  service code
  main.py             FastAPI app and routes
  worker.py           arq worker
  models.py           SQLAlchemy models
  schemas.py          Pydantic schemas
  crud.py             database writes/queries
  queue.py            Redis/arq enqueue helpers
tests/                pytest tests
```

## Run with Docker

```bash
docker compose up --build
```

API: `http://localhost:8000`

Useful endpoints:

- `POST /events`
- `POST /events/batch`
- `GET /events/{event_id}`
- `GET /health`
- `GET /metrics`
- `GET /docs`

## Run locally

```bash
pip install -r requirements.txt
cp .env.example .env
uvicorn app.main:app --reload
```

Start the worker in a second terminal:

```bash
arq app.worker.WorkerSettings
```

## Example

```bash
curl -X POST http://localhost:8000/events \
  -H "Content-Type: application/json" \
  -d '{
        "event_id": "evt-001",
        "event_type": "order.created",
        "source": "checkout-api",
        "payload": {"amount": 42, "currency": "USD"}
      }'
```

Duplicate `event_id` values are accepted as duplicates and are not queued again.

## Configuration

All settings use the `EVENTSVC_` prefix. See `.env.example`.

| Variable | Default |
|---|---|
| `EVENTSVC_DATABASE_URL` | `postgresql+asyncpg://postgres:postgres@localhost:5432/events` |
| `EVENTSVC_REDIS_URL` | `redis://localhost:6379/0` |
| `EVENTSVC_LOG_LEVEL` | `INFO` |
| `EVENTSVC_LOG_JSON` | `true` |
| `EVENTSVC_MAX_BATCH_SIZE` | `1000` |
| `EVENTSVC_MAX_PAYLOAD_BYTES` | `262144` |
| `EVENTSVC_MAX_RETRIES` | `3` |
| `EVENTSVC_RETRY_DEFER_SECONDS` | `5` |

## Tests

```bash
pytest -m "not integration"
```

Integration tests need Postgres and Redis:

```bash
docker compose up -d postgres redis
pytest -m integration
```
