from prometheus_client import Counter, Histogram

# --- Ingestion-side metrics -------------------------------------------------

EVENTS_RECEIVED = Counter(
    "events_received_total",
    "Total number of events received by the API, before dedup",
    ["event_type"],
)

EVENTS_DUPLICATE = Counter(
    "events_duplicate_total",
    "Total number of events rejected as duplicates",
    ["event_type"],
)

EVENT_QUEUE_ENQUEUED = Counter(
    "events_enqueued_total",
    "Total number of events successfully enqueued for async processing",
    ["event_type"],
)

# --- Worker-side metrics -----------------------------------------------------

EVENTS_PROCESSED = Counter(
    "events_processed_total",
    "Total number of events that finished processing, by final status",
    ["event_type", "status"],
)

EVENTS_RETRIED = Counter(
    "events_retried_total",
    "Total number of event processing retries",
    ["event_type"],
)

EVENTS_DEAD_LETTER = Counter(
    "events_dead_letter_total",
    "Total number of events that exhausted retries and were dead-lettered",
    ["event_type"],
)

EVENT_PROCESSING_DURATION = Histogram(
    "event_processing_duration_seconds",
    "Time spent executing business logic for a single event",
    ["event_type"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
)
