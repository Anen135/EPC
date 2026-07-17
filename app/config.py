from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "event-processing-service"

    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/events"
    db_pool_size: int = 10
    db_max_overflow: int = 20

    redis_url: str = "redis://localhost:6379/0"

    log_level: str = "INFO"
    log_json: bool = True

    max_batch_size: int = 1000
    max_payload_bytes: int = 256 * 1024

    max_retries: int = 3
    retry_defer_seconds: int = 5

    model_config = SettingsConfigDict(env_file=".env", env_prefix="EVENTSVC_", extra="ignore")


settings = Settings()
