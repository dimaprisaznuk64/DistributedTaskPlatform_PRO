from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "DistributedTaskPlatform_PRO"
    app_env: str = "dev"
    debug: bool = False
    api_v1_prefix: str = "/api/v1"

    cors_origins: str = "*"
    queued_reconcile_seconds: float = 60.0

    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/distributed_platform"
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"
    redis_url: str = "redis://localhost:6379/0"

    log_level: str = "INFO"
    log_json: bool = False
    outbox_poll_seconds: float = 2.0
    task_max_attempts: int = 3
    task_execution_timeout_seconds: int = 60

    retry_base_seconds: float = 1.0
    retry_backoff_factor: float = 2.0
    retry_max_delay_seconds: float = 300.0
    retry_scheduler_poll_seconds: float = 2.0

    worker_id: str = "worker-1"
    worker_heartbeat_seconds: float = 15.0
    worker_heartbeat_timeout_seconds: float = 60.0
    task_lease_seconds: float = 60.0

    exchange_name: str = "tasks"
    routing_key_task_created: str = "task_created"
    queue_name: str = "task_executions"
    queue_max_priority: int = 10

    redis_events_enabled: bool = True
    events_channel: str = "task_platform.events"

    @property
    def cors_origins_list(self) -> list[str]:
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
