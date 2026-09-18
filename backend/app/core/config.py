from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "DistributedTaskPlatform_PRO"
    app_env: str = "dev"
    debug: bool = False
    api_v1_prefix: str = "/api/v1"

    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/distributed_platform"
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"

    log_level: str = "INFO"
    outbox_poll_seconds: float = 2.0
    task_max_attempts: int = 3
    exchange_name: str = "tasks"
    routing_key_task_created: str = "task_created"
    worker_id: str = "worker-1"
    task_execution_timeout_seconds: int = 60


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
