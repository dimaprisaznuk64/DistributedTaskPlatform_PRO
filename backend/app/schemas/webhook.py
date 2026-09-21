from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class WebhookCreateRequest(BaseModel):
    name: str = Field(default="", max_length=255)
    url: str = Field(min_length=1, max_length=2048)
    events: list[str] = Field(default_factory=lambda: ["*"])
    secret: str | None = Field(default=None, max_length=512)


class WebhookUpdateRequest(BaseModel):
    name: str | None = Field(default=None, max_length=255)
    url: str | None = Field(default=None, max_length=2048)
    events: list[str] | None = None
    is_active: bool | None = None
    secret: str | None = Field(default=None, max_length=512)


class WebhookInfo(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    name: str
    url: str
    events: list[str]
    is_active: bool
    created_at: datetime
    updated_at: datetime


class DeliveryInfo(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    subscription_id: int
    event_type: str
    task_id: int | None
    status: str
    attempts: int
    next_retry_at: datetime | None
    last_error: str | None
    created_at: datetime


class DeliveryList(BaseModel):
    total: int
    items: list[DeliveryInfo]


class TestResult(BaseModel):
    ok: bool
    detail: str
    status_code: int | None = None
    deliveries_created: int = 0


class WebhookEventPayload(BaseModel):
    id: int
    event: str
    subscription_id: int
    task: dict[str, Any]
    timestamp: datetime
