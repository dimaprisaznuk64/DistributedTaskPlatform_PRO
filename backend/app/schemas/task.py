from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

TaskPayload = dict[str, Any]

TaskPriority = Literal["critical", "high", "normal", "low"]


class TaskCreate(BaseModel):
    task_type: str = Field(min_length=1, max_length=100)
    payload: TaskPayload = Field(default_factory=dict)
    priority: TaskPriority = "normal"
    max_attempts: int | None = Field(default=None, ge=1, le=10)
    idempotency_key: str | None = Field(default=None, max_length=255)
    schedule_at: datetime | None = Field(
        default=None, description="Запуск не раніше цього часу (UTC)"
    )
    depends_on: list[int] | None = Field(
        default=None,
        max_length=100,
        description="ID батьківських задач: запуститься, коли всі вони успішно завершаться",
    )


class AttemptInfo(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    attempt_number: int
    worker_id: str | None
    status: str
    error_message: str | None
    result: dict[str, Any] | None
    started_at: datetime | None
    finished_at: datetime | None


class TaskEventInfo(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    event_type: str
    old_status: str | None
    new_status: str
    metadata: dict[str, Any] | None = Field(default=None, validation_alias="details")
    created_at: datetime


class TaskInfo(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    task_type: str
    payload: dict[str, Any]
    status: str
    priority: str
    max_attempts: int
    attempts: int
    idempotency_key: str | None
    scheduled_at: datetime | None
    started_at: datetime | None
    finished_at: datetime | None
    result: dict[str, Any] | None
    last_error: str | None
    created_by: int | None
    batch_id: int | None = None
    dlq_requeue_count: int = 0
    dlq_retry_at: datetime | None = None
    created_at: datetime
    updated_at: datetime


class TaskDetail(TaskInfo):
    attempts_: list[AttemptInfo] = []
    events_: list[TaskEventInfo] = []


class TaskList(BaseModel):
    total: int
    items: list[TaskInfo]


class TaskEventTimeline(BaseModel):
    task_id: int
    events: list[TaskEventInfo]


class ErrorBody(BaseModel):
    detail: str


class RetryResult(BaseModel):
    task: TaskInfo


class CancelResult(BaseModel):
    task: TaskInfo


class CreateResult(BaseModel):
    task: TaskInfo


class BulkCreateRequest(BaseModel):
    tasks: list[TaskCreate] = Field(min_length=1, max_length=1000)


class BulkOperationItem(BaseModel):
    task_id: int
    reason: str = ""


class BulkOperationResult(BaseModel):
    total: int
    succeeded: list[TaskInfo]
    conflicts: list[BulkOperationItem]


class BulkIdsRequest(BaseModel):
    task_ids: list[int] = Field(min_length=1, max_length=1000)
