from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from app.schemas.task import TaskCreate, TaskInfo


class BatchCreateRequest(BaseModel):
    name: str = Field(default="", max_length=255)
    tasks: list[TaskCreate] = Field(min_length=1, max_length=1000)


class BatchProgress(BaseModel):
    total: int = 0
    queued: int = 0
    running: int = 0
    succeeded: int = 0
    failed: int = 0
    cancelled: int = 0
    pending: int = 0
    completed: int = 0


class BatchInfo(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    name: str
    created_by: int | None
    created_at: datetime
    updated_at: datetime
    progress: BatchProgress = BatchProgress()


class BatchCreateResult(BaseModel):
    batch: BatchInfo
    created: list[TaskInfo]


class BatchTaskList(BaseModel):
    batch: BatchInfo
    total: int
    items: list[TaskInfo]


class BatchList(BaseModel):
    total: int
    items: list[BatchInfo]


class BatchActionResult(BaseModel):
    batch: BatchInfo
    actions: list[dict[str, Any]]
