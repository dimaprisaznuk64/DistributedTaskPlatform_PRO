from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict


class WorkerInfo(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    worker_id: str
    hostname: str
    pid: int
    status: str
    started_at: datetime
    last_heartbeat_at: datetime
    completed_tasks: int
    failed_tasks: int
