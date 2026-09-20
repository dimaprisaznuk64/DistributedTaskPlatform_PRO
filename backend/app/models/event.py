from __future__ import annotations

from datetime import datetime

from sqlalchemy import JSON, DateTime, ForeignKey, String, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base

TASK_EVENT_TYPES = (
    "task.created",
    "task.queued",
    "task.scheduled",
    "task.invalidated",
    "task.running",
    "task.attempt_started",
    "task.attempt_failed",
    "task.succeeded",
    "task.failed",
    "task.cancelled",
    "task.retried",
    "task.retry_scheduled",
    "task.requeued",
    "task.dead_lettered",
    "task.dlq_requeued",
)


class TaskEvent(Base):
    __tablename__ = "task_events"

    id: Mapped[int] = mapped_column(primary_key=True)
    task_id: Mapped[int] = mapped_column(ForeignKey("tasks.id", ondelete="CASCADE"), index=True)
    event_type: Mapped[str] = mapped_column(String(50), nullable=False)
    old_status: Mapped[str | None] = mapped_column(String(20))
    new_status: Mapped[str] = mapped_column(String(20), nullable=False)
    details: Mapped[dict | None] = mapped_column("metadata", JSON)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    task: Mapped[Task] = relationship("Task", back_populates="events_")  # noqa: F821
