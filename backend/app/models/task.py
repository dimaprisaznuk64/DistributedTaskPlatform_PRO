from __future__ import annotations

from datetime import datetime

from sqlalchemy import JSON, DateTime, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base, TimestampMixin

TASK_STATUSES = (
    "created",
    "queued",
    "running",
    "retry_scheduled",
    "scheduled",
    "success",
    "failed",
    "cancelled",
    "dead_letter",
    "timeout",
)

TASK_PRIORITIES = ("critical", "high", "normal", "low")


class Task(Base, TimestampMixin):
    __tablename__ = "tasks"

    id: Mapped[int] = mapped_column(primary_key=True)
    task_type: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    payload: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="created", index=True)
    priority: Mapped[str] = mapped_column(String(20), nullable=False, default="normal")
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=3)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    idempotency_key: Mapped[str | None] = mapped_column(String(255), unique=True, index=True)
    scheduled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    lease_owner: Mapped[str | None] = mapped_column(String(100))
    lease_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), index=True
    )
    result: Mapped[dict | None] = mapped_column(JSON)
    last_error: Mapped[str | None] = mapped_column(String(2000))
    created_by: Mapped[int | None] = mapped_column(Integer, nullable=True)

    attempts_: Mapped[list[TaskAttempt]] = relationship(  # noqa: F821
        "TaskAttempt",
        back_populates="task",
        cascade="all, delete-orphan",
        order_by="TaskAttempt.attempt_number",
    )
    events_: Mapped[list[TaskEvent]] = relationship(  # noqa: F821
        "TaskEvent",
        back_populates="task",
        cascade="all, delete-orphan",
        order_by="TaskEvent.id",
    )
