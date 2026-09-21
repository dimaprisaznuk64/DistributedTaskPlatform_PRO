from __future__ import annotations

from sqlalchemy import Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base, TimestampMixin

BATCH_STATUSES = ("running", "partial_failed", "complete", "cancelled")


class TaskBatch(Base, TimestampMixin):
    __tablename__ = "task_batches"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    created_by: Mapped[int | None] = mapped_column(Integer, nullable=True)
