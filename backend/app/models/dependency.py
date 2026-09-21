from __future__ import annotations

from sqlalchemy import ForeignKey, Index
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class TaskDependency(Base):
    """Ребро графа залежностей: child_task_id стартує після parent_task_id.

    Дитина в status=created (bлокована), поки всі батьки не завершаться успішно;
    якщо хтось із батьків завершується невдало — дитина скасовується.
    """

    __tablename__ = "task_dependencies"

    parent_task_id: Mapped[int] = mapped_column(
        ForeignKey("tasks.id", ondelete="CASCADE"), primary_key=True
    )
    child_task_id: Mapped[int] = mapped_column(
        ForeignKey("tasks.id", ondelete="CASCADE"), primary_key=True
    )

    __table_args__ = (Index("ix_task_dependencies_child", "child_task_id"),)
