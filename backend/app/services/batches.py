from __future__ import annotations

from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.models.batch import TaskBatch
from app.models.task import TASK_STATUSES, Task
from app.services import tasks as tasks_service
from app.services.tasks import TaskConflictError

BATCH_PROGRESS_KEYS = ("queued", "running", "succeeded", "failed", "cancelled", "pending")


class BatchLimitError(Exception):
    pass


async def create_batch(
    session: AsyncSession,
    *,
    name: str,
    specs: list[dict],
    created_by: int | None,
) -> tuple[TaskBatch, list[Task]]:
    if len(specs) > settings.batch_max_tasks:
        raise BatchLimitError(
            f"Забагато задач у батчі: {len(specs)} > {settings.batch_max_tasks}"
        )
    batch = TaskBatch(name=name, created_by=created_by)
    session.add(batch)
    await session.flush()
    created: list[Task] = []
    for spec in specs:
        task = await tasks_service.create_task(
            session,
            task_type=spec["task_type"],
            payload=spec["payload"],
            priority=spec["priority"],
            max_attempts=spec["max_attempts"],
            idempotency_key=spec["idempotency_key"],
            schedule_at=spec["schedule_at"],
            batch_id=batch.id,
            depends_on=spec.get("depends_on"),
            created_by=created_by,
        )
        created.append(task)
    await session.flush()
    return batch, created


async def get_batch(session: AsyncSession, batch_id: int) -> TaskBatch | None:
    return await session.get(TaskBatch, batch_id)


async def get_progress(session: AsyncSession, batch_id: int) -> dict[str, int]:
    rows = (
        await session.execute(
            select(Task.status, func.count(Task.id))
            .where(Task.batch_id == batch_id)
            .group_by(Task.status)
        )
    ).all()
    counts = dict(rows)
    pending = sum(
        counts.get(s, 0)
        for s in ("created", "retry_scheduled", "scheduled", "dead_letter")
        if s != "success"
    )
    queued = counts.get("queued", 0)
    running = counts.get("running", 0)
    succeeded = counts.get("success", 0)
    failed = counts.get("failed", 0) + counts.get("timeout", 0)
    cancelled = counts.get("cancelled", 0)
    total = sum(counts.get(s, 0) for s in TASK_STATUSES)
    completed = succeeded + failed + cancelled
    return {
        "total": total,
        "queued": queued,
        "running": running,
        "succeeded": succeeded,
        "failed": failed,
        "cancelled": cancelled,
        "pending": max(pending, 0),
        "completed": completed,
    }


def build_info(batch: TaskBatch, progress: dict[str, int]) -> dict[str, Any]:
    return {
        "id": batch.id,
        "name": batch.name,
        "created_by": batch.created_by,
        "created_at": batch.created_at,
        "updated_at": batch.updated_at,
        "progress": progress,
    }


async def list_batches(
    session: AsyncSession, *, limit: int = 20, offset: int = 0
) -> list[TaskBatch]:
    stmt = select(TaskBatch).order_by(TaskBatch.id.desc()).limit(limit).offset(offset)
    rows = (await session.execute(stmt)).scalars()
    return list(rows)


async def count_batches(session: AsyncSession) -> int:
    result = await session.execute(select(func.count(TaskBatch.id)))
    return int(result.scalar_one())


async def list_batch_tasks(
    session: AsyncSession, *, batch_id: int, limit: int, offset: int
) -> tuple[list[Task], int]:
    base = select(Task).where(Task.batch_id == batch_id)
    count = int(
        (
            await session.execute(select(func.count(Task.id)).where(Task.batch_id == batch_id))
        ).scalar_one()
    )
    rows = (
        await session.execute(base.order_by(Task.id.desc()).limit(limit).offset(offset))
    ).scalars()
    return list(rows), count


async def cancel_batch(session: AsyncSession, batch_id: int) -> tuple[int, list[dict]]:
    """Скасовує всі незавершені задачі батча; повертає (кількість, конфлікти)."""
    batch = await get_batch(session, batch_id)
    if batch is None:
        raise TaskConflictError("Батч не знайдено")
    rows = (
        await session.execute(select(Task).where(Task.batch_id == batch_id))
    ).scalars()
    cancelled = 0
    conflicts: list[dict] = []
    for task in rows:
        try:
            await tasks_service.cancel_task(session, task.id)
            cancelled += 1
        except TaskConflictError as exc:
            conflicts.append({"task_id": task.id, "reason": str(exc)})
    return cancelled, conflicts
