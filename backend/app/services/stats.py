from __future__ import annotations

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.attempt import ATTEMPT_STATUSES, TaskAttempt
from app.models.task import TASK_PRIORITIES, TASK_STATUSES, Task
from app.models.worker import WORKER_STATUSES, Worker


def _counts_by(rows) -> dict[str, int]:
    return {label: int(count or 0) for label, count in rows}


async def build_stats(session: AsyncSession) -> dict:
    by_status_rows = (
        await session.execute(
            select(Task.status, func.count(Task.id)).group_by(Task.status)
        )
    ).all()
    by_priority_rows = (
        await session.execute(
            select(Task.priority, func.count(Task.id)).group_by(Task.priority)
        )
    ).all()
    by_type_rows = (
        await session.execute(
            select(Task.task_type, func.count(Task.id)).group_by(Task.task_type)
        )
    ).all()
    by_attempt_rows = (
        await session.execute(
            select(TaskAttempt.status, func.count(TaskAttempt.id)).group_by(
                TaskAttempt.status
            )
        )
    ).all()
    worker_rows = (
        await session.execute(
            select(Worker.status, func.count(Worker.id)).group_by(Worker.status)
        )
    ).all()

    attempt_statuses = {s: 0 for s in ATTEMPT_STATUSES}
    attempt_statuses.update(_counts_by(by_attempt_rows))
    workers = {s: 0 for s in WORKER_STATUSES}
    workers.update(_counts_by(worker_rows))

    by_status = {s: 0 for s in TASK_STATUSES}
    by_status.update(_counts_by(by_status_rows))
    by_priority = {p: 0 for p in TASK_PRIORITIES}
    by_priority.update(_counts_by(by_priority_rows))
    by_type = _counts_by(by_type_rows)

    return {
        "tasks": {
            "total": sum(by_status.values()),
            "by_status": by_status,
            "by_priority": by_priority,
            "by_type": by_type,
        },
        "attempts": {
            "total": sum(attempt_statuses.values()),
            "by_status": attempt_statuses,
        },
        "workers": {
            "total": sum(workers.values()),
            "by_status": workers,
        },
    }
