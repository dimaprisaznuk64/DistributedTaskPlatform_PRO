from __future__ import annotations

import logging
from datetime import UTC, datetime

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.core import metrics
from app.core.config import settings
from app.models.event import TaskEvent
from app.models.outbox import OutboxEvent
from app.models.task import Task

logger = logging.getLogger(__name__)

VALID_TRANSITIONS: dict[str, set[str]] = {
    "created": {"queued", "scheduled", "cancelled"},
    "queued": {"running", "cancelled"},
    "running": {"success", "failed", "retry_scheduled", "cancelled", "dead_letter", "queued"},
    "retry_scheduled": {"queued", "cancelled"},
    "scheduled": {"queued", "cancelled"},
    "failed": {"queued"},
    "cancelled": {"queued"},
    "dead_letter": {"queued"},
}

TERMINAL = {"success", "failed", "cancelled", "dead_letter"}
CANCELABLE = {"created", "queued", "running", "retry_scheduled", "scheduled"}
RETRYABLE = {"failed", "cancelled", "dead_letter"}


class InvalidTransitionError(Exception):
    pass


class TaskConflictError(Exception):
    pass


def can_transition(old_status: str, new_status: str) -> bool:
    return new_status in VALID_TRANSITIONS.get(old_status, set())


def compute_retry_delay_seconds(attempts: int) -> float:
    """Експоненціальний backoff: base * factor^(attempts-1), обмежений максимумом."""
    delay = settings.retry_base_seconds * (settings.retry_backoff_factor ** (attempts - 1))
    return min(delay, settings.retry_max_delay_seconds)


def _record_event(
    session: AsyncSession,
    task: Task,
    event_type: str,
    new_status: str,
    metadata: dict | None = None,
) -> None:
    session.add(
        TaskEvent(
            task_id=task.id,
            event_type=event_type,
            old_status=task.status,
            new_status=new_status,
            details=metadata,
        )
    )


async def set_status(
    session: AsyncSession,
    task: Task,
    new_status: str,
    *,
    event_type: str,
    metadata: dict | None = None,
) -> None:
    """Єдина точка зміни статусу; валідує перехід і пише подію."""
    if task.status == new_status:
        return
    if not can_transition(task.status, new_status):
        raise InvalidTransitionError(
            f"Перехід {task.status!r} -> {new_status!r} заборонений для задачі {task.id}"
        )
    if new_status in TERMINAL and task.finished_at is None:
        task.finished_at = datetime.now(UTC)
    if new_status == "running" and task.started_at is None:
        task.started_at = datetime.now(UTC)
    if new_status == "queued":
        task.finished_at = None
        task.scheduled_at = None
    if task.status == "running" and new_status != "running":
        task.lease_owner = None
        task.lease_expires_at = None
    _record_event(session, task, event_type, new_status, metadata)
    task.status = new_status
    await session.flush()


async def create_task(
    session: AsyncSession,
    *,
    task_type: str,
    payload: dict,
    priority: str = "normal",
    max_attempts: int | None = None,
    idempotency_key: str | None = None,
    created_by: int | None = None,
    schedule_at: datetime | None = None,
) -> Task:
    task = Task(
        task_type=task_type,
        payload=payload,
        priority=priority,
        max_attempts=max_attempts if max_attempts is not None else settings.task_max_attempts,
        idempotency_key=idempotency_key,
        scheduled_at=schedule_at,
        status="created",
        created_by=created_by,
    )
    session.add(task)
    await session.flush()
    metrics.tasks_created_total.inc()
    _record_event(session, task, "task.created", "created")
    if schedule_at is not None and schedule_at > datetime.now(UTC):
        task.scheduled_at = schedule_at
        await set_status(session, task, "scheduled", event_type="task.scheduled")
        return task
    await set_status(session, task, "queued", event_type="task.queued")
    session.add(
        OutboxEvent(
            routing_key=settings.routing_key_task_created,
            payload={
                "task_id": task.id,
                "task_type": task.task_type,
                "payload": task.payload,
                "priority": task.priority,
            },
        )
    )
    return task


async def get_task(session: AsyncSession, task_id: int) -> Task | None:
    stmt = (
        select(Task)
        .options(selectinload(Task.attempts_), selectinload(Task.events_))
        .where(Task.id == task_id)
    )
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def get_by_idempotency_key(session: AsyncSession, key: str) -> Task | None:
    stmt = select(Task).where(Task.idempotency_key == key)
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def list_tasks(
    session: AsyncSession,
    *,
    status: str | None = None,
    task_type: str | None = None,
    priority: str | None = None,
    limit: int = 20,
    offset: int = 0,
) -> list[Task]:
    stmt = select(Task).order_by(Task.id.desc()).limit(limit).offset(offset)
    if status:
        stmt = stmt.where(Task.status == status)
    if task_type:
        stmt = stmt.where(Task.task_type == task_type)
    if priority:
        stmt = stmt.where(Task.priority == priority)
    result = await session.execute(stmt)
    return list(result.scalars().all())


async def count_tasks(
    session: AsyncSession,
    *,
    status: str | None = None,
    task_type: str | None = None,
    priority: str | None = None,
) -> int:
    stmt = select(func.count(Task.id))
    if status:
        stmt = stmt.where(Task.status == status)
    if task_type:
        stmt = stmt.where(Task.task_type == task_type)
    if priority:
        stmt = stmt.where(Task.priority == priority)
    result = await session.execute(stmt)
    return int(result.scalar_one())


async def get_events(session: AsyncSession, task_id: int) -> list[TaskEvent]:
    stmt = select(TaskEvent).where(TaskEvent.task_id == task_id).order_by(TaskEvent.id.asc())
    result = await session.execute(stmt)
    return list(result.scalars().all())


async def manual_retry(session: AsyncSession, task_id: int) -> Task | None:
    task = await get_task(session, task_id)
    if task is None:
        return None
    if task.status not in RETRYABLE:
        raise TaskConflictError(
            f"Повтор дозволений лише для {sorted(RETRYABLE)}, статус: {task.status}"
        )
    await set_status(
        session,
        task,
        "queued",
        event_type="task.retried",
        metadata={"attempts_so_far": task.attempts},
    )
    session.add(
        OutboxEvent(
            routing_key=settings.routing_key_task_created,
            payload={
                "task_id": task.id,
                "task_type": task.task_type,
                "payload": task.payload,
                "priority": task.priority,
                "reattempt": True,
            },
        )
    )
    return task


async def cancel_task(session: AsyncSession, task_id: int) -> Task | None:
    task = await get_task(session, task_id)
    if task is None:
        return None
    if task.status not in CANCELABLE:
        raise TaskConflictError(f"Скасувати можна лише {sorted(CANCELABLE)}, статус: {task.status}")
    await set_status(session, task, "cancelled", event_type="task.cancelled")
    return task
