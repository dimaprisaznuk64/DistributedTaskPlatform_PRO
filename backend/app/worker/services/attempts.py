from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sqlalchemy import or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core import metrics
from app.models.attempt import TaskAttempt
from app.models.task import Task
from app.services.tasks import (
    _record_event,
    compute_retry_delay_seconds,
    set_status,
)


def _now() -> datetime:
    return datetime.now(UTC)


def _record_execution_metrics(task: Task, attempt: TaskAttempt, outcome: str) -> None:
    metrics.attempts_total.labels(outcome=outcome).inc()
    if attempt.started_at is not None and attempt.finished_at is not None:
        duration = (attempt.finished_at - attempt.started_at).total_seconds()
        metrics.task_duration_seconds.labels(task_type=task.task_type).observe(duration)


async def _still_running(session: AsyncSession, task: Task) -> bool:
    """Перевіряє у БД, що задача досі running (її могли скасувати в процесі)."""
    current = await session.scalar(select(Task.status).where(Task.id == task.id))
    return current == "running"


async def try_acquire_lease(
    session: AsyncSession,
    task_id: int,
    worker_id: str,
    lease_seconds: float,
) -> bool:
    """Атомарно віддає задачу воркеру: status=queued і відсутній/прострочений lease."""
    result = await session.execute(
        update(Task)
        .where(
            Task.id == task_id,
            Task.status == "queued",
            or_(
                Task.lease_owner.is_(None),
                Task.lease_expires_at.is_(None),
                Task.lease_expires_at < _now(),
            ),
        )
        .values(
            lease_owner=worker_id,
            lease_expires_at=_now() + timedelta(seconds=lease_seconds),
        )
        .execution_options(synchronize_session=False)
    )
    return result.rowcount == 1


async def renew_lease(
    session: AsyncSession,
    task_id: int,
    worker_id: str,
    lease_seconds: float,
) -> bool:
    """Подовжує lease задачі, поки її все ще виконує цей воркер."""
    result = await session.execute(
        update(Task)
        .where(Task.id == task_id, Task.lease_owner == worker_id, Task.status == "running")
        .values(lease_expires_at=_now() + timedelta(seconds=lease_seconds))
        .execution_options(synchronize_session=False)
    )
    return result.rowcount == 1


async def start_attempt(session: AsyncSession, task: Task) -> TaskAttempt:
    """Позначає задачу running, збільшує лічильник, створює запис спроби."""
    await set_status(session, task, "running", event_type="task.running")
    attempt_number = task.attempts + 1
    task.attempts = attempt_number
    if task.started_at is None:
        task.started_at = _now()
    attempt = TaskAttempt(
        task_id=task.id,
        attempt_number=attempt_number,
        status="running",
        worker_id=None,  # проставляє робочий процес перед коммітом
        started_at=_now(),
    )
    session.add(attempt)
    await session.flush()
    _record_event(
        session,
        task,
        "task.attempt_started",
        "running",
        {"attempt_number": attempt_number},
    )
    return attempt


async def record_success(
    session: AsyncSession,
    task: Task,
    attempt: TaskAttempt,
    result: dict,
) -> None:
    attempt.status = "success"
    attempt.result = result
    attempt.finished_at = _now()
    if not await _still_running(session, task):
        await session.flush()
        return
    task.result = result
    task.finished_at = _now()
    await set_status(session, task, "success", event_type="task.succeeded")
    _record_execution_metrics(task, attempt, "success")


async def record_failure(
    session: AsyncSession,
    task: Task,
    attempt: TaskAttempt,
    error: str,
    *,
    retryable: bool = True,
) -> None:
    error_truncated = error[:2000]
    attempt.status = "failed"
    attempt.error_message = error_truncated
    attempt.finished_at = _now()
    if not await _still_running(session, task):
        await session.flush()
        return
    task.last_error = error_truncated
    if retryable and task.attempts < task.max_attempts:
        backoff = compute_retry_delay_seconds(task.attempts)
        scheduled = _now() + timedelta(seconds=backoff)
        task.scheduled_at = scheduled
        await set_status(
            session,
            task,
            "retry_scheduled",
            event_type="task.retry_scheduled",
            metadata={
                "attempt_number": attempt.attempt_number,
                "backoff_seconds": backoff,
                "retry_at": scheduled.isoformat(),
            },
        )
    else:
        new_status = "dead_letter" if retryable else "failed"
        await set_status(
            session,
            task,
            new_status,
            event_type="task.dead_lettered" if retryable else "task.failed",
            metadata={"attempt_number": attempt.attempt_number, "error": error_truncated},
        )
    _record_execution_metrics(task, attempt, task.status)
