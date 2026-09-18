from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy.ext.asyncio import AsyncSession

from app.models.attempt import TaskAttempt
from app.models.task import Task
from app.services.tasks import _record_event, set_status


def _now() -> datetime:
    return datetime.now(UTC)


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
    await set_status(session, task, "success", event_type="task.succeeded")
    attempt.status = "success"
    attempt.result = result
    attempt.finished_at = _now()
    task.result = result
    task.finished_at = _now()


async def record_failure(
    session: AsyncSession,
    task: Task,
    attempt: TaskAttempt,
    error: str,
) -> None:
    error_truncated = error[:2000]
    await set_status(
        session,
        task,
        "failed",
        event_type="task.failed",
        metadata={"attempt_number": attempt.attempt_number, "error": error_truncated},
    )
    attempt.status = "failed"
    attempt.error_message = error_truncated
    attempt.finished_at = _now()
    task.last_error = error_truncated
    task.finished_at = _now()
