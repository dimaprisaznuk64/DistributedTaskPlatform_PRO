from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta

from sqlalchemy import or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core import metrics
from app.core.config import settings
from app.db.session import session_factory
from app.models.attempt import TaskAttempt
from app.models.outbox import OutboxEvent
from app.models.task import Task
from app.models.worker import Worker
from app.services import events as events_service
from app.services import tasks as tasks_service

logger = logging.getLogger(__name__)

DUE_STATUSES = ("retry_scheduled", "scheduled")
STUCK_STATUSES = ("queued", "running")


async def requeue_orphaned_queued_tasks(limit: int = 200) -> int:
    """Повторно публікує queued-задачі, що «зависли» без повідомлення в черзі.

    Якщо задача довше queued_reconcile_seconds лежить у status=queued і ніхто
    не тримає на ній активний lease, її повідомлення, найімовірніше, загубилося
    (напр. після втрати черги). Повторна публікація безпечна: дублікат виконується
    не більш ніж один раз завдяки атомарному lease-захопленню.
    """
    now = datetime.now(UTC)
    cutoff = now - timedelta(seconds=settings.queued_reconcile_seconds)
    count = 0
    async with session_factory() as session:
        result = await session.execute(
            _lock(
                select(Task)
                .where(
                    Task.status == "queued",
                    Task.updated_at <= cutoff,
                    or_(
                        Task.lease_owner.is_(None),
                        Task.lease_expires_at.is_(None),
                        Task.lease_expires_at < now,
                    ),
                )
                .order_by(Task.updated_at.asc())
                .limit(limit),
                session,
            )
        )
        stuck = list(result.scalars().all())
        for task in stuck:
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
            count += 1
        await session.commit()
    if count:
        logger.info("Повторно опубліковано orphan-задач (queued): %s", count)
        metrics.retries_total.inc(count)
    for task in stuck:
        events_service.schedule_task_event("task.updated", task)
    return count


def _lock(stmt, session: AsyncSession):
    """FOR UPDATE SKIP LOCKED (Postgres) — воркери не крадуть ту саму задачу."""
    if session.get_bind().dialect.name == "postgresql":
        return stmt.with_for_update(skip_locked=True)
    return stmt


async def requeue_due_tasks(limit: int = 200) -> int:
    """Повертає в чергу задачі (scheduled / retry_scheduled), час яких настав."""
    now = datetime.now(UTC)
    count = 0
    async with session_factory() as session:
        result = await session.execute(
            _lock(
                select(Task)
                .where(Task.status.in_(DUE_STATUSES), Task.scheduled_at <= now)
                .order_by(Task.scheduled_at.asc())
                .limit(limit),
                session,
            )
        )
        due = list(result.scalars().all())
        for task in due:
            event_type = "task.retried" if task.status == "retry_scheduled" else "task.queued"
            await tasks_service.set_status(
                session,
                task,
                "queued",
                event_type=event_type,
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
            count += 1
        await session.commit()
    if count:
        metrics.retries_total.inc(count)
    for task in due:
        events_service.schedule_task_event("task.updated", task)
    return count


async def requeue_dlq_tasks(limit: int = 200) -> int:
    """Повертає в чергу задачі з DLQ, чий час повторної спроби настав.

    Dead-letter задачі не лежать назавжди: кожна отримує до
    dlq_retry_max_cycles повторних циклів з інтервалом dlq_retry_interval_seconds;
    після вичерпання циклів лишається в terminal dead_letter.
    """
    if not settings.dlq_retry_enabled:
        return 0
    now = datetime.now(UTC)
    count = 0
    async with session_factory() as session:
        result = await session.execute(
            _lock(
                select(Task)
                .where(
                    Task.status == "dead_letter",
                    Task.dlq_retry_at.is_not(None),
                    Task.dlq_retry_at <= now,
                    Task.dlq_requeue_count < settings.dlq_retry_max_cycles,
                )
                .order_by(Task.dlq_retry_at.asc())
                .limit(limit),
                session,
            )
        )
        due = list(result.scalars().all())
        for task in due:
            task.dlq_requeue_count += 1
            await tasks_service.set_status(
                session,
                task,
                "queued",
                event_type="task.dlq_requeued",
                metadata={
                    "dlq_requeue_count": task.dlq_requeue_count,
                    "max_cycles": settings.dlq_retry_max_cycles,
                },
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
            count += 1
        await session.commit()
    if count:
        metrics.retries_total.inc(count)
        logger.info("З DLQ у чергу повернуто: %s", count)
    for task in due:
        events_service.schedule_task_event("task.updated", task)
    return count


async def recover_stuck_tasks(limit: int = 50) -> int:
    """Мітить мертвих воркерів і повертає в чергу задачі з простроченим lease."""
    now = datetime.now(UTC)
    heartbeat_cutoff = now - timedelta(seconds=settings.worker_heartbeat_timeout_seconds)
    count = 0
    async with session_factory() as session:
        marked = await session.execute(
            update(Worker)
            .where(Worker.last_heartbeat_at < heartbeat_cutoff, Worker.status == "alive")
            .values(status="dead")
            .returning(Worker.worker_id)
        )
        dead_worker_ids = [row[0] for row in marked.all()]
        if dead_worker_ids:
            logger.info("Позначено мертвих воркерів: %s", dead_worker_ids)

        result = await session.execute(
            _lock(
                select(Task)
                .where(
                    Task.lease_expires_at.is_not(None),
                    Task.lease_expires_at < now,
                    Task.status.in_(STUCK_STATUSES),
                )
                .order_by(Task.lease_expires_at.asc())
                .limit(limit),
                session,
            )
        )
        stuck = list(result.scalars().all())
        for task in stuck:
            if task.status == "running":
                attempt = await session.scalar(
                    select(TaskAttempt)
                    .where(
                        TaskAttempt.task_id == task.id, TaskAttempt.status == "running"
                    )
                    .order_by(TaskAttempt.attempt_number.desc())
                    .limit(1)
                )
                if attempt is not None:
                    attempt.status = "failed"
                    attempt.error_message = "Lease прострочився — задачу повернуто в чергу"
                    attempt.finished_at = now
                await tasks_service.set_status(
                    session,
                    task,
                    "queued",
                    event_type="task.requeued",
                    metadata={
                        "reason": "lease_expired",
                        "lease_owner": task.lease_owner,
                        "attempt_number": (
                            attempt.attempt_number if attempt is not None else None
                        ),
                    },
                )
            else:
                task.lease_owner = None
                task.lease_expires_at = None
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
            count += 1
        await session.commit()
    if count:
        metrics.lease_timeouts_total.inc(count)
    for worker_id in dead_worker_ids:
        events_service.schedule_worker_event("worker.dead", worker_id)
    for task in stuck:
        events_service.schedule_task_event("task.updated", task)
    return count
