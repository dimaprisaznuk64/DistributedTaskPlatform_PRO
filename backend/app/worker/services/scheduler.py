from __future__ import annotations

import logging
from datetime import UTC, datetime

from sqlalchemy import select

from app.core.config import settings
from app.db.session import session_factory
from app.models.outbox import OutboxEvent
from app.models.task import Task
from app.services import tasks as tasks_service

logger = logging.getLogger(__name__)


async def requeue_due_tasks(limit: int = 200) -> int:
    """Повертає в чергу задачі зі статусом retry_scheduled, час яких настав."""
    now = datetime.now(UTC)
    count = 0
    async with session_factory() as session:
        result = await session.execute(
            select(Task)
            .where(Task.status == "retry_scheduled", Task.scheduled_at <= now)
            .order_by(Task.scheduled_at.asc())
            .limit(limit)
        )
        due = list(result.scalars().all())
        for task in due:
            await tasks_service.set_status(
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
            count += 1
        await session.commit()
    return count
