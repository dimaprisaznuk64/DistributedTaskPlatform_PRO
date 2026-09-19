from __future__ import annotations

import logging

from fastapi import APIRouter, Response
from prometheus_client import CONTENT_TYPE_LATEST
from sqlalchemy import func, select

from app.core import metrics
from app.db.session import session_factory
from app.models.outbox import OUTBOX_PENDING, OutboxEvent
from app.models.task import TASK_STATUSES, Task
from app.models.worker import Worker
from app.services import queue_stats

logger = logging.getLogger(__name__)

router = APIRouter(tags=["metrics"])


@router.get("/metrics")
async def metrics_endpoint() -> Response:
    """Prometheus-метрика: DB-зріз (гаджі) + accumulated лічильники."""
    async with session_factory() as session:
        rows = (
            await session.execute(select(Task.status, func.count(Task.id)).group_by(Task.status))
        ).all()
        counts = {status: 0 for status in TASK_STATUSES}
        for status, count in rows:
            counts[status] = int(count or 0)
        for status, count in counts.items():
            metrics.tasks_status_gauge.labels(status=status).set(count)

        alive = int(
(
                    await session.execute(
                        select(func.count(Worker.id)).where(Worker.status == "alive")
                    )
                ).scalar_one()
        )
        total_workers = int(
            (await session.execute(select(func.count(Worker.id)))).scalar_one()
        )
        pending = int(
            (
                await session.execute(
                    select(func.count(OutboxEvent.id)).where(
                        OutboxEvent.status == OUTBOX_PENDING
                    )
                )
            ).scalar_one()
        )
        await session.commit()

    metrics.workers_active.set(alive)
    metrics.workers_total.set(total_workers)
    metrics.outbox_pending.set(pending)

    depth = await queue_stats.get_queue_depth()
    metrics.rabbitmq_queue_depth.set(depth if depth is not None else -1)

    return Response(content=metrics.render_metrics(), media_type=CONTENT_TYPE_LATEST)
