from __future__ import annotations

import logging
from datetime import UTC, datetime

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.worker import WORKER_STATUSES, Worker

logger = logging.getLogger(__name__)

WORKER_ALIVE = "alive"
WORKER_DEAD = "dead"


def _now() -> datetime:
    return datetime.now(UTC)


async def register_worker(
    session: AsyncSession,
    worker_id: str,
    *,
    hostname: str,
    pid: int,
) -> Worker:
    """Створює запис воркера або відновлює heartbeat, якщо він уже існує."""
    stmt = select(Worker).where(Worker.worker_id == worker_id)
    worker = (await session.execute(stmt)).scalar_one_or_none()
    now = _now()
    if worker is None:
        worker = Worker(
            worker_id=worker_id,
            hostname=hostname,
            pid=pid,
            status=WORKER_ALIVE,
            started_at=now,
            last_heartbeat_at=now,
        )
        session.add(worker)
    else:
        worker.status = WORKER_ALIVE
        worker.pid = pid
        worker.hostname = hostname
        worker.started_at = now
        worker.last_heartbeat_at = now
    await session.flush()
    return worker


async def heartbeat(session: AsyncSession, worker_id: str) -> Worker | None:
    """Оновлює last_heartbeat_at; повертає None, якщо воркер не зареєстрований."""
    stmt = select(Worker).where(Worker.worker_id == worker_id)
    worker = (await session.execute(stmt)).scalar_one_or_none()
    if worker is None:
        return None
    worker.last_heartbeat_at = _now()
    worker.status = WORKER_ALIVE
    await session.flush()
    return worker


async def mark_dead_workers(session: AsyncSession, stale_before: datetime) -> int:
    """Мітить dead усіх воркерів без актуального heartbeat."""
    result = await session.execute(
        update(Worker)
        .where(Worker.last_heartbeat_at < stale_before, Worker.status == WORKER_ALIVE)
        .values(status=WORKER_DEAD)
    )
    return int(result.rowcount or 0)


async def list_workers(session: AsyncSession) -> list[Worker]:
    stmt = select(Worker).order_by(Worker.last_heartbeat_at.desc())
    result = await session.execute(stmt)
    return list(result.scalars().all())


def is_valid_status(value: str) -> bool:
    return value in WORKER_STATUSES
