from __future__ import annotations

import logging

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.models.dependency import TaskDependency
from app.models.outbox import OutboxEvent
from app.models.task import Task
from app.services.tasks import InvalidTransitionError, TaskConflictError, set_status

logger = logging.getLogger(__name__)

_TERMINAL_NON_SUCCESS = {"failed", "cancelled", "dead_letter", "timeout"}


async def register_dependencies(
    session: AsyncSession, task_id: int, parent_ids: list[int]
) -> None:
    """Валідує батьків і реєструє ребра; дитина лишається blocked (created)."""
    parent_ids = list(dict.fromkeys(parent_ids))
    if task_id in parent_ids:
        raise TaskConflictError("Задача не може залежати від самої себе")
    existing = set(
        (
            await session.execute(select(Task.id).where(Task.id.in_(parent_ids)))
        ).scalars()
    )
    missing = [pid for pid in parent_ids if pid not in existing]
    if missing:
        raise TaskConflictError(f"Батьківські задачі не знайдено: {missing}")
    await _ensure_no_cycle(session, task_id, parent_ids)
    for pid in parent_ids:
        session.add(TaskDependency(parent_task_id=pid, child_task_id=task_id))


async def _ensure_no_cycle(
    session: AsyncSession, task_id: int, parent_ids: list[int]
) -> None:
    """BFS вгору по ланцюгу залежностей: якщо дійшли до task_id — цикл."""
    seen = set()
    queue = list(parent_ids)
    while queue:
        current = queue.pop()
        if current == task_id:
            raise TaskConflictError("Виявлено циклічну залежність задач")
        if current in seen:
            continue
        seen.add(current)
        parents = (
            await session.execute(
                select(TaskDependency.parent_task_id).where(
                    TaskDependency.child_task_id == current
                )
            )
        ).scalars()
        for pid in parents:
            if pid == task_id:
                raise TaskConflictError("Виявлено циклічну залежність задач")
            queue.append(pid)


async def _children_of(session: AsyncSession, task_id: int) -> list[int]:
    return list(
        (
            await session.execute(
                select(TaskDependency.child_task_id).where(
                    TaskDependency.parent_task_id == task_id
                )
            )
        ).scalars()
    )


async def _all_parents_succeeded(session: AsyncSession, child_id: int) -> bool:
    parent_ids = list(
        (
            await session.execute(
                select(TaskDependency.parent_task_id).where(
                    TaskDependency.child_task_id == child_id
                )
            )
        ).scalars()
    )
    if not parent_ids:
        return True
    statuses = set(
        (
            await session.execute(select(Task.status).where(Task.id.in_(parent_ids)))
        ).scalars()
    )
    return len(statuses) == 1 and "success" in statuses


async def advance_children(session: AsyncSession, task: Task) -> None:
    """Після terminal-статусу батька просуває/скасовує заблокованих дітей."""
    child_ids = await _children_of(session, task.id)
    if not child_ids:
        return
    for child_id in child_ids:
        child = await session.get(Task, child_id)
        if child is None or child.status != "created":
            continue
        if task.status == "success":
            if await _all_parents_succeeded(session, child.id):
                await _unblock(session, child, task.id)
        else:
            await _cancel_blocked(session, child, task.id, task.status)


async def _unblock(session: AsyncSession, child: Task, by_task_id: int) -> None:
    await set_status(
        session,
        child,
        "queued",
        event_type="task.unblocked",
        metadata={"unblocked_by": by_task_id},
    )
    session.add(
        OutboxEvent(
            routing_key=settings.routing_key_task_created,
            payload={
                "task_id": child.id,
                "task_type": child.task_type,
                "payload": child.payload,
                "priority": child.priority,
            },
        )
    )


async def _cancel_blocked(
    session: AsyncSession, child: Task, parent_id: int, parent_status: str
) -> None:
    try:
        await set_status(
            session,
            child,
            "cancelled",
            event_type="task.cancelled",
            metadata={
                "reason": "parent_terminal_non_success",
                "parent_task_id": parent_id,
                "parent_status": parent_status,
            },
        )
    except InvalidTransitionError:
        logger.warning("Неможливо скасувати заблоковану дитину %s", child.id)
