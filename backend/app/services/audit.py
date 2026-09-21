from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.audit import AuditLog

logger = logging.getLogger(__name__)


async def record(
    session: AsyncSession,
    *,
    actor: Any | None = None,
    actor_id: int | None = None,
    actor_username: str | None = None,
    action: str,
    resource_type: str,
    resource_id: str | None = None,
    details: dict[str, Any] | None = None,
    ip_address: str | None = None,
) -> AuditLog:
    """Пише audit-запис у поточній транзакції (коміт робить викликаючий код)."""
    if actor is not None:
        actor_id = getattr(actor, "id", None)
        actor_username = getattr(actor, "username", actor_username)
    entry = AuditLog(
        actor_id=actor_id,
        actor_username=actor_username,
        action=action,
        resource_type=resource_type,
        resource_id=resource_id,
        details=details,
        ip_address=ip_address,
    )
    session.add(entry)
    await session.flush()
    return entry


async def list_audit(
    session: AsyncSession,
    *,
    actor_id: int | None = None,
    action: str | None = None,
    resource_type: str | None = None,
    search: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> list[AuditLog]:
    stmt = select(AuditLog).order_by(AuditLog.id.desc()).limit(limit).offset(offset)
    if actor_id is not None:
        stmt = stmt.where(AuditLog.actor_id == actor_id)
    if action:
        stmt = stmt.where(AuditLog.action == action)
    if resource_type:
        stmt = stmt.where(AuditLog.resource_type == resource_type)
    if search:
        like = f"%{search}%"
        stmt = stmt.where(
            or_(
                AuditLog.actor_username.ilike(like),
                AuditLog.action.ilike(like),
                AuditLog.resource_type.ilike(like),
            )
        )
    result = await session.execute(stmt)
    return list(result.scalars().all())


async def count_audit(
    session: AsyncSession,
    *,
    actor_id: int | None = None,
    action: str | None = None,
    resource_type: str | None = None,
    search: str | None = None,
) -> int:
    stmt = select(func.count(AuditLog.id))
    if actor_id is not None:
        stmt = stmt.where(AuditLog.actor_id == actor_id)
    if action:
        stmt = stmt.where(AuditLog.action == action)
    if resource_type:
        stmt = stmt.where(AuditLog.resource_type == resource_type)
    if search:
        like = f"%{search}%"
        stmt = stmt.where(
            or_(
                AuditLog.actor_username.ilike(like),
                AuditLog.action.ilike(like),
                AuditLog.resource_type.ilike(like),
            )
        )
    result = await session.execute(stmt)
    return int(result.scalar_one())
