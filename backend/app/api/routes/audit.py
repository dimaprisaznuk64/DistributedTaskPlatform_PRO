from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_session
from app.schemas.audit import AuditInfo, AuditList
from app.services import audit as audit_service
from app.services import auth as auth_service

router = APIRouter(
    prefix="/audit",
    tags=["audit"],
    dependencies=[Depends(auth_service.require_roles("admin"))],
)


@router.get("", response_model=AuditList)
async def list_audit(
    actor_id: int | None = None,
    action: str | None = None,
    resource_type: str | None = None,
    q: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    session: AsyncSession = Depends(get_session),
) -> AuditList:
    rows = await audit_service.list_audit(
        session,
        actor_id=actor_id,
        action=action,
        resource_type=resource_type,
        search=q,
        limit=limit,
        offset=offset,
    )
    total = await audit_service.count_audit(
        session, actor_id=actor_id, action=action, resource_type=resource_type, search=q
    )
    return AuditList(total=total, items=[AuditInfo.model_validate(r) for r in rows])
