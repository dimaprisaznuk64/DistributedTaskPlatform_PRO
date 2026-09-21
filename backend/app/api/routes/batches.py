from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_session
from app.models.user import User
from app.schemas.batch import (
    BatchActionResult,
    BatchCreateRequest,
    BatchCreateResult,
    BatchInfo,
    BatchList,
    BatchTaskList,
)
from app.schemas.task import TaskInfo
from app.services import audit as audit_service
from app.services import auth as auth_service
from app.services import batches as batches_service
from app.services import events as events_service

router = APIRouter(
    prefix="/batches",
    tags=["batches"],
    dependencies=[Depends(auth_service.get_current_user)],
)

OPERATOR = auth_service.require_roles("operator", "admin")


@router.post("", response_model=BatchCreateResult, status_code=status.HTTP_201_CREATED)
async def create_batch(
    request: Request,
    body: BatchCreateRequest,
    user: User = Depends(OPERATOR),
    session: AsyncSession = Depends(get_session),
) -> BatchCreateResult:
    specs = [
        {
            "task_type": t.task_type,
            "payload": t.payload,
            "priority": t.priority,
            "max_attempts": t.max_attempts,
            "idempotency_key": t.idempotency_key,
            "schedule_at": t.schedule_at,
            "depends_on": t.depends_on,
        }
        for t in body.tasks
    ]
    try:
        batch, created = await batches_service.create_batch(
            session, name=body.name, specs=specs, created_by=user.id
        )
    except batches_service.BatchLimitError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    client_ip = request.client.host if request.client else None
    await audit_service.record(
        session,
        actor=user,
        action="batch.create",
        resource_type="batch",
        resource_id=str(batch.id),
        details={"tasks": len(created)},
        ip_address=client_ip,
    )
    await session.commit()
    for task in created:
        events_service.schedule_task_event("task.updated", task)
    progress = await batches_service.get_progress(session, batch.id)
    return BatchCreateResult(
        batch=BatchInfo(**batches_service.build_info(batch, progress)),
        created=[TaskInfo.model_validate(t) for t in created],
    )


@router.get("", response_model=BatchList)
async def list_batches(
    limit: int = Query(default=20, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    session: AsyncSession = Depends(get_session),
) -> BatchList:
    batches = await batches_service.list_batches(session, limit=limit, offset=offset)
    total = await batches_service.count_batches(session)
    items: list[BatchInfo] = []
    for batch in batches:
        progress = await batches_service.get_progress(session, batch.id)
        items.append(BatchInfo(**batches_service.build_info(batch, progress)))
    return BatchList(total=total, items=items)


@router.get("/{batch_id}", response_model=BatchTaskList)
async def get_batch(
    batch_id: int,
    limit: int = Query(default=20, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    session: AsyncSession = Depends(get_session),
) -> BatchTaskList:
    batch = await batches_service.get_batch(session, batch_id)
    if batch is None:
        raise HTTPException(status_code=404, detail=f"Батч {batch_id} не знайдено")
    tasks, total = await batches_service.list_batch_tasks(
        session, batch_id=batch_id, limit=limit, offset=offset
    )
    progress = await batches_service.get_progress(session, batch_id)
    return BatchTaskList(
        batch=BatchInfo(**batches_service.build_info(batch, progress)),
        total=total,
        items=[TaskInfo.model_validate(t) for t in tasks],
    )


@router.post("/{batch_id}/cancel", response_model=BatchActionResult)
async def cancel_batch(
    request: Request,
    batch_id: int,
    user: User = Depends(OPERATOR),
    session: AsyncSession = Depends(get_session),
) -> BatchActionResult:
    try:
        cancelled, conflicts = await batches_service.cancel_batch(session, batch_id)
    except batches_service.TaskConflictError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    client_ip = request.client.host if request.client else None
    await audit_service.record(
        session,
        actor=user,
        action="batch.cancel",
        resource_type="batch",
        resource_id=str(batch_id),
        details={"cancelled": cancelled},
        ip_address=client_ip,
    )
    await session.commit()
    batch = await batches_service.get_batch(session, batch_id)
    progress = await batches_service.get_progress(session, batch_id)
    return BatchActionResult(
        batch=BatchInfo(**batches_service.build_info(batch, progress)),
        actions=[{"task_id": c["task_id"], "reason": c["reason"]} for c in conflicts],
    )
