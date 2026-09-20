from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from fastapi.responses import JSONResponse
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_session
from app.models.task import TASK_PRIORITIES, TASK_STATUSES
from app.models.user import User
from app.schemas.task import (
    CancelResult,
    CreateResult,
    RetryResult,
    TaskCreate,
    TaskDetail,
    TaskEventTimeline,
    TaskInfo,
    TaskList,
)
from app.services import auth as auth_service
from app.services import events as events_service
from app.services import tasks as tasks_service

router = APIRouter(
    prefix="/tasks",
    tags=["tasks"],
    dependencies=[Depends(auth_service.get_current_user)],
)

OPERATOR = auth_service.require_roles("operator", "admin")


def _conflict(detail: str) -> HTTPException:
    return HTTPException(status_code=status.HTTP_409_CONFLICT, detail=detail)


@router.post("", response_model=CreateResult, status_code=status.HTTP_201_CREATED)
async def create_task(
    request: Request,
    body: TaskCreate,
    _: User = Depends(OPERATOR),
    session: AsyncSession = Depends(get_session),
) -> CreateResult | JSONResponse:
    from app.core.rate_limit import task_create_allowed

    client_ip = request.client.host if request.client else "unknown"
    if not await task_create_allowed(client_ip):
        raise HTTPException(status_code=429, detail="Забагато створених задач, спробуйте пізніше")
    if body.idempotency_key is not None:
        existing = await tasks_service.get_by_idempotency_key(session, body.idempotency_key)
        if existing is not None:
            return JSONResponse(
                status_code=status.HTTP_200_OK,
                content=CreateResult(task=TaskInfo.model_validate(existing)).model_dump(
                    mode="json"
                ),
            )
    try:
        task = await tasks_service.create_task(
            session,
            task_type=body.task_type,
            payload=body.payload,
            priority=body.priority,
            max_attempts=body.max_attempts,
            idempotency_key=body.idempotency_key,
            schedule_at=body.schedule_at,
        )
    except IntegrityError as exc:
        raise _conflict("Задача з таким idempotency_key вже існує") from exc
    await session.commit()
    events_service.schedule_task_event("task.updated", task)
    return CreateResult(task=TaskInfo.model_validate(task))


@router.get("", response_model=TaskList)
async def list_tasks(
    status_: str | None = Query(default=None, alias="status", pattern="|".join(TASK_STATUSES)),
    task_type: str | None = None,
    priority: str | None = Query(default=None, pattern="|".join(TASK_PRIORITIES)),
    limit: int = Query(default=20, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    session: AsyncSession = Depends(get_session),
) -> TaskList:
    items = await tasks_service.list_tasks(
        session,
        status=status_,
        task_type=task_type,
        priority=priority,
        limit=limit,
        offset=offset,
    )
    total = await tasks_service.count_tasks(
        session, status=status_, task_type=task_type, priority=priority
    )
    return TaskList(total=total, items=[TaskInfo.model_validate(t) for t in items])


def _not_found(task_id: int) -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_404_NOT_FOUND, detail=f"Задача {task_id} не знайдена"
    )


@router.get("/{task_id}", response_model=TaskDetail)
async def get_task(
    task_id: int,
    session: AsyncSession = Depends(get_session),
) -> TaskDetail:
    task = await tasks_service.get_task(session, task_id)
    if task is None:
        raise _not_found(task_id)
    detail = TaskDetail.model_validate(task)
    detail.attempts_ = [a for a in task.attempts_]
    detail.events_ = [e for e in task.events_]
    return detail


@router.post("/{task_id}/retry", response_model=RetryResult)
async def retry_task(
    task_id: int,
    _: User = Depends(OPERATOR),
    session: AsyncSession = Depends(get_session),
) -> RetryResult:
    try:
        task = await tasks_service.manual_retry(session, task_id)
    except tasks_service.TaskConflictError as exc:
        raise _conflict(str(exc)) from exc
    if task is None:
        raise _not_found(task_id)
    await session.commit()
    events_service.schedule_task_event("task.updated", task)
    return RetryResult(task=TaskInfo.model_validate(task))


@router.post("/{task_id}/cancel", response_model=CancelResult)
async def cancel_task(
    task_id: int,
    _: User = Depends(OPERATOR),
    session: AsyncSession = Depends(get_session),
) -> CancelResult:
    try:
        task = await tasks_service.cancel_task(session, task_id)
    except tasks_service.TaskConflictError as exc:
        raise _conflict(str(exc)) from exc
    if task is None:
        raise _not_found(task_id)
    await session.commit()
    events_service.schedule_task_event("task.updated", task)
    return CancelResult(task=TaskInfo.model_validate(task))


@router.get("/{task_id}/events", response_model=TaskEventTimeline)
async def task_events(
    task_id: int,
    session: AsyncSession = Depends(get_session),
) -> TaskEventTimeline:
    events = await tasks_service.get_events(session, task_id)
    if not events and await tasks_service.get_task(session, task_id) is None:
        raise _not_found(task_id)
    return TaskEventTimeline(task_id=task_id, events=events)
