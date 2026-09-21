from __future__ import annotations

from datetime import UTC, datetime

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, status
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_session
from app.models.event import TASK_EVENT_TYPES
from app.models.user import User
from app.models.webhook import WebhookDelivery, WebhookSubscription
from app.schemas.webhook import (
    DeliveryInfo,
    DeliveryList,
    TestResult,
    WebhookCreateRequest,
    WebhookInfo,
    WebhookUpdateRequest,
)
from app.services import audit as audit_service
from app.services import auth as auth_service
from app.services import webhooks as webhooks_service

router = APIRouter(
    prefix="/webhooks",
    tags=["webhooks"],
    dependencies=[Depends(auth_service.get_current_user)],
)

ADMIN = auth_service.require_roles("admin")


def _sub_info(sub: WebhookSubscription) -> WebhookInfo:
    return WebhookInfo.model_validate(sub)


def _validate_events(events: list[str]) -> None:
    allowed = set(TASK_EVENT_TYPES) | {"*"}
    unknown = [e for e in events if e not in allowed]
    if unknown:
        raise HTTPException(status_code=400, detail=f"Невідомі події: {unknown}")


@router.post("", response_model=WebhookInfo, status_code=status.HTTP_201_CREATED)
async def create_webhook(
    request: Request,
    body: WebhookCreateRequest,
    admin: User = Depends(ADMIN),
    session: AsyncSession = Depends(get_session),
) -> WebhookInfo:
    _validate_events(body.events)
    sub = WebhookSubscription(
        name=body.name,
        url=body.url,
        secret=body.secret,
        events=body.events,
        is_active=True,
    )
    session.add(sub)
    await session.flush()
    client_ip = request.client.host if request.client else None
    await audit_service.record(
        session,
        actor=admin,
        action="webhook.create",
        resource_type="webhook",
        resource_id=str(sub.id),
        ip_address=client_ip,
    )
    await session.commit()
    return _sub_info(sub)


@router.get("", response_model=list[WebhookInfo])
async def list_webhooks(
    _: User = Depends(ADMIN),
    session: AsyncSession = Depends(get_session),
) -> list[WebhookInfo]:
    rows = (
        await session.execute(
            select(WebhookSubscription).order_by(WebhookSubscription.id.desc())
        )
    ).scalars()
    return [_sub_info(s) for s in rows]


@router.patch("/{webhook_id}", response_model=WebhookInfo)
async def update_webhook(
    request: Request,
    webhook_id: int,
    body: WebhookUpdateRequest,
    admin: User = Depends(ADMIN),
    session: AsyncSession = Depends(get_session),
) -> WebhookInfo:
    sub = await session.get(WebhookSubscription, webhook_id)
    if sub is None:
        raise HTTPException(status_code=404, detail="Підписку не знайдено")
    if body.events is not None:
        _validate_events(body.events)
        sub.events = body.events
    if body.name is not None:
        sub.name = body.name
    if body.url is not None:
        sub.url = body.url
    if body.is_active is not None:
        sub.is_active = body.is_active
    if body.secret is not None:
        sub.secret = body.secret
    await session.flush()
    client_ip = request.client.host if request.client else None
    await audit_service.record(
        session,
        actor=admin,
        action="webhook.update",
        resource_type="webhook",
        resource_id=str(webhook_id),
        ip_address=client_ip,
    )
    await session.commit()
    return _sub_info(sub)


@router.delete("/{webhook_id}", status_code=status.HTTP_204_NO_CONTENT, response_class=Response)
async def delete_webhook(
    request: Request,
    webhook_id: int,
    admin: User = Depends(ADMIN),
    session: AsyncSession = Depends(get_session),
) -> Response:
    sub = await session.get(WebhookSubscription, webhook_id)
    if sub is None:
        raise HTTPException(status_code=404, detail="Підписку не знайдено")
    client_ip = request.client.host if request.client else None
    await audit_service.record(
        session,
        actor=admin,
        action="webhook.delete",
        resource_type="webhook",
        resource_id=str(webhook_id),
        ip_address=client_ip,
    )
    await session.delete(sub)
    await session.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post("/{webhook_id}/test", response_model=TestResult, status_code=status.HTTP_200_OK)
async def test_webhook(
    request: Request,
    webhook_id: int,
    admin: User = Depends(ADMIN),
    session: AsyncSession = Depends(get_session),
) -> TestResult:
    sub = await session.get(WebhookSubscription, webhook_id)
    if sub is None:
        raise HTTPException(status_code=404, detail="Підписку не знайдено")
    payload = {
        "event": "test.ping",
        "subscription_id": sub.id,
        "delivery_id": 0,
        "timestamp": datetime.now(UTC).isoformat(),
        "task": {
            "id": 0,
            "task_type": "none",
            "status": "none",
            "priority": "normal",
            "attempts": 0,
            "task_id": 0,
        },
    }
    ok, status_code, error = await webhooks_service.send_now(sub, payload)
    client_ip = request.client.host if request.client else None
    await audit_service.record(
        session,
        actor=admin,
        action="webhook.test",
        resource_type="webhook",
        resource_id=str(webhook_id),
        details={"ok": ok},
        ip_address=client_ip,
    )
    await session.commit()
    return TestResult(ok=ok, detail=error or "ok", status_code=status_code)


@router.get("/deliveries", response_model=DeliveryList)
async def list_deliveries(
    _: User = Depends(ADMIN),
    subscription_id: int | None = None,
    status_name: str | None = Query(default=None, alias="status"),
    limit: int = Query(default=20, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    session: AsyncSession = Depends(get_session),
) -> DeliveryList:
    stmt = select(WebhookDelivery).order_by(WebhookDelivery.id.desc()).limit(limit).offset(offset)
    count_stmt = select(func.count()).select_from(WebhookDelivery)
    if subscription_id is not None:
        stmt = stmt.where(WebhookDelivery.subscription_id == subscription_id)
        count_stmt = count_stmt.where(WebhookDelivery.subscription_id == subscription_id)
    if status_name:
        stmt = stmt.where(WebhookDelivery.status == status_name)
        count_stmt = count_stmt.where(WebhookDelivery.status == status_name)
    rows = (await session.execute(stmt)).scalars()
    total = int((await session.execute(count_stmt)).scalar_one())
    return DeliveryList(total=total, items=[DeliveryInfo.model_validate(d) for d in rows])
