from __future__ import annotations

import hashlib
import hmac
import json
import logging
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core import metrics
from app.core.config import settings
from app.db.session import session_factory
from app.models.webhook import (
    WEBHOOK_DELIVERY_FAILED,
    WEBHOOK_DELIVERY_PENDING,
    WEBHOOK_DELIVERY_SENT,
    WebhookDelivery,
    WebhookSubscription,
)

logger = logging.getLogger(__name__)


def _now() -> datetime:
    return datetime.now(UTC)


def _matches(sub: WebhookSubscription, event_type: str) -> bool:
    if not sub.events:
        return False
    return "*" in sub.events or event_type in sub.events


def _sign(secret: str | None, body: bytes) -> str:
    key = (secret or "").encode("utf-8")
    return "sha256=" + hmac.new(key, body, hashlib.sha256).hexdigest()


def _task_payload(task: Any) -> dict[str, Any]:
    return {
        "id": task.id,
        "task_type": task.task_type,
        "status": task.status,
        "priority": task.priority,
        "attempts": task.attempts,
        "task_id": task.id,
    }


async def enqueue_for(session: AsyncSession, event_type: str, task: Any) -> None:
    """Ставить webhook-доставки для події (в одній транзакції зі зміною статусу)."""
    subs = list(
        (
            await session.execute(
                select(WebhookSubscription).where(WebhookSubscription.is_active.is_(True))
            )
        ).scalars()
    )
    if not subs:
        return
    now = _now()
    for sub in subs:
        if not _matches(sub, event_type):
            continue
        delivery = WebhookDelivery(
            subscription_id=sub.id,
            event_type=event_type,
            task_id=task.id,
            payload={},
            status=WEBHOOK_DELIVERY_PENDING,
            next_retry_at=now,
        )
        session.add(delivery)
        await session.flush()
        delivery.payload = {
            "event": event_type,
            "subscription_id": sub.id,
            "delivery_id": delivery.id,
            "timestamp": now.isoformat(),
            "task": _task_payload(task),
        }


async def send_now(sub: WebhookSubscription, payload: dict) -> tuple[bool, int | None, str]:
    """Надсилає сповіщення синхронно (для тест-ендпоінта та диспетчера)."""
    body = json.dumps(payload, default=str).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "X-Platform-Event": payload.get("event", ""),
        "X-Platform-Signature": _sign(sub.secret, body),
        "X-Platform-Integration": "DistributedTaskPlatform",
    }
    try:
        async with httpx.AsyncClient(timeout=settings.webhook_timeout_seconds) as client:
            response = await client.post(sub.url, content=body, headers=headers)
        if response.is_success:
            return True, response.status_code, ""
        return False, response.status_code, f"HTTP {response.status_code}: {response.text[:500]}"
    except httpx.HTTPError as exc:
        return False, None, f"HTTPError: {exc}"
    except Exception as exc:
        return False, None, f"{type(exc).__name__}: {exc}"


def _lock(stmt, session: AsyncSession):
    if session.get_bind().dialect.name == "postgresql":
        return stmt.with_for_update(skip_locked=True)
    return stmt


async def dispatch_due(limit: int = 100) -> int:
    """Відправляє due pending-доставки з ретраями та backoff."""
    now = _now()
    count = 0
    async with session_factory() as session:
        result = await session.execute(
            _lock(
                select(WebhookDelivery)
                .where(
                    WebhookDelivery.status == WEBHOOK_DELIVERY_PENDING,
                    WebhookDelivery.next_retry_at <= now,
                )
                .order_by(WebhookDelivery.id.asc())
                .limit(limit),
                session,
            )
        )
        deliveries = list(result.scalars().all())
        for delivery in deliveries:
            sub = await session.get(WebhookSubscription, delivery.subscription_id)
            if sub is None or not sub.is_active:
                delivery.status = WEBHOOK_DELIVERY_FAILED
                delivery.last_error = "Підписку видалено або деактивовано"
                count += 1
                continue
            ok, status_code, error = await send_now(sub, delivery.payload)
            if ok:
                delivery.status = WEBHOOK_DELIVERY_SENT
                delivery.last_error = None
                metrics.webhook_deliveries_total.labels(outcome="sent").inc()
            else:
                delivery.attempts += 1
                delivery.last_error = error[:1000]
                if delivery.attempts >= settings.webhook_max_attempts:
                    delivery.status = WEBHOOK_DELIVERY_FAILED
                    metrics.webhook_deliveries_total.labels(outcome="failed").inc()
                    logger.warning(
                        "Webhook-доставка #%s вичерпала спроби (%s)",
                        delivery.id,
                        error,
                    )
                else:
                    delivery.next_retry_at = now + _retry_delay(delivery.attempts)
            count += 1
        await session.commit()
    return count


def _retry_delay(attempts_so_far: int) -> timedelta:
    return timedelta(seconds=settings.webhook_retry_backoff_seconds * (2 ** (attempts_so_far - 1)))
