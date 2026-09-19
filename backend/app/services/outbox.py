from __future__ import annotations

import contextlib
import json
import logging
from datetime import UTC, datetime

import aio_pika
from sqlalchemy import select

from app.core.config import settings
from app.db.session import session_factory
from app.models.outbox import OUTBOX_PENDING, OUTBOX_SENT, OutboxEvent

logger = logging.getLogger(__name__)

_connection: aio_pika.abc.AbstractRobustConnection | None = None

PRIORITY_TO_RABBIT: dict[str, int] = {"critical": 9, "high": 6, "normal": 3, "low": 1}


def _priority_value(payload: dict) -> int:
    return PRIORITY_TO_RABBIT.get(payload.get("priority"), 3)


async def get_connection() -> aio_pika.abc.AbstractRobustConnection:
    global _connection
    if _connection is None or _connection.is_closed:
        _connection = await aio_pika.connect_robust(settings.rabbitmq_url)
    return _connection


async def _ensure_exchange(
    channel: aio_pika.abc.AbstractChannel,
) -> aio_pika.abc.AbstractExchange:
    return await channel.declare_exchange(
        settings.exchange_name, aio_pika.ExchangeType.TOPIC, durable=True, auto_delete=False
    )


async def publish_pending_events(limit: int = 200) -> int:
    """Опитує outbox, публікує невідправлені події в RabbitMQ, мітить sent."""
    async with session_factory() as session:
        result = await session.execute(
            select(OutboxEvent)
            .where(OutboxEvent.status == OUTBOX_PENDING)
            .order_by(OutboxEvent.id.asc())
            .limit(limit)
        )
        events = list(result.scalars().all())
        if not events:
            return 0

        connection = await get_connection()
        async with connection.channel() as channel:
            exchange = await _ensure_exchange(channel)
            for outbox in events:
                try:
                    await exchange.publish(
                        aio_pika.Message(
                            body=json.dumps(outbox.payload, default=str).encode(),
                            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                            content_type="application/json",
                            priority=_priority_value(outbox.payload),
                        ),
                        routing_key=outbox.routing_key,
                    )
                    outbox.status = OUTBOX_SENT
                    outbox.processed_at = datetime.now(UTC)
                    outbox.last_error = None
                    outbox.attempts += 1
                except Exception as exc:
                    outbox.attempts += 1
                    outbox.last_error = str(exc)[:1000]
                    logger.warning("Публікація outbox #%s не вдалась: %s", outbox.id, exc)
            await session.commit()
        return len(events)


async def close_connection() -> None:
    global _connection
    if _connection is not None:
        with contextlib.suppress(Exception):
            await _connection.close()
        _connection = None
