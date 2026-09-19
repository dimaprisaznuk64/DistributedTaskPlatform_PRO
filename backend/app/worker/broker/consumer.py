from __future__ import annotations

import json
import logging
from collections.abc import AsyncIterator

import aio_pika

from app.core.config import settings

logger = logging.getLogger(__name__)


async def consume_tasks() -> AsyncIterator[aio_pika.abc.AbstractIncomingMessage]:
    """Підписується на чергу task_executions і віддає повідомлення по одному."""
    connection = await aio_pika.connect_robust(settings.rabbitmq_url)
    async with connection:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=1)
        exchange = await channel.declare_exchange(
            settings.exchange_name, aio_pika.ExchangeType.TOPIC, durable=True, auto_delete=False
        )
        queue = await channel.declare_queue(
            settings.queue_name,
            durable=True,
            arguments={"x-max-priority": settings.queue_max_priority},
        )
        await queue.bind(exchange, routing_key=settings.routing_key_task_created)

        logger.info("Worker %s слухає чергу %s", settings.worker_id, settings.queue_name)
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                yield message


def decode_payload(message: aio_pika.abc.AbstractIncomingMessage) -> dict:
    body = json.loads(message.body.decode())
    if not isinstance(body, dict):
        raise ValueError(f"Тіло повідомлення не є об'єктом: {body!r}")
    return body
