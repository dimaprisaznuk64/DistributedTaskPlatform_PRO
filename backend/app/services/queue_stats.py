from __future__ import annotations

import logging

import aio_pika

from app.core.config import settings

logger = logging.getLogger(__name__)


async def get_queue_depth() -> int | None:
    """Кількість повідомлень у RabbitMQ-черзі; None, якщо черга недоступна."""
    try:
        connection = await aio_pika.connect_robust(settings.rabbitmq_url, timeout=3)
        try:
            async with connection.channel() as channel:
                declaration = await channel.queue_declare(
                    settings.queue_name, passive=True
                )
                return int(declaration.message_count)
        finally:
            await connection.close()
    except Exception as exc:
        logger.debug("Статистика черги недоступна: %s", exc)
        return None
