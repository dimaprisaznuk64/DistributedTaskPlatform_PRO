from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator

from app.core.config import settings
from app.db.redis import get_redis

logger = logging.getLogger(__name__)

_background: set[asyncio.Task] = set()


def _keep(task: asyncio.Task) -> None:
    _background.add(task)
    task.add_done_callback(_background.discard)


def _frame(event_type: str, payload: dict) -> str:
    return json.dumps(
        {"type": event_type, "payload": payload},
        default=str,
    )


def schedule_task_event(event_type: str, task) -> None:
    """Запускає публікацію у фоні (після коміту); посилання зберігається до завершення."""
    _keep(asyncio.create_task(publish_task_event(event_type, task)))


def schedule_worker_event(event_type: str, worker_id: str) -> None:
    _keep(asyncio.create_task(publish_worker_event(event_type, worker_id)))


async def publish_event(event_type: str, **payload) -> None:
    """Публікує подію в Redis-канал: best-effort, не повинен ламати основний флоу."""
    if not settings.redis_events_enabled:
        return
    try:
        redis = await get_redis()
        await redis.publish(settings.events_channel, _frame(event_type, payload))
    except Exception as exc:
        logger.debug("Redis publish не вдалось: %s", exc)


async def publish_task_event(event_type: str, task) -> None:
    await publish_event(
        event_type,
        task_id=task.id,
        task_type=task.task_type,
        status=task.status,
        priority=task.priority,
        attempts=task.attempts,
    )


async def publish_worker_event(event_type: str, worker_id: str) -> None:
    await publish_event(event_type, worker_id=worker_id)


async def event_stream() -> AsyncIterator[str]:
    """Потік live-подій з Redis каналу у вигляді JSON-строк."""
    if not settings.redis_events_enabled:
        return
    redis = await get_redis()
    pubsub = redis.pubsub()
    await pubsub.subscribe(settings.events_channel)
    try:
        while True:
            message = await pubsub.get_message(
                ignore_subscribe_messages=True, timeout=1.0
            )
            if message is None:
                continue
            yield message["data"]
    finally:
        await pubsub.unsubscribe()
        await pubsub.aclose()
