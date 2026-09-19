from __future__ import annotations

import contextlib

from redis.asyncio import Redis

from app.core.config import settings

_redis: Redis | None = None


async def get_redis() -> Redis:
    """Лінивий єдиний клієнт Redis (розподіл live-подій для дашборда)."""
    global _redis
    if _redis is None:
        _redis = Redis.from_url(
            settings.redis_url,
            decode_responses=True,
            socket_connect_timeout=2.0,
            socket_timeout=5.0,
        )
    return _redis


async def close_redis() -> None:
    global _redis
    if _redis is not None:
        with contextlib.suppress(Exception):
            await _redis.aclose()
        _redis = None
