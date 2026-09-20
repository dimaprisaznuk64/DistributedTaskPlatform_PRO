from __future__ import annotations

import logging
import secrets
import threading
import time

import jwt
from starlette.responses import JSONResponse

from app.core.config import settings
from app.db.redis import get_redis

logger = logging.getLogger(__name__)

SKIP_PATHS = {"/", "/health", "/metrics", "/docs", "/openapi.json", "/redoc"}

SLIDING_WINDOW_SCRIPT = """
local key = KEYS[1]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local max = tonumber(ARGV[3])
redis.call('ZREMRANGEBYSCORE', key, '-inf', now - window)
local count = redis.call('ZCARD', key)
if count >= max then
    return 0
end
redis.call('ZADD', key, now, ARGV[4])
redis.call('PEXPIRE', key, window * 1000)
return 1
"""


class LocalRateLimiter:
    """In-process sliding window fallback (якщо Redis недоступний чи вимкнений)."""

    def __init__(self, max_requests: int, window_seconds: float) -> None:
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._hits: dict[str, list[float]] = {}
        self._lock = threading.Lock()

    def allow(self, key: str) -> bool:
        now = time.monotonic()
        cutoff = now - self.window_seconds
        with self._lock:
            timestamps = self._hits.setdefault(key, [])
            timestamps[:] = [t for t in timestamps if t > cutoff]
            if len(timestamps) >= self.max_requests:
                return False
            timestamps.append(now)
            return True

    def reset(self, key: str | None = None) -> None:
        with self._lock:
            if key is None:
                self._hits.clear()
            else:
                self._hits.pop(key, None)


class RedisRateLimiter:
    """Sliding window у Redis (атомарний Lua-скрипт) — спільний для всіх реплік."""

    def __init__(self, max_requests: int, window_seconds: float) -> None:
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._last_failure: float | None = None

    @property
    def available(self) -> bool:
        if self._last_failure is None:
            return True
        return time.monotonic() - self._last_failure > 30.0

    async def allow(self, key: str) -> bool | None:
        """Повертає None, якщо Redis недоступний (тоді клієнт має впасти на локальний)."""
        if not self.available:
            return None
        try:
            redis = await get_redis()
            allowed = await redis.eval(
                SLIDING_WINDOW_SCRIPT,
                1,
                _redis_key(key),
                time.time(),
                self.window_seconds,
                self.max_requests,
                f"{time.time()}:{secrets.token_hex(4)}",
            )
            return bool(allowed)
        except Exception as exc:
            logger.warning("Redis rate limiter недоступний, перехід на локальний: %s", exc)
            self._last_failure = time.monotonic()
            return None


def _redis_key(key: str) -> str:
    return f"rl:{key}"


class RateLimiter:
    """Розподілений лімітер з фолбеком: Redis (Lua) → in-process локальний."""

    def __init__(self, max_requests: int, window_seconds: float) -> None:
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.local = LocalRateLimiter(max_requests, window_seconds)
        self.redis = RedisRateLimiter(max_requests, window_seconds)

    async def allow(self, key: str) -> bool:
        if settings.rate_limit_redis_enabled:
            result = await self.redis.allow(key)
            if result is not None:
                return result
        return self.local.allow(key)

    def reset(self, key: str | None = None) -> None:
        self.local.reset(key)
        self.redis = RedisRateLimiter(self.max_requests, self.window_seconds)


_rate_limiters: dict[tuple[int, float], RateLimiter] = {}


def get_limiter() -> RateLimiter:
    key = (settings.api_rate_limit_max, settings.api_rate_limit_window_seconds)
    limiter = _rate_limiters.get(key)
    if limiter is None:
        limiter = RateLimiter(*key)
        _rate_limiters[key] = limiter
    return limiter


def reset_rate_limits() -> None:
    for limiter in _rate_limiters.values():
        limiter.reset()


def _client_key(scope: dict) -> str:
    """Повертає ключ ліміту: user id з access-токена або IP клієнта."""
    headers = scope.get("headers", [])
    auth_header = None
    for name, value in headers:
        if name == b"authorization":
            auth_header = value.decode("latin-1")
            break
    if auth_header and auth_header.lower().startswith("bearer "):
        token = auth_header[7:]
        try:
            payload = jwt.decode(
                token, settings.jwt_secret, algorithms=[settings.jwt_algorithm]
            )
            if payload.get("type") == "access":
                return f"user:{payload['sub']}"
        except jwt.PyJWTError:
            pass
    client = scope.get("client")
    return f"ip:{client[0] if client else 'unknown'}"


class RateLimitMiddleware:
    """Рейт-ліміт для всього HTTP API (Redis, фолбек локальний)."""

    def __init__(self, app) -> None:
        self.app = app

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        path = scope.get("path", "")
        if path in SKIP_PATHS:
            await self.app(scope, receive, send)
            return
        key = _client_key(scope)
        limiter = get_limiter()
        if not await limiter.allow(key):
            response = JSONResponse(
                status_code=429,
                content={"detail": "Забагато запитів, спробуйте пізніше"},
                headers={"Retry-After": str(int(limiter.window_seconds))},
            )
            await response(scope, receive, send)
            return
        await self.app(scope, receive, send)
