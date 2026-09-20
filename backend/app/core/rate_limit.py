from __future__ import annotations

import logging
import time

import jwt
from starlette.responses import JSONResponse

from app.core.config import settings

logger = logging.getLogger(__name__)

SKIP_PATHS = {"/", "/health", "/metrics", "/docs", "/openapi.json", "/redoc"}


class SlidingWindowRateLimiter:
    """In-process sliding window rate limiter per key (user або IP)."""

    def __init__(self, max_requests: int, window_seconds: float) -> None:
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._hits: dict[str, list[float]] = {}

    def allow(self, key: str) -> bool:
        now = time.monotonic()
        cutoff = now - self.window_seconds
        timestamps = self._hits.setdefault(key, [])
        timestamps[:] = [t for t in timestamps if t > cutoff]
        if len(timestamps) >= self.max_requests:
            return False
        timestamps.append(now)
        return True

    def reset(self, key: str | None = None) -> None:
        if key is None:
            self._hits.clear()
        else:
            self._hits.pop(key, None)


_rate_limiters: dict[tuple[int, float], SlidingWindowRateLimiter] = {}


def get_limiter() -> SlidingWindowRateLimiter:
    key = (settings.api_rate_limit_max, settings.api_rate_limit_window_seconds)
    limiter = _rate_limiters.get(key)
    if limiter is None:
        limiter = SlidingWindowRateLimiter(*key)
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
    """Рейт-ліміт для всього HTTP API (in-process, per-user/per-IP)."""

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
        if not limiter.allow(key):
            response = JSONResponse(
                status_code=429,
                content={"detail": "Забагато запитів, спробуйте пізніше"},
                headers={"Retry-After": str(int(limiter.window_seconds))},
            )
            await response(scope, receive, send)
            return
        await self.app(scope, receive, send)
