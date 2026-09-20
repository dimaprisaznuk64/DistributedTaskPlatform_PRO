from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime, timedelta
from time import monotonic

import bcrypt
import jwt
from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.db.session import session_factory
from app.models.refresh_token import RefreshToken
from app.models.user import User

logger = logging.getLogger(__name__)

bearer_scheme = HTTPBearer(auto_error=False)

TOKEN_TYPE_ACCESS = "access"
TOKEN_TYPE_REFRESH = "refresh"


def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def verify_password(password: str, password_hash: str) -> bool:
    try:
        return bcrypt.checkpw(password.encode("utf-8"), password_hash.encode("utf-8"))
    except ValueError:
        return False


def _create_token(subject: int, token_type: str, expires_delta: timedelta) -> str:
    now = datetime.now(UTC)
    payload = {
        "sub": str(subject),
        "type": token_type,
        "jti": str(uuid.uuid4()),
        "iat": now,
        "exp": now + expires_delta,
    }
    return jwt.encode(payload, settings.jwt_secret, algorithm=settings.jwt_algorithm)


def create_access_token(user_id: int) -> str:
    return _create_token(
        user_id,
        TOKEN_TYPE_ACCESS,
        timedelta(minutes=settings.access_token_expire_minutes),
    )


def create_refresh_token(user_id: int) -> str:
    return _create_token(
        user_id,
        TOKEN_TYPE_REFRESH,
        timedelta(days=settings.refresh_token_expire_days),
    )


def decode_token(token: str) -> dict:
    return jwt.decode(
        token,
        settings.jwt_secret,
        algorithms=[settings.jwt_algorithm],
    )


async def persist_refresh_token(
    session: AsyncSession, user_id: int, payload: dict
) -> RefreshToken:
    """Зберігає refresh-токен у БД (поверхнево — фактично jti + exp з JWT)."""
    jti = payload.get("jti")
    if not jti:
        raise ValueError("refresh-токен без jti")
    token = RefreshToken(
        user_id=user_id,
        jti=jti,
        expires_at=datetime.fromtimestamp(payload["exp"], tz=UTC),
    )
    session.add(token)
    return token


async def _stored_refresh(session: AsyncSession, jti: str) -> RefreshToken | None:
    return await session.scalar(
        select(RefreshToken).where(RefreshToken.jti == jti).limit(1)
    )


def _utc_aware(dt: datetime | None) -> datetime | None:
    if dt is None or dt.tzinfo is not None:
        return dt
    return dt.replace(tzinfo=UTC)


async def refresh_is_valid(session: AsyncSession, payload: dict) -> RefreshToken | None:
    """Перевіряє, що refresh-токен існує, не revoked і не прострочений."""
    stored = await _stored_refresh(session, payload.get("jti", ""))
    if stored is None:
        return None
    if stored.revoked_at is not None:
        return None
    if _utc_aware(stored.expires_at) <= datetime.now(UTC):
        return None
    return stored


async def revoke_refresh(session: AsyncSession, stored: RefreshToken) -> None:
    stored.revoked_at = datetime.now(UTC)


async def get_user_by_id(session: AsyncSession, user_id: int) -> User | None:
    return await session.get(User, user_id)


async def get_user_by_username(session: AsyncSession, username: str) -> User | None:
    return await session.scalar(
        select(User).where(User.username == username, User.is_active.is_(True))
    )


async def bootstrap_admin() -> None:
    """Створює адміна при першому запуску (якщо користувачів ще немає)."""
    async with session_factory() as session:
        existing = await session.scalar(select(User.id).limit(1))
        if existing is not None:
            return
        session.add(
            User(
                username=settings.admin_username,
                password_hash=hash_password(settings.admin_password),
                role="admin",
            )
        )
        await session.commit()
        logger.info("Створено bootstrap адміна %r", settings.admin_username)


async def get_current_user(
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
) -> User:
    if credentials is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Не авторизовано")
    try:
        payload = decode_token(credentials.credentials)
        if payload.get("type") != TOKEN_TYPE_ACCESS:
            raise jwt.InvalidTokenError
        user_id = int(payload["sub"])
    except (jwt.PyJWTError, KeyError, ValueError) as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Недійсний або прострочений токен",
        ) from exc
    async with session_factory() as session:
        user = await get_user_by_id(session, user_id)
    if user is None or not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Користувача не знайдено",
        )
    return user


def require_roles(*roles: str):
    allowed = set(roles)

    def dependency(user: User = Depends(get_current_user)) -> User:
        if user.role not in allowed:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Недостатньо прав: потрібна роль з {sorted(allowed)}",
            )
        return user

    return dependency


_login_window: dict[str, list[float]] = {}


async def auth_rate_limiter(request: Request) -> None:
    """Простий in-process sliding window для auth-ендпоінтів (за IP)."""
    client = request.client.host if request.client else "unknown"
    now = monotonic()
    window = now - settings.auth_rate_limit_minutes * 60
    timestamps = [t for t in _login_window.get(client, []) if t > window]
    if len(timestamps) >= settings.auth_rate_limit_max:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Забагато запитів, спробуйте пізніше",
        )
    timestamps.append(now)
    _login_window[client] = timestamps
