from __future__ import annotations

import hashlib
import secrets
from datetime import UTC, datetime, timedelta

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.models.api_token import ApiToken


def _hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def generate_token() -> str:
    return f"{settings.api_token_prefix}{secrets.token_hex(32)}"


async def create_token(
    session: AsyncSession, *, user_id: int, name: str, ttl_days: int | None = None
) -> tuple[str, ApiToken]:
    token = generate_token()
    expires_at = None
    if ttl_days is not None and ttl_days > 0:
        expires_at = datetime.now(UTC) + timedelta(days=ttl_days)
    elif settings.api_token_expire_days > 0:
        expires_at = datetime.now(UTC) + timedelta(days=settings.api_token_expire_days)
    record = ApiToken(
        name=name,
        user_id=user_id,
        token_hash=_hash_token(token),
        expires_at=expires_at,
    )
    session.add(record)
    await session.flush()
    return token, record


async def revoke_token(
    session: AsyncSession, token_id: int, user_id: int, *, admin: bool = False
) -> bool:
    stmt = select(ApiToken).where(ApiToken.id == token_id)
    if not admin:
        stmt = stmt.where(ApiToken.user_id == user_id)
    record = await session.scalar(stmt)
    if record is None:
        return False
    if record.revoked_at is None:
        record.revoked_at = datetime.now(UTC)
    return True


async def list_tokens(
    session: AsyncSession, *, user_id: int | None = None, limit: int = 100, offset: int = 0
) -> list[ApiToken]:
    stmt = select(ApiToken).order_by(ApiToken.id.desc()).limit(limit).offset(offset)
    if user_id is not None:
        stmt = stmt.where(ApiToken.user_id == user_id)
    result = await session.execute(stmt)
    return list(result.scalars().all())


async def count_tokens(session: AsyncSession, *, user_id: int | None = None) -> int:
    from sqlalchemy import func

    stmt = select(func.count(ApiToken.id))
    if user_id is not None:
        stmt = stmt.where(ApiToken.user_id == user_id)
    result = await session.execute(stmt)
    return int(result.scalar_one())


def is_api_token(token: str) -> bool:
    return token.startswith(settings.api_token_prefix)


def _utc_aware(dt: datetime | None) -> datetime | None:
    if dt is None or dt.tzinfo is not None:
        return dt
    return dt.replace(tzinfo=UTC)


async def authenticate(session: AsyncSession, token: str) -> ApiToken | None:
    """Знаходить невідкликаний, не прострочений активний токен за хешем."""
    record = await session.scalar(
        select(ApiToken).where(ApiToken.token_hash == _hash_token(token)).limit(1)
    )
    if record is None:
        return None
    now = datetime.now(UTC)
    if record.revoked_at is not None:
        return None
    if not record.is_active:
        return None
    expires = _utc_aware(record.expires_at)
    if expires is not None and expires <= now:
        return None
    return record
