from __future__ import annotations

from datetime import UTC, datetime

import jwt
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_session
from app.models.user import User
from app.schemas.user import (
    LoginRequest,
    RefreshRequest,
    RegisterRequest,
    RoleChangeRequest,
    TokenResponse,
    UserOut,
)
from app.services import auth

router = APIRouter(prefix="/auth", tags=["auth"])


def _user_out(user: User) -> UserOut:
    return UserOut.model_validate(user)


@router.post("/register", response_model=UserOut, status_code=status.HTTP_201_CREATED)
async def register(
    body: RegisterRequest,
    _: None = Depends(auth.auth_rate_limiter),
    session: AsyncSession = Depends(get_session),
) -> UserOut:
    exists = await session.scalar(
        select(User.id).where(User.username == body.username)
    )
    if exists is not None:
        raise HTTPException(status_code=409, detail="Користувач вже існує")
    user = User(
        username=body.username,
        password_hash=auth.hash_password(body.password),
        role="viewer",
    )
    session.add(user)
    await session.commit()
    await session.refresh(user)
    return _user_out(user)


@router.post("/login", response_model=TokenResponse)
async def login(
    body: LoginRequest,
    _: None = Depends(auth.auth_rate_limiter),
    session: AsyncSession = Depends(get_session),
) -> TokenResponse:
    user = await auth.get_user_by_username(session, body.username)
    if user is None or not auth.verify_password(body.password, user.password_hash):
        raise HTTPException(status_code=401, detail="Невірний логін або пароль")
    user.last_login_at = datetime.now(UTC)
    await session.commit()
    access = auth.create_access_token(user.id)
    refresh = auth.create_refresh_token(user.id)
    return TokenResponse(
        access_token=access,
        refresh_token=refresh,
        user=_user_out(user),
    )


@router.post("/refresh", response_model=TokenResponse)
async def refresh(
    body: RefreshRequest,
    _: None = Depends(auth.auth_rate_limiter),
    session: AsyncSession = Depends(get_session),
) -> TokenResponse:
    try:
        payload = auth.decode_token(body.refresh_token)
        if payload.get("type") != auth.TOKEN_TYPE_REFRESH:
            raise jwt.InvalidTokenError
        user_id = int(payload["sub"])
    except (jwt.PyJWTError, KeyError, ValueError) as exc:
        raise HTTPException(status_code=401, detail="Недійсний refresh-токен") from exc
    user = await auth.get_user_by_id(session, user_id)
    if user is None or not user.is_active:
        raise HTTPException(status_code=401, detail="Користувача не знайдено")
    return TokenResponse(
        access_token=auth.create_access_token(user.id),
        refresh_token=body.refresh_token,
        user=_user_out(user),
    )


@router.get("/me", response_model=UserOut)
async def me(user: User = Depends(auth.get_current_user)) -> UserOut:
    return _user_out(user)


@router.post("/users/{user_id}/role", response_model=UserOut)
async def set_role(
    user_id: int,
    body: RoleChangeRequest,
    admin: User = Depends(auth.require_roles("admin")),
    session: AsyncSession = Depends(get_session),
) -> UserOut:
    target = await session.get(User, user_id)
    if target is None:
        raise HTTPException(status_code=404, detail="Користувача не знайдено")
    target.role = body.role
    await session.commit()
    await session.refresh(target)
    return _user_out(target)
