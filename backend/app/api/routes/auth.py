from __future__ import annotations

import logging
from datetime import UTC, datetime

import jwt
from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_session
from app.models.user import User
from app.schemas.api_token import (
    ApiTokenCreated,
    ApiTokenCreateRequest,
    ApiTokenInfo,
    ApiTokenList,
)
from app.schemas.user import (
    ChangePasswordRequest,
    LoginRequest,
    RefreshRequest,
    RegisterRequest,
    RoleChangeRequest,
    TokenResponse,
    UserOut,
    UserToggleActiveRequest,
)
from app.services import api_tokens as api_tokens_service
from app.services import audit as audit_service
from app.services import auth

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/auth", tags=["auth"])


def _user_out(user: User) -> UserOut:
    return UserOut.model_validate(user)


@router.post("/register", response_model=UserOut, status_code=status.HTTP_201_CREATED)
async def register(
    body: RegisterRequest,
    request: Request,
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
    await session.flush()
    client = request.client.host if request.client else None
    await audit_service.record(
        session,
        actor=user,
        action="user.register",
        resource_type="user",
        resource_id=str(user.id),
        ip_address=client,
    )
    await session.commit()
    await session.refresh(user)
    return _user_out(user)


@router.post("/login", response_model=TokenResponse)
async def login(
    body: LoginRequest,
    request: Request,
    _: None = Depends(auth.login_failure_limiter),
    session: AsyncSession = Depends(get_session),
) -> TokenResponse:
    client = request.client.host if request.client else "unknown"
    user = await auth.get_user_by_username(session, body.username)
    if user is None or not auth.verify_password(body.password, user.password_hash):
        auth.record_login_failure(client)
        raise HTTPException(status_code=401, detail="Невірний логін або пароль")
    auth.clear_login_failures(client)
    user.last_login_at = datetime.now(UTC)
    refresh = auth.create_refresh_token(user.id)
    await auth.persist_refresh_token(session, user.id, auth.decode_token(refresh))
    await audit_service.record(
        session,
        actor=user,
        action="auth.login",
        resource_type="user",
        resource_id=str(user.id),
        ip_address=client,
    )
    await session.commit()
    return TokenResponse(
        access_token=auth.create_access_token(user.id),
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

    stored = await auth.refresh_is_valid(session, payload)
    if stored is None:
        raise HTTPException(status_code=401, detail="Недійсний або відкликаний refresh-токен")

    user = await auth.get_user_by_id(session, user_id)
    if user is None or not user.is_active:
        raise HTTPException(status_code=401, detail="Користувача не знайдено")

    # Ротація: старий токен анулюється, видається нова пара
    await auth.revoke_refresh(session, stored)
    new_refresh = auth.create_refresh_token(user.id)
    await auth.persist_refresh_token(session, user.id, auth.decode_token(new_refresh))
    await session.commit()
    return TokenResponse(
        access_token=auth.create_access_token(user.id),
        refresh_token=new_refresh,
        user=_user_out(user),
    )


@router.post("/logout", status_code=status.HTTP_204_NO_CONTENT, response_class=Response)
async def logout(
    body: RefreshRequest,
    _: None = Depends(auth.auth_rate_limiter),
    session: AsyncSession = Depends(get_session),
) -> Response:
    try:
        payload = auth.decode_token(body.refresh_token)
        if payload.get("type") != auth.TOKEN_TYPE_REFRESH:
            raise jwt.InvalidTokenError
    except (jwt.PyJWTError, KeyError, ValueError) as exc:
        raise HTTPException(status_code=401, detail="Недійсний refresh-токен") from exc
    stored = await auth.refresh_is_valid(session, payload)
    if stored is not None:
        await auth.revoke_refresh(session, stored)
        await session.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.get("/me", response_model=UserOut)
async def me(user: User = Depends(auth.get_current_user)) -> UserOut:
    return _user_out(user)


@router.post("/change-password", response_model=UserOut)
async def change_password(
    body: ChangePasswordRequest,
    request: Request,
    current: User = Depends(auth.get_current_user),
    session: AsyncSession = Depends(get_session),
) -> UserOut:
    user = await session.get(User, current.id)
    if user is None or not user.is_active:
        raise HTTPException(status_code=401, detail="Користувача не знайдено")
    if not auth.verify_password(body.current_password, user.password_hash):
        raise HTTPException(status_code=400, detail="Поточний пароль неправильний")
    if body.current_password == body.new_password:
        raise HTTPException(status_code=400, detail="Новий пароль збігається з поточним")
    user.password_hash = auth.hash_password(body.new_password)
    revoked = await auth.revoke_all_refresh_tokens(session, user.id)
    await audit_service.record(
        session,
        actor=user,
        action="user.change_password",
        resource_type="user",
        resource_id=str(user.id),
        details={"revoked_refresh": revoked},
        ip_address=request.client.host if request.client else None,
    )
    await session.commit()
    await session.refresh(user)
    logger.info("Змінено пароль користувача %s, відкликано refresh: %s", user.username, revoked)
    return _user_out(user)


@router.post("/users/{user_id}/active", response_model=UserOut)
async def set_active(
    user_id: int,
    body: UserToggleActiveRequest,
    request: Request,
    admin: User = Depends(auth.require_roles("admin")),
    session: AsyncSession = Depends(get_session),
) -> UserOut:
    target = await session.get(User, user_id)
    if target is None:
        raise HTTPException(status_code=404, detail="Користувача не знайдено")
    target.is_active = body.is_active
    revoked = 0
    if not body.is_active:
        revoked = await auth.revoke_all_refresh_tokens(session, target.id)
    await audit_service.record(
        session,
        actor=admin,
        action="user.set_active",
        resource_type="user",
        resource_id=str(user_id),
        details={"is_active": body.is_active, "revoked_refresh": revoked},
        ip_address=request.client.host if request.client else None,
    )
    await session.commit()
    await session.refresh(target)
    return _user_out(target)


@router.post("/users/{user_id}/role", response_model=UserOut)
async def set_role(
    user_id: int,
    body: RoleChangeRequest,
    request: Request,
    admin: User = Depends(auth.require_roles("admin")),
    session: AsyncSession = Depends(get_session),
) -> UserOut:
    target = await session.get(User, user_id)
    if target is None:
        raise HTTPException(status_code=404, detail="Користувача не знайдено")
    old_role = target.role
    target.role = body.role
    await audit_service.record(
        session,
        actor=admin,
        action="user.role_change",
        resource_type="user",
        resource_id=str(user_id),
        details={"old_role": old_role, "new_role": body.role},
        ip_address=request.client.host if request.client else None,
    )
    await session.commit()
    await session.refresh(target)
    return _user_out(target)


@router.post(
    "/tokens", response_model=ApiTokenCreated, status_code=status.HTTP_201_CREATED
)
async def create_api_token(
    body: ApiTokenCreateRequest,
    request: Request,
    current: User = Depends(auth.get_current_user),
    session: AsyncSession = Depends(get_session),
) -> ApiTokenCreated:
    token, record = await api_tokens_service.create_token(
        session, user_id=current.id, name=body.name
    )
    client = request.client.host if request.client else None
    await audit_service.record(
        session,
        actor=current,
        action="api_token.create",
        resource_type="api_token",
        resource_id=str(record.id),
        details={"name": body.name},
        ip_address=client,
    )
    await session.commit()
    return ApiTokenCreated(
        token=token, api_token=ApiTokenInfo.model_validate(record)
    )


@router.get("/tokens", response_model=ApiTokenList)
async def list_api_tokens(
    current: User = Depends(auth.get_current_user),
    session: AsyncSession = Depends(get_session),
) -> ApiTokenList:
    rows = await api_tokens_service.list_tokens(session, user_id=current.id, limit=500)
    return ApiTokenList(
        total=len(rows), items=[ApiTokenInfo.model_validate(r) for r in rows]
    )


@router.delete(
    "/tokens/{token_id}", status_code=status.HTTP_204_NO_CONTENT, response_class=Response
)
async def revoke_api_token(
    token_id: int,
    request: Request,
    current: User = Depends(auth.get_current_user),
    session: AsyncSession = Depends(get_session),
) -> Response:
    is_admin = current.role == "admin"
    revoked = await api_tokens_service.revoke_token(
        session, token_id, current.id, admin=is_admin
    )
    if not revoked:
        raise HTTPException(status_code=404, detail="Токен не знайдено")
    await audit_service.record(
        session,
        actor=current,
        action="api_token.revoke",
        resource_type="api_token",
        resource_id=str(token_id),
        ip_address=request.client.host if request.client else None,
    )
    await session.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)
