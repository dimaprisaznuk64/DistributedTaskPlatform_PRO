from __future__ import annotations

import pytest
from sqlalchemy import select

import app.services.auth as auth_service
from app.models.user import User


def _auth(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


@pytest.mark.asyncio
async def test_unauthenticated_rejected(client, session_factory) -> None:
    admin_token = client.headers["Authorization"]
    client.headers.pop("Authorization")
    try:
        response = await client.get("/api/v1/stats")
        assert response.status_code == 401
        response = await client.post("/api/v1/tasks", json={"task_type": "echo"})
        assert response.status_code == 401
    finally:
        client.headers["Authorization"] = admin_token


@pytest.mark.asyncio
async def test_register_login_me_flow(client, session_factory) -> None:
    registered = await client.post(
        "/api/v1/auth/register",
        json={"username": "alice", "password": "strong-pass-123"},
    )
    assert registered.status_code == 201
    assert registered.json()["role"] == "viewer"

    logged = await client.post(
        "/api/v1/auth/login",
        json={"username": "alice", "password": "strong-pass-123"},
    )
    assert logged.status_code == 200
    body = logged.json()
    assert body["token_type"] == "bearer"
    assert body["access_token"] and body["refresh_token"]
    assert body["user"]["username"] == "alice"

    me = await client.get("/api/v1/auth/me", headers=_auth(body["access_token"]))
    assert me.status_code == 200
    assert me.json()["username"] == "alice"


@pytest.mark.asyncio
async def test_login_wrong_password(client, session_factory) -> None:
    response = await client.post(
        "/api/v1/auth/login",
        json={"username": "admin", "password": "wrong-password"},
    )
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_register_duplicate_conflict(client, session_factory) -> None:
    await client.post(
        "/api/v1/auth/register",
        json={"username": "dupe", "password": "strong-pass-123"},
    )
    duplicate = await client.post(
        "/api/v1/auth/register",
        json={"username": "dupe", "password": "another-pass-123"},
    )
    assert duplicate.status_code == 409


@pytest.mark.asyncio
async def test_register_weak_password_rejected(client, session_factory) -> None:
    response = await client.post(
        "/api/v1/auth/register", json={"username": "bob", "password": "short"}
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_refresh_rotates_access(client, session_factory) -> None:
    logged = await client.post(
        "/api/v1/auth/login", json={"username": "admin", "password": "admin"}
    )
    refresh_token = logged.json()["refresh_token"]

    refreshed = await client.post(
        "/api/v1/auth/refresh", json={"refresh_token": refresh_token}
    )
    assert refreshed.status_code == 200
    assert refreshed.json()["access_token"]


@pytest.mark.asyncio
async def test_access_token_cannot_be_used_as_refresh(client, session_factory) -> None:
    logged = await client.post(
        "/api/v1/auth/login", json={"username": "admin", "password": "admin"}
    )
    access_token = logged.json()["access_token"]
    response = await client.post(
        "/api/v1/auth/refresh", json={"refresh_token": access_token}
    )
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_viewer_cannot_create_task(client, session_factory) -> None:
    await client.post(
        "/api/v1/auth/register",
        json={"username": "viewer1", "password": "strong-pass-123"},
    )
    viewed = await client.post(
        "/api/v1/auth/login",
        json={"username": "viewer1", "password": "strong-pass-123"},
    )
    token = viewed.json()["access_token"]

    created = await client.post(
        "/api/v1/tasks",
        json={"task_type": "echo"},
        headers=_auth(token),
    )
    assert created.status_code == 403

    listed = await client.get("/api/v1/tasks", headers=_auth(token))
    assert listed.status_code == 200


@pytest.mark.asyncio
async def test_admin_promotes_operator_then_can_create(client, session_factory) -> None:
    await client.post(
        "/api/v1/auth/register",
        json={"username": "op1", "password": "strong-pass-123"},
    )
    async with session_factory() as session:
        op = await session.scalar(select(User).where(User.username == "op1"))
        op_id = op.id

    promoted = await client.post(f"/api/v1/auth/users/{op_id}/role", json={"role": "operator"})
    assert promoted.status_code == 200
    assert promoted.json()["role"] == "operator"

    operator_login = await client.post(
        "/api/v1/auth/login", json={"username": "op1", "password": "strong-pass-123"}
    )
    token = operator_login.json()["access_token"]
    created = await client.post(
        "/api/v1/tasks", json={"task_type": "echo"}, headers=_auth(token)
    )
    assert created.status_code == 201


@pytest.mark.asyncio
async def test_operator_cannot_change_roles(client, session_factory) -> None:
    await client.post(
        "/api/v1/auth/register",
        json={"username": "op2", "password": "strong-pass-123"},
    )
    async with session_factory() as session:
        op = await session.scalar(select(User).where(User.username == "op2"))
        op_id = op.id

    await client.post(f"/api/v1/auth/users/{op_id}/role", json={"role": "operator"})
    login = await client.post(
        "/api/v1/auth/login", json={"username": "op2", "password": "strong-pass-123"}
    )
    token = login.json()["access_token"]

    response = await client.post(
        "/api/v1/auth/users/1/role", json={"role": "viewer"}, headers=_auth(token)
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_invalid_token_rejected(client, session_factory) -> None:
    response = await client.get("/api/v1/stats", headers=_auth("not-a-jwt"))
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_deactivated_user_rejected(client, session_factory) -> None:
    await client.post(
        "/api/v1/auth/register",
        json={"username": "ghost", "password": "strong-pass-123"},
    )
    login = await client.post(
        "/api/v1/auth/login", json={"username": "ghost", "password": "strong-pass-123"}
    )
    token = login.json()["access_token"]

    async with session_factory() as session:
        user = await session.scalar(select(User).where(User.username == "ghost"))
        user.is_active = False
        await session.commit()

    response = await client.get("/api/v1/stats", headers=_auth(token))
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_password_stored_hashed(client, session_factory) -> None:
    await client.post(
        "/api/v1/auth/register",
        json={"username": "plaintext", "password": "strong-pass-123"},
    )
    async with session_factory() as session:
        user = await session.scalar(select(User).where(User.username == "plaintext"))
    assert user.password_hash != "strong-pass-123"
    assert auth_service.verify_password("strong-pass-123", user.password_hash)
