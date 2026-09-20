from __future__ import annotations

import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool

from app.core.config import settings
from app.db.base import Base
from app.db.session import get_session
from app.main import app


@pytest_asyncio.fixture
async def db() -> AsyncEngine:
    engine = create_async_engine(
        "sqlite+aiosqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    await engine.dispose()


@pytest_asyncio.fixture
async def session_factory(db: AsyncEngine) -> async_sessionmaker:
    return async_sessionmaker(db, expire_on_commit=False, class_=AsyncSession)


async def _login_as_admin(client: AsyncClient, factory: async_sessionmaker) -> None:
    from sqlalchemy import select

    import app.services.auth as auth_service
    from app.models.user import User

    async with factory() as session:
        if (await session.scalar(select(User.id).limit(1))) is None:
            session.add(
                User(
                    username="admin",
                    password_hash=auth_service.hash_password("admin"),
                    role="admin",
                )
            )
            await session.commit()
    response = await client.post(
        "/api/v1/auth/login", json={"username": "admin", "password": "admin"}
    )
    assert response.status_code == 200, response.text
    token = response.json()["access_token"]
    client.headers["Authorization"] = f"Bearer {token}"


@pytest_asyncio.fixture
async def client(db: AsyncEngine) -> AsyncClient:
    settings.redis_events_enabled = False
    factory = async_sessionmaker(db, expire_on_commit=False, class_=AsyncSession)

    import app.db.session as db_session
    import app.services.auth as auth_service

    auth_service.session_factory = factory
    db_session.session_factory = factory
    auth_service._login_window.clear()

    import app.core.rate_limit as rate_limit

    rate_limit.reset_rate_limits()

    async def _get_session():
        async with factory() as session:
            yield session

    app.dependency_overrides[get_session] = _get_session
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        await _login_as_admin(ac, factory)
        yield ac
    app.dependency_overrides.clear()
