from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.router import api_router
from app.api.routes.health import router as health_router
from app.core.config import settings
from app.services import outbox as outbox_service

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    logging.basicConfig(level=settings.log_level)
    relay_task = asyncio.create_task(_outbox_relay_loop())
    try:
        yield
    finally:
        relay_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await relay_task
        await outbox_service.close_connection()


async def _outbox_relay_loop() -> None:
    logger.info("Outbox relay запущено (кожні %.1fс)", settings.outbox_poll_seconds)
    while True:
        try:
            await outbox_service.publish_pending_events()
        except Exception:
            logger.exception("Outbox relay: помилка тику")
        await asyncio.sleep(settings.outbox_poll_seconds)


app = FastAPI(
    title=settings.app_name,
    version="0.1.0",
    lifespan=lifespan,
    docs_url="/docs",
    openapi_url="/openapi.json",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health_router)
app.include_router(api_router)
