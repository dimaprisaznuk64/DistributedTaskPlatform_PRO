from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse

from app.api.router import api_router
from app.api.routes.health import router as health_router
from app.api.routes.metrics import router as metrics_router
from app.core.config import settings
from app.core.logging import setup_logging
from app.core.metrics import PrometheusMiddleware
from app.core.rate_limit import RateLimitMiddleware
from app.core.version import VERSION
from app.db.redis import close_redis as close_redis_client
from app.services import auth as auth_service
from app.services import outbox as outbox_service

logger = logging.getLogger(__name__)

DASHBOARD_HTML = Path(__file__).parent / "static" / "dashboard.html"


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    setup_logging()
    await auth_service.bootstrap_admin()
    relay_task = asyncio.create_task(_outbox_relay_loop())
    webhook_task = asyncio.create_task(_webhook_dispatcher_loop())
    try:
        yield
    finally:
        relay_task.cancel()
        webhook_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await relay_task
            await webhook_task
        await outbox_service.close_connection()
        await close_redis_client()


async def _outbox_relay_loop() -> None:
    logger.info("Outbox relay запущено (кожні %.1fс)", settings.outbox_poll_seconds)
    while True:
        try:
            await outbox_service.publish_pending_events()
        except Exception:
            logger.exception("Outbox relay: помилка тику")
        await asyncio.sleep(settings.outbox_poll_seconds)


async def _webhook_dispatcher_loop() -> None:
    from app.services import webhooks as webhooks_service

    logger.info("Webhook-диспетчер запущено (кожні %.1fс)", settings.webhook_poll_seconds)
    while True:
        try:
            dispatched = await webhooks_service.dispatch_due()
            if dispatched:
                logger.info("Webhook-доставок оброблено: %s", dispatched)
        except Exception:
            logger.exception("Webhook-диспетчер: помилка тику")
        await asyncio.sleep(settings.webhook_poll_seconds)


app = FastAPI(
    title=settings.app_name,
    version=VERSION,
    lifespan=lifespan,
    docs_url="/docs",
    openapi_url="/openapi.json",
)

cors_origins = settings.cors_origins_list
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials="*" not in cors_origins,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(PrometheusMiddleware)
app.add_middleware(RateLimitMiddleware)


@app.get("/", include_in_schema=False, response_class=HTMLResponse)
async def dashboard() -> HTMLResponse:
    return HTMLResponse(DASHBOARD_HTML.read_text(encoding="utf-8"))


app.include_router(health_router)
app.include_router(metrics_router)
app.include_router(api_router)
