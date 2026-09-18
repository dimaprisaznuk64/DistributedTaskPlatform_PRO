from fastapi import APIRouter

from app.api.routes.health import router as health_router
from app.api.routes.tasks import router as tasks_router
from app.core.config import settings

api_router = APIRouter()
api_router.include_router(health_router, prefix=settings.api_v1_prefix)
api_router.include_router(tasks_router, prefix=settings.api_v1_prefix)
