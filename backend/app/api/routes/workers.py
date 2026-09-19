from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_session
from app.schemas.worker import WorkerInfo
from app.services import workers as workers_service

router = APIRouter(prefix="/workers", tags=["workers"])


@router.get("", response_model=list[WorkerInfo])
async def list_workers(
    session: AsyncSession = Depends(get_session),
) -> list[WorkerInfo]:
    return await workers_service.list_workers(session)
