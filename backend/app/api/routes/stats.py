from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_session
from app.services import queue_stats
from app.services import stats as stats_service

router = APIRouter(prefix="/stats", tags=["stats"])


@router.get("")
async def get_stats(
    session: AsyncSession = Depends(get_session),
) -> dict:
    body = await stats_service.build_stats(session)
    body["queue"] = {"depth": await queue_stats.get_queue_depth()}
    return body
