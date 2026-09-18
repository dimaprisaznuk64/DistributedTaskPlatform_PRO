from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel

from app.db.session import check_database

router = APIRouter(tags=["health"])


class HealthBody(BaseModel):
    status: str
    database: bool


@router.get("/health", response_model=HealthBody)
async def health() -> HealthBody:
    return HealthBody(status="ok", database=await check_database())
