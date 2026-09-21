from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class ApiTokenCreateRequest(BaseModel):
    name: str = Field(min_length=1, max_length=100)


class ApiTokenCreated(BaseModel):
    token: str
    api_token: ApiTokenInfo


class ApiTokenInfo(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    name: str
    user_id: int
    last_used_at: datetime | None
    expires_at: datetime | None
    revoked_at: datetime | None
    created_at: datetime


class ApiTokenList(BaseModel):
    total: int
    items: list[ApiTokenInfo]
