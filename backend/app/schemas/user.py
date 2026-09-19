from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field

from app.models.user import ROLES


class UserOut(BaseModel):
    id: int
    username: str
    role: str
    created_at: datetime
    last_login_at: datetime | None = None
    model_config = {"from_attributes": True}


class RegisterRequest(BaseModel):
    username: str = Field(min_length=3, max_length=100)
    password: str = Field(min_length=8, max_length=128)


class LoginRequest(BaseModel):
    username: str
    password: str


class RefreshRequest(BaseModel):
    refresh_token: str


class TokenResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    user: UserOut


class RoleChangeRequest(BaseModel):
    role: str = Field(pattern="|".join(ROLES))
