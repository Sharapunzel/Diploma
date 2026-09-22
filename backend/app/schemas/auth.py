from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class LocalLoginRequest(BaseModel):
    username: str = Field(min_length=1)
    password: str = Field(min_length=1, max_length=128)


class PrincipalDTO(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: UUID
    username: str | None
    email: str | None
    display_name: str


class SessionResponse(BaseModel):
    authenticated: bool
    user: PrincipalDTO | None = None
    role: str | None = None
    permissions: list[str] = Field(default_factory=list)
    authentication_method: str | None = None
    csrf_token: str | None = None


class ErrorResponse(BaseModel):
    code: str
    message: str
    request_id: str
    details: dict | None = None
