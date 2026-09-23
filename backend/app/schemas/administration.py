from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, EmailStr, Field, field_validator, model_validator


def trimmed(value: str) -> str:
    value = value.strip()
    if not value:
        raise ValueError("must not be blank")
    return value


class PageQuery(BaseModel):
    limit: int = Field(default=50, ge=1, le=100)
    offset: int = Field(default=0, ge=0)


class UserCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    id: UUID | None = None
    username: str = Field(min_length=1, max_length=128)
    password: str = Field(
        min_length=12,
        max_length=128,
        json_schema_extra={"format": "password", "writeOnly": True},
    )
    display_name: str = Field(min_length=1, max_length=200)
    email: EmailStr | None = Field(default=None, max_length=320)
    role_id: UUID
    is_active: bool = True

    _username = field_validator("username")(trimmed)
    _display_name = field_validator("display_name")(trimmed)

class UserPatch(BaseModel):
    model_config = ConfigDict(extra="forbid")
    username: str | None = Field(default=None, min_length=1, max_length=128)
    display_name: str | None = Field(default=None, min_length=1, max_length=200)
    email: EmailStr | None = Field(default=None, max_length=320)

    @model_validator(mode="after")
    def nonempty(self):
        if not self.model_fields_set:
            raise ValueError("patch must not be empty")
        for field_name in ("username", "display_name"):
            if field_name in self.model_fields_set and getattr(self, field_name) is None:
                raise ValueError(f"{field_name} must not be null")
        return self

    @field_validator("username", "display_name")
    @classmethod
    def trim_fields(cls, value):
        return value if value is None else trimmed(value)


class UserRoleUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    role_id: UUID
    role_managed_by_oidc: bool


class PasswordUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    password: str = Field(
        min_length=12,
        max_length=128,
        json_schema_extra={"format": "password", "writeOnly": True},
    )


class UserDTO(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: UUID
    username: str | None
    oidc_issuer: str | None
    oidc_subject: str | None
    email: str | None
    display_name: str
    role_id: UUID | None
    role_managed_by_oidc: bool
    is_active: bool
    created_at: datetime
    updated_at: datetime
    last_login_at: datetime | None
    authentication_method: Literal["local", "oidc"] | None = None


class UserPage(BaseModel):
    items: list[UserDTO]
    total: int
    limit: int
    offset: int


class RolePatch(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str = Field(min_length=1, max_length=200)
    _name = field_validator("name")(trimmed)


class RoleDTO(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: UUID
    name: str
    permissions: list[str]
    priority: int
    created_at: datetime
    updated_at: datetime


class MappingCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    id: UUID | None = None
    issuer: str
    claim_name: str = Field(min_length=1, max_length=200)
    claim_value: str = Field(min_length=1, max_length=320)
    role_id: UUID

    @field_validator("issuer")
    @classmethod
    def issuer_url(cls, value):
        from urllib.parse import urlparse
        value = trimmed(value)
        parsed = urlparse(value)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc or parsed.query or parsed.fragment:
            raise ValueError("issuer must be an absolute HTTP(S) URL without query or fragment")
        return value

    _claim_name = field_validator("claim_name")(trimmed)
    _claim_value = field_validator("claim_value")(trimmed)


class MappingPatch(BaseModel):
    model_config = ConfigDict(extra="forbid")
    issuer: str | None = None
    claim_name: str | None = Field(default=None, min_length=1, max_length=200)
    claim_value: str | None = Field(default=None, min_length=1, max_length=320)
    role_id: UUID | None = None

    @field_validator("issuer")
    @classmethod
    def issuer_url(cls, value):
        if value is None:
            return value
        from urllib.parse import urlparse
        value = trimmed(value)
        parsed = urlparse(value)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc or parsed.query or parsed.fragment:
            raise ValueError("issuer must be an absolute HTTP(S) URL without query or fragment")
        return value

    @field_validator("claim_name", "claim_value")
    @classmethod
    def trim_claims(cls, value):
        return None if value is None else trimmed(value)

    @model_validator(mode="after")
    def nonempty(self):
        if not self.model_fields_set:
            raise ValueError("patch must not be empty")
        for field_name in ("issuer", "claim_name", "claim_value", "role_id"):
            if field_name in self.model_fields_set and getattr(self, field_name) is None:
                raise ValueError(f"{field_name} must not be null")
        return self


class MappingDTO(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: UUID
    issuer: str
    claim_name: str
    claim_value: str
    role_id: UUID | None
    created_at: datetime
    updated_at: datetime


class MappingPage(BaseModel):
    items: list[MappingDTO]
    total: int
    limit: int
    offset: int


class SettingDTO(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: UUID
    key: str
    value: Any
    category: str
    version: int
    is_public: bool
    updated_by_user_id: UUID | None
    created_at: datetime
    updated_at: datetime


class SettingUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    value: Any
    version: int = Field(ge=1)


class NormalizerCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    id: UUID | None = None
    name: str = Field(min_length=1, max_length=200)
    description: str | None = None
    rule: str = Field(min_length=1)
    _name = field_validator("name")(trimmed)

    @field_validator("rule")
    @classmethod
    def nonblank_rule(cls, value):
        if not value.strip():
            raise ValueError("rule must not be blank")
        return value


class NormalizerPatch(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str | None = Field(default=None, min_length=1, max_length=200)
    description: str | None = None
    rule: str | None = Field(default=None, min_length=1)
    version: int = Field(ge=1)

    @model_validator(mode="after")
    def has_changes(self):
        if not self.model_fields_set - {"version"}:
            raise ValueError("patch must contain a change")
        for field_name in ("name", "rule"):
            if field_name in self.model_fields_set and getattr(self, field_name) is None:
                raise ValueError(f"{field_name} must not be null")
        return self

    @field_validator("name")
    @classmethod
    def trim_name(cls, value):
        return value if value is None else trimmed(value)

    @field_validator("rule")
    @classmethod
    def nonblank_rule(cls, value):
        if value is not None and not value.strip():
            raise ValueError("rule must not be blank")
        return value


class NormalizerDTO(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: UUID
    name: str
    description: str | None
    rule: str
    version: int
    created_by_user_id: UUID | None
    updated_by_user_id: UUID | None
    created_at: datetime
    updated_at: datetime


class NormalizerPage(BaseModel):
    items: list[NormalizerDTO]
    total: int
    limit: int
    offset: int
