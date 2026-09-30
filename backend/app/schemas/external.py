import re
from datetime import datetime
from typing import Literal
from urllib.parse import urlsplit
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator, model_validator

from .administration import trimmed

_INDEX = re.compile(r"^[a-z0-9][a-z0-9._-]{0,254}$")
_PATTERN = re.compile(r"^[a-z0-9][a-z0-9._*?-]{0,254}$")
TargetType = Literal["index", "index_pattern", "data_stream", "data_stream_pattern"]
TARGET_FIELDS = ("index_name", "index_pattern", "data_stream_name", "data_stream_pattern")
TYPE_FIELD = dict(zip(TargetType.__args__, TARGET_FIELDS))


def valid_url(value: str) -> str:
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError as error:
        raise ValueError("invalid indexer URL") from error
    if (parsed.scheme != "https" or not parsed.hostname or parsed.username is not None
            or parsed.password is not None or "?" in value or "#" in value
            or (port is not None and not 1 <= port <= 65535)
            or "\\" in value or any(ord(c) < 33 for c in value)
            or "//" in parsed.path or "%" in parsed.path
            or "/../" in parsed.path or "/./" in parsed.path
            or parsed.path.endswith(("/..", "/."))):
        raise ValueError("invalid indexer URL")
    return value.rstrip("/")


def valid_target(value: str, pattern: bool = False) -> str:
    if not ( _PATTERN if pattern else _INDEX).fullmatch(value) or value.startswith(("_", ".")):
        raise ValueError("invalid index name or pattern")
    if pattern and not any(char in value for char in "*?"):
        raise ValueError("index pattern requires * or ?")
    return value


class ExternalConnectionCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str = Field(min_length=1, max_length=200)
    base_url: str = Field(max_length=2048)
    username: str = Field(min_length=1, max_length=200)
    password: SecretStr

    _name = field_validator("name", "username")(trimmed)
    _url = field_validator("base_url")(valid_url)

    @field_validator("password")
    @classmethod
    def password_required(cls, value):
        if not value.get_secret_value():
            raise ValueError("password must not be empty")
        return value


class ExternalConnectionPatch(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str | None = Field(None, min_length=1, max_length=200)
    base_url: str | None = Field(None, max_length=2048)
    username: str | None = Field(None, min_length=1, max_length=200)
    password: SecretStr | None = None

    @model_validator(mode="after")
    def nonempty(self):
        if not self.model_fields_set or any(getattr(self, key) is None for key in self.model_fields_set):
            raise ValueError("patch fields must be present and non-null")
        return self

    @field_validator("name", "username")
    @classmethod
    def trim_fields(cls, value):
        return trimmed(value) if value is not None else value

    @field_validator("base_url")
    @classmethod
    def check_url(cls, value):
        return valid_url(value) if value is not None else value

    @field_validator("password")
    @classmethod
    def check_password(cls, value):
        if value is not None and not value.get_secret_value():
            raise ValueError("password must not be empty")
        return value


class ExternalConnectionDTO(BaseModel):
    id: UUID
    name: str
    base_url: str
    username: str
    has_password: bool
    has_ca: bool
    created_at: datetime
    updated_at: datetime


class ExternalConnectionPage(BaseModel):
    items: list[ExternalConnectionDTO]
    total: int
    limit: int
    offset: int


class ExternalSourceCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str = Field(min_length=1, max_length=200)
    external_connection_id: UUID
    target_type: TargetType
    index_name: str | None = None
    index_pattern: str | None = None
    data_stream_name: str | None = None
    data_stream_pattern: str | None = None

    _name = field_validator("name")(trimmed)

    @model_validator(mode="after")
    def target(self):
        field = TYPE_FIELD[self.target_type]
        if any((getattr(self, key) is not None) != (key == field) for key in TARGET_FIELDS):
            raise ValueError("provide exactly the target selected by target_type")
        valid_target(getattr(self, field), field.endswith("pattern"))
        return self


class ExternalSourcePatch(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str | None = Field(None, min_length=1, max_length=200)
    external_connection_id: UUID | None = None
    target_type: TargetType | None = None
    index_name: str | None = None
    index_pattern: str | None = None
    data_stream_name: str | None = None
    data_stream_pattern: str | None = None

    @model_validator(mode="after")
    def patch(self):
        if not self.model_fields_set:
            raise ValueError("patch must not be empty")
        if "name" in self.model_fields_set and self.name is None:
            raise ValueError("name must not be null")
        if "external_connection_id" in self.model_fields_set and self.external_connection_id is None:
            raise ValueError("external_connection_id must not be null")
        selected = [key for key in TARGET_FIELDS if key in self.model_fields_set]
        if selected or "target_type" in self.model_fields_set:
            if self.target_type is None or selected != [TYPE_FIELD[self.target_type]]:
                raise ValueError("target_type and its one target field are required together")
            value = getattr(self, selected[0])
            if value is None:
                raise ValueError("target must not be null")
            valid_target(value, selected[0].endswith("pattern"))
        return self

    @field_validator("name")
    @classmethod
    def trim_name(cls, value):
        return trimmed(value) if value is not None else value


class ExternalSourceDTO(BaseModel):
    id: UUID
    source_type: Literal["external"]
    name: str
    external_connection_id: UUID
    target_type: TargetType
    index_name: str | None
    index_pattern: str | None
    data_stream_name: str | None
    data_stream_pattern: str | None
    is_enabled: bool
    created_at: datetime
    updated_at: datetime


class ExternalSourcePage(BaseModel):
    items: list[ExternalSourceDTO]
    total: int
    limit: int
    offset: int


class IndexPage(BaseModel):
    items: list[str]
    total: int
    limit: int
    offset: int


class ExternalTestResponse(BaseModel):
    status: Literal["ok"]
