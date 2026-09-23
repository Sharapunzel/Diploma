import ipaddress
import re
from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .administration import trimmed

_HOST = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?$")


def validate_bootstrap(value: str) -> str:
    if not value or value != value.strip() or any(char.isspace() or ord(char) < 32 for char in value):
        raise ValueError("invalid bootstrap server")
    if value.startswith("["):
        match = re.fullmatch(r"\[([^]]+)\]:(\d+)", value)
        if not match:
            raise ValueError("invalid bootstrap server")
        try:
            ipaddress.IPv6Address(match.group(1))
        except ValueError as error:
            raise ValueError("invalid bootstrap server") from error
        port = int(match.group(2))
    else:
        host, separator, port_text = value.rpartition(":")
        if not separator or not _HOST.fullmatch(host) or ":" in host or not port_text.isdigit():
            raise ValueError("invalid bootstrap server")
        port = int(port_text)
    if not 1 <= port <= 65535:
        raise ValueError("invalid bootstrap server")
    return value


class KafkaConnectionCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    id: UUID | None = None
    name: str = Field(min_length=1, max_length=200)
    bootstrap_servers: list[str] = Field(min_length=1, max_length=16)
    security_protocol: Literal["PLAINTEXT"] = "PLAINTEXT"

    _name = field_validator("name")(trimmed)
    _servers = field_validator("bootstrap_servers")(
        lambda values: validate_unique_servers(values)
    )


def validate_unique_servers(values: list[str]) -> list[str]:
    checked = [validate_bootstrap(value) for value in values]
    if len(set(checked)) != len(checked):
        raise ValueError("bootstrap servers must be unique")
    return checked


class KafkaConnectionPatch(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str | None = Field(default=None, min_length=1, max_length=200)
    bootstrap_servers: list[str] | None = Field(default=None, min_length=1, max_length=16)
    security_protocol: Literal["PLAINTEXT"] | None = None

    @model_validator(mode="after")
    def nonempty(self):
        if not self.model_fields_set:
            raise ValueError("patch must not be empty")
        if "name" in self.model_fields_set and self.name is None:
            raise ValueError("name must not be null")
        if "bootstrap_servers" in self.model_fields_set and self.bootstrap_servers is None:
            raise ValueError("bootstrap_servers must not be null")
        if "security_protocol" in self.model_fields_set and self.security_protocol is None:
            raise ValueError("security_protocol must not be null")
        return self

    @field_validator("name")
    @classmethod
    def trim_name(cls, value):
        return value if value is None else trimmed(value)

    @field_validator("bootstrap_servers")
    @classmethod
    def validate_servers(cls, value):
        return value if value is None else validate_unique_servers(value)


class KafkaConnectionDTO(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: UUID
    name: str
    bootstrap_servers: list[str]
    security_protocol: str
    extra_config: dict
    created_at: datetime
    updated_at: datetime


class KafkaConnectionPage(BaseModel):
    items: list[KafkaConnectionDTO]
    total: int
    limit: int
    offset: int


class KafkaTestResponse(BaseModel):
    status: Literal["ok"]
    broker_count: int
    topic_count: int
    latency_ms: float


class TopicDTO(BaseModel):
    name: str
    partition_count: int


class TopicPage(BaseModel):
    items: list[TopicDTO]
    total: int


class SourceCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    id: UUID | None = None
    name: str = Field(min_length=1, max_length=200)
    connection_id: UUID
    topic_name: str = Field(min_length=1, max_length=500)

    _name = field_validator("name")(trimmed)
    _topic = field_validator("topic_name")(trimmed)


class SourcePatch(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str | None = Field(default=None, min_length=1, max_length=200)
    connection_id: UUID | None = None
    topic_name: str | None = Field(default=None, min_length=1, max_length=500)

    @model_validator(mode="after")
    def nonempty(self):
        if not self.model_fields_set:
            raise ValueError("patch must not be empty")
        for field_name in ("name", "connection_id", "topic_name"):
            if field_name in self.model_fields_set and getattr(self, field_name) is None:
                raise ValueError(f"{field_name} must not be null")
        return self

    @field_validator("name", "topic_name")
    @classmethod
    def trim_fields(cls, value):
        return value if value is None else trimmed(value)


class SourceNormalizerUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    normalizer_id: UUID | None


class SourceDTO(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: UUID
    name: str
    connection_id: UUID
    normalizer_id: UUID | None
    topic_name: str
    is_enabled: bool
    created_at: datetime
    updated_at: datetime


class SourcePage(BaseModel):
    items: list[SourceDTO]
    total: int
    limit: int
    offset: int
