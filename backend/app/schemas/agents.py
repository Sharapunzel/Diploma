import re
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class AgentTopicRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    connection_id: UUID
    topic_name: str = Field(min_length=1, max_length=249)
    action: Literal["create", "attach"]

    @field_validator("topic_name")
    @classmethod
    def topic_is_valid(cls, value: str) -> str:
        if value != value.strip() or any(ord(char) < 33 for char in value):
            raise ValueError("invalid Kafka topic name")
        if value in {".", ".."} or not re.fullmatch(r"[A-Za-z0-9._-]{1,249}", value):
            raise ValueError("invalid Kafka topic name")
        return value


class AgentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str = Field(min_length=1, max_length=200)
    description: str | None = Field(default=None, max_length=2000)
    topics: list[AgentTopicRequest] = Field(min_length=1, max_length=50)

    @model_validator(mode="after")
    def topics_are_unique(self):
        keys = [(topic.connection_id, topic.topic_name) for topic in self.topics]
        if len(keys) != len(set(keys)):
            raise ValueError("duplicate Kafka topic assignment")
        return self


class AgentPatch(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str | None = Field(default=None, min_length=1, max_length=200)
    description: str | None = Field(default=None, max_length=2000)


class AgentTopicDTO(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    connection_id: UUID
    source_id: UUID
    topic_name: str
    topic_identity: str | None
    state: str


class AgentDTO(BaseModel):
    id: UUID
    name: str
    description: str | None
    state: str
    access_revision: int
    credentials_claimable: bool
    safe_error: str | None
    operation_id: UUID | None = None
    topics: list[AgentTopicDTO]


class AgentPage(BaseModel):
    items: list[AgentDTO]
    total: int
    limit: int
    offset: int


class ProvisioningOperationDTO(BaseModel):
    id: UUID
    agent_id: UUID
    kind: str
    state: str
    attempts: int
    safe_error: str | None


class OneTimeCredentials(BaseModel):
    agent_id: UUID
    revision: int
    username: str
    password: str
