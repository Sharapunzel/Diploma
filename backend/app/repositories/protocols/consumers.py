from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol
from uuid import UUID


@dataclass(frozen=True)
class ConsumerSourceConfig:
    source_id: UUID
    connection_id: UUID
    bootstrap_servers: tuple[str, ...]
    security_protocol: str
    topic_name: str
    kafka_topic_identity: str | None


class ConsumerStateRepository(Protocol):
    def list_enabled_sources(self) -> list[ConsumerSourceConfig]: ...
    def get_enabled_source(self, source_id: UUID) -> ConsumerSourceConfig | None: ...
    def has_durable_history(
        self, connection_id: UUID, topic_name: str, partition: int | None = None
    ) -> bool: ...
    def durable_topic_identities(
        self, connection_id: UUID, topic_name: str
    ) -> set[str | None]: ...
    def set_topic_identity_if_current(
        self, source_id: UUID, expected: str | None, identity: str
    ) -> bool: ...
