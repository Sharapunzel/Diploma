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
        self, connection_id: UUID, topic_name: str, partition: int | None = None,
        topic_id: str | None = None,
    ) -> bool: ...
    def known_gap_end(self, connection_id: UUID, topic_id: str, partition: int) -> int | None: ...
    def has_legacy_history(self, connection_id: UUID, topic_name: str) -> bool: ...
    def bind_cluster(self, connection_id: UUID, cluster_id: str) -> bool: ...
    def record_retention_gap(
        self, source_id: UUID, topic_id: str, partition: int, start: int, end: int
    ) -> bool: ...
    def archive_recreated(self, source_id: UUID, old_id: str, new_id: str) -> bool: ...
    def durable_topic_identities(
        self, source_id: UUID
    ) -> set[str | None]: ...
    def set_topic_identity_if_current(
        self, source_id: UUID, expected: str | None, identity: str
    ) -> bool: ...
