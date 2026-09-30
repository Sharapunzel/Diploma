from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy.exc import IntegrityError

from ...core.errors import DomainError
from ...kafka import KafkaConnectionConfig, KafkaMetadataClient, KafkaMetadataError
from ...models import KafkaConnection, Source
from ...repositories.protocols import UnitOfWork
from ...repositories.protocols.administration import NormalizerRepository
from ...repositories.protocols.connections import KafkaConnectionRepository, SourceRepository
from ...schemas.connections import (
    KafkaConnectionCreate,
    KafkaConnectionPatch,
    SourceCreate,
    SourceNormalizerUpdate,
    SourcePatch,
)
from ..protocols.consumers import SourceConsumerLifecycle


def current_time() -> datetime:
    return datetime.now(UTC)


def fail(code: str, message: str, status: int = 404, details: dict | None = None) -> None:
    raise DomainError(code, message, status, details)


def kafka_error(error: KafkaMetadataError) -> None:
    if error.kind == "timeout":
        fail("kafka_timeout", "Kafka metadata request timed out", 504)
    fail("kafka_unavailable", "Kafka is unavailable", 503)


def metadata_for(client: KafkaMetadataClient, config: KafkaConnectionConfig, timeout: int):
    try:
        return client.metadata(config, timeout)
    except KafkaMetadataError as error:
        kafka_error(error)


def executable_rule(rule: object) -> bool:
    return (
        isinstance(rule, dict)
        and type(rule.get("format_version")) is int
        and rule["format_version"] == 1
    )


class KafkaConnectionServiceImpl:
    def __init__(
        self,
        repository: KafkaConnectionRepository,
        uow: UnitOfWork,
        client: KafkaMetadataClient,
        timeout: int,
        lifecycle: SourceConsumerLifecycle | None = None,
    ) -> None:
        self.repository, self.uow, self.client, self.timeout = repository, uow, client, timeout
        self.lifecycle = lifecycle

    def list(self, query: str | None, limit: int, offset: int) -> tuple[list[KafkaConnection], int]:
        return self.repository.list(query, limit, offset)

    def get(self, connection_id: UUID) -> KafkaConnection:
        connection = self.repository.find_by_id(connection_id)
        if connection is None:
            fail("connection_not_found", "Kafka connection not found")
        return connection

    def create(self, data: KafkaConnectionCreate) -> KafkaConnection:
        connection = KafkaConnection(
            id=data.id,
            name=data.name,
            bootstrap_servers=data.bootstrap_servers,
            security_protocol=data.security_protocol,
        )
        try:
            self.repository.add(connection)
            self.uow.commit()
            return connection
        except IntegrityError as error:
            self.uow.rollback()
            constraint = getattr(getattr(error.orig, "diag", None), "constraint_name", "")
            fail("connection_name_conflict" if constraint == "uq_kafka_connections_name" else "integrity_conflict", "Kafka connection conflicts with existing data", 409)
        except Exception:
            self.uow.rollback()
            raise

    def update(self, connection_id: UUID, data: KafkaConnectionPatch) -> KafkaConnection:
        connection = self.repository.find_by_id_for_update(connection_id)
        if connection is None:
            fail("connection_not_found", "Kafka connection not found")
        values = data.model_dump(exclude_unset=True)
        changing_config = any(key in values for key in ("bootstrap_servers", "security_protocol"))
        if changing_config and self.repository.has_enabled_sources(connection.id):
            fail("source_enabled", "Enabled sources prevent connection changes", 409)
        if changing_config and self.repository.has_sources(connection.id):
            fail("source_exists", "Registered sources prevent Kafka cluster changes", 409)
        if changing_config:
            values["cluster_identity"] = None
        try:
            self.repository.update(connection, values, current_time())
            self.uow.commit()
            return connection
        except IntegrityError as error:
            self.uow.rollback()
            constraint = getattr(getattr(error.orig, "diag", None), "constraint_name", "")
            fail("connection_name_conflict" if constraint == "uq_kafka_connections_name" else "integrity_conflict", "Kafka connection conflicts with existing data", 409)
        except Exception:
            self.uow.rollback()
            raise

    def delete(self, connection_id: UUID) -> None:
        connection = self.get(connection_id)
        try:
            self.repository.delete(connection)
            self.uow.commit()
            if self.lifecycle is not None:
                self.lifecycle.wake()
        except Exception:
            self.uow.rollback()
            raise

    def test(self, connection_id: UUID) -> dict:
        connection = self.get(connection_id)
        config = KafkaConnectionConfig(tuple(connection.bootstrap_servers), connection.security_protocol)
        self.uow.rollback()
        metadata = metadata_for(self.client, config, self.timeout)
        return {
            "status": "ok",
            "broker_count": metadata.broker_count,
            "topic_count": len(metadata.topics),
            "latency_ms": metadata.latency_ms,
        }

    def topics(self, connection_id: UUID, include_internal: bool) -> tuple[list[dict], int]:
        connection = self.get(connection_id)
        registered = self.repository.current_topic_ids(connection_id)
        config = KafkaConnectionConfig(tuple(connection.bootstrap_servers), connection.security_protocol)
        self.uow.rollback()
        metadata = metadata_for(self.client, config, self.timeout)
        topics = [topic for topic in metadata.topics if include_internal or not topic.name.startswith("__")]
        topics.sort(key=lambda topic: topic.name)
        result = [
            {
                "name": topic.name,
                "partition_count": topic.partition_count,
                "is_registered": bool(topic.identity and registered.get(topic.name) == topic.identity),
            }
            for topic in topics
        ]
        return result, len(result)


class SourceServiceImpl:
    def __init__(
        self,
        sources: SourceRepository,
        connections: KafkaConnectionRepository,
        normalizers: NormalizerRepository,
        uow: UnitOfWork,
        client: KafkaMetadataClient,
        timeout: int,
        lifecycle: SourceConsumerLifecycle | None = None,
    ) -> None:
        self.sources, self.connections, self.normalizers = sources, connections, normalizers
        self.uow, self.client, self.timeout = uow, client, timeout
        self.lifecycle = lifecycle

    def list(self, query, connection_id, normalizer_id, is_enabled, limit, offset):
        return self.sources.list(query, connection_id, normalizer_id, is_enabled, limit, offset)

    def get(self, source_id: UUID) -> Source:
        source = self.sources.find_by_id(source_id)
        if source is None or source.source_type != "kafka":
            fail("source_not_found", "Source not found")
        return source

    def _connection(self, connection_id: UUID) -> KafkaConnection:
        connection = self.connections.find_by_id(connection_id)
        if connection is None:
            fail("connection_not_found", "Kafka connection not found")
        return connection

    def _check_topic(self, connection: KafkaConnection, topic_name: str):
        config = KafkaConnectionConfig(tuple(connection.bootstrap_servers), connection.security_protocol)
        self.uow.rollback()
        metadata = metadata_for(self.client, config, self.timeout)
        topic = next((item for item in metadata.topics if item.name == topic_name), None)
        if topic is None:
            fail("topic_not_found", "Kafka topic not found", 404)
        if not isinstance(topic.identity, str) or not topic.identity.strip():
            fail("topic_identity_unavailable", "Kafka topic identity is unavailable", 503)
        if not isinstance(metadata.cluster_identity, str) or not metadata.cluster_identity.strip():
            fail("cluster_identity_unavailable", "Kafka cluster identity is unavailable", 503)
        return topic, metadata.cluster_identity

    def _bind_cluster(self, connection: KafkaConnection, cluster_id: str) -> None:
        try:
            self.connections.bind_cluster(connection, cluster_id)
        except (IntegrityError, ValueError):
            self.uow.rollback()
            fail("cluster_connection_conflict", "Kafka cluster is already registered or changed", 409)

    def create(self, data: SourceCreate) -> Source:
        connection = self._connection(data.connection_id)
        snapshot = (connection.id, tuple(connection.bootstrap_servers), connection.security_protocol, data.topic_name)
        topic, cluster_id = self._check_topic(connection, data.topic_name)
        connection = self.connections.find_by_id_for_update(snapshot[0])
        if connection is None or (connection.id, tuple(connection.bootstrap_servers), connection.security_protocol, data.topic_name) != snapshot:
            fail("source_configuration_changed", "Connection configuration changed during Kafka check", 409)
        self._bind_cluster(connection, cluster_id)
        try:
            previous = self.sources.find_current_by_topic_for_update(
                connection.id, data.topic_name
            )
            stopped_source_id = None
            if previous is not None:
                if previous.kafka_topic_identity in (None, topic.identity):
                    fail("source_topic_conflict", "Source already registered for topic", 409)
                if not self.sources.archive_recreated(
                    previous.id, previous.kafka_topic_identity, topic.identity
                ):
                    fail("source_configuration_changed", "Source configuration changed", 409)
                stopped_source_id = previous.id
            source = Source(
                id=data.id,
                name=data.name,
                connection_id=connection.id,
                topic_name=data.topic_name,
                kafka_topic_identity=topic.identity,
                is_enabled=False,
                normalizer_id=None,
            )
            self.sources.add(source)
            self.uow.commit()
            if stopped_source_id is not None and self.lifecycle is not None:
                self.lifecycle.stop_source(stopped_source_id)
            return source
        except IntegrityError as error:
            self.uow.rollback()
            constraint = getattr(getattr(error.orig, "diag", None), "constraint_name", "")
            fail("source_topic_conflict" if "sources_current_" in constraint else "integrity_conflict", "Source conflicts with existing data", 409)
        except Exception:
            self.uow.rollback()
            raise

    def update(self, source_id: UUID, data: SourcePatch) -> Source:
        source = self.get(source_id)
        if source.is_archived:
            fail("source_archived", "Archived source cannot be changed", 409)
        values = data.model_dump(exclude_unset=True)
        if source.is_enabled and any(key in values for key in ("connection_id", "topic_name")):
            fail("source_enabled", "Enabled source connection or topic cannot change", 409)
        target_connection_id = values.get("connection_id", source.connection_id)
        target_topic = values.get("topic_name", source.topic_name)
        if "connection_id" not in values and "topic_name" not in values:
            source = self.sources.find_by_id_for_update(source_id)
            if source is None or source.is_archived:
                fail("source_archived", "Archived source cannot be changed", 409)
        if "connection_id" in values or "topic_name" in values:
            connection = self._connection(target_connection_id)
            snapshot = (source.id, source.connection_id, source.topic_name, source.is_enabled,
                        connection.id, tuple(connection.bootstrap_servers), connection.security_protocol, target_topic)
            topic, cluster_id = self._check_topic(connection, target_topic)
            old_connection_id = snapshot[1]
            checked_target_connection_id = snapshot[4]
            connection_ids = sorted(
                {old_connection_id, checked_target_connection_id}, key=str
            )
            locked_connections = {}
            for connection_id_to_lock in connection_ids:
                locked_connections[connection_id_to_lock] = (
                    self.connections.find_by_id_for_update(connection_id_to_lock)
                )
            source = self.sources.find_by_id_for_update(source_id)
            current_connection = locked_connections.get(checked_target_connection_id)
            current = (source.id if source else None, source.connection_id if source else None,
                       source.topic_name if source else None, source.is_enabled if source else None,
                       current_connection.id if current_connection else None,
                       tuple(current_connection.bootstrap_servers) if current_connection else None,
                       current_connection.security_protocol if current_connection else None, target_topic)
            if source is None or current_connection is None or current != snapshot:
                fail("source_configuration_changed", "Source configuration changed during Kafka check", 409)
            if source.is_archived:
                fail("source_archived", "Archived source cannot be changed", 409)
            if source.is_enabled:
                fail("source_enabled", "Enabled source connection or topic cannot change", 409)
            self._bind_cluster(current_connection, cluster_id)
            if (
                source.kafka_topic_identity is not None
                and source.kafka_topic_identity != topic.identity
            ):
                self.uow.rollback()
                fail("topic_identity_changed", "Kafka topic generation changed; register a new source", 409)
            old_history = self.sources.has_durable_history(
                source.connection_id, source.topic_name
            )
            target_history = self.sources.has_durable_history(
                target_connection_id, target_topic
            )
            if source.kafka_topic_identity is None and not old_history and not target_history:
                values["kafka_topic_identity"] = topic.identity
        try:
            self.sources.update(source, values, current_time())
            self.uow.commit()
            return source
        except IntegrityError as error:
            self.uow.rollback()
            constraint = getattr(getattr(error.orig, "diag", None), "constraint_name", "")
            fail("source_topic_conflict" if "sources_current_" in constraint else "integrity_conflict", "Source conflicts with existing data", 409)
        except Exception:
            self.uow.rollback()
            raise

    def delete(self, source_id: UUID) -> None:
        source = self.get(source_id)
        try:
            self.sources.delete(source)
            self.uow.commit()
            if self.lifecycle is not None:
                self.lifecycle.stop_source(source_id)
        except Exception:
            self.uow.rollback()
            raise

    def set_normalizer(self, source_id: UUID, data: SourceNormalizerUpdate) -> Source:
        source = self.sources.find_by_id_for_update(source_id)
        if source is None or source.source_type != "kafka":
            fail("source_not_found", "Source not found")
        if source.is_archived:
            fail("source_archived", "Archived source cannot be changed", 409)
        if source.is_enabled:
            fail("source_enabled", "Enabled source normalizer cannot change", 409)
        if data.normalizer_id is not None and self.normalizers.find_by_id(data.normalizer_id) is None:
            fail("normalizer_not_found", "Normalizer not found")
        try:
            self.sources.update(source, {"normalizer_id": data.normalizer_id}, current_time())
            self.uow.commit()
            return source
        except Exception:
            self.uow.rollback()
            raise

    def enable(self, source_id: UUID) -> Source:
        source = self.get(source_id)
        if source.is_archived:
            fail("source_archived", "Archived source cannot be enabled", 409)
        if source.normalizer_id is None:
            fail("source_normalizer_required", "Source requires a normalizer", 409)
        connection = self._connection(source.connection_id)
        normalizer = self.normalizers.find_by_id(source.normalizer_id)
        if normalizer is None:
            fail("normalizer_not_found", "Normalizer not found")
        if not executable_rule(normalizer.rule):
            fail("normalizer_legacy_incompatible", "Normalizer rule must be upgraded to v1", 409)
        snapshot = (source.connection_id, source.topic_name, source.normalizer_id, source.is_enabled,
                    connection.id, tuple(connection.bootstrap_servers), connection.security_protocol)
        topic, cluster_id = self._check_topic(connection, source.topic_name)
        if source.kafka_topic_identity is not None and source.kafka_topic_identity != topic.identity:
            fail("topic_recreated", "Kafka topic identity changed; await source archival", 409)
        checked = snapshot
        connection = self.connections.find_by_id_for_update(checked[0])
        source = self.sources.find_by_id_for_update(source_id)
        if source is None or connection is None:
            fail("source_configuration_changed", "Source configuration changed during Kafka check", 409)
        current = (source.connection_id, source.topic_name, source.normalizer_id, source.is_enabled,
                   connection.id, tuple(connection.bootstrap_servers), connection.security_protocol)
        current_normalizer = self.normalizers.find_by_id_for_update(source.normalizer_id)
        if current != checked or current_normalizer is None:
            fail("source_configuration_changed", "Source configuration changed during Kafka check", 409)
        if source.is_archived or (
            source.kafka_topic_identity is not None
            and source.kafka_topic_identity != topic.identity
        ):
            fail("source_configuration_changed", "Source generation changed during Kafka check", 409)
        self._bind_cluster(connection, cluster_id)
        if source.kafka_topic_identity is None and self.sources.has_durable_history(
            source.connection_id, source.topic_name
        ):
            fail("topic_identity_history_unknown", "Legacy topic identity needs explicit resolution", 409)
        if not executable_rule(current_normalizer.rule):
            fail("normalizer_legacy_incompatible", "Normalizer rule must be upgraded to v1", 409)
        try:
            self.sources.update(
                source,
                {"is_enabled": True, "kafka_topic_identity": topic.identity},
                current_time(),
            )
            self.uow.commit()
            if self.lifecycle is not None:
                self.lifecycle.wake()
            return source
        except Exception:
            self.uow.rollback()
            raise

    def disable(self, source_id: UUID) -> Source:
        source = self.sources.find_by_id_for_update(source_id)
        if source is None or source.source_type != "kafka":
            fail("source_not_found", "Source not found")
        try:
            self.sources.update(source, {"is_enabled": False}, current_time())
            self.uow.commit()
            if self.lifecycle is not None:
                self.lifecycle.stop_source(source_id)
            return source
        except Exception:
            self.uow.rollback()
            raise
