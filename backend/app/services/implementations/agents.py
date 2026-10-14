from uuid import UUID, uuid4

from cryptography.fernet import InvalidToken
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from ...agent_credentials import decrypt_pending
from ...core.errors import DomainError
from ...kafka import KafkaConnectionConfig, KafkaMetadataClient, KafkaMetadataError
from ...models import (
    FluentBitAgent,
    FluentBitAgentTopic,
    KafkaConnection,
    KafkaProvisioningOperation,
    ProcessedKafkaRecord,
    Source,
)
from ...repositories.sqlalchemy.agents import SqlAlchemyAgentRepository
from ...schemas.agents import AgentCreate, AgentPatch, OneTimeCredentials


def _fail(code: str, message: str, status: int = 409):
    raise DomainError(code, message, status)


class AgentLifecycleServiceImpl:
    def __init__(self, session: Session, metadata: KafkaMetadataClient, settings):
        self.session, self.metadata, self.settings = session, metadata, settings
        self.repository = SqlAlchemyAgentRepository(session)

    def _prior(self, request_key: str, kind: str, agent_id: UUID | None, request: dict):
        prior = self.repository.by_request_key(request_key)
        if prior is None:
            return None
        agent, operation = prior
        if (
            operation.kind != kind
            or (agent_id is not None and agent.id != agent_id)
            or operation.payload.get("request") != request
        ):
            _fail("idempotency_key_conflict", "Idempotency key belongs to another request")
        return prior

    def _resolve_key_race(self, request_key: str, kind: str, agent_id: UUID | None, request: dict):
        self.session.rollback()
        prior = self._prior(request_key, kind, agent_id, request)
        if prior is not None:
            return prior
        _fail("agent_topic_conflict", "Topic or agent state changed concurrently")

    def list(self, limit: int, offset: int):
        items, total = self.repository.list(limit, offset)
        result = []
        for item in items:
            operation = self.repository.latest_operation(item.id)
            result.append(
                {
                    "agent": item,
                    "topics": self.repository.topics(item.id),
                    "operation_id": operation.id if operation else None,
                }
            )
        return result, total

    def get(self, agent_id: UUID):
        agent = self.repository.get(agent_id)
        if agent is None:
            _fail("agent_not_found", "Agent not found", 404)
        operation = self.repository.latest_operation(agent.id)
        return {
            "agent": agent,
            "topics": self.repository.topics(agent.id),
            "operation_id": operation.id if operation else None,
        }

    def create(self, data: AgentCreate, request_key: str):
        request = data.model_dump(mode="json")
        prior = self._prior(request_key, "register", None, request)
        if prior:
            return prior[0], prior[1]
        requests = [(item.connection_id, item.topic_name, item.action) for item in data.topics]
        if len(set(requests)) != len(requests) or len({(c, n) for c, n, _ in requests}) != len(
            requests
        ):
            _fail("duplicate_agent_topic", "Topic is repeated in this request", 422)
        agent = FluentBitAgent(id=uuid4(), name=data.name.strip(), description=data.description)
        assignments, payload, cluster_ids = [], [], set()
        for item in data.topics:
            connection = self.session.scalar(
                select(KafkaConnection)
                .where(KafkaConnection.id == item.connection_id)
                .with_for_update()
            )
            if connection is None:
                _fail("connection_not_found", "Kafka connection not found", 404)
            prior = self._prior(request_key, "register", None, request)
            if prior:
                return prior
            config = KafkaConnectionConfig(
                tuple(connection.bootstrap_servers), connection.security_protocol
            )
            try:
                snapshot = self.metadata.metadata(
                    config, self.settings.kafka_metadata_timeout_seconds
                )
            except KafkaMetadataError:
                _fail("kafka_unavailable", "Kafka metadata could not be checked", 503)
            if not snapshot.cluster_identity:
                _fail("cluster_identity_unavailable", "Kafka cluster identity is unavailable", 503)
            if connection.cluster_identity not in (None, snapshot.cluster_identity):
                _fail("cluster_identity_changed", "Kafka cluster identity changed", 409)
            connection.cluster_identity = snapshot.cluster_identity
            cluster_ids.add(snapshot.cluster_identity)
            found = next(
                (topic for topic in snapshot.topics if topic.name == item.topic_name), None
            )
            if item.action == "create" and found:
                _fail("topic_already_exists", "Topic exists; choose attach after reviewing it", 409)
            if item.action == "attach" and (found is None or not found.identity):
                _fail("topic_not_found", "Existing topic or its identity is unavailable", 404)
            current = self.session.scalar(
                select(Source)
                .where(
                    Source.connection_id == connection.id,
                    Source.topic_name == item.topic_name,
                    Source.is_archived.is_(False),
                )
                .with_for_update()
            )
            if item.action == "create" and current is not None:
                _fail("source_topic_conflict", "A source already uses this topic", 409)
            if current is not None:
                if current.source_type != "kafka":
                    _fail("source_topic_conflict", "Source type conflicts with Kafka topic", 409)
                if current.kafka_topic_identity not in (None, found.identity if found else None):
                    _fail(
                        "topic_identity_changed",
                        "Topic generation differs from registered source",
                        409,
                    )
                if current.kafka_topic_identity is None:
                    history = self.session.scalar(
                        select(ProcessedKafkaRecord.id)
                        .where(
                            ProcessedKafkaRecord.connection_identity == connection.id,
                            ProcessedKafkaRecord.kafka_topic == item.topic_name,
                        )
                        .limit(1)
                    )
                    if history:
                        _fail(
                            "topic_identity_history_unknown",
                            "Existing topic has legacy history requiring explicit resolution",
                            409,
                        )
                    current.kafka_topic_identity = found.identity
                source = current
            else:
                source = Source(
                    name=f"agent-{agent.id.hex[:12]}-{item.topic_name}",
                    connection_id=connection.id,
                    topic_name=item.topic_name,
                    kafka_topic_identity=found.identity if found else None,
                    source_type="kafka",
                    is_enabled=False,
                    normalizer_id=None,
                    is_archived=False,
                )
                self.session.add(source)
                self.session.flush()
            assignments.append(
                FluentBitAgentTopic(
                    agent_id=agent.id,
                    connection_id=connection.id,
                    source_id=source.id,
                    cluster_identity=snapshot.cluster_identity,
                    topic_name=item.topic_name,
                    topic_identity=found.identity if found else None,
                    state="pending",
                )
            )
            payload.append(
                {
                    "connection_id": str(connection.id),
                    "topic_name": item.topic_name,
                    "source_id": str(source.id),
                    "action": item.action,
                    "topic_identity": found.identity if found else None,
                    "cluster_identity": snapshot.cluster_identity,
                }
            )
        if len(cluster_ids) != 1:
            _fail(
                "multiple_kafka_clusters",
                "All assigned topics must belong to the configured Kafka cluster",
                409,
            )
        agent.connection_id = data.topics[0].connection_id
        agent.cluster_identity = next(iter(cluster_ids))
        operation = KafkaProvisioningOperation(
            agent_id=agent.id,
            kind="register",
            state="pending",
            request_key=request_key,
            payload={"topics": payload, "request": request},
        )
        try:
            self.repository.add(agent, operation, assignments)
            self.session.commit()
            return agent, operation
        except IntegrityError:
            return self._resolve_key_race(request_key, "register", None, request)

    def update(self, agent_id: UUID, data: AgentPatch):
        agent = self.repository.get(agent_id, lock=True)
        if agent is None:
            _fail("agent_not_found", "Agent not found", 404)
        values = data.model_dump(exclude_unset=True)
        if "name" in values and values["name"] is not None:
            values["name"] = values["name"].strip()
        self.repository.update(agent, values)
        self.session.commit()
        return self.get(agent_id)

    def operation(self, operation_id: UUID):
        operation = self.repository.operation(operation_id)
        if operation is None:
            _fail("agent_operation_not_found", "Provisioning operation not found", 404)
        return operation

    def claim_credentials(self, agent_id: UUID):
        if self.settings.agent_credentials_key_file is None:
            _fail("agent_credentials_unavailable", "Credential delivery is not configured", 503)
        try:
            result = self.repository.claim_credentials(agent_id)
            if result is None:
                _fail(
                    "agent_credentials_unavailable",
                    "No ready one-time credentials are available",
                    409,
                )
            agent, ciphertext = result
            username, password = decrypt_pending(
                self.settings.agent_credentials_key_file, ciphertext
            )
            self.session.commit()
            return OneTimeCredentials(
                agent_id=agent.id,
                revision=agent.access_revision,
                username=username,
                password=password,
            )
        except (InvalidToken, OSError, ValueError):
            self.session.rollback()
            _fail(
                "agent_credentials_unavailable", "Credential delivery could not be completed", 503
            )

    def revoke(self, agent_id: UUID, request_key: str):
        request = {"agent_id": str(agent_id)}
        prior = self._prior(request_key, "revoke", agent_id, request)
        if prior:
            return prior[1]
        agent = self.repository.get(agent_id, lock=True)
        if agent is None:
            _fail("agent_not_found", "Agent not found", 404)
        prior = self._prior(request_key, "revoke", agent_id, request)
        if prior:
            return prior[1]
        latest = self.repository.latest_operation(agent_id)
        if latest is not None and latest.state in {"pending", "running", "retry"}:
            _fail("agent_operation_in_progress", "Wait for the current operation to finish", 409)
        if agent.state == "revoked":
            operation = KafkaProvisioningOperation(
                agent_id=agent.id,
                kind="revoke",
                state="succeeded",
                request_key=request_key,
                payload={"request": request},
            )
            self.session.add(operation)
            try:
                self.session.commit()
                return operation
            except IntegrityError:
                return self._resolve_key_race(request_key, "revoke", agent_id, request)[1]
        agent.state = "revocation_pending"
        agent.safe_error = None
        operation = KafkaProvisioningOperation(
            agent_id=agent.id,
            kind="revoke",
            state="pending",
            request_key=request_key,
            payload={"request": request},
        )
        self.session.add(operation)
        try:
            self.session.commit()
            return operation
        except IntegrityError:
            return self._resolve_key_race(request_key, "revoke", agent_id, request)[1]

    def rotate(self, agent_id: UUID, request_key: str):
        request = {"agent_id": str(agent_id)}
        prior = self._prior(request_key, "rotate", agent_id, request)
        if prior:
            return prior[1]
        agent = self.repository.get(agent_id, lock=True)
        if agent is None:
            _fail("agent_not_found", "Agent not found", 404)
        prior = self._prior(request_key, "rotate", agent_id, request)
        if prior:
            return prior[1]
        latest = self.repository.latest_operation(agent_id)
        if agent.state != "ready" or (
            latest is not None and latest.kind == "rotate" and latest.state != "succeeded"
        ):
            _fail("agent_operation_in_progress", "Wait for the current operation to finish", 409)
        old_revision = agent.access_revision
        agent.access_revision += 1
        agent.pending_credentials_ciphertext = None
        agent.state = "provisioning"
        agent.safe_error = None
        operation = KafkaProvisioningOperation(
            agent_id=agent.id,
            kind="rotate",
            state="pending",
            request_key=request_key,
            payload={"old_revision": old_revision, "request": request},
        )
        self.session.add(operation)
        try:
            self.session.commit()
            return operation
        except IntegrityError:
            return self._resolve_key_race(request_key, "rotate", agent_id, request)[1]

    def add_topic(self, agent_id: UUID, data, request_key: str):
        request = {"agent_id": str(agent_id), **data.model_dump(mode="json")}
        prior = self._prior(request_key, "add_topic", agent_id, request)
        if prior:
            return prior[1]
        agent = self.repository.get(agent_id, lock=True)
        if agent is None:
            _fail("agent_not_found", "Agent not found", 404)
        prior = self._prior(request_key, "add_topic", agent_id, request)
        if prior:
            return prior[1]
        if agent.state != "ready":
            _fail(
                "agent_operation_in_progress",
                "Agent must be ready before changing topic assignments",
                409,
            )
        connection = self.session.get(KafkaConnection, data.connection_id)
        if connection is None:
            _fail("connection_not_found", "Kafka connection not found", 404)
        config = KafkaConnectionConfig(
            tuple(connection.bootstrap_servers), connection.security_protocol
        )
        try:
            snapshot = self.metadata.metadata(config, self.settings.kafka_metadata_timeout_seconds)
        except KafkaMetadataError:
            _fail("kafka_unavailable", "Kafka metadata could not be checked", 503)
        if (
            not snapshot.cluster_identity
            or snapshot.cluster_identity != connection.cluster_identity
            or snapshot.cluster_identity != agent.cluster_identity
        ):
            _fail("cluster_identity_changed", "Kafka cluster identity changed", 409)
        found = next((topic for topic in snapshot.topics if topic.name == data.topic_name), None)
        if data.action == "create" and found:
            _fail("topic_already_exists", "Topic exists; choose attach after reviewing it", 409)
        if data.action == "attach" and (found is None or not found.identity):
            _fail("topic_not_found", "Existing topic or its identity is unavailable", 404)
        current = self.session.scalar(
            select(Source)
            .where(
                Source.connection_id == connection.id,
                Source.topic_name == data.topic_name,
                Source.is_archived.is_(False),
            )
            .with_for_update()
        )
        if data.action == "create" and current is not None:
            _fail("source_topic_conflict", "A source already uses this topic", 409)
        if current is not None:
            if current.kafka_topic_identity not in (None, found.identity if found else None):
                _fail(
                    "topic_identity_changed", "Topic generation differs from registered source", 409
                )
            current_assignment = self.session.scalar(
                select(ProcessedKafkaRecord.id)
                .where(
                    ProcessedKafkaRecord.connection_identity == connection.id,
                    ProcessedKafkaRecord.kafka_topic == data.topic_name,
                )
                .limit(1)
            )
            if current.kafka_topic_identity is None and current_assignment:
                _fail(
                    "topic_identity_history_unknown",
                    "Existing topic has legacy history requiring explicit resolution",
                    409,
                )
            if current.kafka_topic_identity is None and found:
                current.kafka_topic_identity = found.identity
            source = current
        else:
            source = Source(
                id=uuid4(),
                name=f"agent-{agent.id.hex[:12]}-{data.topic_name}",
                source_type="kafka",
                connection_id=connection.id,
                topic_name=data.topic_name,
                kafka_topic_identity=found.identity if found else None,
                is_enabled=False,
                is_archived=False,
                normalizer_id=None,
            )
            self.session.add(source)
            self.session.flush()
        assignment = FluentBitAgentTopic(
            agent_id=agent.id,
            connection_id=connection.id,
            source_id=source.id,
            cluster_identity=snapshot.cluster_identity,
            topic_name=data.topic_name,
            topic_identity=found.identity if found else None,
            state="pending",
        )
        operation = KafkaProvisioningOperation(
            agent_id=agent.id,
            kind="add_topic",
            state="pending",
            request_key=request_key,
            payload={
                "request": request,
                "topics": [
                    {
                        "connection_id": str(connection.id),
                        "topic_name": data.topic_name,
                        "source_id": str(source.id),
                        "action": data.action,
                        "topic_identity": found.identity if found else None,
                        "cluster_identity": snapshot.cluster_identity,
                    }
                ],
            },
        )
        try:
            agent.state = "provisioning"
            self.session.add_all([assignment, operation])
            self.session.commit()
            return operation
        except IntegrityError:
            return self._resolve_key_race(request_key, "add_topic", agent_id, request)[1]

    def remove_topic(self, agent_id: UUID, topic_id: UUID, request_key: str):
        request = {"agent_id": str(agent_id), "topic_id": str(topic_id)}
        prior = self._prior(request_key, "remove_topic", agent_id, request)
        if prior:
            return prior[1]
        agent = self.repository.get(agent_id, lock=True)
        if agent is None:
            _fail("agent_not_found", "Agent not found", 404)
        prior = self._prior(request_key, "remove_topic", agent_id, request)
        if prior:
            return prior[1]
        if agent.state not in {"ready", "error"}:
            _fail(
                "agent_operation_in_progress",
                "Agent must be ready before changing topic assignments",
                409,
            )
        assignment = self.session.scalar(
            select(FluentBitAgentTopic)
            .where(
                FluentBitAgentTopic.id == topic_id,
                FluentBitAgentTopic.agent_id == agent.id,
            )
            .with_for_update()
        )
        if assignment is None:
            _fail("agent_topic_not_found", "Agent topic assignment not found", 404)
        assignment.state = "removal_pending"
        agent.state = "provisioning"
        operation = KafkaProvisioningOperation(
            agent_id=agent.id,
            kind="remove_topic",
            state="pending",
            request_key=request_key,
            payload={"topic_id": str(assignment.id), "request": request},
        )
        self.session.add(operation)
        try:
            self.session.commit()
            return operation
        except IntegrityError:
            return self._resolve_key_race(request_key, "remove_topic", agent_id, request)[1]
