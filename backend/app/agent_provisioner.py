"""Isolated polling worker for durable Kafka agent provisioning operations."""

from __future__ import annotations

import logging
import time
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

from sqlalchemy import select, text

from .agent_credentials import encrypt_pending, new_password, principal
from .agent_kafka_admin import AgentKafkaAdminProfile, ConfluentAgentKafkaAdmin
from .config import settings
from .db import Database
from .kafka import ConfluentKafkaMetadataClient, KafkaConnectionConfig
from .kafka_security import KafkaSecurityProfile
from .models import (
    FluentBitAgent,
    FluentBitAgentTopic,
    KafkaConnection,
    KafkaProvisioningOperation,
    Source,
)

LOG = logging.getLogger("app.agent_provisioner")
MAX_ATTEMPTS = 5


class TopicCreationUnconfirmed(Exception):
    kind = "topic_creation_unconfirmed"


class AgentProvisioner:
    def __init__(
        self,
        database: Database,
        admin: ConfluentAgentKafkaAdmin,
        metadata: ConfluentKafkaMetadataClient,
        timeout: int = 10,
        credentials_key_file=None,
    ):
        self.database, self.admin, self.metadata, self.timeout = database, admin, metadata, timeout
        self.credentials_key_file = credentials_key_file or settings.agent_credentials_key_file
        self._claimed: dict[UUID, UUID] = {}

    def _assert_owner(self, session, operation):
        # Read the committed row without flushing the local ORM snapshot. The
        # row lock fences a previous worker after its advisory connection dies.
        current = session.execute(
            select(KafkaProvisioningOperation.owner_token, KafkaProvisioningOperation.state)
            .where(KafkaProvisioningOperation.id == operation.id)
            .with_for_update()
            .execution_options(autoflush=False)
        ).one_or_none()
        if (
            current is None
            or current.owner_token != self._claimed.get(operation.id)
            or current.state != "running"
        ):
            session.rollback()
            raise RuntimeError("stale provisioning owner")

    def _commit_progress(self, session, operation):
        self._assert_owner(session, operation)
        session.commit()

    def claim(self):
        with self.database.session_factory() as session:
            cutoff = datetime.now(UTC) - timedelta(minutes=2)
            query = (
                select(KafkaProvisioningOperation)
                .where(
                    KafkaProvisioningOperation.state.in_(["pending", "retry", "running"]),
                    (KafkaProvisioningOperation.attempts < MAX_ATTEMPTS)
                    | (KafkaProvisioningOperation.state == "running"),
                )
                .where(
                    (KafkaProvisioningOperation.state == "pending")
                    | (
                        (KafkaProvisioningOperation.state == "retry")
                        & (
                            KafkaProvisioningOperation.updated_at
                            < datetime.now(UTC) - timedelta(seconds=30)
                        )
                    )
                    | (
                        (KafkaProvisioningOperation.state == "running")
                        & (KafkaProvisioningOperation.updated_at < cutoff)
                    )
                )
                .order_by(KafkaProvisioningOperation.created_at)
                .with_for_update(skip_locked=True)
                .limit(20)
            )
            operation = None
            for candidate in session.scalars(query):
                if candidate.state == "running" and not session.scalar(
                    select(text("pg_try_advisory_xact_lock(hashtextextended(:key, 0))")).params(
                        key=str(candidate.id)
                    )
                ):
                    continue
                operation = candidate
                break
            if operation is None:
                session.commit()
                return None
            operation.state = "running"
            if operation.attempts < MAX_ATTEMPTS:
                operation.attempts += 1
            operation.owner_token = uuid4()
            operation.updated_at = datetime.now(UTC)
            session.commit()
            self._claimed[operation.id] = operation.owner_token
            return operation.id

    def execute(self, operation_id: UUID):
        owner_token = self._claimed.get(operation_id)
        if owner_token is None:
            return
        # This session-level lock survives the worker's progress commits. A stale
        # claim may only be reassigned after the owning connection has closed.
        lock_connection = self.database.engine.connect()
        locked = False
        try:
            locked = lock_connection.scalar(
                text("SELECT pg_try_advisory_lock(hashtextextended(:key, 0))"),
                {"key": str(operation_id)},
            )
            if not locked:
                return
            with self.database.session_factory() as session:
                operation = session.scalar(
                    select(KafkaProvisioningOperation)
                    .where(KafkaProvisioningOperation.id == operation_id)
                    .with_for_update()
                )
                if (
                    operation is None
                    or operation.state != "running"
                    or operation.owner_token != owner_token
                ):
                    return
                agent = session.scalar(
                    select(FluentBitAgent)
                    .where(FluentBitAgent.id == operation.agent_id)
                    .with_for_update()
                )
                if agent is None:
                    raise ValueError("agent record unavailable")
                if operation.kind == "register":
                    self._register(session, agent, operation)
                elif operation.kind == "add_topic":
                    self._add_topic(session, agent, operation)
                elif operation.kind == "remove_topic":
                    self._remove_topic(session, agent, operation)
                elif operation.kind == "revoke":
                    self._revoke(session, agent, operation)
                elif operation.kind == "rotate":
                    self._rotate(session, agent, operation)
                else:
                    raise ValueError("unsupported operation")
                unresolved = session.scalar(
                    select(FluentBitAgentTopic.id)
                    .where(
                        FluentBitAgentTopic.agent_id == agent.id,
                        FluentBitAgentTopic.state == "revocation_pending",
                    )
                    .limit(1)
                )
                if unresolved is not None:
                    agent.state = "error"
                    agent.safe_error = (
                        "Отзыв ACL после смены поколения топика ожидает подтверждения."
                    )
                else:
                    agent.safe_error = None
                self._assert_owner(session, operation)
                operation.state = "succeeded"
                operation.owner_token = None
                operation.safe_error = None
                session.commit()
        except Exception as error:  # noqa: BLE001 - worker must persist every operation failure and stay alive
            # Store only a stable category. Kafka exception text and credentials never enter logs/DB.
            category = getattr(error, "kind", None) or "provisioning_failed"
            with self.database.session_factory() as session:
                operation = session.scalar(
                    select(KafkaProvisioningOperation)
                    .where(KafkaProvisioningOperation.id == operation_id)
                    .with_for_update()
                )
                if operation is None or operation.owner_token != owner_token:
                    return
                if category == TopicCreationUnconfirmed.kind:
                    try:
                        self._cancel_uncertain_creation(session, operation)
                    except Exception:  # noqa: BLE001 - keep every reservation until ACL cleanup is confirmed
                        category = "topic_creation_cleanup_pending"
                    else:
                        operation.state = "failed"
                        operation.owner_token = None
                        operation.safe_error = TopicCreationUnconfirmed.kind
                        agent = session.get(FluentBitAgent, operation.agent_id)
                        agent.state = "error" if operation.kind == "register" else "ready"
                        agent.safe_error = (
                            "Создание топика не подтверждено. Доступ отозван; "
                            "проверьте topic ID и зарегистрируйте топик заново как существующий."
                        )
                        session.commit()
                        return
                operation.safe_error = category
                operation.state = "failed" if operation.attempts >= MAX_ATTEMPTS else "retry"
                operation.owner_token = None
                operation.updated_at = datetime.now(UTC)
                agent = session.get(FluentBitAgent, operation.agent_id)
                if agent:
                    agent.state = "error" if operation.kind != "revoke" else "revocation_pending"
                    agent.safe_error = (
                        "\u041f\u0440\u043e\u0432\u0435\u0440\u0438\u0442\u044c \u0441\u043e\u0441\u0442\u043e\u044f\u043d\u0438\u0435 Kafka \u0438 \u043f\u043e\u0432\u0442\u043e\u0440\u0438\u0442\u044c \u043e\u043f\u0435\u0440\u0430\u0446\u0438\u044e"
                        if operation.state == "retry"
                        else "\u041e\u043f\u0435\u0440\u0430\u0446\u0438\u044f \u043d\u0435 \u0437\u0430\u0432\u0435\u0440\u0448\u0435\u043d\u0430; \u0442\u0440\u0435\u0431\u0443\u0435\u0442\u0441\u044f \u0441\u0432\u0435\u0440\u043a\u0430 Kafka"
                    )
                for assignment in session.scalars(
                    select(FluentBitAgentTopic).where(
                        FluentBitAgentTopic.agent_id == operation.agent_id,
                        FluentBitAgentTopic.state == "pending",
                    )
                ):
                    assignment.state = "error"
                session.commit()
            LOG.warning(
                "agent_provisioning_operation_failed",
                extra={
                    "operation_id": str(operation_id),
                    "category": category,
                },
            )
        finally:
            if locked:
                lock_connection.execute(
                    text("SELECT pg_advisory_unlock(hashtextextended(:key, 0))"),
                    {"key": str(operation_id)},
                )
            lock_connection.close()
            self._claimed.pop(operation_id, None)

    def reconcile_ready_assignments(self):
        with self.database.session_factory() as session:
            entries = list(
                session.execute(
                    select(FluentBitAgentTopic, FluentBitAgent)
                    .join(FluentBitAgent, FluentBitAgent.id == FluentBitAgentTopic.agent_id)
                    .where(FluentBitAgentTopic.state.in_(["ready", "revocation_pending"]))
                )
            )
        for assignment, agent in entries:
            try:
                config, cluster_id = self._connection_config_for_assignment(assignment)
                if assignment.state == "ready":
                    snapshot = self.metadata.metadata_for_topic(
                        config, assignment.topic_name, self.timeout
                    )
                    topic = next(iter(snapshot.topics), None)
                    if (
                        cluster_id == assignment.cluster_identity
                        and snapshot.cluster_identity == assignment.cluster_identity
                        and topic is not None
                        and topic.identity == assignment.topic_identity
                    ):
                        continue
                    with self.database.session_factory() as session:
                        current_agent = session.scalar(
                            select(FluentBitAgent)
                            .where(FluentBitAgent.id == agent.id)
                            .with_for_update()
                        )
                        current = session.scalar(
                            select(FluentBitAgentTopic)
                            .where(FluentBitAgentTopic.id == assignment.id)
                            .with_for_update()
                        )
                        if current is None or current.state != "ready" or current_agent is None:
                            continue
                        current.state = "revocation_pending"
                        current_agent.state = "error"
                        current_agent.safe_error = (
                            "Поколение Kafka topic изменилось; отзыв ACL ожидает подтверждения."
                        )
                        session.commit()
            except Exception:  # noqa: BLE001 - reconciliation continues independently per topic
                LOG.warning(
                    "agent_topic_reconciliation_metadata_failed", extra={"agent_id": str(agent.id)}
                )
                continue
            revoke_confirmed = False
            try:
                with self.database.session_factory() as session:
                    revisions = self._known_revisions(session, agent)
                for revision in revisions:
                    self.admin.remove_write(
                        config,
                        assignment.topic_name,
                        principal(str(agent.id), revision),
                        self.timeout,
                    )
                revoke_confirmed = True
            except Exception:  # noqa: BLE001 - retain reservation and mark operator action required
                LOG.warning(
                    "agent_topic_reconciliation_acl_revoke_unconfirmed",
                    extra={"agent_id": str(agent.id)},
                )
            with self.database.session_factory() as session:
                current_assignment = session.scalar(
                    select(FluentBitAgentTopic)
                    .where(FluentBitAgentTopic.id == assignment.id)
                    .with_for_update()
                )
                if current_assignment is None or current_assignment.state != "revocation_pending":
                    session.rollback()
                    continue
                if revoke_confirmed:
                    current_assignment.state = "error"
                session.commit()

    def _connection_config_for_assignment(self, assignment: FluentBitAgentTopic):
        with self.database.session_factory() as session:
            return self._connection_config(session, assignment.connection_id)

    @staticmethod
    def _known_revisions(session, agent):
        revisions = [agent.access_revision]
        for previous in session.scalars(
            select(KafkaProvisioningOperation).where(
                KafkaProvisioningOperation.agent_id == agent.id,
                KafkaProvisioningOperation.kind == "rotate",
            )
        ):
            old_revision = int(previous.payload["old_revision"])
            if old_revision not in revisions:
                revisions.append(old_revision)
        return revisions

    def _connection_config(self, session, connection_id: UUID):
        connection = session.get(KafkaConnection, connection_id)
        if connection is None or not connection.cluster_identity:
            raise ValueError("Kafka connection unavailable")
        return KafkaConnectionConfig(
            tuple(connection.bootstrap_servers), connection.security_protocol
        ), connection.cluster_identity

    def _topic_for_operation(
        self, session, operation, item, config, cluster_id, assignment, source
    ):
        name = item["topic_name"]
        snapshot = self.metadata.metadata_for_topic(config, name, self.timeout)
        current = next(iter(snapshot.topics), None)
        if snapshot.cluster_identity not in (None, cluster_id):
            raise ValueError("Kafka cluster identity changed")
        if item["action"] == "create" and assignment.topic_identity is None:
            started = operation.payload.get("creation_started", [])
            if current is not None:
                raise TopicCreationUnconfirmed()
            if name not in started:
                operation.payload = {**operation.payload, "creation_started": [*started, name]}
                self._commit_progress(session, operation)
            self.admin.create_topic(config, name, str(operation.id), self.timeout)
            snapshot = self.metadata.metadata_for_topic(config, name, self.timeout)
            current = next(iter(snapshot.topics), None)
        if current is None or not current.identity or snapshot.cluster_identity != cluster_id:
            raise ValueError("Kafka topic identity unavailable")
        expected = assignment.topic_identity or item.get("topic_identity")
        if expected is not None and current.identity != expected:
            raise ValueError("Kafka topic generation changed")
        if assignment.topic_identity is None:
            assignment.topic_identity = current.identity
            source.kafka_topic_identity = current.identity
            self._commit_progress(session, operation)
        return current

    def _cancel_uncertain_creation(self, session, operation):
        agent = session.get(FluentBitAgent, operation.agent_id)
        assignments = list(
            session.scalars(
                select(FluentBitAgentTopic).where(
                    FluentBitAgentTopic.agent_id == operation.agent_id
                )
            )
        )
        if operation.kind == "register":
            connection_id = UUID(operation.payload["topics"][0]["connection_id"])
            config, _ = self._connection_config(session, connection_id)
            self.admin.remove_principal(
                config, principal(str(agent.id), agent.access_revision), self.timeout
            )
            for assignment in assignments:
                session.delete(assignment)
            agent.pending_credentials_ciphertext = None
        else:
            item = operation.payload["topics"][0]
            config, _ = self._connection_config(session, UUID(item["connection_id"]))
            self.admin.remove_write(
                config,
                item["topic_name"],
                principal(str(agent.id), agent.access_revision),
                self.timeout,
            )
            for assignment in assignments:
                if assignment.topic_name == item["topic_name"]:
                    session.delete(assignment)

    def _register(self, session, agent: FluentBitAgent, operation: KafkaProvisioningOperation):
        config_and_cluster = [
            self._connection_config(session, UUID(item["connection_id"]))
            for item in operation.payload["topics"]
        ]
        configs = [item[0] for item in config_and_cluster]
        agent_principal = principal(str(agent.id), agent.access_revision)
        if agent.pending_credentials_ciphertext is None:
            password = new_password()
            agent.pending_credentials_ciphertext = encrypt_pending(
                self.credentials_key_file, agent_principal, password
            )
            self._commit_progress(session, operation)
        from .agent_credentials import decrypt_pending

        username, password = decrypt_pending(
            self.credentials_key_file, agent.pending_credentials_ciphertext
        )
        self.admin.set_credential(configs[0], username, password, self.timeout)
        for item, (config, cluster_id) in zip(operation.payload["topics"], config_and_cluster):
            if cluster_id != item["cluster_identity"]:
                raise ValueError("Kafka cluster identity changed")
            name = item["topic_name"]
            assignment = session.scalar(
                select(FluentBitAgentTopic)
                .where(
                    FluentBitAgentTopic.agent_id == agent.id,
                    FluentBitAgentTopic.connection_id == UUID(item["connection_id"]),
                    FluentBitAgentTopic.topic_name == name,
                )
                .with_for_update()
            )
            source = session.get(Source, UUID(item["source_id"]))
            if assignment is None or source is None:
                raise ValueError("Reserved source or topic assignment disappeared")
            current = self._topic_for_operation(
                session, operation, item, config, cluster_id, assignment, source
            )
            self.admin.grant_write(config, name, username, self.timeout)
            assignment.state = "ready"
            source.kafka_topic_identity = current.identity
            operation.updated_at = datetime.now(UTC)
            self._commit_progress(session, operation)
        agent.state = "ready"

    def _revoke(self, session, agent: FluentBitAgent, operation: KafkaProvisioningOperation):
        assignments = list(
            session.scalars(
                select(FluentBitAgentTopic).where(FluentBitAgentTopic.agent_id == agent.id)
            )
        )
        revisions = self._known_revisions(session, agent)
        config, cluster_id = self._connection_config(session, agent.connection_id)
        if cluster_id != agent.cluster_identity or any(
            assignment.cluster_identity != agent.cluster_identity for assignment in assignments
        ):
            raise ValueError("Kafka cluster identity changed")
        for revision in revisions:
            self.admin.remove_principal(config, principal(str(agent.id), revision), self.timeout)
        for assignment in assignments:
            session.delete(assignment)
        agent.pending_credentials_ciphertext = None
        agent.state = "revoked"
        agent.connection_id = None

    def _rotate(self, session, agent: FluentBitAgent, operation: KafkaProvisioningOperation):
        assignments = list(
            session.scalars(
                select(FluentBitAgentTopic).where(FluentBitAgentTopic.agent_id == agent.id)
            )
        )
        if not assignments:
            raise ValueError("agent has no assigned topics")
        configs = {}
        for assignment in assignments:
            config, cluster_id = self._connection_config(session, assignment.connection_id)
            if cluster_id != assignment.cluster_identity:
                raise ValueError("Kafka cluster identity changed")
            configs.setdefault(assignment.cluster_identity, config)
        old_username = principal(str(agent.id), int(operation.payload["old_revision"]))
        new_username = principal(str(agent.id), agent.access_revision)
        if agent.pending_credentials_ciphertext is None:
            agent.pending_credentials_ciphertext = encrypt_pending(
                self.credentials_key_file, new_username, new_password()
            )
            self._commit_progress(session, operation)
        from .agent_credentials import decrypt_pending

        new_username, new_password_value = decrypt_pending(
            self.credentials_key_file, agent.pending_credentials_ciphertext
        )
        # Revoke the previous identity before enabling the new one. A failed
        # rotation remains paused and can be retried with the same encrypted credential.
        for config in configs.values():
            self.admin.remove_principal(config, old_username, self.timeout)
        first_config = next(iter(configs.values()))
        self.admin.set_credential(first_config, new_username, new_password_value, self.timeout)
        for assignment in assignments:
            config, _ = self._connection_config(session, assignment.connection_id)
            snapshot = self.metadata.metadata_for_topic(config, assignment.topic_name, self.timeout)
            topic = next(iter(snapshot.topics), None)
            if (
                snapshot.cluster_identity != assignment.cluster_identity
                or topic is None
                or topic.identity != assignment.topic_identity
            ):
                raise ValueError("Kafka topic generation changed")
            self.admin.grant_write(config, assignment.topic_name, new_username, self.timeout)
            operation.updated_at = datetime.now(UTC)
            self._commit_progress(session, operation)
        agent.state = "ready"

    def _add_topic(self, session, agent: FluentBitAgent, operation: KafkaProvisioningOperation):
        item = operation.payload["topics"][0]
        config, cluster_id = self._connection_config(session, UUID(item["connection_id"]))
        if cluster_id != item["cluster_identity"]:
            raise ValueError("Kafka cluster identity changed")
        assignment = session.scalar(
            select(FluentBitAgentTopic)
            .where(
                FluentBitAgentTopic.agent_id == agent.id,
                FluentBitAgentTopic.connection_id == UUID(item["connection_id"]),
                FluentBitAgentTopic.topic_name == item["topic_name"],
            )
            .with_for_update()
        )
        source = session.get(Source, UUID(item["source_id"]))
        if assignment is None or source is None:
            raise ValueError("Reserved source or topic assignment changed")
        topic = self._topic_for_operation(
            session, operation, item, config, cluster_id, assignment, source
        )
        username = principal(str(agent.id), agent.access_revision)
        self.admin.grant_write(config, item["topic_name"], username, self.timeout)
        assignment.state = "ready"
        source.kafka_topic_identity = topic.identity
        agent.state = "ready"

    def _remove_topic(self, session, agent: FluentBitAgent, operation: KafkaProvisioningOperation):
        assignment = session.scalar(
            select(FluentBitAgentTopic)
            .where(
                FluentBitAgentTopic.id == UUID(operation.payload["topic_id"]),
                FluentBitAgentTopic.agent_id == agent.id,
            )
            .with_for_update()
        )
        if assignment is None:
            agent.state = "ready"
            return
        config, cluster_id = self._connection_config(session, assignment.connection_id)
        if cluster_id != assignment.cluster_identity:
            raise ValueError("Kafka cluster identity changed")
        username = principal(str(agent.id), agent.access_revision)
        self.admin.remove_write(config, assignment.topic_name, username, self.timeout)
        session.delete(assignment)
        agent.state = "ready"


def main():
    if settings.process_role != "provisioner":
        raise RuntimeError("agent provisioner must use PROCESS_ROLE=provisioner")
    if settings.kafka_admin_password_file is None or settings.agent_credentials_key_file is None:
        raise RuntimeError("agent provisioning secrets are not configured")
    profile = AgentKafkaAdminProfile(
        settings.kafka_security_protocol or "",
        settings.kafka_admin_username or "",
        settings.kafka_admin_password_file,
        settings.kafka_ca_file,
    )
    runtime_profile = KafkaSecurityProfile(
        settings.kafka_security_protocol or "",
        settings.kafka_runtime_username or "",
        settings.kafka_runtime_password_file,
        settings.kafka_ca_file,
        tuple(settings.kafka_dev_allowed_bootstrap_servers),
    )
    worker = AgentProvisioner(
        Database(settings.database_url),
        ConfluentAgentKafkaAdmin(profile),
        ConfluentKafkaMetadataClient(runtime_profile),
        settings.kafka_metadata_timeout_seconds,
        settings.agent_credentials_key_file,
    )
    next_reconcile = 0.0
    while True:
        if time.monotonic() >= next_reconcile:
            worker.reconcile_ready_assignments()
            next_reconcile = time.monotonic() + 30.0
        operation_id = worker.claim()
        if operation_id is None:
            time.sleep(1)
            continue
        worker.execute(operation_id)


if __name__ == "__main__":
    main()
