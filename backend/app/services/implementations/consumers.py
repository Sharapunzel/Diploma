from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, replace
from threading import Event, RLock, Thread
from time import monotonic
from uuid import UUID

from sqlalchemy.orm import Session, sessionmaker

from ...kafka import (
    KafkaAssignment,
    KafkaConnectionConfig,
    KafkaConsumer,
    KafkaConsumerError,
    KafkaConsumerFactory,
    KafkaMessage,
    KafkaMetadataClient,
    KafkaMetadataError,
    KafkaPartition,
    KafkaRevocation,
)
from ...repositories.protocols.consumers import ConsumerSourceConfig
from ...repositories.protocols.processing import KafkaCoordinates
from ...repositories.sqlalchemy import SqlAlchemyUnitOfWork
from ...repositories.sqlalchemy.consumers import SqlAlchemyConsumerStateRepository
from ...repositories.sqlalchemy.processing import SqlAlchemyProcessingRepository
from ..protocols.processing import DurableProcessingService
from .processing import (
    DurableProcessingServiceImpl,
    ProcessingConfigurationError,
    ProcessingStorageError,
    ProcessingSystemError,
)

LOGGER = logging.getLogger("app.kafka_consumers")


@dataclass(frozen=True)
class ConsumerWorkerState:
    status: str
    reason: str | None = None


class _SourceWorker:
    def __init__(
        self,
        config: ConsumerSourceConfig,
        session_factory: sessionmaker[Session],
        engine,
        metadata_client: KafkaMetadataClient,
        factory: KafkaConsumerFactory,
        metadata_timeout: float,
        poll_timeout: float,
        retry_delay: float,
    ) -> None:
        self.config = config
        self.session_factory = session_factory
        self.engine = engine
        self.metadata_client = metadata_client
        self.factory = factory
        self.metadata_timeout = metadata_timeout
        self.poll_timeout = poll_timeout
        self.retry_delay = retry_delay
        self.stop_requested = Event()
        self.lifecycle_stop_requested = Event()
        self.thread = Thread(
            target=self._run,
            name=f"kafka-source-{config.source_id}",
            daemon=False,
        )
        self._state = ConsumerWorkerState("starting")
        self._state_lock = RLock()
        self.consumer: KafkaConsumer | None = None
        self.blocked: dict[int, tuple[KafkaMessage, str, float]] = {}
        self.buffered: dict[int, dict[int, KafkaMessage]] = defaultdict(dict)
        self.expected_offsets: dict[int, int] = {}

    @property
    def state(self) -> ConsumerWorkerState:
        with self._state_lock:
            return self._state

    def _set_state(self, status: str, reason: str | None = None) -> None:
        with self._state_lock:
            self._state = ConsumerWorkerState(status, reason)
        LOGGER.info(
            "kafka_consumer_state",
            extra={
                "source_id": str(self.config.source_id),
                "connection_id": str(self.config.connection_id),
                "topic": self.config.topic_name,
                "status": status,
                "reason": reason,
            },
        )

    def start(self) -> None:
        self.thread.start()

    def stop(self, *, lifecycle: bool = False) -> None:
        if lifecycle:
            self.lifecycle_stop_requested.set()
        self.stop_requested.set()

    def join(self, timeout: float | None = None) -> bool:
        self.thread.join(timeout)
        return not self.thread.is_alive()

    @property
    def is_alive(self) -> bool:
        return self.thread.is_alive()

    def _repository(self, callback):
        with self.session_factory() as session:
            repository = SqlAlchemyConsumerStateRepository(session)
            result = callback(repository, session)
            return result

    def _topic_is_safe(self) -> bool:
        try:
            metadata = self.metadata_client.metadata(
                KafkaConnectionConfig(
                    self.config.bootstrap_servers, self.config.security_protocol
                ),
                self.metadata_timeout,
            )
        except KafkaMetadataError:
            self._set_state("retrying", "kafka_metadata_unavailable")
            return False
        topic = next(
            (item for item in metadata.topics if item.name == self.config.topic_name),
            None,
        )
        if topic is None:
            self._set_state("stopped", "topic_not_found")
            return False
        identity = topic.identity
        if not isinstance(identity, str) or not identity.strip():
            history = self._repository(
                lambda repository, _: repository.has_durable_history(
                    self.config.connection_id, self.config.topic_name
                )
            )
            self._set_state(
                "stopped",
                "topic_identity_unavailable_after_history"
                if history
                else "topic_identity_unavailable",
            )
            return False
        stored = self.config.kafka_topic_identity
        identities = self._repository(
            lambda repository, _: repository.durable_topic_identities(
                self.config.connection_id, self.config.topic_name
            )
        )
        if identities and (stored is None or identities != {identity} or stored != identity):
            self._set_state("stopped", "topic_identity_history_mismatch")
            return False
        if stored == identity:
            return True

        def store_identity(repository, session) -> bool:
            changed = repository.set_topic_identity_if_current(
                self.config.source_id, stored, identity
            )
            if changed:
                session.commit()
            else:
                session.rollback()
            return changed

        if not self._repository(store_identity):
            self._set_state("stopped", "source_configuration_changed")
            return False
        self.config = replace(self.config, kafka_topic_identity=identity)
        return True

    def _assign(self, event: KafkaAssignment) -> bool:
        if event.topic != self.config.topic_name or not self._topic_is_safe():
            if self.state.status == "stopped":
                self.stop_requested.set()
            return False
        assert self.consumer is not None
        requested = tuple(
            KafkaPartition(self.config.topic_name, partition, 0)
            for partition in event.partitions
        )
        try:
            committed = self.consumer.committed(requested, self.metadata_timeout)
            positions: list[KafkaPartition] = []
            for partition, position in zip(requested, committed, strict=True):
                low, high = self.consumer.watermarks(
                    partition.topic, partition.partition, self.metadata_timeout
                )
                if position is None:
                    history = self._repository(
                        lambda repository, _, partition_number=partition.partition: repository.has_durable_history(
                            self.config.connection_id,
                            self.config.topic_name,
                            partition_number,
                        )
                    )
                    if history:
                        self._set_state("stopped", "position_unknown_after_history")
                        self.stop_requested.set()
                        return False
                    position = low
                if position < low or position > high:
                    self._set_state("stopped", "position_outside_available_range")
                    self.stop_requested.set()
                    return False
                positions.append(
                    KafkaPartition(partition.topic, partition.partition, position)
                )
            self.consumer.assign(tuple(positions))
            self.expected_offsets = {
                item.partition: item.offset for item in positions
            }
            self._set_state("waiting")
            return True
        except KafkaConsumerError:
            self._set_state("retrying", "kafka_position_unavailable")
            return False

    def _block(self, message: KafkaMessage, action: str) -> None:
        assert self.consumer is not None
        partition = KafkaPartition(message.topic, message.partition, message.offset)
        self.blocked[message.partition] = (
            message,
            action,
            monotonic() + self.retry_delay,
        )
        try:
            self.consumer.pause((partition,))
        except KafkaConsumerError:
            self._set_state("retrying", "kafka_pause_unavailable")

    def _retry_blocked(self) -> None:
        if self.consumer is None:
            return
        for partition, (message, action, due) in tuple(self.blocked.items()):
            if due > monotonic():
                continue
            marker = KafkaPartition(message.topic, partition, message.offset)
            try:
                if action == "commit":
                    self.consumer.commit(
                        (KafkaPartition(message.topic, partition, message.offset + 1),),
                        self.metadata_timeout,
                    )
                    self.expected_offsets[partition] = message.offset + 1
                else:
                    self.consumer.seek(marker)
                self.consumer.resume((marker,))
                del self.blocked[partition]
                self._set_state("waiting")
            except KafkaConsumerError:
                self.blocked[partition] = (
                    message,
                    action,
                    monotonic() + self.retry_delay,
                )
                self._set_state("retrying", "kafka_commit_unavailable")

    def _handle_message(self, message: KafkaMessage) -> None:
        expected = self.expected_offsets.get(message.partition)
        if expected is None or message.offset > expected or message.partition in self.blocked:
            self.buffered[message.partition][message.offset] = message
            return
        if message.offset < expected:
            return
        self._set_state("processing")
        self._process(message)

    def _drain_buffered(self) -> None:
        for partition, messages in tuple(self.buffered.items()):
            if partition in self.blocked:
                continue
            expected = self.expected_offsets.get(partition)
            message = messages.pop(expected, None) if expected is not None else None
            if message is not None:
                self._handle_message(message)
            if not messages:
                del self.buffered[partition]

    def _process(self, message: KafkaMessage) -> None:
        assert self.consumer is not None
        try:
            with self.session_factory() as session:
                service: DurableProcessingService = DurableProcessingServiceImpl(
                    SqlAlchemyProcessingRepository(session),
                    SqlAlchemyUnitOfWork(session),
                    self.engine,
                )
                result = service.process(
                    self.config.source_id,
                    coordinates=KafkaCoordinates(
                        connection_id=self.config.connection_id,
                        topic=message.topic,
                        partition=message.partition,
                        offset=message.offset,
                        kafka_topic_identity=self.config.kafka_topic_identity,
                    ),
                    payload=message.value,
                    received_at=message.received_at,
                )
            if result.outcome not in {
                "stored_complete",
                "stored_partial",
                "stored_failed",
                "already_processed",
            }:
                raise ProcessingSystemError("unexpected_processing_outcome")
        except ProcessingConfigurationError:
            self._set_state("stopped", "processing_configuration_invalid")
            self.stop_requested.set()
            return
        except (ProcessingStorageError, ProcessingSystemError):
            self._set_state("retrying", "processing_not_durable")
            self._block(message, "process")
            return
        try:
            self.consumer.commit(
                (KafkaPartition(message.topic, message.partition, message.offset + 1),),
                self.metadata_timeout,
            )
            self.expected_offsets[message.partition] = message.offset + 1
            self._set_state("waiting")
        except KafkaConsumerError:
            self._set_state("retrying", "kafka_commit_unavailable")
            self._block(message, "commit")

    def _run(self) -> None:
        try:
            while not self.stop_requested.is_set():
                if not self._topic_is_safe():
                    if self.state.status == "stopped":
                        return
                    self.stop_requested.wait(self.retry_delay)
                    continue
                try:
                    self.consumer = self.factory.create(
                        KafkaConnectionConfig(
                            self.config.bootstrap_servers,
                            self.config.security_protocol,
                        ),
                        f"diploma-source-{self.config.source_id}",
                    )
                    self.consumer.subscribe(self.config.topic_name)
                    break
                except KafkaConsumerError:
                    self._set_state("retrying", "kafka_consumer_unavailable")
                    self.stop_requested.wait(self.retry_delay)
            pending_assignment: KafkaAssignment | None = None
            while not self.stop_requested.is_set() and self.consumer is not None:
                self._retry_blocked()
                self._drain_buffered()
                if pending_assignment is not None:
                    if self._assign(pending_assignment):
                        pending_assignment = None
                    elif not self.stop_requested.is_set():
                        self.stop_requested.wait(self.retry_delay)
                    continue
                try:
                    event = self.consumer.poll(self.poll_timeout)
                except KafkaConsumerError:
                    self._set_state("retrying", "kafka_poll_unavailable")
                    self.stop_requested.wait(self.retry_delay)
                    continue
                if event is None:
                    continue
                if isinstance(event, KafkaAssignment):
                    pending_assignment = event
                elif isinstance(event, KafkaRevocation):
                    self.blocked.clear()
                    self.buffered.clear()
                    self.expected_offsets.clear()
                    try:
                        self.consumer.unassign()
                    except KafkaConsumerError:
                        self._set_state("retrying", "kafka_revoke_unavailable")
                    self._set_state("waiting")
                elif isinstance(event, KafkaMessage) and event.topic == self.config.topic_name:
                    self._handle_message(event)
        except Exception:
            LOGGER.exception(
                "kafka_consumer_unexpected_failure",
                extra={"source_id": str(self.config.source_id)},
            )
            self._set_state("stopped", "consumer_runtime_failure")
        finally:
            if self.consumer is not None:
                try:
                    self.consumer.close()
                except Exception:  # noqa: BLE001 -- best-effort shutdown only.
                    LOGGER.warning("kafka_consumer_close_failed", extra={"source_id": str(self.config.source_id)})
            if self.state.status != "stopped":
                self._set_state("stopped", "stopped_by_lifecycle")


class KafkaSourceSupervisor:
    def __init__(
        self,
        session_factory: sessionmaker[Session],
        engine,
        metadata_client: KafkaMetadataClient,
        factory: KafkaConsumerFactory,
        metadata_timeout: float,
        poll_timeout: float,
        retry_delay: float,
        sync_interval: float,
    ) -> None:
        self.session_factory = session_factory
        self.engine = engine
        self.metadata_client = metadata_client
        self.factory = factory
        self.metadata_timeout = metadata_timeout
        self.poll_timeout = poll_timeout
        self.retry_delay = retry_delay
        self.sync_interval = sync_interval
        self._stop = Event()
        self._wake = Event()
        self._thread: Thread | None = None
        self._lock = RLock()
        self._workers: dict[UUID, _SourceWorker] = {}

    def start(self) -> None:
        with self._lock:
            if self._thread is not None:
                return
            self._thread = Thread(
                target=self._run, name="kafka-source-supervisor", daemon=False
            )
            self._thread.start()

    def wake(self) -> None:
        self._wake.set()

    def stop_source(self, source_id: UUID) -> None:
        with self._lock:
            worker = self._workers.get(source_id)
        if worker is not None:
            worker.stop(lifecycle=True)
        self.wake()

    def stop(self) -> bool:
        self._stop.set()
        self.wake()
        with self._lock:
            workers = tuple(self._workers.values())
            thread = self._thread
        for worker in workers:
            worker.stop()
        deadline = monotonic() + self.metadata_timeout + self.poll_timeout + self.retry_delay + 1
        stopped = True
        for worker in workers:
            stopped = worker.join(max(0, deadline - monotonic())) and stopped
        if thread is not None:
            thread.join(max(0, deadline - monotonic()))
        stopped = stopped and all(not worker.is_alive for worker in workers)
        stopped = stopped and (thread is None or not thread.is_alive())
        if not stopped:
            LOGGER.critical("kafka_supervisor_shutdown_refused")
        return stopped

    def _enabled(self) -> list[ConsumerSourceConfig]:
        with self.session_factory() as session:
            return SqlAlchemyConsumerStateRepository(session).list_enabled_sources()

    def _start_worker(self, config: ConsumerSourceConfig) -> None:
        worker = _SourceWorker(
            config,
            self.session_factory,
            self.engine,
            self.metadata_client,
            self.factory,
            self.metadata_timeout,
            self.poll_timeout,
            self.retry_delay,
        )
        self._workers[config.source_id] = worker
        worker.start()

    def _sync(self) -> None:
        desired = {item.source_id: item for item in self._enabled()}
        with self._lock:
            if self._stop.is_set():
                return
            for source_id, worker in tuple(self._workers.items()):
                config = desired.get(source_id)
                if worker.stop_requested.is_set() and not worker.is_alive:
                    if (
                        worker.lifecycle_stop_requested.is_set()
                        or config is None
                        or config != worker.config
                    ):
                        del self._workers[source_id]
                        continue
                    # Keep terminal failures visible and avoid rapid restart loops.
                    continue
                if config is None or config != worker.config:
                    worker.stop()
                    if worker.join(self.poll_timeout + self.retry_delay + 1):
                        del self._workers[source_id]
                    else:
                        LOGGER.error(
                            "kafka_worker_stop_incomplete",
                            extra={"source_id": str(source_id)},
                        )
            for source_id, config in desired.items():
                if source_id not in self._workers:
                    self._start_worker(config)

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self._sync()
            except Exception:
                LOGGER.exception("kafka_supervisor_sync_failed")
            self._wake.wait(self.sync_interval)
            self._wake.clear()
