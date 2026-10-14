from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime
from threading import RLock
from typing import Protocol

from .kafka_security import KafkaSecurityConfigurationError, KafkaSecurityProfile


@dataclass(frozen=True)
class KafkaConnectionConfig:
    bootstrap_servers: tuple[str, ...]
    security_protocol: str


def client_security_options(
    config: KafkaConnectionConfig, profile: KafkaSecurityProfile | None
) -> dict[str, str]:
    # The persisted protocol remains legacy metadata only. It can never provide
    # runtime credentials or select a transport.
    if profile is None:
        raise KafkaSecurityConfigurationError(
            "Kafka security profile is required for broker access"
        )
    return profile.client_options(config.bootstrap_servers)


@dataclass(frozen=True)
class KafkaTopic:
    name: str
    partition_count: int
    identity: str | None = None


@dataclass(frozen=True)
class KafkaMetadata:
    broker_count: int
    topics: tuple[KafkaTopic, ...]
    latency_ms: float
    cluster_identity: str | None = None


class KafkaMetadataError(Exception):
    def __init__(self, kind: str) -> None:
        super().__init__(kind)
        self.kind = kind


def kafka_failure_kind(error) -> str:
    """Return a safe operator category without exposing librdkafka text."""
    from confluent_kafka import KafkaError

    kafka_error = error.args[0] if isinstance(error, Exception) and error.args else error
    code = getattr(kafka_error, "code", lambda: None)()
    if code == KafkaError._SSL:
        return "tls"
    if code == KafkaError._AUTHENTICATION:
        return "authentication"
    if code in (KafkaError.TOPIC_AUTHORIZATION_FAILED,
                KafkaError.GROUP_AUTHORIZATION_FAILED,
                KafkaError.CLUSTER_AUTHORIZATION_FAILED):
        return "authorization"
    if code == KafkaError._TIMED_OUT:
        return "timeout"
    return "unavailable"


class KafkaFailureObserver:
    """Keep one pending callback cause for the next operation on this client."""

    def __init__(self) -> None:
        self._observed: deque[str] = deque(maxlen=1)
        self._lock = RLock()

    def __call__(self, error) -> None:
        kind = kafka_failure_kind(error)
        with self._lock:
            pending = self._observed[0] if self._observed else None
            if kind in {"tls", "authentication", "authorization"} or pending is None:
                self._observed.append(kind)

    def clear(self) -> None:
        with self._lock:
            self._observed.clear()

    def take(self) -> str | None:
        with self._lock:
            return self._observed.popleft() if self._observed else None

    def classify(self, error) -> str:
        kind = kafka_failure_kind(error)
        if kind in {"tls", "authentication", "authorization"}:
            self.clear()
            return kind
        observed = self.take()
        return observed if observed in {"tls", "authentication", "authorization"} else kind


def diagnose_security_failure(config: KafkaConnectionConfig,
                              profile: KafkaSecurityProfile | None) -> str:
    """Inspect librdkafka's poll errors when metadata reports only transport failure."""
    if profile is None:
        return "unavailable"
    from time import monotonic

    from confluent_kafka import Consumer, KafkaException

    probes: list[tuple[Consumer, KafkaFailureObserver]] = []
    options = client_security_options(config, profile)
    for family in ("v4", "v6"):
        observer = KafkaFailureObserver()
        try:
            consumer = Consumer(
                {
                    "bootstrap.servers": ",".join(config.bootstrap_servers),
                    **options,
                    "broker.address.family": family,
                    "group.id": "diploma-source-security-diagnostic",
                    "enable.auto.commit": False,
                    "allow.auto.create.topics": False,
                    "error_cb": observer,
                }
            )
            consumer.subscribe(["__diploma_security_probe_nonexistent__"])
            probes.append((consumer, observer))
        except KafkaException:
            continue
    if not probes:
        return "unavailable"
    try:
        deadline = monotonic() + 2
        while monotonic() < deadline:
            for consumer, observer in probes:
                message = consumer.poll(min(0.1, max(0, deadline - monotonic())))
                if message is not None and message.error():
                    observed = observer.classify(message.error())
                    if observed in {"tls", "authentication", "authorization"}:
                        return observed
                observed = observer.classify("unavailable")
                if observed in {"tls", "authentication", "authorization"}:
                    return observed
    finally:
        for consumer, _ in probes:
            consumer.close()
    return "unavailable"


class KafkaMetadataClient(Protocol):
    def metadata(self, config: KafkaConnectionConfig, timeout: float) -> KafkaMetadata: ...
    def metadata_for_topic(self, config: KafkaConnectionConfig, topic_name: str,
                           timeout: float) -> KafkaMetadata: ...


class ConfluentKafkaMetadataClient:
    def __init__(self, security_profile: KafkaSecurityProfile | None = None) -> None:
        self._lock = RLock()
        self._security_profile = security_profile
        self._cluster_ids: dict[KafkaConnectionConfig, str | None] = {}

    def _client(self, config: KafkaConnectionConfig, observer: KafkaFailureObserver):
        from confluent_kafka.admin import AdminClient

        return AdminClient(
            {
                "bootstrap.servers": ",".join(config.bootstrap_servers),
                **client_security_options(config, self._security_profile),
                "allow.auto.create.topics": False,
                "error_cb": observer,
            }
        )

    def metadata(self, config: KafkaConnectionConfig, timeout: float) -> KafkaMetadata:
        from time import perf_counter

        from confluent_kafka import KafkaError, KafkaException
        from confluent_kafka.admin import _TopicCollection

        observer = KafkaFailureObserver()
        started = perf_counter()
        try:
            client = self._client(config, observer)
            cluster = client.list_topics(timeout=timeout)
            identities: dict[str, str] = {}
            describe_topics = getattr(client, "describe_topics", None)
            if callable(describe_topics):
                descriptions = describe_topics(_TopicCollection(list(cluster.topics)))
                identities = {
                    name: str(description.result(timeout).topic_id)
                    for name, description in descriptions.items()
                }
        except KafkaException as error:
            kind = observer.classify(error)
            if kind == "unavailable":
                kind = diagnose_security_failure(config, self._security_profile)
            raise KafkaMetadataError(kind) from error
        except KafkaSecurityConfigurationError as error:
            raise KafkaMetadataError("configuration") from error
        except TimeoutError as error:
            kind = observer.classify(error)
            if kind == "unavailable":
                kind = diagnose_security_failure(config, self._security_profile)
            raise KafkaMetadataError("timeout" if kind == "unavailable" else kind) from error
        for metadata in cluster.topics.values():
            topic_error = getattr(metadata, "error", None)
            if topic_error is not None and topic_error.code() != KafkaError.NO_ERROR:
                kind = observer.classify(topic_error)
                raise KafkaMetadataError(kind)
        topics = tuple(
            KafkaTopic(
                name=name,
                partition_count=len(metadata.partitions),
                identity=identities.get(name),
            )
            for name, metadata in cluster.topics.items()
        )
        cluster_id = getattr(cluster, "cluster_id", None)
        with self._lock:
            self._cluster_ids[config] = cluster_id
        return KafkaMetadata(
            broker_count=len(cluster.brokers),
            topics=topics,
            latency_ms=round((perf_counter() - started) * 1000, 3),
            cluster_identity=cluster_id,
        )

    def metadata_for_topic(
        self, config: KafkaConnectionConfig, topic_name: str, timeout: float
    ) -> KafkaMetadata:
        from time import perf_counter

        from confluent_kafka import KafkaError, KafkaException
        from confluent_kafka.admin import _TopicCollection

        observer = KafkaFailureObserver()
        started = perf_counter()
        try:
            client = self._client(config, observer)
            cluster = client.list_topics(topic=topic_name, timeout=timeout)
            descriptions = client.describe_topics(
                _TopicCollection([topic_name])
            )
            description = descriptions[topic_name].result(timeout)
        except KafkaException as error:
            code = getattr(error.args[0], "code", lambda: None)() if error.args else None
            if code == KafkaError.UNKNOWN_TOPIC_OR_PART:
                return KafkaMetadata(0, (), round((perf_counter() - started) * 1000, 3), None)
            kind = observer.classify(error)
            if kind == "unavailable":
                kind = diagnose_security_failure(config, self._security_profile)
            raise KafkaMetadataError(kind) from error
        except KafkaSecurityConfigurationError as error:
            raise KafkaMetadataError("configuration") from error
        except TimeoutError as error:
            kind = observer.classify(error)
            if kind == "unavailable":
                kind = diagnose_security_failure(config, self._security_profile)
            raise KafkaMetadataError("timeout" if kind == "unavailable" else kind) from error
        return KafkaMetadata(
            broker_count=len(cluster.brokers),
            topics=(KafkaTopic(topic_name, len(description.partitions), str(description.topic_id)),),
            latency_ms=round((perf_counter() - started) * 1000, 3),
            cluster_identity=cluster.cluster_id,
        )


@dataclass(frozen=True)
class KafkaPartition:
    topic: str
    partition: int
    offset: int


@dataclass(frozen=True)
class KafkaMessage:
    topic: str
    partition: int
    offset: int
    value: bytes | None
    received_at: datetime


@dataclass(frozen=True)
class KafkaAssignment:
    topic: str
    partitions: tuple[int, ...]


@dataclass(frozen=True)
class KafkaRevocation:
    topic: str
    partitions: tuple[int, ...]


class KafkaConsumerError(RuntimeError):
    def __init__(self, kind: str) -> None:
        super().__init__(kind)
        self.kind = kind


KafkaConsumerEvent = KafkaMessage | KafkaAssignment | KafkaRevocation


class KafkaConsumer(Protocol):
    def subscribe(self, topic: str) -> None: ...
    def poll(self, timeout: float) -> KafkaConsumerEvent | None: ...
    def assign(self, partitions: tuple[KafkaPartition, ...]) -> None: ...
    def unassign(self) -> None: ...
    def seek(self, partition: KafkaPartition) -> None: ...
    def pause(self, partitions: tuple[KafkaPartition, ...]) -> None: ...
    def resume(self, partitions: tuple[KafkaPartition, ...]) -> None: ...
    def committed(
        self, partitions: tuple[KafkaPartition, ...], timeout: float
    ) -> tuple[int | None, ...]: ...
    def watermarks(self, topic: str, partition: int, timeout: float) -> tuple[int, int]: ...
    def commit(self, offsets: tuple[KafkaPartition, ...], timeout: float) -> None: ...
    def close(self) -> None: ...


class KafkaConsumerFactory(Protocol):
    def create(self, config: KafkaConnectionConfig, group_id: str) -> KafkaConsumer: ...


class ConfluentKafkaConsumer:
    def __init__(self, client) -> None:
        self.client = client
        self.events: deque[KafkaAssignment | KafkaRevocation] = deque()
        self.messages: deque[KafkaMessage] = deque()
        self.commit_results: deque[tuple[object, tuple[object, ...]]] = deque()
        self.failure_observer = KafkaFailureObserver()
        self.assigned = False

    def _on_error(self, error) -> None:
        self.failure_observer(error)

    def _on_commit(self, error, partitions) -> None:
        self.commit_results.append((error, tuple(partitions or ())))

    def _failure(self, error) -> KafkaConsumerError:
        return KafkaConsumerError(self.failure_observer.classify(error))

    def subscribe(self, topic: str) -> None:
        from confluent_kafka import KafkaException

        try:
            self.client.subscribe(
                [topic], on_assign=self._on_assign, on_revoke=self._on_revoke
            )
        except KafkaException as error:
            raise self._failure(error) from error

    def _on_assign(self, _, partitions) -> None:
        self.assigned = False
        self.events.append(
            KafkaAssignment(
                topic=partitions[0].topic if partitions else "",
                partitions=tuple(partition.partition for partition in partitions),
            )
        )

    def _on_revoke(self, _, partitions) -> None:
        self.assigned = False
        self.events.append(
            KafkaRevocation(
                topic=partitions[0].topic if partitions else "",
                partitions=tuple(partition.partition for partition in partitions),
            )
        )

    def poll(self, timeout: float) -> KafkaConsumerEvent | None:
        if self.events:
            return self.events.popleft()
        if self.assigned and self.messages:
            return self.messages.popleft()
        from confluent_kafka import KafkaError, KafkaException

        try:
            message = self.client.poll(timeout)
        except KafkaException as error:
            raise self._failure(error) from error
        if message is None and (kind := self.failure_observer.take()):
            raise KafkaConsumerError(kind)
        if message is not None and message.error() is None:
            self.failure_observer.clear()
            self.messages.append(
                KafkaMessage(
                    topic=message.topic(),
                    partition=message.partition(),
                    offset=message.offset(),
                    value=message.value(),
                    received_at=datetime.now(UTC),
                )
            )
        if self.events:
            return self.events.popleft()
        if self.assigned and self.messages:
            return self.messages.popleft()
        if message is None:
            return None
        error = message.error()
        if error is not None:
            if error.code() == KafkaError._PARTITION_EOF:
                if kind := self.failure_observer.take():
                    raise KafkaConsumerError(kind)
                return None
            raise self._failure(error)
        return None

    def assign(self, partitions: tuple[KafkaPartition, ...]) -> None:
        from confluent_kafka import KafkaException, TopicPartition

        try:
            self.client.assign(
                [TopicPartition(item.topic, item.partition, item.offset) for item in partitions]
            )
            self.assigned = True
        except KafkaException as error:
            raise self._failure(error) from error

    def unassign(self) -> None:
        from confluent_kafka import KafkaException

        try:
            self.client.assign([])
            self.assigned = False
        except KafkaException as error:
            raise self._failure(error) from error

    def seek(self, partition: KafkaPartition) -> None:
        from confluent_kafka import KafkaException, TopicPartition

        try:
            self.client.seek(
                TopicPartition(partition.topic, partition.partition, partition.offset)
            )
        except KafkaException as error:
            raise self._failure(error) from error

    def pause(self, partitions: tuple[KafkaPartition, ...]) -> None:
        from confluent_kafka import KafkaException, TopicPartition

        try:
            self.client.pause(
                [TopicPartition(item.topic, item.partition) for item in partitions]
            )
        except KafkaException as error:
            raise self._failure(error) from error

    def resume(self, partitions: tuple[KafkaPartition, ...]) -> None:
        from confluent_kafka import KafkaException, TopicPartition

        try:
            self.client.resume(
                [TopicPartition(item.topic, item.partition) for item in partitions]
            )
        except KafkaException as error:
            raise self._failure(error) from error

    def committed(
        self, partitions: tuple[KafkaPartition, ...], timeout: float
    ) -> tuple[int | None, ...]:
        from confluent_kafka import OFFSET_INVALID, KafkaException, TopicPartition

        try:
            result = self.client.committed(
                [TopicPartition(item.topic, item.partition) for item in partitions],
                timeout=timeout,
            )
        except KafkaException as error:
            raise self._failure(error) from error
        return tuple(None if item.offset == OFFSET_INVALID else item.offset for item in result)

    def watermarks(self, topic: str, partition: int, timeout: float) -> tuple[int, int]:
        from confluent_kafka import KafkaException, TopicPartition

        try:
            return self.client.get_watermark_offsets(
                TopicPartition(topic, partition), timeout
            )
        except KafkaException as error:
            raise self._failure(error) from error

    def commit(self, offsets: tuple[KafkaPartition, ...], timeout: float) -> None:
        from time import monotonic

        from confluent_kafka import KafkaException, TopicPartition

        if timeout <= 0:
            raise KafkaConsumerError("timeout")

        try:
            self.client.commit(
                offsets=[
                    TopicPartition(item.topic, item.partition, item.offset)
                    for item in offsets
                ],
                asynchronous=True,
            )
        except KafkaException as error:
            raise self._failure(error) from error

        deadline = monotonic() + timeout
        while not self.commit_results:
            if kind := self.failure_observer.take():
                raise KafkaConsumerError(kind)
            remaining = deadline - monotonic()
            if remaining <= 0:
                raise KafkaConsumerError("timeout")
            try:
                message = self.client.poll(min(remaining, 0.1))
            except KafkaException as error:
                raise self._failure(error) from error
            if message is None:
                if kind := self.failure_observer.take():
                    raise KafkaConsumerError(kind)
                continue
            if message.error() is None:
                self.messages.append(
                    KafkaMessage(
                        topic=message.topic(),
                        partition=message.partition(),
                        offset=message.offset(),
                        value=message.value(),
                        received_at=datetime.now(UTC),
                    )
                )
                if kind := self.failure_observer.take():
                    raise KafkaConsumerError(kind)
            else:
                from confluent_kafka import KafkaError

                if message.error().code() != KafkaError._PARTITION_EOF:
                    kind = (
                        self.failure_observer.classify(message.error())
                    )
                    raise KafkaConsumerError(kind)
                if kind := self.failure_observer.take():
                    raise KafkaConsumerError(kind)
        error, partitions = self.commit_results.popleft()
        if error is not None:
            raise self._failure(KafkaException(error))
        requested = {(item.topic, item.partition): item.offset for item in offsets}
        observed = {
            (item.topic, item.partition): item.offset for item in partitions
        }
        if any(observed.get(key) != offset for key, offset in requested.items()):
            raise KafkaConsumerError("commit_failed")
        self.failure_observer.clear()

    def close(self) -> None:
        from confluent_kafka import KafkaException

        try:
            self.client.close()
        except KafkaException as error:
            raise self._failure(error) from error


class ConfluentKafkaConsumerFactory:
    def __init__(self, operation_timeout: float = 5,
                 security_profile: KafkaSecurityProfile | None = None) -> None:
        self.operation_timeout = operation_timeout
        self.security_profile = security_profile

    def create(self, config: KafkaConnectionConfig, group_id: str) -> KafkaConsumer:
        from confluent_kafka import Consumer

        try:
            security_options = client_security_options(config, self.security_profile)
        except KafkaSecurityConfigurationError as error:
            raise KafkaConsumerError("configuration") from error
        adapter = ConfluentKafkaConsumer(None)
        client = Consumer(
            {
                "bootstrap.servers": ",".join(config.bootstrap_servers),
                **security_options,
                "group.id": group_id,
                "enable.auto.commit": False,
                "enable.auto.offset.store": False,
                "auto.offset.reset": "earliest",
                "enable.partition.eof": True,
                "allow.auto.create.topics": False,
                "socket.timeout.ms": int(self.operation_timeout * 1000),
                "on_commit": adapter._on_commit,
                "error_cb": adapter._on_error,
            }
        )
        adapter.client = client
        return adapter
