from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime
from threading import RLock
from typing import Protocol


@dataclass(frozen=True)
class KafkaConnectionConfig:
    bootstrap_servers: tuple[str, ...]
    security_protocol: str


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


class KafkaMetadataClient(Protocol):
    def metadata(self, config: KafkaConnectionConfig, timeout: float) -> KafkaMetadata: ...


class ConfluentKafkaMetadataClient:
    def __init__(self) -> None:
        self._lock = RLock()
        self._clients: dict[KafkaConnectionConfig, object] = {}
        self._cluster_ids: dict[KafkaConnectionConfig, str | None] = {}

    def _client(self, config: KafkaConnectionConfig):
        from confluent_kafka.admin import AdminClient

        with self._lock:
            client = self._clients.get(config)
            if client is None:
                client = AdminClient(
                    {
                        "bootstrap.servers": ",".join(config.bootstrap_servers),
                        "security.protocol": config.security_protocol,
                        "allow.auto.create.topics": False,
                    }
                )
                self._clients[config] = client
            return client

    def metadata(self, config: KafkaConnectionConfig, timeout: float) -> KafkaMetadata:
        from time import perf_counter

        from confluent_kafka import KafkaError, KafkaException
        from confluent_kafka.admin import _TopicCollection

        started = perf_counter()
        try:
            client = self._client(config)
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
            code = getattr(error.args[0], "code", lambda: None)() if error.args else None
            kind = "timeout" if code == KafkaError._TIMED_OUT else "unavailable"
            raise KafkaMetadataError(kind) from error
        except TimeoutError as error:
            raise KafkaMetadataError("timeout") from error
        for metadata in cluster.topics.values():
            topic_error = getattr(metadata, "error", None)
            if topic_error is not None and topic_error.code() != KafkaError.NO_ERROR:
                kind = "timeout" if topic_error.code() == KafkaError._TIMED_OUT else "unavailable"
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

        with self._lock:
            cluster_id = self._cluster_ids.get(config)
        if not cluster_id:
            return self.metadata(config, timeout)
        started = perf_counter()
        try:
            descriptions = self._client(config).describe_topics(
                _TopicCollection([topic_name])
            )
            description = descriptions[topic_name].result(timeout)
        except KafkaException as error:
            code = getattr(error.args[0], "code", lambda: None)() if error.args else None
            if code == KafkaError.UNKNOWN_TOPIC_OR_PART:
                return KafkaMetadata(0, (), round((perf_counter() - started) * 1000, 3), cluster_id)
            raise KafkaMetadataError(
                "timeout" if code == KafkaError._TIMED_OUT else "unavailable"
            ) from error
        except TimeoutError as error:
            raise KafkaMetadataError("timeout") from error
        return KafkaMetadata(
            broker_count=0,
            topics=(KafkaTopic(topic_name, len(description.partitions), str(description.topic_id)),),
            latency_ms=round((perf_counter() - started) * 1000, 3),
            cluster_identity=cluster_id,
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
        self.assigned = False

    def _on_commit(self, error, partitions) -> None:
        self.commit_results.append((error, tuple(partitions or ())))

    @staticmethod
    def _failure(error) -> KafkaConsumerError:
        from confluent_kafka import KafkaError

        code = getattr(error.args[0], "code", lambda: None)() if error.args else None
        return KafkaConsumerError(
            "timeout" if code == KafkaError._TIMED_OUT else "unavailable"
        )

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
        if message is not None and message.error() is None:
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
                return None
            kind = "timeout" if error.code() == KafkaError._TIMED_OUT else "unavailable"
            raise KafkaConsumerError(kind)
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
            remaining = deadline - monotonic()
            if remaining <= 0:
                raise KafkaConsumerError("timeout")
            try:
                message = self.client.poll(min(remaining, 0.1))
            except KafkaException as error:
                raise self._failure(error) from error
            if message is None:
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
            else:
                from confluent_kafka import KafkaError

                if message.error().code() != KafkaError._PARTITION_EOF:
                    kind = (
                        "timeout"
                        if message.error().code() == KafkaError._TIMED_OUT
                        else "unavailable"
                    )
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

    def close(self) -> None:
        from confluent_kafka import KafkaException

        try:
            self.client.close()
        except KafkaException as error:
            raise self._failure(error) from error


class ConfluentKafkaConsumerFactory:
    def __init__(self, operation_timeout: float = 5) -> None:
        self.operation_timeout = operation_timeout

    def create(self, config: KafkaConnectionConfig, group_id: str) -> KafkaConsumer:
        from confluent_kafka import Consumer

        adapter = ConfluentKafkaConsumer(None)
        client = Consumer(
            {
                "bootstrap.servers": ",".join(config.bootstrap_servers),
                "security.protocol": config.security_protocol,
                "group.id": group_id,
                "enable.auto.commit": False,
                "enable.auto.offset.store": False,
                "auto.offset.reset": "earliest",
                "enable.partition.eof": True,
                "allow.auto.create.topics": False,
                "socket.timeout.ms": int(self.operation_timeout * 1000),
                "on_commit": adapter._on_commit,
            }
        )
        adapter.client = client
        return adapter
