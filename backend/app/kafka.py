from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class KafkaConnectionConfig:
    bootstrap_servers: tuple[str, ...]
    security_protocol: str


@dataclass(frozen=True)
class KafkaTopic:
    name: str
    partition_count: int


@dataclass(frozen=True)
class KafkaMetadata:
    broker_count: int
    topics: tuple[KafkaTopic, ...]
    latency_ms: float


class KafkaMetadataError(Exception):
    def __init__(self, kind: str) -> None:
        super().__init__(kind)
        self.kind = kind


class KafkaMetadataClient(Protocol):
    def metadata(self, config: KafkaConnectionConfig, timeout: float) -> KafkaMetadata: ...


class ConfluentKafkaMetadataClient:
    def metadata(self, config: KafkaConnectionConfig, timeout: float) -> KafkaMetadata:
        from time import perf_counter

        from confluent_kafka import KafkaError, KafkaException
        from confluent_kafka.admin import AdminClient

        started = perf_counter()
        try:
            client = AdminClient(
                {
                    "bootstrap.servers": ",".join(config.bootstrap_servers),
                    "security.protocol": config.security_protocol,
                    "allow.auto.create.topics": False,
                }
            )
            cluster = client.list_topics(timeout=timeout)
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
            KafkaTopic(name=name, partition_count=len(metadata.partitions))
            for name, metadata in cluster.topics.items()
        )
        return KafkaMetadata(
            broker_count=len(cluster.brokers),
            topics=topics,
            latency_ms=round((perf_counter() - started) * 1000, 3),
        )
