from dataclasses import dataclass
from time import monotonic
from typing import Protocol

from .kafka import KafkaConnectionConfig

MAX_DEBUG_RECORDS = 50
MAX_DEBUG_BYTES = 1_048_576
MAX_DEBUG_PARTITIONS = 128


@dataclass(frozen=True)
class DebugRecord:
    partition: int
    offset: int
    value: bytes | None


class DebugReader(Protocol):
    def read(
        self, config: KafkaConnectionConfig, topic: str, timeout: float
    ) -> list[DebugRecord]: ...


class DebugReadError(Exception):
    def __init__(self, kind: str) -> None:
        super().__init__(kind)
        self.kind = kind


class ConfluentDebugReader:
    """Read a frozen, globally bounded tail without joining or committing a group."""

    def read(self, config: KafkaConnectionConfig, topic: str, timeout: float) -> list[DebugRecord]:
        from confluent_kafka import Consumer, KafkaException, TopicPartition

        client = Consumer(
            {
                "bootstrap.servers": ",".join(config.bootstrap_servers),
                "security.protocol": config.security_protocol,
                "group.id": "diploma-debug-isolated",
                "enable.auto.commit": False,
                "enable.auto.offset.store": False,
                "allow.auto.create.topics": False,
                "auto.offset.reset": "error",
                "fetch.message.max.bytes": MAX_DEBUG_BYTES,
                "max.partition.fetch.bytes": MAX_DEBUG_BYTES,
                "fetch.max.bytes": MAX_DEBUG_BYTES,
                "queued.max.messages.kbytes": MAX_DEBUG_BYTES // 1024,
            }
        )
        deadline = monotonic() + timeout
        try:
            metadata = client.list_topics(topic=topic, timeout=max(0.1, deadline - monotonic()))
            item = metadata.topics.get(topic)
            if item is None or item.error is not None:
                raise DebugReadError("unavailable")
            partitions = sorted(item.partitions)
            if len(partitions) > MAX_DEBUG_PARTITIONS:
                raise DebugReadError("too_many_partitions")

            # Freeze both bounds before assigning. The selected set contains no more
            # than 50 offsets in total; each partition contributes a contiguous tail.
            watermarks: dict[int, tuple[int, int]] = {}
            for partition in partitions:
                remaining = deadline - monotonic()
                if remaining <= 0:
                    raise DebugReadError("timeout")
                low, high = client.get_watermark_offsets(
                    TopicPartition(topic, partition), timeout=remaining, cached=False
                )
                watermarks[partition] = (low, high)

            selected: list[tuple[int, int]] = []
            distance = 1
            while len(selected) < MAX_DEBUG_RECORDS:
                added = False
                for partition in partitions:
                    low, high = watermarks[partition]
                    offset = high - distance
                    if offset >= low:
                        selected.append((partition, offset))
                        added = True
                        if len(selected) == MAX_DEBUG_RECORDS:
                            break
                if not added:
                    break
                distance += 1

            if not selected:
                return []

            max_distance: dict[int, int] = {}
            for partition, offset in selected:
                max_distance[partition] = max(
                    max_distance.get(partition, 0), watermarks[partition][1] - offset
                )
            assignments = [
                TopicPartition(topic, partition, high - max_distance[partition])
                for partition, (_, high) in watermarks.items()
                if partition in max_distance
            ]
            expected = set(selected)
            next_offset = {
                partition: high - max_distance[partition]
                for partition, (_, high) in watermarks.items()
                if partition in max_distance
            }
            client.assign(assignments)
            found: dict[tuple[int, int], DebugRecord] = {}
            total_bytes = 0
            while len(found) < len(expected):
                remaining = deadline - monotonic()
                if remaining <= 0:
                    raise DebugReadError("timeout")
                message = client.poll(min(0.25, max(0.01, remaining)))
                if message is None:
                    continue
                if message.error():
                    raise DebugReadError("unavailable")
                key = (message.partition(), message.offset())
                if key[0] in next_offset and key[1] != next_offset[key[0]]:
                    raise DebugReadError("incomplete")
                if key[0] in next_offset:
                    next_offset[key[0]] += 1
                if key not in expected:
                    # Messages at or beyond frozen high watermarks are not candidates.
                    continue
                value = message.value()
                size = len(value) if value is not None else 0
                if total_bytes + size > MAX_DEBUG_BYTES:
                    raise DebugReadError("byte_limit")
                found[key] = DebugRecord(key[0], key[1], value)
                total_bytes += size

            return sorted(
                found.values(),
                key=lambda row: (-(watermarks[row.partition][1] - row.offset), row.partition),
            )
        except KafkaException as error:
            raise DebugReadError("unavailable") from error
        finally:
            client.close()
