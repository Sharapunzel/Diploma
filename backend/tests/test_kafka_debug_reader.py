import sys
from types import ModuleType, SimpleNamespace

import pytest

from app.kafka import KafkaConnectionConfig
from app.kafka_debug import (
    MAX_DEBUG_BYTES,
    ConfluentDebugReader,
    DebugReadError,
)


class FakeMessage:
    def __init__(self, partition, offset, value):
        self._partition, self._offset, self._value = partition, offset, value

    def error(self):
        return None

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def value(self):
        return self._value


class FakeConsumer:
    def __init__(self, options, bounds, messages, additions=(), skip=()):
        self.options = options
        self.bounds = bounds
        self.messages = messages
        self.additions = additions
        self.skip = set(skip)
        self.assigned = []
        self.closed = False
        self.topic_partitions = set()

    def list_topics(self, **_):
        return SimpleNamespace(
            topics={
                "logs": SimpleNamespace(error=None, partitions={p: object() for p in self.bounds})
            }
        )

    def get_watermark_offsets(self, tp, **_):
        return self.bounds[tp.partition]

    def assign(self, assignments):
        self.assigned = assignments
        # New messages arrive after the high watermarks were captured.
        for partition, offset, value in self.additions:
            self.messages.setdefault(partition, {})[offset] = value
        self.pending = []
        for tp in assignments:
            _, high = self.bounds[tp.partition]
            for offset in range(tp.offset, high):
                if (tp.partition, offset) not in self.skip and offset in self.messages.get(
                    tp.partition, {}
                ):
                    self.pending.append(
                        FakeMessage(tp.partition, offset, self.messages[tp.partition][offset])
                    )
        self.pending.reverse()

    def poll(self, _):
        return self.pending.pop() if self.pending else None

    def close(self):
        self.closed = True


def run_reader(monkeypatch, bounds, messages, **kwargs):
    state = {}

    def consumer(options):
        client = FakeConsumer(options, bounds, messages, **kwargs)
        state["client"] = client
        return client

    fake_module = ModuleType("confluent_kafka")
    fake_module.Consumer = consumer
    fake_module.KafkaException = type("KafkaException", (Exception,), {})
    fake_module.TopicPartition = lambda topic=None, partition=0, offset=0: SimpleNamespace(
        topic=topic, partition=partition, offset=offset
    )
    monkeypatch.setitem(sys.modules, "confluent_kafka", fake_module)
    config = KafkaConnectionConfig(("broker:9092",), "PLAINTEXT")
    result = ConfluentDebugReader().read(config, "logs", 1)
    return result, state["client"]


def test_empty_and_single_partition(monkeypatch):
    no_partitions, no_partition_client = run_reader(monkeypatch, {}, {})
    assert no_partitions == [] and no_partition_client.assigned == []
    empty, client = run_reader(monkeypatch, {0: (0, 0)}, {0: {}})
    assert empty == [] and client.assigned == [] and client.closed
    rows, _ = run_reader(monkeypatch, {0: (0, 3)}, {0: {0: b"a", 1: b"b", 2: b"c"}})
    assert [(row.partition, row.offset) for row in rows] == [(0, 0), (0, 1), (0, 2)]


def test_selects_only_50_global_candidates_across_partitions(monkeypatch):
    bounds = {partition: (0, 40) for partition in range(3)}
    messages = {partition: {offset: b"x" for offset in range(40)} for partition in bounds}
    rows, client = run_reader(monkeypatch, bounds, messages)
    assert len(rows) == 50
    assert len(client.assigned) == 3
    assert sum(client.bounds[tp.partition][1] - tp.offset for tp in client.assigned) == 50
    assert len({(row.partition, row.offset) for row in rows}) == 50
    assert rows == sorted(rows, key=lambda row: (-(39 - row.offset), row.partition))


def test_uses_frozen_high_watermarks_when_new_messages_arrive(monkeypatch):
    rows, _ = run_reader(
        monkeypatch,
        {0: (0, 2)},
        {0: {0: b"old", 1: b"tail"}},
        additions=((0, 2, b"new"),),
    )
    assert [row.offset for row in rows] == [0, 1]


def test_global_byte_limit_fails_explicitly(monkeypatch):
    count = 10
    bounds = {partition: (0, count) for partition in range(6)}
    messages = {
        partition: {offset: b"x" * (MAX_DEBUG_BYTES // 50 + 1) for offset in range(count)}
        for partition in bounds
    }
    with pytest.raises(DebugReadError) as error:
        run_reader(monkeypatch, bounds, messages)
    assert error.value.kind == "byte_limit"


def test_missing_offset_fails_without_backfilling(monkeypatch):
    with pytest.raises(DebugReadError) as error:
        run_reader(monkeypatch, {0: (0, 5)}, {0: {0: b"a", 1: b"b", 3: b"d", 4: b"e"}})
    assert error.value.kind == "incomplete"


def test_reader_never_subscribes_or_commits(monkeypatch):
    rows, client = run_reader(monkeypatch, {0: (0, 2)}, {0: {0: b"a", 1: b"b"}})
    assert len(rows) == 2
    assert client.options["enable.auto.commit"] is False
    assert client.options["enable.auto.offset.store"] is False
    assert client.options["group.id"] == "diploma-debug-isolated"
    assert client.options["fetch.max.bytes"] == MAX_DEBUG_BYTES
    assert client.options["queued.max.messages.kbytes"] == MAX_DEBUG_BYTES // 1024
    assert all(not hasattr(client, name) for name in ("subscribe", "commit"))
