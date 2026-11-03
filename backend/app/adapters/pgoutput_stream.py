"""Minimal libpq logical replication transport with explicit durable feedback."""

import re
import select
import struct
import time
from collections import deque
from datetime import UTC, datetime

from psycopg import pq
from psycopg.conninfo import make_conninfo

from .pgoutput import DecodedCommit, InvalidReplicationMessage, PgOutputDecoder

_IDENTIFIER = re.compile(r"[a-z_][a-z0-9_]{0,62}\Z")
_EPOCH = datetime(2000, 1, 1, tzinfo=UTC)


class ReplicationStream:
    """One slot reader. The caller persists a commit before calling acknowledge()."""

    def __init__(self, url: str, slot: str, publication: str, decoder: PgOutputDecoder):
        if not _IDENTIFIER.fullmatch(slot) or not _IDENTIFIER.fullmatch(publication):
            raise ValueError("invalid replication identifier")
        self.url, self.slot, self.publication, self.decoder = url, slot, publication, decoder
        self.connection: pq.PGconn | None = None
        self.durable_lsn = 0
        self.delivered_lsn = 0
        self.pending: deque[int] = deque()

    def __enter__(self):
        conninfo = make_conninfo(self.url, replication="database", connect_timeout=5)
        self.connection = pq.PGconn.connect(conninfo.encode())
        if self.connection.status != pq.ConnStatus.OK:
            message = self.connection.error_message.decode("utf-8", errors="replace")
            self.connection.finish()
            self.connection = None
            raise ConnectionError(f"replication connection unavailable: {message}")
        command = (f"START_REPLICATION SLOT {self.slot} LOGICAL 0/0 "
                   f"(proto_version '1', publication_names '{self.publication}')")
        result = self.connection.exec_(command.encode())
        if result.status != pq.ExecStatus.COPY_BOTH:
            self.connection.finish()
            self.connection = None
            raise ConnectionError("replication slot unavailable")
        return self

    def __exit__(self, *_args):
        if self.connection is not None:
            self.connection.finish()
            self.connection = None

    def acknowledge(self, end_lsn: int) -> None:
        if not self.pending or self.pending[0] != end_lsn:
            raise ValueError("acknowledgement must follow delivered commit order")
        self.pending.popleft()
        self.durable_lsn = end_lsn
        self._feedback()

    def _feedback(self) -> None:
        if self.connection is None:
            raise ConnectionError("replication connection is closed")
        micros = int((datetime.now(UTC) - _EPOCH).total_seconds() * 1_000_000)
        frame = b"r" + struct.pack("!QQQqB", self.durable_lsn, self.durable_lsn,
                                   self.durable_lsn, micros, 0)
        if self.connection.put_copy_data(frame) != 1 or self.connection.flush() != 0:
            raise ConnectionError("replication feedback failed")

    def read_commit(self, timeout: float) -> DecodedCommit | None:
        if self.connection is None:
            raise ConnectionError("replication connection is closed")
        if self.pending:
            raise RuntimeError("persist and acknowledge the previous commit before reading")
        deadline = time.monotonic() + timeout
        while True:
            size, payload = self.connection.get_copy_data(1)
            if size == 0:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return None
                ready, _, _ = select.select([self.connection.socket], [], [], remaining)
                if not ready:
                    return None
                self.connection.consume_input()
                continue
            if size < 0:
                raise ConnectionError("replication stream ended")
            frame = bytes(payload)
            if frame[:1] == b"k" and len(frame) == 18:
                if frame[-1] == 1:
                    self._feedback()
                continue
            if frame[:1] != b"w" or len(frame) < 25:
                raise InvalidReplicationMessage("invalid replication frame")
            commit = self.decoder.feed(frame[25:])
            if commit is not None:
                if commit.end_lsn < self.delivered_lsn:
                    raise InvalidReplicationMessage("regressing commit position")
                self.delivered_lsn = commit.end_lsn
                self.pending.append(commit.end_lsn)
                return commit
