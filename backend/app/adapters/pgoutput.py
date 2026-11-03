"""Strict decoder for pgoutput protocol v1 transactions of a narrow publication.

This module only parses messages. Durability and WAL acknowledgement belong to
the caller; seeing a commit here never advances a replication slot.
"""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from struct import error as StructError
from struct import unpack_from

_EPOCH = datetime(2000, 1, 1, tzinfo=UTC)


class InvalidReplicationMessage(ValueError):
    """A stream is unsafe to acknowledge or cannot be interpreted."""


@dataclass(frozen=True, slots=True)
class DecodedCommit:
    xid: int
    commit_lsn: int
    end_lsn: int
    committed_at: datetime
    rows: tuple[tuple[str | None, ...], ...]


class _Reader:
    def __init__(self, data: bytes) -> None:
        self.data = data
        self.position = 0

    def take(self, length: int) -> bytes:
        if length < 0 or self.position + length > len(self.data):
            raise InvalidReplicationMessage("truncated pgoutput message")
        result = self.data[self.position:self.position + length]
        self.position += length
        return result

    def number(self, fmt: str) -> int:
        size = {"B": 1, "H": 2, "I": 4, "i": 4, "Q": 8, "q": 8}[fmt]
        try:
            return unpack_from("!" + fmt, self.take(size))[0]
        except StructError as exc:
            raise InvalidReplicationMessage("invalid numeric field") from exc

    def string(self) -> str:
        try:
            end = self.data.index(b"\0", self.position)
            result = self.data[self.position:end].decode("utf-8")
        except (ValueError, UnicodeDecodeError) as exc:
            raise InvalidReplicationMessage("invalid pgoutput string") from exc
        self.position = end + 1
        return result

    def finish(self) -> None:
        if self.position != len(self.data):
            raise InvalidReplicationMessage("unexpected trailing message data")


class PgOutputDecoder:
    def __init__(self, schema: str, table: str, columns: tuple[str, ...], *, max_rows: int = 10000) -> None:
        if not schema or not table or not columns or max_rows < 1:
            raise ValueError("publication and row limit must be configured")
        self.schema, self.table, self.columns = schema, table, columns
        self.max_rows = max_rows
        self.relations: dict[int, tuple[str, ...]] = {}
        self.xid: int | None = None
        self.rows: list[tuple[str | None, ...]] = []

    def feed(self, message: bytes) -> DecodedCommit | None:
        r = _Reader(message)
        tag = r.take(1)
        if tag == b"R":
            oid, schema, table = r.number("I"), r.string(), r.string()
            r.number("B")  # replica identity
            count = r.number("H")
            names = []
            for _ in range(count):
                r.number("B")  # key flag
                names.append(r.string())
                r.number("I")  # type oid
                r.number("i")  # type modifier
            r.finish()
            if (schema, table, tuple(names)) != (self.schema, self.table, self.columns):
                raise InvalidReplicationMessage("unexpected publication relation")
            self.relations[oid] = tuple(names)
        elif tag == b"B":
            if self.xid is not None:
                raise InvalidReplicationMessage("nested transaction")
            r.number("Q")  # final LSN
            r.number("q")  # commit time, repeated by Commit
            self.xid = r.number("I")
            self.rows = []
            r.finish()
        elif tag == b"I":
            if self.xid is None:
                raise InvalidReplicationMessage("insert outside transaction")
            oid = r.number("I")
            if oid not in self.relations or r.take(1) != b"N":
                raise InvalidReplicationMessage("unknown relation or tuple")
            count = r.number("H")
            if count != len(self.columns) or len(self.rows) >= self.max_rows:
                raise InvalidReplicationMessage("unexpected tuple width or row limit exceeded")
            values: list[str | None] = []
            for _ in range(count):
                value_kind = r.take(1)
                if value_kind == b"n":
                    values.append(None)
                elif value_kind == b"t":
                    try:
                        values.append(r.take(r.number("I")).decode("utf-8"))
                    except UnicodeDecodeError as exc:
                        raise InvalidReplicationMessage("invalid tuple text") from exc
                else:
                    raise InvalidReplicationMessage("unsupported tuple field")
            r.finish()
            self.rows.append(tuple(values))
        elif tag == b"C":
            if self.xid is None or r.number("B") != 0:
                raise InvalidReplicationMessage("invalid transaction commit")
            commit_lsn, end_lsn, micros = r.number("Q"), r.number("Q"), r.number("q")
            r.finish()
            if end_lsn < commit_lsn:
                raise InvalidReplicationMessage("invalid commit LSN")
            try:
                committed_at = _EPOCH + timedelta(microseconds=micros)
            except OverflowError as exc:
                raise InvalidReplicationMessage("commit time out of range") from exc
            result = DecodedCommit(self.xid, commit_lsn, end_lsn, committed_at, tuple(self.rows))
            self.xid, self.rows = None, []
            return result
        else:
            raise InvalidReplicationMessage("unsupported pgoutput message")
        return None
