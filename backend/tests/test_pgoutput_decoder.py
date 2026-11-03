from datetime import UTC, datetime, timedelta
from struct import pack

import pytest

from app.adapters.pgoutput import InvalidReplicationMessage, PgOutputDecoder


def messages():
    relation = (b"R" + pack("!I", 42) + b"logs\x00markers\x00" + b"d" + pack("!H", 2)
                + b"\x01id\x00" + pack("!Ii", 25, -1)
                + b"\x00kind\x00" + pack("!Ii", 25, -1))
    begin = b"B" + pack("!QqI", 10, 1234567, 55)
    insert = (b"I" + pack("!I", 42) + b"N" + pack("!H", 2)
              + b"t" + pack("!I", 3) + b"abc" + b"t" + pack("!I", 5) + b"event")
    commit = b"C\x00" + pack("!QQq", 10, 20, 1234567)
    return relation, begin, insert, commit


def test_commit_is_emitted_only_after_all_messages_and_has_wal_timestamp():
    decoder = PgOutputDecoder("logs", "markers", ("id", "kind"))
    relation, begin, insert, commit = messages()
    assert decoder.feed(relation) is None
    assert decoder.feed(begin) is None
    assert decoder.feed(insert) is None
    result = decoder.feed(commit)
    assert result is not None
    assert result.xid == 55 and result.end_lsn == 20
    assert result.committed_at == datetime(2000, 1, 1, tzinfo=UTC) + timedelta(microseconds=1234567)
    assert result.rows == (("abc", "event"),)


def test_unknown_relation_and_truncated_messages_fail_closed():
    relation, begin, insert, commit = messages()
    decoder = PgOutputDecoder("logs", "other", ("id", "kind"))
    with pytest.raises(InvalidReplicationMessage):
        decoder.feed(relation)
    decoder = PgOutputDecoder("logs", "markers", ("id", "kind"))
    decoder.feed(relation)
    decoder.feed(begin)
    with pytest.raises(InvalidReplicationMessage):
        decoder.feed(insert[:-1])
    with pytest.raises(InvalidReplicationMessage):
        decoder.feed(commit[:-1])


def test_excessive_transaction_cannot_be_accepted():
    relation, begin, insert, _ = messages()
    decoder = PgOutputDecoder("logs", "markers", ("id", "kind"), max_rows=1)
    for message in (relation, begin, insert):
        decoder.feed(message)
    with pytest.raises(InvalidReplicationMessage):
        decoder.feed(insert)
