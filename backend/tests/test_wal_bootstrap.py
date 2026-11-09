"""Ready capture validation is read-only and samples feedback before durability."""

from types import SimpleNamespace

import pytest

from app.adapters import wal_bootstrap as wal

PASSWORD = "synthetic_wal_password_for_tests_123"


class Result:
    def __init__(self, row=None, rows=None):
        self.row, self.rows = row, rows

    def fetchone(self):
        return self.row

    def fetchall(self):
        return self.rows


class Capture:
    def __init__(self):
        self.info = SimpleNamespace(dbname="diploma_test")
        self.slot = ("pgoutput", "diploma_test", "reserved", 1024, 10, 20, 123)
        self.durable = (20, 30)
        self.owner = (wal.ROLE, "diploma_test", "walsender")
        self.privileges = (True,) * 5
        self.flags = (False, True, False, False, False)
        self.boundary = ("saved boundary", None)
        self.calls = []
        self.verifier = wal.scram_verifier(PASSWORD)

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        pass

    def execute(self, query, params=None):
        self.calls.append(query)
        if "pg_advisory" in query:
            return Result((None,))
        if query.startswith("SHOW"):
            return Result(("logical" if "wal_level" in query else "1GB",))
        if "SELECT availability_verified_from_at" in query:
            return Result(self.boundary)
        if "SELECT plugin, database" in query:
            return Result(self.slot)
        if "pg_stat_activity" in query:
            return Result(self.owner)
        if "SELECT durable_lsn,pg_wal" in query:
            # A decoder may commit and acknowledge here; this fresh read must
            # follow the sampled feedback, rather than a stale snapshot first.
            assert any("SELECT plugin, database" in call for call in self.calls[:-1])
            return Result(self.durable)
        if "pg_publication_tables" in query:
            return Result(rows=[("logs", "availability_markers", ["id", "source_identity", "kind", "event_id"], None)])
        if "pg_publication WHERE" in query:
            return Result(self.flags)
        if "pg_authid" in query:
            return Result((True, True, False, self.verifier))
        if "has_schema_privilege" in query:
            return Result(self.privileges)
        if query.startswith("SELECT 1 FROM logs.replication_progress"):
            return Result((1,))
        if "SELECT EXISTS (SELECT 1 FROM pg_locks" in query:
            return Result((False,))
        if "SELECT active FROM pg_replication_slots" in query:
            return Result((True,))
        raise AssertionError(query)


def test_ready_bootstrap_is_read_only_with_active_decoder(monkeypatch):
    conn = Capture()
    monkeypatch.setattr(wal.psycopg, "connect", lambda *_args, **_kwargs: conn)
    wal.bootstrap("postgresql://test", PASSWORD)
    assert not any(call.startswith(("ALTER", "CREATE", "GRANT", "INSERT", "UPDATE")) for call in conn.calls)
    assert not any("pg_locks" in call for call in conn.calls)


@pytest.mark.parametrize("field,value,reason", [
    ("slot", None, "missing after capture"),
    ("slot", ("test_decoding", "diploma_test", "reserved", 1024, 10, 20, None), "incompatible"),
    ("slot", ("pgoutput", "other", "reserved", 1024, 10, 20, None), "incompatible"),
    ("slot", ("pgoutput", "diploma_test", "lost", None, 10, 20, None), "WAL"),
    ("slot", ("pgoutput", "diploma_test", "unreserved", 100, 10, 20, None), "WAL"),
    ("slot", ("pgoutput", "diploma_test", "reserved", 0, 10, 20, None), "WAL"),
    ("slot", ("pgoutput", "diploma_test", "reserved", 1024, None, 20, None), "positions"),
    ("slot", ("pgoutput", "diploma_test", "reserved", 1024, 21, 20, None), "positions"),
    ("durable", None, "unproven"),
    ("durable", (19, 30), "ahead"),
    ("durable", (31, 30), "unproven"),
    ("owner", ("unexpected", "diploma_test", "walsender"), "unexpected active reader"),
    ("flags", (False, True, True, False, False), "publication"),
    ("privileges", (True, False, True, True, True), "privileges"),
    ("verifier", wal.scram_verifier("another_synthetic_password_123"), "credentials"),
])
def test_invalid_existing_capture_is_refused(monkeypatch, field, value, reason):
    conn = Capture()
    setattr(conn, field, value)
    monkeypatch.setattr(wal.psycopg, "connect", lambda *_args, **_kwargs: conn)
    with pytest.raises(RuntimeError, match=reason):
        wal.bootstrap("postgresql://test", PASSWORD)
    assert not any(call.startswith(("ALTER", "CREATE", "GRANT", "INSERT", "UPDATE")) for call in conn.calls)


def test_upgrade_with_active_decoder_is_refused_before_mutation(monkeypatch):
    conn = Capture()
    conn.boundary = (None, None)
    monkeypatch.setattr(wal.psycopg, "connect", lambda *_args, **_kwargs: conn)
    with pytest.raises(RuntimeError, match="stopped decoder"):
        wal.bootstrap("postgresql://test", PASSWORD)
    assert not any(call.startswith(("ALTER", "CREATE", "GRANT", "INSERT", "UPDATE")) for call in conn.calls)


@pytest.mark.parametrize("verifier", [None, "", "md5invalid", "SCRAM-SHA-256$0:xxx$xx:xx"])
def test_password_verifier_fails_closed(verifier):
    assert not wal.verify_password(PASSWORD, verifier)


def test_password_verifier_matches_without_rotating():
    verifier = wal.scram_verifier(PASSWORD)
    assert wal.verify_password(PASSWORD, verifier)
    assert not wal.verify_password(PASSWORD + "x", verifier)
