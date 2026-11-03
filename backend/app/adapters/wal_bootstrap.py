"""Bootstrap the narrow logical publication and its dedicated replication role."""

import base64
import hashlib
import hmac
import os
import re
from pathlib import Path

import psycopg
from psycopg import sql

SLOT = "diploma_availability"
PUBLICATION = "diploma_availability"
ROLE = "diploma_wal_reader"


def read_setting(name: str) -> str:
    path = os.environ.get(name + "_FILE")
    value = Path(path).read_text(encoding="utf-8").strip() if path else os.environ.get(name, "")
    if not value:
        raise ValueError(f"{name} is required")
    return value


def scram_verifier(password: str) -> str:
    # Restrict to safe ASCII so server and client SASL preparation agree.
    if not re.fullmatch(r"[A-Za-z0-9_-]{24,128}", password):
        raise ValueError("WAL reader password must be 24-128 URL-safe ASCII characters")
    salt = os.urandom(16)
    salted = hashlib.pbkdf2_hmac("sha256", password.encode("ascii"), salt, 4096)
    client_key = hmac.digest(salted, b"Client Key", "sha256")
    stored = hashlib.sha256(client_key).digest()
    server = hmac.digest(salted, b"Server Key", "sha256")
    encode = lambda value: base64.b64encode(value).decode("ascii")
    return f"SCRAM-SHA-256$4096:{encode(salt)}${encode(stored)}:{encode(server)}"


def bootstrap(url: str, password: str) -> None:
    url = url.replace("postgresql+psycopg:", "postgresql:")
    verifier = scram_verifier(password)
    with psycopg.connect(url, autocommit=True) as conn:
        conn.execute("SELECT pg_advisory_lock(1380205361)")
        try:
            if conn.execute("SHOW wal_level").fetchone()[0] != "logical":
                raise RuntimeError("logical WAL is not enabled")
            if conn.execute("SHOW max_slot_wal_keep_size").fetchone()[0] == "-1":
                raise RuntimeError("logical slot WAL limit must be finite")
            exists = conn.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (ROLE,)).fetchone()
            if not exists:
                conn.execute(sql.SQL("CREATE ROLE {} LOGIN REPLICATION PASSWORD {}").format(
                    sql.Identifier(ROLE), sql.Literal(verifier)))
            else:
                attrs = conn.execute("SELECT rolcanlogin, rolreplication, rolsuper FROM pg_roles WHERE rolname=%s", (ROLE,)).fetchone()
                if attrs != (True, True, False):
                    raise RuntimeError("existing WAL reader role has unexpected privileges")
                conn.execute(sql.SQL("ALTER ROLE {} PASSWORD {}").format(
                    sql.Identifier(ROLE), sql.Literal(verifier)))
            conn.execute(sql.SQL("GRANT USAGE ON SCHEMA logs TO {}").format(sql.Identifier(ROLE)))
            conn.execute(sql.SQL("GRANT SELECT, INSERT, UPDATE (availability_at) ON logs.availability_markers TO {}").format(sql.Identifier(ROLE)))
            conn.execute(sql.SQL("GRANT SELECT, INSERT, UPDATE ON logs.replication_progress TO {}").format(sql.Identifier(ROLE)))
            conn.execute(sql.SQL("GRANT pg_read_all_stats TO {}").format(sql.Identifier(ROLE)))
            publication = conn.execute("SELECT 1 FROM pg_publication WHERE pubname=%s", (PUBLICATION,)).fetchone()
            if not publication:
                conn.execute(sql.SQL("CREATE PUBLICATION {} FOR TABLE logs.availability_markers "
                                     "(id, source_identity, kind, event_id) WITH (publish='insert')").format(
                                         sql.Identifier(PUBLICATION)))
            shape = conn.execute("""SELECT schemaname, tablename, attnames, rowfilter
                FROM pg_publication_tables WHERE pubname=%s""", (PUBLICATION,)).fetchall()
            flags = conn.execute("""SELECT puballtables, pubinsert, pubupdate, pubdelete, pubtruncate
                FROM pg_publication WHERE pubname=%s""", (PUBLICATION,)).fetchone()
            if (shape != [("logs", "availability_markers", ["id", "source_identity", "kind", "event_id"], None)]
                    or flags != (False, True, False, False, False)):
                raise RuntimeError("publication is not restricted to the expected marker fields")
            # Never silently recreate a slot after marker capture has begun.
            slot = conn.execute("""SELECT plugin, database, wal_status, active FROM pg_replication_slots
                WHERE slot_name=%s""", (SLOT,)).fetchone()
            progress = conn.execute("SELECT durable_lsn FROM logs.replication_progress WHERE id=1").fetchone()
            markers = conn.execute("SELECT EXISTS (SELECT 1 FROM logs.availability_markers)").fetchone()[0]
            if slot is None:
                if progress is not None or markers:
                    raise RuntimeError("replication slot missing after capture began")
                conn.execute("SELECT * FROM pg_create_logical_replication_slot(%s, 'pgoutput')", (SLOT,))
            elif slot[0] != "pgoutput" or slot[1] != conn.info.dbname or slot[2] == "lost" or slot[3]:
                raise RuntimeError("replication slot is incompatible, lost or active")
            confirmed = conn.execute("""SELECT pg_wal_lsn_diff(confirmed_flush_lsn, '0/0'::pg_lsn)
                FROM pg_replication_slots WHERE slot_name=%s""", (SLOT,)).fetchone()[0]
            if progress is not None and confirmed > progress[0]:
                raise RuntimeError("slot position is ahead of durable materialization")
            conn.execute("INSERT INTO logs.replication_progress (id, durable_lsn) VALUES (1, %s) ON CONFLICT DO NOTHING", (confirmed,))
        finally:
            conn.execute("SELECT pg_advisory_unlock(1380205361)")
