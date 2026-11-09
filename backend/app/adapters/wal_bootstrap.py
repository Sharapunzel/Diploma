"""Bootstrap the narrow logical publication and its dedicated replication role."""

import base64
import hashlib
import hmac
import os
import re
from pathlib import Path
from uuid import uuid4

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


def verify_password(password: str, verifier: str | None) -> bool:
    """Check the configured credential even when local pg_hba uses trust."""
    try:
        algorithm, parameters, keys = verifier.split("$")
        iterations, salt = parameters.split(":")
        stored, server = keys.split(":")
        if algorithm != "SCRAM-SHA-256" or not 1 <= int(iterations) <= 1000000:
            return False
        salted = hashlib.pbkdf2_hmac("sha256", password.encode("ascii"),
            base64.b64decode(salt, validate=True), int(iterations))
        actual_stored = hashlib.sha256(hmac.digest(salted, b"Client Key", "sha256")).digest()
        return (hmac.compare_digest(actual_stored, base64.b64decode(stored, validate=True))
                and hmac.compare_digest(hmac.digest(salted, b"Server Key", "sha256"),
                                        base64.b64decode(server, validate=True)))
    except (AttributeError, ValueError, UnicodeError):
        return False


def check_publication(conn) -> None:
    shape = conn.execute("""SELECT schemaname, tablename, attnames, rowfilter
        FROM pg_publication_tables WHERE pubname=%s""", (PUBLICATION,)).fetchall()
    flags = conn.execute("""SELECT puballtables, pubinsert, pubupdate, pubdelete, pubtruncate
        FROM pg_publication WHERE pubname=%s""", (PUBLICATION,)).fetchone()
    if (shape != [("logs", "availability_markers", ["id", "source_identity", "kind", "event_id"], None)]
            or flags != (False, True, False, False, False)):
        raise RuntimeError("publication is not restricted to the expected marker fields")


def check_existing(conn, url: str, password: str) -> None:
    """Validate initialized capture without writes, role changes or slot resets.

    Sample slot feedback BEFORE reading durable progress in a fresh READ COMMITTED
    statement. Feedback follows materialization commit, so a live decoder cannot
    produce the false conflict caused by reading durable progress first.
    """
    slot = conn.execute("""SELECT plugin, database, wal_status, safe_wal_size,
        pg_wal_lsn_diff(restart_lsn,'0/0'), pg_wal_lsn_diff(confirmed_flush_lsn,'0/0'),
        active_pid FROM pg_replication_slots WHERE slot_name=%s""", (SLOT,)).fetchone()
    if slot is None:
        raise RuntimeError("replication slot missing after capture began")
    plugin, database, status, safe, restart, confirmed, pid = slot
    if plugin != "pgoutput" or database != conn.info.dbname:
        raise RuntimeError("replication slot is incompatible")
    if status not in {"reserved", "extended"} or safe is None or safe <= 0:
        raise RuntimeError("replication WAL is unavailable or near its budget")
    if restart is None or confirmed is None or restart > confirmed:
        raise RuntimeError("replication positions are unproven")
    if pid is not None:
        owner = conn.execute("""SELECT usename,datname,backend_type FROM pg_stat_activity
            WHERE pid=%s""", (pid,)).fetchone()
        if owner != (ROLE, database, "walsender"):
            raise RuntimeError("replication slot has an unexpected active reader")
    durable = conn.execute("""SELECT durable_lsn,pg_wal_lsn_diff(pg_current_wal_lsn(),'0/0')
        FROM logs.replication_progress WHERE id=1""").fetchone()
    if durable is None or not confirmed <= durable[0] <= durable[1]:
        raise RuntimeError("slot position is ahead of durable materialization or unproven")
    check_publication(conn)
    role = conn.execute("""SELECT rolcanlogin,rolreplication,rolsuper,rolpassword
        FROM pg_authid WHERE rolname=%s""", (ROLE,)).fetchone()
    if role is None or role[:3] != (True, True, False):
        raise RuntimeError("existing WAL reader role has unexpected privileges")
    if not verify_password(password, role[3]):
        raise RuntimeError("WAL reader credentials do not match initialized capture")
    privileges = conn.execute("""SELECT
        has_schema_privilege(%s,'logs','USAGE'),
        (has_table_privilege(%s,'logs.availability_markers','SELECT') AND
         has_table_privilege(%s,'logs.availability_markers','INSERT')),
        has_column_privilege(%s,'logs.availability_markers','availability_at','UPDATE'),
        (has_table_privilege(%s,'logs.replication_progress','SELECT') AND
         has_table_privilege(%s,'logs.replication_progress','INSERT') AND
         has_table_privilege(%s,'logs.replication_progress','UPDATE')),
        pg_has_role(%s,'pg_read_all_stats','USAGE')""", (ROLE,) * 8).fetchone()
    if not all(privileges):
        raise RuntimeError("WAL reader privileges are incomplete")
    try:
        with psycopg.connect(url, user=ROLE, password=password, connect_timeout=5) as reader:
            reader.execute("SELECT 1 FROM logs.replication_progress LIMIT 1")
    except psycopg.Error:
        raise RuntimeError("WAL reader connection authentication failed") from None


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
            boundary = conn.execute("""SELECT availability_verified_from_at,boundary_barrier_id
                FROM logs.replication_progress WHERE id=1""").fetchone()
            if boundary is not None and boundary != (None, None):
                check_existing(conn, url, password)
                return
            # Mutating first/upgrade bootstrap still requires stopped writers
            # and decoder. Check before any ALTER ROLE, grants or initialization.
            if conn.execute("""SELECT EXISTS (SELECT 1 FROM pg_locks
                WHERE relation IN ('logs.parsed_logs'::regclass, 'app.sources'::regclass)
                AND granted AND mode IN ('RowShareLock','RowExclusiveLock')
                AND pid <> pg_backend_pid())""").fetchone()[0]:
                raise RuntimeError("writers_not_stopped")
            active = conn.execute("SELECT active FROM pg_replication_slots WHERE slot_name=%s", (SLOT,)).fetchone()
            if active and active[0]:
                raise RuntimeError("replication slot is active; initialization requires stopped decoder")
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
            # Bootstrap runs with all old writers stopped. Never reconstruct a
            # first cutover from an LSN, a heartbeat or the earliest marker.
            if progress is None and not markers:
                conn.execute("""INSERT INTO logs.replication_progress
                    (id,durable_lsn,legacy_cutover_at,availability_verified_from_at,boundary_origin)
                    SELECT 1,%s,at,at,'first_bootstrap' FROM (SELECT clock_timestamp() AS at) AS boundary
                    ON CONFLICT DO NOTHING""", (confirmed,))
            else:
                conn.execute("INSERT INTO logs.replication_progress (id,durable_lsn) VALUES (1,%s) ON CONFLICT DO NOTHING", (confirmed,))
                boundary = conn.execute("SELECT availability_verified_from_at,boundary_barrier_id FROM logs.replication_progress WHERE id=1").fetchone()
                if boundary == (None, None):
                    barrier = uuid4()
                    # Commit marker and its identity together. The decoder, not
                    # bootstrap wall time, establishes the durable future bound.
                    with conn.transaction():
                        conn.execute("INSERT INTO logs.availability_markers (id,source_identity,kind) VALUES (%s,%s,'barrier')", (barrier, uuid4()))
                        conn.execute("UPDATE logs.replication_progress SET boundary_barrier_id=%s,boundary_origin='upgrade_barrier_pending' WHERE id=1", (barrier,))
        finally:
            conn.execute("SELECT pg_advisory_unlock(1380205361)")
