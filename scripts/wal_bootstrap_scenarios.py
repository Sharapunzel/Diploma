"""Linux/WAL regression, executed inside the existing isolated Compose profile.

The caller starts postgres/migrate/wal-decoder and passes this source to the API
image's Python. No Docker socket or host credentials enter the container.
"""

import threading
import time
from uuid import uuid4

import psycopg
from app.adapters.wal_bootstrap import bootstrap, read_setting


def main():
    url = read_setting("DATABASE_URL").replace("postgresql+psycopg:", "postgresql:")
    password = read_setting("WAL_READER_PASSWORD")
    stop = threading.Event()
    errors = []
    markers = []
    with psycopg.connect(url, autocommit=True) as db:
        assert db.info.dbname == "diploma_test", "Requires isolated profile database"
        boundary_sql = """SELECT legacy_cutover_at,availability_verified_from_at,
            boundary_barrier_id,boundary_origin FROM logs.replication_progress WHERE id=1"""
        boundary = db.execute(boundary_sql).fetchone()
        role = db.execute("SELECT rolpassword FROM pg_authid WHERE rolname='diploma_wal_reader'").fetchone()
        initial = db.execute("SELECT durable_lsn FROM logs.replication_progress WHERE id=1").fetchone()[0]
        assert db.execute("SELECT active FROM pg_replication_slots WHERE slot_name='diploma_availability'").fetchone()[0]

        def writer():
            try:
                with psycopg.connect(url, autocommit=True) as conn:
                    while not stop.is_set():
                        marker = uuid4()
                        conn.execute("INSERT INTO logs.availability_markers (id,source_identity,kind) VALUES (%s,%s,'barrier')", (marker, uuid4()))
                        markers.append(marker)
                        stop.wait(.01)
            except psycopg.Error as error:
                errors.append(type(error).__name__)

        thread = threading.Thread(target=writer)
        thread.start()
        try:
            for _ in range(15):
                bootstrap(url, password)
        finally:
            stop.set()
            thread.join(10)
        assert not thread.is_alive() and not errors and markers
        deadline = time.monotonic() + 20
        while db.execute("SELECT count(*) FROM logs.availability_markers WHERE id=ANY(%s) AND availability_at IS NULL", (markers,)).fetchone()[0]:
            assert time.monotonic() < deadline, "Live decoder stopped progressing"
            time.sleep(.05)
        assert db.execute("SELECT durable_lsn FROM logs.replication_progress WHERE id=1").fetchone()[0] > initial
        assert db.execute(boundary_sql).fetchone() == boundary
        assert db.execute("SELECT rolpassword FROM pg_authid WHERE rolname='diploma_wal_reader'").fetchone() == role
        try:
            bootstrap(url, "incorrect_synthetic_password_123")
        except RuntimeError as error:
            assert "credentials" in str(error)
        else:
            raise AssertionError("Wrong credentials accepted")
        # Revoking INSERT doesn't interfere with materialization's UPDATE and
        # must not be silently repaired by a repeated ready-state bootstrap.
        db.execute("REVOKE INSERT ON logs.availability_markers FROM diploma_wal_reader")
        try:
            try:
                bootstrap(url, password)
            except RuntimeError as error:
                assert "privileges" in str(error)
            else:
                raise AssertionError("Missing privilege accepted")
        finally:
            db.execute("GRANT INSERT ON logs.availability_markers TO diploma_wal_reader")
        db.execute("ALTER PUBLICATION diploma_availability SET (publish='insert,update')")
        try:
            try:
                bootstrap(url, password)
            except RuntimeError as error:
                assert "publication" in str(error)
            else:
                raise AssertionError("Wrong publication accepted")
        finally:
            db.execute("ALTER PUBLICATION diploma_availability SET (publish='insert')")
        bootstrap(url, password)
        assert db.execute(boundary_sql).fetchone() == boundary
    print("Linux WAL gate passed: live decoder/writer race, unchanged boundaries/credentials, monotone progress, negative credentials/ACL/publication", flush=True)


if __name__ == "__main__":
    main()
