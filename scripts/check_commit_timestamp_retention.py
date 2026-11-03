"""Reproduce the TASK-30 retention gate on an isolated project PostgreSQL.

Run: python -B scripts/check_commit_timestamp_retention.py
Uses the backend-db Compose preparation and cleanup, without application builds.
Exit 0 confirms the counterexample, not readiness of working aggregation.
"""

from __future__ import annotations

import json
import os
import shutil
from uuid import uuid4

import psycopg
from psycopg.conninfo import make_conninfo

import agent_test_profiles as profiles


def main() -> None:
    run = profiles.ROOT / ".test_runs" / "runs" / f"commit-retention-{uuid4().hex}"
    run.mkdir(parents=True, exist_ok=False)
    env = {
        **os.environ, "TEMP": str(run), "TMP": str(run), "TMPDIR": str(run),
        "PYTHONDONTWRITEBYTECODE": "1",
    }
    before = None
    compose = None
    project = None
    try:
        before = profiles.inventory(env)
        (run / "docker-before.json").write_text(json.dumps(before), encoding="utf-8")
        env, compose, project = profiles.prepare(run, "backend-db")
        overlay = run / "commit-tracking.json"
        overlay.write_text(json.dumps({"services": {"postgres": {
            "command": ["postgres", "-c", "track_commit_timestamp=on"],
        }}}), encoding="utf-8")
        compose += ["-f", str(overlay)]
        profiles.check_config(compose, project, env, run)
        print(f"Profile=backend-db; project={project}; PostgreSQL={env['POSTGRES_BIND']}; run={run}",
              flush=True)
        profiles.command([*compose, "up", "-d", "--wait", "postgres"], env=env)
        url = env["TEST_DATABASE_URL"].replace("postgresql+psycopg:", "postgresql:")
        with psycopg.connect(url, autocommit=True) as conn:
            print(conn.execute("SELECT version()").fetchone()[0], flush=True)
            assert conn.execute("SHOW track_commit_timestamp").fetchone()[0] == "on"
            conn.execute("""CREATE TABLE commit_retention_probe (
                id integer PRIMARY KEY, writer_xid xid NOT NULL,
                materialized_at timestamptz
            )""")
            # INSERT completes a real transaction; no synthetic commit timestamp.
            conn.execute("""INSERT INTO commit_retention_probe (id, writer_xid)
                VALUES (1, pg_current_xact_id()::text::xid)""")
            xid, committed_at = conn.execute("""SELECT writer_xid,
                pg_xact_commit_timestamp(writer_xid) FROM commit_retention_probe""").fetchone()
            assert committed_at is not None
            print(f"Before vacuum: writer_xid={xid}; commit={committed_at.isoformat()}",
                  flush=True)
            # Control: materialization while the metadata is still available.
            conn.execute("""UPDATE commit_retention_probe
                SET materialized_at = pg_xact_commit_timestamp(writer_xid)""")
            # Accelerate the cluster-wide XID horizon without waiting months or
            # generating millions of transactions. All databases belong to this
            # disposable cluster; never run this against a development database.
            conn.execute("ALTER DATABASE template0 ALLOW_CONNECTIONS true")
        for database in ("diploma_test", "postgres", "template1", "template0"):
            with psycopg.connect(make_conninfo(url, dbname=database),
                                 autocommit=True) as conn:
                conn.execute("VACUUM FREEZE")
        profiles.command([*compose, "restart", "postgres"], env=env)
        profiles.command([*compose, "up", "-d", "--wait", "postgres"], env=env)
        with psycopg.connect(url, autocommit=True) as conn:
            lost, retained = conn.execute("""SELECT
                pg_xact_commit_timestamp(writer_xid), materialized_at
                FROM commit_retention_probe WHERE id = 1""").fetchone()
            assert lost is None, "Expected the advanced horizon to remove commit metadata"
            assert retained == committed_at, "Materialized control must survive"
            print("After vacuum and restart: commit metadata=NULL; "
                  "early materialized control unchanged; row retained", flush=True)
            print("COUNTEREXAMPLE CONFIRMED: an unmaterialized commit is unrecoverable. "
                  "TASK-30 retention gate has not passed.", flush=True)
    finally:
        # Keep the run inventory on cleanup failure for precise recovery.
        if before is not None:
            if compose and project:
                profiles.command([*compose, "down", "--volumes", "--remove-orphans"],
                                 env=env)
            profiles.remove_new_images(before, env, project or "", "backend-db")
            after = profiles.inventory(env)
            (run / "docker-after.json").write_text(json.dumps(after), encoding="utf-8")
            profiles.compare_inventory(before, after, project or "unassigned")
            print("Docker inventory unchanged; no new images or build cache", flush=True)
        target = run.resolve()
        root = (profiles.ROOT / ".test_runs" / "runs").resolve()
        if target == root or not target.is_relative_to(root):
            raise RuntimeError(f"Unexpected cleanup path: {target}")
        shutil.rmtree(target)
        print("Temporary run directory removed", flush=True)


if __name__ == "__main__":
    main()
