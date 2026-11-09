"""Full Compose up/down/up regression shared by development and production smoke."""

import hashlib
import json
from pathlib import Path

SNAPSHOT = """
import json
from sqlalchemy import create_engine,text
from app.config import settings
engine=create_engine(settings.database_url)
with engine.connect() as db:
 assert db.execute(text('SELECT current_database()')).scalar_one()=='diploma_test'
 slot=db.execute(text(\"\"\"SELECT slot_name,plugin,database,
  pg_wal_lsn_diff(restart_lsn,'0/0'),pg_wal_lsn_diff(confirmed_flush_lsn,'0/0')
  FROM pg_replication_slots WHERE slot_name='diploma_availability'\"\"\")).one()
 progress=db.execute(text('SELECT durable_lsn,legacy_cutover_at,availability_verified_from_at,boundary_barrier_id,boundary_origin FROM logs.replication_progress WHERE id=1')).one()
 facts={}
 for table in ['logs.parsed_logs','logs.working_window_points']:
  facts[table]=dict(db.execute(text('SELECT id::text,row_to_json(t)::text FROM '+table+' t')).all())
 facts['marker_identity']=dict(db.execute(text("SELECT id::text,jsonb_build_array(source_identity,kind,event_id,created_at)::text FROM logs.availability_markers")).all())
 facts['materialized_markers']=dict(db.execute(text("SELECT id::text,availability_at::text FROM logs.availability_markers WHERE availability_at IS NOT NULL")).all())
 print(json.dumps({'slot':list(slot),'progress':list(progress),'facts':facts},default=str))
engine.dispose()
"""


def capture_lifecycle(compose, env, production, command, smoke_http, record_volumes):
    def snapshot():
        output = command([*compose, "exec", "-T", "api", "python", "-c", SNAPSHOT], env=env, capture=True)
        return json.loads(output.strip().splitlines()[-1])

    def check_running():
        # Compose emits one JSON object per line on current Docker Desktop.
        output = command([*compose, "ps", "--all", "--format", "json"], env=env, capture=True)
        rows = json.loads(output) if output.lstrip().startswith("[") else [json.loads(line) for line in output.splitlines() if line.strip()]
        states = {row["Service"]: row for row in rows}
        for service in ("migrate", "wal-bootstrap"):
            assert states[service]["State"] == "exited" and states[service]["ExitCode"] == 0, service
        for service in ("api", "aggregate-worker", "wal-decoder"):
            assert states[service]["State"] == "running", service
        command([*compose, "exec", "-T", "wal-decoder", "python", "-m", "app.wal_health"], env=env)
        smoke_http(env, production)

    def secret_snapshot():
        run = Path(env["TEMP"]).resolve()
        return {name: (hashlib.sha256(Path(value).read_bytes()).hexdigest(), Path(value).stat().st_mode)
                for name, value in env.items() if name.endswith("_FILE")
                and Path(value).resolve().is_relative_to(run) and Path(value).is_file()}

    def volumes():
        output = command(["docker", "volume", "ls", "--filter",
                          "label=com.docker.compose.project=" + env["COMPOSE_PROJECT_NAME"],
                          "--format", "{{.Name}}"], env=env, capture=True)
        names = output.splitlines()
        return command(["docker", "volume", "inspect", *sorted(names)], env=env, capture=True)

    check_running()
    record_volumes()
    before, secrets, saved_volumes = snapshot(), secret_snapshot(), volumes()
    assert all(before["facts"].values()), "Lifecycle requires seeded events and working points"
    for cycle in ("running-up", "down-up"):
        if cycle == "down-up":
            command([*compose, "down"], env=env)
            assert volumes() == saved_volumes, "Ordinary down removed persistent volumes"
        command([*compose, "up", "--build", "-d"], env=env)
        command([*compose, "up", "-d", "--wait"], env=env)
        check_running()
        record_volumes()
        after = snapshot()
        assert after["slot"][:3] == before["slot"][:3]
        assert all(int(after["slot"][i]) >= int(before["slot"][i]) for i in (3, 4))
        assert after["progress"][1:] == before["progress"][1:]
        assert int(after["progress"][0]) >= int(before["progress"][0])
        for table, facts in before["facts"].items():
            assert all(after["facts"][table].get(key) == value for key, value in facts.items()), table
        assert secret_snapshot() == secrets and volumes() == saved_volumes
        before = after
    print("Full Compose lifecycle passed: running up and up/down/up preserved volumes, secrets, capture boundaries, LSN progress, events and points", flush=True)
