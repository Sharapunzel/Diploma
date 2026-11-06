"""Reusable aggregate HTTP/worker smoke in the existing isolated Compose profiles."""

import json
import secrets
import time
from datetime import UTC, datetime, timedelta
from itertools import pairwise
from uuid import uuid4

import httpx


def aggregate_runtime_smoke(compose, env, production, command, browser=None):
    """Exercise the real proxy/API/decoder/worker, including restart and history."""
    username, password = "aggregate-" + uuid4().hex, secrets.token_urlsafe(24)
    setup = """
import json,os
from datetime import UTC,datetime
from uuid import UUID,uuid4
from app.config import settings
from app.db import Database
from app.core.security import hash_password
from app.models import User,Source,KafkaConnection,ReplicationProgress
from sqlalchemy.engine import make_url
assert make_url(settings.database_url).database=='diploma_test'
db=Database(settings.database_url)
with db.session_factory.begin() as s:
 s.add(User(username=os.environ['SMOKE_USER'],password_hash=hash_password(os.environ['SMOKE_PASSWORD']),display_name='Aggregate smoke',role_id=UUID('00000000-0000-4000-8000-000000000001')))
 connection=KafkaConnection(id=uuid4(),name=str(uuid4()),bootstrap_servers=['kafka:9092'],cluster_identity='aggregate-smoke')
 s.add(connection);s.flush()
 sources=[]
 for name in ['authentication','web']:
  source=Source(id=uuid4(),name=name+' '+str(uuid4()),connection_id=connection.id,topic_name=str(uuid4()),is_enabled=False)
  s.add(source);sources.append(str(source.id))
 progress=s.get(ReplicationProgress,1)
 print(json.dumps({'sources':sources,'connection':str(connection.id),'verified_from':progress.availability_verified_from_at.isoformat()}))
db.dispose()
"""
    # argv contains only synthetic credentials for this disposable profile.
    output = command([*compose, "exec", "-T", "-e", f"SMOKE_USER={username}",
        "-e", f"SMOKE_PASSWORD={password}", "api", "python", "-c", setup], env=env, capture=True)
    data = json.loads(output.strip().splitlines()[-1])
    seed = """
import json,os
from datetime import UTC,datetime
from uuid import UUID,uuid4
from sqlalchemy import select
from app.config import settings
from app.db import Database
from app.models import Source,ParsedLog,ProcessedKafkaRecord
from app.repositories.sqlalchemy.processing import SqlAlchemyProcessingRepository
from app.normalization.convert import convert_value
from app.ecs import PackagedEcsCatalog
db=Database(settings.database_url)
catalog=PackagedEcsCatalog.load()
created=convert_value(catalog.get('event.created'),'2000-01-01T05:21:10+05:21:10')
import time
if os.environ.get('SMOKE_START'):
 start=datetime.fromisoformat(os.environ['SMOKE_START'])
 while datetime.now(UTC)<start:
  time.sleep(.01)
offset=int(os.environ.get('SMOKE_OFFSET','1'))
with db.session_factory.begin() as s:
 for index,source_id in enumerate(json.loads(os.environ['SMOKE_SOURCES'])):
  source=s.scalar(select(Source).where(Source.id==UUID(source_id)).with_for_update(read=True))
  now=datetime.now(UTC);rid=uuid4()
  record=ProcessedKafkaRecord(id=rid,connection_id=source.connection_id,connection_identity=source.connection_id,kafka_topic_identity=source_id,source_id=source.id,kafka_topic=source.topic_name,kafka_partition=0,kafka_offset=offset,result_status='complete',backend_received_at=now,backend_processed_at=now)
  ecs={'@timestamp':'2000-01-01T00:00:00Z','source':{'ip':'192.0.2.1'},'cloud':{'entity':{'attributes':{'managed':True}}}}
  ecs.update({'event':{'category':['authentication'],'outcome':'failure'},'user':{'id':'synthetic-user'}} if index==0 else {'event':{'category':['web']},'http':{'request':{'method':'GET'}},'network':{'bytes':100}})
  ecs['event'].update({'created':created,'dataset':'synthetic_'+str(index)})
  ecs['event']['start']=convert_value(catalog.get('event.start'),'0001-01-01T00:00:00Z' if index==0 else '9999-12-31T23:59:59.999999Z')
  ecs['event']['end']=convert_value(catalog.get('event.end'),'0001-01-01T00:00:00+01:00' if index==0 else '9999-12-31T23:59:59-01:00')
  event=ParsedLog(source_id=source.id,connection_id=source.connection_id,normalizer_version=1,normalizer_name='synthetic',source_name=source.name,connection_name='synthetic',kafka_topic=source.topic_name,kafka_partition=0,kafka_offset=offset,deduplication_key=str(rid),fluent_bit_collected_at=now,backend_received_at=now,backend_processed_at=now,raw='synthetic',ecs_data=ecs)
  assert SqlAlchemyProcessingRepository(s).store_event(record,event)
db.dispose()
"""
    port = env["HTTPS_BIND" if production else "PROXY_BIND"].split(":")[1]
    origin = f"{'https' if production else 'http'}://localhost:{port}"
    verified = datetime.fromisoformat(data["verified_from"])
    anchor = datetime.now(UTC).replace(microsecond=0) + timedelta(seconds=4)
    assert anchor > verified
    command([*compose, "exec", "-T", "-e", "SMOKE_SOURCES=" + json.dumps(data["sources"]),
             "-e", "SMOKE_START=" + anchor.isoformat(), "-e", "SMOKE_OFFSET=0",
             "api", "python", "-c", seed], env=env)
    # The fixture is one transaction, so both sources have the same real commit
    # timestamp. Wait for durable WAL materialization before choosing its one-second
    # preview window; process startup/SQL flush time is not a commit-time oracle.
    materialized = """
import json,os,time
from uuid import UUID
from sqlalchemy import select
from app.config import settings
from app.db import Database
from app.models import AvailabilityMarker
sources=[UUID(value) for value in json.loads(os.environ['SMOKE_SOURCES'])]
db=Database(settings.database_url);deadline=time.monotonic()+20
while True:
 with db.session_factory() as s:
  values=s.scalars(select(AvailabilityMarker.availability_at).where(AvailabilityMarker.kind=='event',AvailabilityMarker.source_identity.in_(sources))).all()
 if len(values)==len(sources) and all(value is not None for value in values):
  assert len(set(values))==1, 'Synthetic sources did not share one commit'
  print(values[0].isoformat());break
 assert time.monotonic()<deadline, 'Seed event WAL markers were not materialized'
 time.sleep(.05)
db.dispose()
"""
    committed = command([*compose, "exec", "-T", "-e", "SMOKE_SOURCES=" + json.dumps(data["sources"]),
                         "api", "python", "-c", materialized], env=env, capture=True)
    commit_time = datetime.fromisoformat(committed.strip().splitlines()[-1])
    print(f"Synthetic seed: requested_start={anchor.isoformat()}, materialized_commit={commit_time.isoformat()}", flush=True)
    anchor = commit_time.replace(microsecond=0)
    assert anchor >= verified, "Seed window precedes verified capture history"
    deadline = time.monotonic() + 20
    while datetime.now(UTC) < anchor + timedelta(seconds=1):
        assert time.monotonic() < deadline
        time.sleep(.05)
    def expect(response, code=200):
        if response.status_code != code:
            raise AssertionError(f"Aggregate HTTP {response.status_code}: {response.text[:500]}")
        return response.json()
    with httpx.Client(base_url=origin, verify=env["KAFKA_CA_FILE"] if production else True, timeout=20) as client:
        expect(client.post("/api/v1/auth/local/login", json={"username": username, "password": password}))
        csrf = {"X-CSRF-Token": expect(client.get("/api/v1/auth/session"))["csrf_token"]}
        ids = []
        expected_features = {}
        for index, source in enumerate(data["sources"]):
            rule = {"features": [{"key": "count", "name": "Count", "op": "count"},
                {"key": "unique", "name": "Unique", "op": "distinct_count", "field": "user.id"}]}
            if index:
                rule["features"].append({"key": "bytes", "name": "Bytes", "op": "avg", "field": "network.bytes"})
                rule["where"] = {"op": "all", "args": [
                    {"op": "has_any", "field": "event.category", "value": ["web"]},
                    {"op": "in", "field": "http.request.method", "value": ["GET", "POST"]}]}
            else:
                rule["where"] = {"op": "has_any", "field": "event.category", "value": ["authentication"]}
                rule["features"] += [
                    {"key": "failures", "name": "Failures", "op": "count", "where": {"op": "eq", "field": "event.outcome", "value": "failure"}},
                    {"key": "ratio", "name": "Ratio", "op": "derive", "expr": {"op": "divide", "args": [{"ref": "failures"}, {"ref": "count"}]}}]
            # Extended operations traverse the same validate/preview/create,
            # historical and permanent-worker paths as legacy features.
            rule["where"] = {"op": "all", "args": [rule["where"],
                {"op": "not", "args": [{"op": "eq", "field": "event.outcome", "value": "ignored"}]},
                {"op": "not_in", "field": "event.dataset", "value": ["ignored"]},
                {"op": "starts_with", "field": "event.dataset", "value": "synthetic_"},
                {"op": "in_cidr", "field": "source.ip", "value": "192.0.2.0/24"},
                {"op": "between", "field": "event.created", "value": ["1999-01-01T00:00:00Z", "2001-01-01T00:00:00Z"]},
                {"op": "has_none", "field": "event.category", "value": ["ignored"]}]}
            rule["features"] += [
                {"key": "mode", "name": "Mode", "op": "mode_share", "field": "event.dataset"},
                {"key": "pct", "name": "Percent", "op": "derive", "expr": {"op": "percent", "args": [{"ref": "mode"}]}},
                {"key": "rate", "name": "Rate", "op": "derive", "expr": {"op": "per_second", "args": [{"ref": "count"}]}},
                {"key": "present", "name": "Present", "op": "present_count", "field": "source.ip"},
                {"key": "managed", "name": "Managed", "op": "true_count", "field": "cloud.entity.attributes.managed"},
                {"key": "date_mode", "name": "Date mode", "op": "mode_count", "field": "event.created"},
                {"key": "date_share", "name": "Date share", "op": "mode_share", "field": "event.created"},
                {"key": "boundary_date", "name": "Boundary date", "op": "mode_count", "field": "event.start"},
                {"key": "invalid_date", "name": "Invalid date", "op": "mode_count", "field": "event.end"},
                {"key": "r2", "name": "Ratio", "op": "derive", "expr": {"op": "ratio", "args": [{"ref": "managed"}, {"ref": "count"}]}}]
            if index:
                rule["features"] += [{"key": op, "name": op, "op": op, "field": "network.bytes",
                                      **({"percentile": .75} if op == "percentile" else {})}
                                     for op in ["range", "variance", "stddev", "median", "percentile", "mode_count"]]
            body = {"source_id": source, "aggregate_rule": rule,
                    "schedule": {"boundary": anchor.isoformat(), "timezone": "UTC", "duration_seconds": 1}}
            validated = expect(client.post("/api/v1/aggregates/validate", json=body, headers=csrf))
            preview_body = {**body, "window_start_utc": anchor.isoformat(), "time_basis": "availability_at"}
            preview = expect(client.post("/api/v1/aggregates/preview", json=preview_body, headers=csrf))
            assert preview["completeness"] == "unverified"
            assert preview["event_count"] == 1, (
                f"Aggregate seeded preview mismatch: source_index={index}, "
                f"start={preview['start_utc']}, end={preview['end_utc']}, "
                f"observed={preview['observed_rows']}, selected={preview['event_count']}, "
                f"basis={preview['time_basis']}, quality={preview['completeness']}"
            )
            created = expect(client.post("/api/v1/aggregates", json={**preview_body,
                "name": "Runtime aggregate", "rule_hash": validated["rule_hash"]},
                headers={**csrf, "Idempotency-Key": uuid4().hex}), 201)
            ids.append(created["id"])
            expected_features[created["id"]] = preview["features"]
            historical = expect(client.post(f"/api/v1/aggregates/{created['id']}/historical-points",
                json={"window_start_utc": anchor.isoformat()}, headers=csrf))
            assert preview["features"] == historical["features"]
            assert preview["event_count"] == historical["event_count"] == 1
            assert [f["key"] for f in preview["features"]] == validated["feature_keys"]
            assert historical["kind"] == "historical" and historical["time_basis"] == "availability_at"
            repeated = expect(client.post(f"/api/v1/aggregates/{created['id']}/historical-points",
                json={"window_start_utc": anchor.isoformat()}, headers=csrf))
            assert repeated["id"] == historical["id"]

        command([*compose, "exec", "-T", "-e", "SMOKE_SOURCES=" + json.dumps(data["sources"]),
                 "api", "python", "-c", seed], env=env)
        def working(aggregate_id):
            return [p for p in expect(client.get(f"/api/v1/aggregates/{aggregate_id}/points"))["items"] if p["kind"] == "working"]
        deadline = time.monotonic() + 25
        while not all(any(p["event_count"] == 1 for p in working(aggregate_id)) for aggregate_id in ids):
            assert time.monotonic() < deadline, "Permanent aggregate worker did not produce points"
            time.sleep(.1)
        normal_delays = [(datetime.fromisoformat(p["actual_completed_at"]) -
                          datetime.fromisoformat(p["scheduled_at"])).total_seconds()
                         for aggregate_id in ids for p in working(aggregate_id)]
        assert normal_delays and max(normal_delays) < 1, normal_delays
        print(f"Controlled 1-second windows: worst completion delay={max(normal_delays):.3f}s", flush=True)
        # Keep two workers alive while testing recovery. PostgreSQL serializes
        # schedule ownership; the alternate process uses the same service/image.
        command([*compose, "stop", "aggregate-worker"], env=env)
        before = {aggregate_id: len(working(aggregate_id)) for aggregate_id in ids}
        change_zone = "from app.config import settings; from app.db import Database; from sqlalchemy import text; db=Database(settings.database_url); s=db.session_factory(); s.execute(text(\"ALTER DATABASE diploma_test SET timezone='Asia/Tokyo'\")); s.commit(); s.close(); db.dispose()"
        command([*compose, "exec", "-T", "api", "python", "-c", change_zone], env=env)
        time.sleep(2.2)
        command([*compose, "up", "-d", "--no-deps", "--no-build", "aggregate-worker"], env=env)
        command([*compose, "exec", "-d", "-e", "TZ=Pacific/Honolulu", "aggregate-worker", "python", "-m", "app.aggregate_worker"], env=env)
        command([*compose, "exec", "-T", "-e", "SMOKE_SOURCES=" + json.dumps(data["sources"]),
                 "-e", "SMOKE_OFFSET=2", "api", "python", "-c", seed], env=env)
        deadline = time.monotonic() + 20
        while not all(len(working(aggregate_id)) >= before[aggregate_id] + 2
                      and sum(p["event_count"] == 1 for p in working(aggregate_id)) >= 2 for aggregate_id in ids):
            assert time.monotonic() < deadline, "Worker did not recover missed windows"
            time.sleep(.1)
        for aggregate_id in ids:
            points = working(aggregate_id)
            assert len({p["start_utc"] for p in points}) == len(points)
            chronological = sorted(points, key=lambda p: datetime.fromisoformat(p["start_utc"]))
            assert all(left["end_utc"] == right["start_utc"] for left, right in pairwise(chronological))
            assert all(datetime.fromisoformat(p["end_utc"]) - datetime.fromisoformat(p["start_utc"]) == timedelta(seconds=1)
                       and p["scheduled_at"] == p["end_utc"] for p in points)
            assert all(p["features"][0]["value"] == p["event_count"] and p["completeness"] != "complete" for p in points)
            event_point = next(p for p in points if p["event_count"] == 1)
            assert event_point["features"] == expected_features[aggregate_id]
            assert event_point["event_time_min"].startswith("2000-01-01")
            values = {f["key"]: f for f in event_point["features"]}
            assert values["mode"]["value"] == 1 and values["pct"]["value"] == 100
            assert values["rate"]["value"] == 1 and values["present"]["value"] == 1
            assert values["managed"]["value"] == values["r2"]["value"] == 1
            assert values["date_mode"]["value"] == values["date_share"]["value"] == 1
            assert values["boundary_date"]["value"] == 1
            assert values["invalid_date"] == {"key": "invalid_date", "value": None,
                "reason": "invalid_input_type", "valid_count": 0, "invalid_count": 1}
            assert all(f["reason"] is None for key, f in values.items() if key != "invalid_date")
            if aggregate_id == ids[1]:
                assert [values[op]["value"] for op in ["range", "variance", "stddev", "median", "percentile", "mode_count"]] == [0, 0, 0, 100, 100, 1]
                assert event_point["features"][2]["value"] == 100
            else:
                assert event_point["features"][3]["value"] == 1
            assert any((datetime.fromisoformat(p["actual_completed_at"]) - datetime.fromisoformat(p["scheduled_at"])).total_seconds() < 1 for p in points)
            expect(client.post(f"/api/v1/aggregates/{aggregate_id}/archive", headers=csrf))
            expect(client.post(f"/api/v1/aggregates/{aggregate_id}/resume", headers=csrf))
            assert expect(client.get(f"/api/v1/aggregates/{aggregate_id}"))["skipped_ranges"]
            expect(client.delete(f"/api/v1/aggregates/{aggregate_id}", headers=csrf))
            stable = expect(client.get(f"/api/v1/aggregates/{aggregate_id}/points"))["total"]
            time.sleep(.15)
            assert expect(client.get(f"/api/v1/aggregates/{aggregate_id}/points"))["total"] == stable
        command([*compose, "exec", "-T", "api", "python", "-c", change_zone.replace("Asia/Tokyo", "UTC")], env=env)
        # Keep the same synthetic user, sources, WAL and worker alive for UI compatibility.
        # The enclosing profile owns the database/Compose cleanup even on browser failure.
        if browser is not None:
            browser_ids = browser({"username": username, "password": password,
                                   "sources": data["sources"], "anchor": anchor.isoformat()})
            for identity in browser_ids:
                expect(client.delete(f"/api/v1/aggregates/{identity}", headers=csrf))
            ids.extend(browser_ids)
        cleanup = """
import json,os
from uuid import UUID
from sqlalchemy import delete
from sqlalchemy.engine import make_url
from app.config import settings
from app.db import Database
from app.models import Aggregate,AggregateSchedule,HistoricalAggregatePoint,WorkingWindowPoint,AvailabilityMarker,ParsedLog,ProcessedKafkaRecord,Source,SourceInterruption,KafkaConnection,User
assert make_url(settings.database_url).database=='diploma_test'
owned=json.loads(os.environ['SMOKE_OWNED']);db=Database(settings.database_url)
with db.session_factory.begin() as s:
 for identity in owned['aggregates']:
  aggregate=s.get(Aggregate,UUID(identity));schedule_id=aggregate.schedule_id
  s.execute(delete(HistoricalAggregatePoint).where(HistoricalAggregatePoint.aggregate_id==aggregate.id))
  s.execute(delete(WorkingWindowPoint).where(WorkingWindowPoint.schedule_id==schedule_id))
  s.delete(aggregate);s.flush()
  s.execute(delete(AggregateSchedule).where(AggregateSchedule.id==schedule_id))
 for identity in owned['sources']:
  source_id=UUID(identity)
  s.execute(delete(ParsedLog).where(ParsedLog.source_id==source_id))
  s.execute(delete(ProcessedKafkaRecord).where(ProcessedKafkaRecord.source_id==source_id))
  s.execute(delete(AvailabilityMarker).where(AvailabilityMarker.source_identity==source_id))
  s.execute(delete(SourceInterruption).where(SourceInterruption.source_identity==source_id))
  s.execute(delete(Source).where(Source.id==source_id))
 s.execute(delete(KafkaConnection).where(KafkaConnection.id==UUID(owned['connection'])))
 s.execute(delete(User).where(User.username==owned['username']))
db.dispose()
"""
        owned = {**data, "aggregates": ids, "username": username}
        command([*compose, "exec", "-T", "-e", "SMOKE_OWNED=" + json.dumps(owned),
                 "api", "python", "-c", cleanup], env=env)
    print("Aggregate proxy/API/real WAL/permanent worker smoke passed: two sources, historical barrier, 1-second windows, restart, archive/delete", flush=True)
