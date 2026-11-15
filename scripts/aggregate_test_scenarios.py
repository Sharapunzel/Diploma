"""Reusable aggregate HTTP/worker smoke in the existing isolated Compose profiles."""

import json
import secrets
import time
from datetime import UTC, datetime, timedelta
from uuid import uuid4


def aggregate_runtime_smoke(compose, env, production, command, browser=None, lifecycle=None, prepare_only=False):
    """Retained preparation for the TASK-41 browser fixture, without calculation."""
    if not prepare_only:
        raise ValueError("Use event_time_test_scenarios.aggregate_event_time_smoke")
    username, password = "aggregate-" + uuid4().hex, secrets.token_urlsafe(24)
    setup = """
import json,os
from datetime import UTC,datetime
from uuid import UUID,uuid4
from app.config import settings
from app.db import Database
from app.core.security import hash_password
from app.models import User,Source,KafkaConnection,ReplicationProgress,Normalizer
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
 empty_source=None
 normalizer_id=None
 if os.environ.get('SMOKE_CATALOG')=='1':
  from app.normalization.compiler import compile_rule
  rule={'format_version':1,'variants':[{'key':'auth','priority':1,'when':{'kind':'contains','value':'synthetic'},'blocks':[{'key':'user','kind':'map_ecs','target':'user.name','source':{'literal':'Andrew'},'required':False},{'key':'action','kind':'map_ecs','target':'event.action','source':{'literal':'logon'},'required':False}]}]}
  from app.ecs import PackagedEcsCatalog
  compile_rule(rule,PackagedEcsCatalog.load())
  normalizer=Normalizer(id=uuid4(),name='Aggregate browser normalizer',rule=rule)
  s.add(normalizer);s.flush()
  normalizer_id=str(normalizer.id)
  for identity in sources:
   s.get(Source,UUID(identity)).normalizer_id=normalizer.id
  empty=Source(id=uuid4(),name='No events '+str(uuid4()),connection_id=connection.id,topic_name=str(uuid4()),is_enabled=False,normalizer_id=normalizer.id)
  s.add(empty);empty_source=str(empty.id)
 progress=s.get(ReplicationProgress,1)
 print(json.dumps({'sources':sources,'empty_source':empty_source,'normalizer':normalizer_id,'connection':str(connection.id),'verified_from':progress.availability_verified_from_at.isoformat()}))
db.dispose()
"""
    # argv contains only synthetic credentials for this disposable profile.
    output = command([*compose, "exec", "-T", "-e", f"SMOKE_USER={username}", "-e", f"SMOKE_CATALOG={int(prepare_only or browser is not None)}",
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
  ecs={'@timestamp':'2000-01-01T00:00:00Z','source':{'ip':'192.0.2.1','bytes':120},'destination':{'bytes':60},'cloud':{'entity':{'attributes':{'managed':True}}}}
  ecs.update({'event':{'category':['authentication'],'outcome':'failure'},'user':{'id':'synthetic-user'}} if index==0 else {'event':{'category':['web']},'http':{'request':{'method':'GET'}},'network':{'bytes':100}})
  ecs['event'].update({'created':created,'dataset':'synthetic_'+str(index),'action':'logon' if index==0 else 'request'})
  ecs.setdefault('user',{})['name']='Andrew'
  ecs['event']['start']=convert_value(catalog.get('event.start'),'0001-01-01T00:00:00Z' if index==0 else '9999-12-31T23:59:59.999999Z')
  ecs['event']['end']=convert_value(catalog.get('event.end'),'0001-01-01T00:00:00+01:00' if index==0 else '9999-12-31T23:59:59-01:00')
  event=ParsedLog(source_id=source.id,connection_id=source.connection_id,normalizer_version=1,normalizer_name='synthetic',source_name=source.name,connection_name='synthetic',kafka_topic=source.topic_name,kafka_partition=0,kafka_offset=offset,deduplication_key=str(rid),fluent_bit_collected_at=now,backend_received_at=now,backend_processed_at=now,raw='synthetic',ecs_data=ecs)
  assert SqlAlchemyProcessingRepository(s).store_event(record,event)
db.dispose()
"""
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
    if prepare_only:
        return {"username": username, "password": password, "sources": data["sources"], "empty_source":data["empty_source"], "anchor": anchor.isoformat()}
