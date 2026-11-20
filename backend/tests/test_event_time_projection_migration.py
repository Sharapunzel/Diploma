"""Backfill original retained rows and keep projection updates atomic."""

from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import MetaData, select, text
from test_event_time_migration import legacy_database  # noqa: F401 - module-local pytest fixture


def test_projection_backfill_preserves_ecs_and_guards_projection_only_updates(request):
    engine, _, migrate = request.getfixturevalue("legacy_database")
    migrate("upgrade", "0021_event_time")
    metadata = MetaData()
    metadata.reflect(engine, schema="logs", only=["parsed_logs"])
    logs = metadata.tables["logs.parsed_logs"]
    cases = [
        ({"@timestamp": "1900-01-01T00:00:00.123456+05:21:10.654321"}, [],
            datetime(1899, 12, 31, 18, 38, 49, 469135, tzinfo=UTC), None),
        ({"@timestamp": "0001-01-01T00:00:00Z"}, [], datetime(1, 1, 1, tzinfo=UTC), None),
        ({"@timestamp": "9999-12-31T23:59:59.999999Z"}, [], datetime.max.replace(tzinfo=UTC), None),
        ({"@timestamp": "2026-02-30T00:00:00Z"}, [], None, "event_time_invalid"),
        ({"@timestamp": "0001-01-01T00:00:00+01:00"}, [], None, "event_time_invalid"),
        ({"@timestamp": "9999-12-31T23:59:59-01:00"}, [], None, "event_time_invalid"),
        ({"@timestamp": True}, [], None, "event_time_invalid"),
        ({"@timestamp": None}, [], None, "event_time_missing"),
        ({}, [], None, "event_time_missing"),
        ({"@timestamp": "2020-01-01T00:00:00Z"}, [{"code": "event_time_fallback"}],
            datetime(2020, 1, 1, tzinfo=UTC), "event_time_fallback"),
    ]
    now, identities = datetime.now(UTC), []
    # These are actual pre-0022 retained rows. Missing processed-record evidence
    # remains missing; backfill does not manufacture durable provenance.
    with engine.begin() as connection:
        for document, diagnostics, _, _ in cases:
            identity = uuid4()
            identities.append(identity)
            connection.execute(logs.insert(), {"id": identity, "normalizer_version": 1,
                "normalizer_name": "legacy", "source_name": "legacy", "connection_name": "legacy",
                "kafka_topic": "synthetic", "kafka_partition": 0, "kafka_offset": 0,
                "deduplication_key": str(identity), "fluent_bit_collected_at": now,
                "backend_received_at": now, "backend_processed_at": now, "raw": "synthetic",
                "ecs_data": document, "normalization_diagnostics": diagnostics})
    migrate("upgrade", "0022_event_time_projection")
    with engine.begin() as connection:
        connection.execute(text("SET LOCAL TimeZone='America/New_York'"))
        for identity, (document, diagnostics, moment, reason) in zip(identities, cases, strict=True):
            row = connection.execute(text("""SELECT ecs_data,normalization_diagnostics,
                processed_record_id,event_time_utc,event_time_reason FROM logs.parsed_logs WHERE id=:id"""),
                {"id": identity}).mappings().one()
            assert row.ecs_data == document and row.normalization_diagnostics == diagnostics
            assert row.processed_record_id is None
            assert (row.event_time_utc.replace(tzinfo=UTC) if row.event_time_utc else None,
                row.event_time_reason) == (moment, reason)
        identity = identities[0]
        connection.execute(text("""UPDATE logs.parsed_logs SET event_time_utc=timestamp '2025-01-01',
            event_time_reason='event_time_missing' WHERE id=:id"""), {"id": identity})
        assert connection.execute(text("SELECT event_time_utc,event_time_reason FROM logs.parsed_logs WHERE id=:id"),
            {"id": identity}).one() == (cases[0][2].replace(tzinfo=None), None)
    # Technical columns can be removed without rewriting ECS or saved results.
    migrate("downgrade", "0021_event_time")
    with engine.connect() as connection:
        assert connection.execute(select(logs.c.ecs_data).where(logs.c.id == identities[0])).scalar_one() == cases[0][0]
    migrate("upgrade", "head")
    migrate("check")
