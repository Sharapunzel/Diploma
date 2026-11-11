"""Full-snapshot source field discovery, budgets, provenance and HTTP permissions."""

import copy
import json
import os
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
import test_aggregate_service as aggregate_cases
from fastapi import Depends
from fastapi.testclient import TestClient
from sqlalchemy import Text, delete, func, select, text

from app.aggregation.field_catalog import FieldCatalogLimits
from app.config import Settings
from app.core.errors import DomainError
from app.core.security import hash_password
from app.db import get_session
from app.dependencies import get_aggregate_service
from app.main import create_app
from app.models import (
    AuthSession,
    AvailabilityMarker,
    Normalizer,
    ParsedLog,
    ProcessedKafkaRecord,
    Role,
    Source,
    User,
)
from app.normalization.compiler import RuleValidationError, compile_rule
from app.normalization.schema import MAX_RULE_BYTES
from app.repositories.sqlalchemy.processing import SqlAlchemyProcessingRepository
from app.services.implementations.aggregate_field_catalog import AggregateFieldCatalogService

service = aggregate_cases.service


@pytest.fixture(scope="module")
def database():
    yield from aggregate_cases.database.__wrapped__()


@pytest.fixture(scope="module")
def catalog():
    return aggregate_cases.catalog.__wrapped__()


@pytest.fixture
def source(database):
    yield from aggregate_cases.source.__wrapped__(database)


def write_events(database, source_id, documents):
    """Use the accepted atomic event/processed-record/marker writer, even with capture active."""
    identities = []
    with database.session_factory.begin() as session:
        origin = session.get(Source, source_id)
        writer = SqlAlchemyProcessingRepository(session)
        for document in documents:
            at = datetime.now(UTC)
            identity = uuid4()
            record = ProcessedKafkaRecord(id=uuid4(), connection_id=origin.connection_id,
                connection_identity=origin.connection_id, source_id=source_id,
                kafka_topic_identity=str(identity), kafka_topic=origin.topic_name,
                kafka_partition=0, kafka_offset=0, result_status="complete",
                backend_received_at=at, backend_processed_at=at)
            event = ParsedLog(id=identity, source_id=source_id, connection_id=origin.connection_id,
                normalizer_version=1, normalizer_name="Synthetic", source_name=origin.name,
                connection_name="Synthetic", kafka_topic=origin.topic_name, kafka_partition=0,
                kafka_offset=0, deduplication_key=str(identity), raw="private raw sentinel",
                fluent_bit_collected_at=at, backend_received_at=at, backend_processed_at=at,
                ecs_data=document)
            assert writer.store_event(record, event)
            identities.append(identity)
    return identities


def normalizer_rule(target="user.name"):
    return {"format_version": 1, "variants": [
        {"key": "auth", "priority": 1, "when": {"kind": "contains", "value": "private sentinel"},
         "blocks": [{"key": "user", "kind": "map_ecs", "target": target,
                     "source": {"literal": "Andrew"}, "required": False}]},
        {"key": "network", "priority": 2, "when": {"kind": "prefix", "value": "web"},
         "blocks": [{"key": "action", "kind": "map_ecs", "target": "event.action",
                     "source": {"literal": "logoff"}, "required": True}]},
    ]}


def assign(database, source_id, rule=None):
    identity = uuid4()
    with database.session_factory.begin() as session:
        session.add(Normalizer(id=identity, name=str(identity), rule=rule or normalizer_rule()))
        session.flush()
        session.get(Source, source_id).normalizer_id = identity
    return identity


def read(database, catalog, source_id, limits=None, **kwargs):
    with database.session_factory() as session:
        api = service(session, catalog)
        if limits is not None:
            api.field_catalog_limits = limits
        result = api.field_catalog(source_id, kwargs.get("q"), kwargs.get("limit", 50), kwargs.get("offset", 0))
        assert not session.in_transaction()
        return result


@contextmanager
def catalog_client(database, catalog, app=None):
    """Own exactly one read-only role/user and all sessions created for it."""
    role, user = uuid4(), uuid4()
    username = user.hex
    try:
        with database.session_factory.begin() as session:
            priority = session.scalar(select(func.max(Role.priority))) + 1
            session.add(Role(id=role, name=str(role), priority=priority, permissions=["aggregates.read"]))
            session.flush()
            session.add(User(id=user, username=username, display_name=username, role_id=role,
                password_hash=hash_password("synthetic-password")))
        app = app or create_app(Settings(environment="test", database_url=os.environ["TEST_DATABASE_URL"]),
            database=database, ecs_catalog=catalog, start_consumers=False)
        with TestClient(app) as client:
            assert client.post("/api/v1/auth/local/login", json={"username": username,
                "password": "synthetic-password"}).status_code == 200
            yield SimpleNamespace(client=client, user_id=user, role_id=role)
    finally:
        # TestClient exits first; delete sessions before their user FK is nulled.
        with database.session_factory.begin() as session:
            session.execute(delete(AuthSession).where(AuthSession.user_id == user))
            session.execute(delete(User).where(User.id == user))
            session.execute(delete(Role).where(Role.id == role))


def auth_identities(database):
    with database.session_factory() as session:
        return {model.__name__: set(session.scalars(select(model.id))) for model in (Role, User, AuthSession)}


@pytest.mark.parametrize("fail_inside", [False, True], ids=["success", "assertion-failure"])
def test_catalog_client_restores_auth_state_even_after_assertion(database, catalog, fail_inside):
    # A second live owner proves cleanup preserves unrelated roles/users/sessions.
    baseline = auth_identities(database)
    with catalog_client(database, catalog):
        before = auth_identities(database)
        def scenario():
            with catalog_client(database, catalog) as owner:
                during = auth_identities(database)
                assert during["Role"] - before["Role"] == {owner.role_id}
                assert during["User"] - before["User"] == {owner.user_id}
                assert len(during["AuthSession"] - before["AuthSession"]) == 1
                if fail_inside:
                    raise AssertionError("synthetic HTTP assertion failure")
                assert owner.client.get("/api/v1/auth/session").status_code == 200
        if fail_inside:
            with pytest.raises(AssertionError, match="synthetic HTTP assertion failure"):
                scenario()
        else:
            scenario()
        assert auth_identities(database) == before
    assert auth_identities(database) == baseline


def test_all_retained_variants_path_segments_and_provenance(database, catalog, source):
    write_events(database, source, [
        {"source": {"bytes": None}, "user": {"name": "Andrew"}, "event": {"category": []}, "labels": {"name": "value"}},
        *[{"network": {"bytes": 1}} for _ in range(60)],
        {"destination": {"ip": "wrong actual type"}, "source.ip": "192.0.2.1",
         "unknown": {"private": "hidden"}, "event": {"original": '{"host":{"name":"not a field"}}'},
         "process": {"Ext": {"token": {"integrity_level_name": "unknown"}}}},
    ])
    normalizer = assign(database, source)
    result = read(database, catalog, source)
    fields = {item["name"]: item for item in result["items"]}
    assert result["observation"] == {"scope": "all_retained_events", "complete": True, "rows": 62}
    assert result["normalizer"] == {"id": str(normalizer), "version": 1}
    assert "source.bytes" in fields and "destination.ip" in fields and "event.category" in fields
    assert "source.ip" not in fields and "host.name" not in fields and "unknown.private" not in fields
    assert fields["user.name"]["provenance"] == ["observed", "normalizer"]
    assert fields["user.name"]["normalizer_origin"] == ["mapping"]
    assert fields["event.action"]["provenance"] == ["normalizer"]
    for name in ("@timestamp", "ecs.version", "event.original"):
        assert "system" in fields[name]["normalizer_origin"]
    assert fields["source.bytes"]["type"] == "long" and fields["destination.ip"]["type"] == "ip"
    assert "private" not in str(result) and "Andrew" not in str(result)
    assert fields["event.category"]["is_array"]
    assert fields["labels"]["reason"] == "unsupported_field_type" and not fields["labels"]["supported"]


def test_empty_no_normalizer_changes_delete_and_hash(database, catalog, source):
    empty = read(database, catalog, source)
    assert empty["items"] == [] and empty["normalizer"] is None
    normalizer = assign(database, source)
    configured = read(database, catalog, source)
    assert configured["items"] and configured["observation"]["rows"] == 0
    assert configured["catalog_hash"] != empty["catalog_hash"]
    ids = write_events(database, source, [{"network": {"bytes": 1}}])
    observed = read(database, catalog, source)
    write_events(database, source, [{"network": {"bytes": 2}}])
    repeated = read(database, catalog, source)
    assert observed["catalog_hash"] == repeated["catalog_hash"]
    with database.session_factory.begin() as session:
        row = session.get(Normalizer, normalizer)
        row.version += 1
    changed = read(database, catalog, source)
    assert changed["catalog_hash"] != repeated["catalog_hash"]
    with database.session_factory.begin() as session:
        session.get(Source, source).normalizer_id = None
        session.execute(delete(AvailabilityMarker).where(AvailabilityMarker.source_identity == source))
        session.execute(delete(ParsedLog).where(ParsedLog.source_id == source))
    assert read(database, catalog, source)["catalog_hash"] == empty["catalog_hash"]
    assert ids


def test_literal_search_pages_hash_and_source_isolation(database, catalog, source):
    write_events(database, source, [{"user": {"name": "A"}, "source": {"ip": "192.0.2.1"}, "network": {"bytes": 2}}])
    result = read(database, catalog, source)
    pages = [read(database, catalog, source, limit=1, offset=i) for i in range(result["total"])]
    assert [page["items"][0]["name"] for page in pages] == sorted(item["name"] for item in result["items"])
    assert all(page["catalog_hash"] == result["catalog_hash"] for page in pages)
    assert read(database, catalog, source, q="USER.NAME")["total"] == 1
    for q in ("%", "_", "\\", "'", " source.ip "):
        assert read(database, catalog, source, q=q)["total"] == 0
    assert read(database, catalog, source, offset=100000)["items"] == []
    with database.session_factory.begin() as session:
        original = session.get(Source, source)
        other = uuid4()
        session.add(Source(id=other, name=str(other), connection_id=original.connection_id,
            topic_name=str(other), is_enabled=True))
    try:
        write_events(database, other, [{"host": {"name": "B"}}])
        assert read(database, catalog, source)["catalog_hash"] == result["catalog_hash"]
        assert "user.name" not in [item["name"] for item in read(database, catalog, other)["items"]]
    finally:
        with database.session_factory.begin() as session:
            session.execute(delete(AvailabilityMarker).where(AvailabilityMarker.source_identity == other))
            session.execute(delete(ParsedLog).where(ParsedLog.source_id == other))
            session.execute(delete(Source).where(Source.id == other))


@pytest.mark.parametrize("factor,value,document", [
    ("events", 1, {"user": {"name": "A"}}),
    ("json_bytes", 1000, {"message": "a" * 10000}),
    ("nodes", 2, {"unknown": {"unknown": 1}}),
    ("depth", 1, {"unknown": {"unknown": 1}}),
    ("paths", 1, {"user": {"name": "A", "id": "B"}}),
])
@pytest.mark.parametrize("query", [{"q": "user.name", "limit": 1}, {"offset": 100000, "limit": 1}])
def test_every_budget_rejects_entire_catalog_before_filtering(database, catalog, source, factor, value, document, query):
    write_events(database, source, [document, document])
    with pytest.raises(DomainError) as error:
        read(database, catalog, source, replace(FieldCatalogLimits(), **{factor: value}), **query)
    assert error.value.code == "field_catalog_budget_exceeded"
    assert error.value.details == {"factor": factor}


@pytest.mark.parametrize("factor,value", [("nodes", 2), ("json_bytes", 20), ("depth", 1), ("paths", 1)])
def test_normalizer_part_obeys_budgets(database, catalog, source, factor, value):
    assign(database, source)
    with pytest.raises(DomainError) as error:
        read(database, catalog, source, replace(FieldCatalogLimits(), **{factor: value}), q="unmatched", limit=1)
    assert error.value.code == "field_catalog_budget_exceeded" and error.value.details["factor"] == factor


@pytest.mark.parametrize("fault", ["schema", "target"])
def test_invalid_assigned_normalizer_never_falls_back_or_leaks(database, catalog, source, fault):
    rule = normalizer_rule("private.secret.target" if fault == "target" else "user.name")
    if fault == "schema":
        rule["private secret"] = "never expose"
    assign(database, source, rule)
    write_events(database, source, [{"user": {"name": "A"}}])
    with pytest.raises(DomainError) as error:
        read(database, catalog, source)
    assert error.value.code == "source_normalizer_invalid"
    assert error.value.status_code == 422 and error.value.details is None
    assert "private" not in str(error.value) and "never expose" not in str(error.value)


def test_snapshot_does_not_mix_concurrent_normalizer_and_event_changes(database, catalog, source):
    normalizer = assign(database, source)
    write_events(database, source, [{"user": {"name": "A"}}])
    original = read(database, catalog, source)
    with database.session_factory() as session:
        api = service(session, catalog)
        preflight = api.repository.catalog_preflight
        def concurrent(*args):
            result = preflight(*args)
            with database.session_factory.begin() as other:
                row = other.get(Normalizer, normalizer)
                changed = copy.deepcopy(row.rule)
                changed["variants"][0]["blocks"][0]["target"] = "host.name"
                row.rule, row.version = changed, row.version + 1
            write_events(database, source, [{"source": {"ip": "192.0.2.1"}}])
            return result
        api.repository.catalog_preflight = concurrent
        snapshot = api.field_catalog(source, None, 50, 0)
    assert snapshot["catalog_hash"] == original["catalog_hash"]
    assert snapshot["observation"]["rows"] == original["observation"]["rows"]
    new = read(database, catalog, source)
    assert new["catalog_hash"] != original["catalog_hash"] and new["normalizer"]["version"] == 2
    assert new["snapshot_at"] > snapshot["snapshot_at"]


def test_sql_timeout_and_total_deadline_rollback(database, catalog, source):
    with database.session_factory() as session:
        api = service(session, catalog)
        original = api.repository.catalog_preflight
        def slow(*args):
            session.execute(select(func.pg_sleep(.1)))
            return original(*args)
        api.repository.catalog_preflight = slow
        api.field_catalog_limits = replace(FieldCatalogLimits(), deadline_seconds=.02)
        with pytest.raises(DomainError) as error:
            api.field_catalog(source, None, 50, 0)
        assert error.value.code == "field_catalog_timeout" and not session.in_transaction()
        assert session.scalar(text("SELECT 1")) == 1


def test_lock_wait_and_expired_scenario_deadline_are_bounded(database, catalog, source):
    with database.session_factory.begin() as holding:
        holding.execute(text("LOCK TABLE app.sources IN ACCESS EXCLUSIVE MODE"))
        with pytest.raises(DomainError) as error:
            read(database, catalog, source, replace(FieldCatalogLimits(), deadline_seconds=.05))
        assert error.value.code == "field_catalog_timeout"
    with pytest.raises(DomainError) as error:
        read(database, catalog, source, replace(FieldCatalogLimits(), deadline_seconds=0))
    assert error.value.code == "field_catalog_timeout"


@pytest.mark.parametrize("documents", [[], [{"message": "event"}], [{"message": "event", "tags": []}]],
    ids=["visited-0", "visited-2", "visited-3"])
def test_deadline_expiring_during_rule_read_prevents_python_traversal(database, catalog, source, monkeypatch, documents):
    from app.services.implementations import aggregate_field_catalog as scenario

    assign(database, source)
    write_events(database, source, documents)
    elapsed = [0.0]
    monkeypatch.setattr(scenario, "time", SimpleNamespace(monotonic=lambda: elapsed[0]))
    class ExpiredRule(dict):
        def values(self):
            raise AssertionError("Traversal must not start after the deadline")
    with database.session_factory() as session:
        api = service(session, catalog)
        original = api.repository.catalog_normalizer_rule
        def late_read(identity):
            raw = ExpiredRule(original(identity))
            elapsed[0] = 11
            return raw
        api.repository.catalog_normalizer_rule = late_read
        with pytest.raises(DomainError) as error:
            api.field_catalog(source, None, 50, 0)
        assert error.value.code == "field_catalog_timeout" and not session.in_transaction()


def test_http_db_nonempty_source_deadline_rejects_before_rule_traversal(database, catalog, source, monkeypatch):
    from app.services.implementations import aggregate_field_catalog as scenario

    assign(database, source)
    write_events(database, source, [{"message": "event"}])
    elapsed, traversed, rolled_back = [0.0], [], []
    monkeypatch.setattr(scenario, "time", SimpleNamespace(monotonic=lambda: elapsed[0]))
    class ExpiredRule(dict):
        def values(self):
            traversed.append(True)
            raise AssertionError("Traversal started after deadline")
    app = create_app(Settings(environment="test", database_url=os.environ["TEST_DATABASE_URL"]),
        database=database, ecs_catalog=catalog, start_consumers=False)
    def dependency(session=Depends(get_session)):
        api = service(session, catalog)
        original = api.repository.catalog_normalizer_rule
        def late_read(identity):
            raw = ExpiredRule(original(identity))
            elapsed[0] = 11
            return raw
        api.repository.catalog_normalizer_rule = late_read
        field_catalog = api.field_catalog
        def checked(*args):
            try:
                return field_catalog(*args)
            finally:
                rolled_back.append(not session.in_transaction())
        api.field_catalog = checked
        return api
    app.dependency_overrides[get_aggregate_service] = dependency
    username = uuid4().hex
    with database.session_factory.begin() as session:
        session.add(User(username=username, display_name=username, password_hash=hash_password("synthetic-password"),
            role_id=UUID("00000000-0000-4000-8000-000000000001")))
    with TestClient(app) as client:
        assert client.post("/api/v1/auth/local/login", json={"username": username, "password": "synthetic-password"}).status_code == 200
        response = client.get(f"/api/v1/aggregates/sources/{source}/field-catalog")
        assert response.status_code == 503 and response.json()["code"] == "field_catalog_timeout"
    assert not traversed and rolled_back == [True]


@pytest.mark.parametrize("kind", ["dict", "list"])
@pytest.mark.parametrize("observed_nodes", [0, 1])
def test_normalizer_node_budget_bounds_sibling_enumeration(catalog, kind, observed_nodes, monkeypatch):
    from app.services.implementations import aggregate_field_catalog as scenario

    enumerated = []
    class TrackedDict(dict):
        def values(self):
            for value in super().values():
                enumerated.append(True)
                yield value
    class TrackedList(list):
        def __iter__(self):
            for value in super().__iter__():
                enumerated.append(True)
                yield value
    raw = TrackedDict({str(i): False for i in range(1000)}) if kind == "dict" else {"children": TrackedList([False] * 1000)}
    closed = []
    @contextmanager
    def snapshot():
        try:
            yield
        finally:
            closed.append(True)
    def forbidden_compile(*args):
        raise AssertionError("Compiler must not run after the node budget is exceeded")
    monkeypatch.setattr(scenario, "compile_rule", forbidden_compile)
    normalizer_id = uuid4()
    repository = SimpleNamespace(catalog_limits=lambda *args: None, now=lambda: datetime.now(UTC),
        source=lambda _: SimpleNamespace(source_type="kafka", normalizer_id=normalizer_id),
        catalog_preflight=lambda *args: (observed_nodes, 0),
        catalog_paths=lambda *args: iter([()] * observed_nodes),
        catalog_normalizer_size=lambda _: (normalizer_id, 1, MAX_RULE_BYTES),
        catalog_normalizer_rule=lambda _: raw)
    api = AggregateFieldCatalogService(repository, SimpleNamespace(read_snapshot=snapshot), catalog,
        replace(FieldCatalogLimits(), nodes=2))
    with pytest.raises(DomainError) as error:
        api.read(uuid4(), None, 1, 100000)
    assert error.value.code == "field_catalog_budget_exceeded" and error.value.details == {"factor": "nodes"}
    # Include at most the next detection node; never eagerly enumerate siblings.
    maximum = (2 if kind == "dict" else 1) - observed_nodes
    assert len(enumerated) <= maximum and closed == [True]


@pytest.mark.parametrize("transport", ["service", "http"])
@pytest.mark.parametrize("reason", ["transport", "compiler"])
def test_oversized_normalizer_uses_transport_and_compiler_metrics(database, catalog, source, transport, reason):
    secret = "PRIVATE_RULE_SENTINEL"
    raw = {"private_config": secret + "x" * (MAX_RULE_BYTES if reason == "compiler" else 100)}
    assign(database, source, raw)
    limits = replace(FieldCatalogLimits(), json_bytes=100) if reason == "transport" else FieldCatalogLimits()
    code = "field_catalog_budget_exceeded" if reason == "transport" else "source_normalizer_invalid"
    details = {"factor": "json_bytes"} if reason == "transport" else None
    if reason == "compiler":
        assert len(json.dumps(raw, ensure_ascii=False, allow_nan=False).encode("utf-8")) > MAX_RULE_BYTES
        with pytest.raises(RuleValidationError, match="size"):
            compile_rule(raw, catalog)
    reads = []
    rolled_back = []
    def configure(api, session):
        original_read = api.repository.catalog_normalizer_rule
        def tracked_read(*args):
            reads.append(True)
            if reason == "transport":
                raise AssertionError("Transport preflight must precede JSON fetch")
            return original_read(*args)
        api.repository.catalog_normalizer_rule = tracked_read
        api.field_catalog_limits = limits
        original = api.field_catalog
        def checked_read(*args, **kwargs):
            try:
                return original(*args, **kwargs)
            finally:
                rolled_back.append(not session.in_transaction())
        api.field_catalog = checked_read
        return api
    if transport == "service":
        with database.session_factory() as session:
            api = configure(service(session, catalog), session)
            with pytest.raises(DomainError) as error:
                api.field_catalog(source, None, 50, 0)
            assert error.value.code == code and error.value.status_code == 422
            assert error.value.details == details and secret not in str(error.value)
            assert not session.in_transaction()
    else:
        app = create_app(Settings(environment="test", database_url=os.environ["TEST_DATABASE_URL"]),
            database=database, ecs_catalog=catalog, start_consumers=False)
        def dependency(session=Depends(get_session)):
            return configure(service(session, catalog), session)
        app.dependency_overrides[get_aggregate_service] = dependency
        with catalog_client(database, catalog, app) as owner:
            response = owner.client.get(f"/api/v1/aggregates/sources/{source}/field-catalog")
            assert response.status_code == 422 and response.json()["code"] == code
            assert response.json().get("details") == details
            for forbidden in (secret, "private_config", "AssertionError", "preflight must", "nesting limits"):
                assert forbidden not in response.text
    assert reads == ([True] if reason == "compiler" else [])
    assert rolled_back == [True]


def test_compact_float_normalizer_with_large_sql_text_has_full_catalog(database, catalog, source):
    targets = ["container.cpu.usage", "container.memory.usage", "event.risk_score", "event.risk_score_norm",
        "gen_ai.request.frequency_penalty", "gen_ai.request.presence_penalty", "gen_ai.request.temperature",
        "gen_ai.request.top_k", "gen_ai.request.top_p", "host.cpu.usage", "host.risk.calculated_score",
        "host.risk.calculated_score_norm"]
    raw = {"format_version": 1, "variants": [
        {"key": f"v{index}", "priority": index, "when": {"kind": "contains", "value": "x"},
         "blocks": [{"key": f"f{number}", "kind": "map_ecs", "target": target,
                     "source": {"literal": 1e-300}, "required": False}
                    for number, target in enumerate(targets)]}
        for index in range(16)]}
    compile_rule(raw, catalog)
    normalizer = assign(database, source, raw)
    with database.session_factory() as session:
        stored, sql_bytes = session.execute(select(Normalizer.rule,
            func.octet_length(Normalizer.rule.cast(Text))).where(Normalizer.id == normalizer)).one()
    python_bytes = len(json.dumps(stored, ensure_ascii=False, allow_nan=False).encode("utf-8"))
    assert python_bytes <= MAX_RULE_BYTES < sql_bytes < FieldCatalogLimits().json_bytes
    compile_rule(stored, catalog)
    expected_names = set(targets) | {"@timestamp", "ecs.version", "event.original"}
    result = read(database, catalog, source)
    with catalog_client(database, catalog) as owner:
        response = owner.client.get(f"/api/v1/aggregates/sources/{source}/field-catalog")
        assert response.status_code == 200
        http = response.json()
    for full in (result, http):
        assert full["total"] == len(expected_names)
        assert {item["name"] for item in full["items"]} == expected_names
        assert full["observation"] == {"scope": "all_retained_events", "complete": True, "rows": 0}
        assert full["normalizer"] == {"id": str(normalizer), "version": 1}
        for item in full["items"]:
            assert item["provenance"] == ["normalizer"]
            assert item["normalizer_origin"] == (["mapping"] if item["name"] in targets else ["system"])
    assert http["items"] == result["items"] and http["catalog_hash"] == result["catalog_hash"]


def test_only_aggregate_read_permission_is_needed_and_errors_are_safe(database, catalog, source):
    normalizer = assign(database, source)
    baseline = auth_identities(database)
    with catalog_client(database, catalog) as owner:
        client = owner.client
        route = f"/api/v1/aggregates/sources/{source}/field-catalog"
        assert client.get(route).status_code == 200
        assert client.get(f"/api/v1/normalizers/{normalizer}").status_code == 403
        assert client.get(f"/api/v1/aggregates/sources/{uuid4()}/field-catalog").status_code == 404
        with database.session_factory.begin() as session:
            row = session.get(Normalizer, normalizer)
            row.rule, row.version = normalizer_rule("private.secret.target"), row.version + 1
        response = client.get(route)
        assert response.status_code == 422 and response.json()["code"] == "source_normalizer_invalid"
        assert "private" not in response.text and "literal" not in response.text and "Andrew" not in response.text
    assert auth_identities(database) == baseline


def test_http_permissions_legacy_catalogs_and_opt_in_metadata(database, catalog, source):
    app = create_app(Settings(environment="test", database_url=os.environ["TEST_DATABASE_URL"]),
        database=database, ecs_catalog=catalog, start_consumers=False)
    def dependency(session=Depends(get_session)):
        return service(session, catalog)
    app.dependency_overrides[get_aggregate_service] = dependency
    names = [uuid4().hex for _ in range(2)]
    with database.session_factory.begin() as session:
        for name, role in zip(names, (1, 2)):
            session.add(User(username=name, display_name=name, password_hash=hash_password("synthetic-password"),
                role_id=UUID(f"00000000-0000-4000-8000-{role:012d}")))
    route = f"/api/v1/aggregates/sources/{source}/field-catalog"
    with TestClient(app) as client:
        assert client.get(route).status_code == 401
        for index, name in enumerate(names):
            assert client.post("/api/v1/auth/local/login", json={"username": name, "password": "synthetic-password"}).status_code == 200
            response = client.get(route)
            assert response.status_code == (200 if index == 0 else 403)
            if index == 0:
                assert response.json()["observation"]["complete"]
                old = client.get("/api/v1/aggregates/operators").json()
                extended = client.get("/api/v1/aggregates/operators?include_extended=true").json()
                assert not {"formula", "aggregate"} & {item["name"] for item in old}
                assert {"formula", "aggregate"} <= {item["name"] for item in extended}
                assert all("allowed_in" not in item and "aggregate_operators" not in item for item in old)
                assert "sum" in next(item for item in extended if item["name"] == "aggregate")["aggregate_operators"]
                fields = client.get(f"/api/v1/aggregates/sources/{source}/fields").json()
                assert fields["total"] > 100 and "provenance" not in fields["items"][0]
                for params in ({"q": "a" * 201}, {"limit": 0}, {"limit": 101}, {"offset": -1}, {"offset": 100001}):
                    assert client.get(route, params=params).status_code == 422
            csrf = client.get("/api/v1/auth/session").json()["csrf_token"]
            assert client.post("/api/v1/auth/logout", headers={"X-CSRF-Token": csrf}).status_code == 200
