from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest
from cryptography.fernet import Fernet

from app.core.errors import DomainError
from app.indexer import HttpIndexerMetadataAdapter
from app.schemas.events import EventSearchRequest
from app.services.implementations.opensearch_events import OpenSearchEventQueryProvider

SECRET = Fernet.generate_key().decode("ascii")
STAMP = "2026-01-01T00:00:00Z"


def source(target_type="index", enabled=True):
    return SimpleNamespace(
        id=uuid4(), source_type="external", is_enabled=enabled,
        updated_at=datetime(2026, 1, 1, tzinfo=UTC), target_type=target_type,
        index_name="logs-one" if target_type == "index" else None,
        index_pattern="logs-*" if target_type == "index_pattern" else None,
        data_stream_name="wazuh-findings-v5-security" if target_type == "data_stream" else None,
        data_stream_pattern="wazuh-findings-v5-*" if target_type == "data_stream_pattern" else None,
    )


def connection():
    return SimpleNamespace(
        id=uuid4(), updated_at=datetime(2026, 1, 1, tzinfo=UTC),
        base_url="https://indexer.test:9200", username="reader",
        encrypted_password=Fernet(SECRET.encode()).encrypt(b"secret").decode(), ca_pem=None,
    )


def caps():
    return {"fields": {
        "@timestamp": {"date": {"searchable": True, "aggregatable": True}},
        "event.action": {"keyword": {"searchable": True, "aggregatable": True}},
        "event.count": {"long": {"searchable": True, "aggregatable": True}},
        "event.enabled": {"boolean": {"searchable": True, "aggregatable": True}},
        "source.ip": {"ip": {"searchable": True, "aggregatable": True}},
        "wazuh.agent.name": {"keyword": {"searchable": True, "aggregatable": True}},
        "conflict.field": {
            "keyword": {"searchable": True, "aggregatable": True},
            "long": {"searchable": True, "aggregatable": True},
        },
        "only.unmapped": {"unmapped": {"searchable": False, "aggregatable": False}},
        "message": {"text": {"searchable": True, "aggregatable": False}},
    }}


class Repository:
    def __init__(self, current_source=None, current_connection=None):
        self.source = current_source or source()
        self.connection = current_connection or connection()

    def external_source(self, source_id):
        if source_id != self.source.id:
            return None
        return self.source, self.connection


class Client:
    def __init__(self):
        self.calls = []
        self.page_hits = []
        self.document_calls = []

    def indices(self, *_):
        return ["logs-one", "logs-2026", "wazuh-findings-v5-security"]

    def data_streams(self, *_):
        return ["wazuh-findings-v5-security", "wazuh-findings-v5-audit"]

    def field_caps(self, *_):
        return caps()

    def mappings(self, *args):
        template = {"mappings": {"properties": {
                "@timestamp": {"type": "date"},
                "event": {"properties": {
                    "action": {"type": "keyword"}, "count": {"type": "long"},
                    "enabled": {"type": "boolean"},
                }},
                "source": {"properties": {"ip": {"type": "ip"}}},
                "wazuh": {"properties": {"agent": {"properties": {
                    "name": {"type": "keyword"},
                }}}},
                "conflict": {"properties": {"field": {"type": "keyword"}}},
                "message": {"type": "text"},
            }}}
        if any(target.startswith("wazuh-findings-v5-") for target in args[-1]):
            return {
                ".ds-wazuh-findings-v5-security-2026.01.01-000001": template,
                ".ds-wazuh-findings-v5-audit-2026.01.01-000001": template,
            }
        return {target: template for target in args[-1]}

    def open_pit(self, *args):
        self.calls.append(("open_pit", args[-1]))
        return {"pit_id": "pit-1"}

    def search(self, *args):
        body = args[-1]
        self.calls.append(("search", body))
        hits = self.page_hits.pop(0) if self.page_hits else []
        return {"pit_id": "pit-1", "timed_out": False,
                "_shards": {"failed": 0, "successful": 1},
                "hits": {"hits": hits}}

    def close_pit(self, *args):
        self.calls.append(("close_pit", args[-1]))
        return {"succeeded": True}

    def get_document(self, *args):
        index, doc_id = args[-2:]
        self.document_calls.append((index, doc_id))
        if index == "wazuh-findings-v5-security":
            index = ".ds-wazuh-findings-v5-security-2026.01.01-000001"
        return {"found": True, "_index": index, "_id": doc_id,
                "_source": {"@timestamp": STAMP, "wazuh": {"agent": {"name": doc_id}}}}

    def find_document(self, *args):
        targets, index, doc_id = args[-3:]
        self.document_calls.append((tuple(targets), index, doc_id))
        return {"timed_out": False, "_shards": {"failed": 0},
                "hits": {"hits": [{"_index": index, "_id": doc_id,
                                    "_source": {"@timestamp": STAMP,
                                                "wazuh": {"agent": {"name": doc_id}}}}]}}

    def data_stream_indices(self, *args):
        raise AssertionError("logical stream reads and mappings must not require the admin endpoint")


def provider(target_type="index", enabled=True):
    repository = Repository(source(target_type, enabled))
    client = Client()
    return OpenSearchEventQueryProvider(repository, client, SECRET), repository, client


@pytest.mark.parametrize(
    ("target_type", "expected"),
    [
        ("index", ["logs-one"]),
        ("index_pattern", ["logs-2026", "logs-one"]),
        ("data_stream", ["wazuh-findings-v5-security"]),
        ("data_stream_pattern", ["wazuh-findings-v5-audit", "wazuh-findings-v5-security"]),
    ],
)
def test_fields_resolve_only_registered_target_kind(target_type, expected):
    service, repository, client = provider(target_type)
    page = service.fields(repository.source.id, "wazuh", 20, 0)
    assert client.calls == []
    assert page.total == 1
    assert page.limit == 20
    assert service._targets(repository.source, repository.connection) == expected


def test_catalog_keeps_wcs_and_marks_conflicts_without_operators():
    service, repository, _ = provider()
    page = service.fields(repository.source.id, None, 100, 0)
    fields = {item.name: item for item in page.items}
    assert "wazuh.agent.name" in fields
    assert fields["wazuh.agent.name"].operators == [
        "eq", "in", "contains", "starts_with", "ends_with"
    ]
    assert fields["conflict.field"].type == "conflict"
    assert fields["conflict.field"].operators == []
    assert "only.unmapped" not in fields
    assert fields["message"].type == "text"
    assert fields["message"].operators == []


@pytest.mark.parametrize(
    ("field", "operator", "value", "query_fragment"),
    [
        ("event.action", "eq", "log*?\\x", {"term": {"event.action": "log*?\\x"}}),
        ("event.action", "in", ["login", "logout"], {"terms": {"event.action": ["login", "logout"]}}),
        ("event.action", "contains", "log*?\\x", {"wildcard": {"event.action": {"value": "*log\\*\\?\\\\x*", "case_insensitive": False}}}),
        ("event.action", "starts_with", "log", {"wildcard": {"event.action": {"value": "log*", "case_insensitive": False}}}),
        ("event.action", "ends_with", "out", {"wildcard": {"event.action": {"value": "*out", "case_insensitive": False}}}),
        ("@timestamp", "gte", STAMP, {"range": {"@timestamp": {"gte": "2026-01-01T00:00:00+00:00"}}}),
        ("@timestamp", "eq", STAMP, {"term": {"@timestamp": "2026-01-01T00:00:00+00:00"}}),
        ("event.enabled", "eq", True, {"term": {"event.enabled": True}}),
        ("source.ip", "eq", "192.0.2.1", {"term": {"source.ip": "192.0.2.1"}}),
    ],
)
def test_supported_filters_are_sent_as_literal_indexer_queries(field, operator, value, query_fragment):
    service, repository, client = provider()
    client.page_hits = []
    service.search(repository.source.id, EventSearchRequest(filters=[{
        "field": field, "operator": operator, "value": value,
    }]))
    query = client.calls[1][1]["query"]["bool"]["filter"]
    assert query_fragment in query


def test_rejects_exists_neq_unknown_and_bad_values_before_opening_pit():
    service, repository, client = provider()
    for filter_ in (
        {"field": "event.action", "operator": "exists"},
        {"field": "event.action", "operator": "neq", "value": "x"},
        {"field": "event.count", "operator": "gte", "value": 2},
        {"field": "event.action", "operator": "eq", "value": 7},
        {"field": "conflict.field", "operator": "eq", "value": "x"},
    ):
        with pytest.raises(DomainError) as error:
            service.search(repository.source.id, EventSearchRequest(filters=[filter_]))
        assert error.value.code == "event_filter_invalid"
    assert not any(call[0] == "open_pit" for call in client.calls)


def test_catalog_does_not_offer_case_insensitive_keyword_mapping():
    service, repository, client = provider()

    def normalized_mappings(*args):
        template = {"mappings": {"properties": {
            "@timestamp": {"type": "date"},
            "event": {"properties": {
                "action": {"type": "keyword", "normalizer": "lowercase"},
                "count": {"type": "long"}, "enabled": {"type": "boolean"},
            }},
            "source": {"properties": {"ip": {"type": "ip"}}},
            "wazuh": {"properties": {"agent": {"properties": {
                "name": {"type": "keyword"},
            }}}},
            "conflict": {"properties": {"field": {"type": "keyword"}}},
            "message": {"type": "text"},
        }}}
        return {target: template for target in args[-1]}

    client.mappings = normalized_mappings
    fields = service.fields(repository.source.id, "event.action", 10, 0)
    assert fields.items[0].name == "event.action"
    assert fields.items[0].operators == []


def test_catalog_keeps_keyword_filter_when_ignore_above_exceeds_value_limit():
    service, repository, client = provider()
    original_mappings = client.mappings

    def mappings(*args):
        first = original_mappings(*args)
        second = original_mappings(*args)
        first["logs-one"]["mappings"]["properties"]["event"]["properties"]["action"][
            "ignore_above"
        ] = 1024
        return {"logs-one": first["logs-one"], "logs-two": second["logs-one"]}

    client.mappings = mappings
    fields = service.fields(repository.source.id, "event.action", 10, 0)
    assert fields.items[0].operators == ["eq", "in", "contains", "starts_with", "ends_with"]


@pytest.mark.parametrize("value, expected", [
    (1773078119040, datetime(2026, 3, 9, 17, 41, 59, 40000, tzinfo=UTC)),
    ("2026-03-09T17:21:59.040+05:00", datetime(2026, 3, 9, 12, 21, 59, 40000, tzinfo=UTC)),
])
def test_event_timestamp_accepts_mapping_date_representations(value, expected):
    assert OpenSearchEventQueryProvider._event_timestamp({"@timestamp": value}) == expected


def test_non_searchable_field_caps_disables_operators_and_timestamp_mapping_is_required():
    service, repository, client = provider()
    original_caps = Client.field_caps.__get__(client)

    def non_searchable(*args):
        result = original_caps(*args)
        result["fields"]["event.action"]["keyword"]["non_searchable_indices"] = ["logs-one"]
        return result

    client.field_caps = non_searchable
    fields = service.fields(repository.source.id, "event.action", 10, 0)
    assert fields.items[0].operators == []

    original_mappings = client.mappings

    def missing_timestamp(*args):
        result = original_mappings(*args)
        del result["logs-one"]["mappings"]["properties"]["@timestamp"]
        return result

    client.mappings = missing_timestamp
    with pytest.raises(DomainError) as error:
        service.fields(repository.source.id, None, 10, 0)
    assert error.value.code == "event_timestamp_unavailable"

    client.mappings = original_mappings
    original_caps = Client.field_caps.__get__(client)

    def non_searchable_timestamp(*args):
        result = original_caps(*args)
        result["fields"]["@timestamp"]["date"]["non_searchable_indices"] = ["logs-one"]
        return result

    client.field_caps = non_searchable_timestamp
    with pytest.raises(DomainError) as error:
        service.fields(repository.source.id, None, 10, 0)
    assert error.value.code == "event_timestamp_unavailable"


@pytest.mark.parametrize("target_type", ["index_pattern", "data_stream_pattern"])
def test_cursor_keeps_snapshot_targets_and_field_semantics_when_family_grows(target_type):
    service, repository, client = provider(target_type)
    hit = {"_id": "doc-1", "_index": "logs-one", "_source": {"@timestamp": STAMP},
           "sort": [STAMP, 1]}
    client.page_hits = [[hit, {**hit, "_id": "doc-2", "sort": [STAMP, 2]}], [hit]]
    query = EventSearchRequest(limit=1, filters=[{"field": "event.action", "operator": "eq", "value": "login"}])
    first = service.search(repository.source.id, query)
    assert first.next_cursor is not None and len(first.next_cursor) < 4096
    original_fields = client.field_caps
    original_targets = client.indices if target_type == "index_pattern" else client.data_streams

    def changed_caps(*args):
        result = original_fields(*args)
        result["fields"]["event.action"] = {"long": {"searchable": True}}
        return result

    def grown_targets(*args):
        name = "logs-new-conflicting" if target_type == "index_pattern" else "wazuh-findings-v5-new"
        return original_targets(*args) + [name]

    client.field_caps = changed_caps
    if target_type == "index_pattern":
        client.indices = grown_targets
    else:
        client.data_streams = grown_targets
    second = service.search(repository.source.id, EventSearchRequest(
        limit=1, filters=query.filters, cursor=first.next_cursor,
    ))
    assert second.items
    assert len([call for call in client.calls if call[0] == "open_pit"]) == 1


def test_error_after_opening_pit_closes_snapshot(monkeypatch):
    service, repository, client = provider()
    client.search = lambda *_: (_ for _ in ()).throw(DomainError("indexer_unavailable", "failure", 503))
    with pytest.raises(DomainError):
        service.search(repository.source.id, EventSearchRequest())
    assert client.calls[-1] == ("close_pit", "pit-1")


def test_invalid_source_timestamp_is_skipped_without_losing_cursor_position():
    service, repository, client = provider()
    invalid = {"_id": "bad", "_index": "logs-one",
               "_source": {"@timestamp": "not-a-date"}, "sort": ["bad", 1]}
    valid = {"_id": "good", "_index": "logs-one",
             "_source": {"@timestamp": "2026-01-01T00:00:00+00:00"},
             "sort": [STAMP, 2]}
    client.page_hits = [[invalid, valid], [valid]]
    first = service.search(repository.source.id, EventSearchRequest(limit=1))
    assert first.items == [] and first.has_more and first.next_cursor
    second = service.search(repository.source.id, EventSearchRequest(
        limit=1, cursor=first.next_cursor,
    ))
    assert [item.fields.get("@timestamp") for item in second.items] == [valid["_source"]["@timestamp"]]


def test_expired_cursor_has_explicit_restart_error():
    service, _, _ = provider()
    service.CURSOR_TTL_SECONDS = -1
    token = service._encode_cursor({
        "source": "source", "configuration": "config", "query": "query",
        "pit": "pit", "targets": [], "after": None, "fields": {},
    })
    with pytest.raises(DomainError) as error:
        service._decode_cursor(token)
    assert error.value.code == "event_cursor_expired"


def test_cursor_keeps_pit_order_and_card_id_is_opaque_and_target_bound():
    service, repository, client = provider("data_stream")
    hit = lambda identity, tie: {
        "_id": identity,
        "_index": ".ds-wazuh-findings-v5-security-2026.01.01-000001",
        "_source": {"@timestamp": STAMP, "wazuh": {"agent": {"name": identity}}},
        "sort": [STAMP, tie],
    }
    client.page_hits = [[hit("doc-1", 1), hit("doc-2", 2)], [hit("doc-2", 2)]]
    first = service.search(repository.source.id, EventSearchRequest(limit=1))
    assert first.has_more and first.next_cursor
    assert first.items[0].id != "doc-1"
    assert ".ds-" not in first.items[0].id
    second = service.search(repository.source.id, EventSearchRequest(limit=1, cursor=first.next_cursor))
    assert not second.has_more
    assert client.calls[-1] == ("close_pit", "pit-1")
    body = [call[1] for call in client.calls if call[0] == "search"][1]
    assert body["search_after"] == [STAMP, 1]
    card = service.get(repository.source.id, first.items[0].id)
    assert card.fields["wazuh"]["agent"]["name"] == "doc-1"
    assert card.details == {"index": ".ds-wazuh-findings-v5-security-2026.01.01-000001"}
    assert client.document_calls[-1] == (
        ("wazuh-findings-v5-security",),
        ".ds-wazuh-findings-v5-security-2026.01.01-000001",
        "doc-1",
    )


@pytest.mark.parametrize("target_type", ["data_stream", "data_stream_pattern"])
def test_card_selects_exact_backing_index_after_duplicate_ids_and_rollover(target_type):
    service, repository, client = provider(target_type)
    index = ".ds-wazuh-findings-v5-security-2026.01.01-000003"
    candidates = [
        ".ds-wazuh-findings-v5-security-2026.01.01-000001",
        ".ds-wazuh-findings-v5-security-2026.01.01-000002",
        index,
    ]

    def find_document(*args):
        targets, requested_index, doc_id = args[-3:]
        client.document_calls.append((tuple(targets), requested_index, doc_id))
        hits = [{"_index": candidate, "_id": doc_id,
                 "_source": {"@timestamp": STAMP, "ordinal": ordinal}}
                for ordinal, candidate in enumerate(candidates)]
        selected = [hit for hit in hits if hit["_index"] == requested_index]
        return {"timed_out": False, "_shards": {"failed": 0},
                "hits": {"hits": selected[:1]}}

    client.find_document = find_document
    configuration = service._config_fingerprint(repository.source, repository.connection)
    target = repository.source.data_stream_name or repository.source.data_stream_pattern
    token = service._event_id(repository.source.id, configuration, target, index, "shared-id")
    card = service.get(repository.source.id, token)
    assert card.fields["ordinal"] == 2
    assert client.document_calls[-1][1:] == (index, "shared-id")
    foreign = service._event_id(repository.source.id, configuration, target, "foreign-index", "shared-id")
    with pytest.raises(DomainError) as error:
        service.get(repository.source.id, foreign)
    assert error.value.code == "event_not_found"


def test_stream_card_indexer_query_filters_both_physical_index_and_id():
    adapter = HttpIndexerMetadataAdapter()
    calls = []

    def capture(*args):
        calls.append(args)
        return {"timed_out": False, "_shards": {"failed": 0}, "hits": {"hits": []}}

    adapter._request = capture
    adapter.find_document("https://indexer.test", "reader", "password", None,
                          ["wazuh-findings-v5-security"], ".ds-security-000003", "shared-id")
    assert calls[0][5] == "/wazuh-findings-v5-security/_search"
    assert calls[0][7] == {
        "size": 1,
        "query": {"bool": {"filter": [
            {"ids": {"values": ["shared-id"]}},
            {"term": {"_index": ".ds-security-000003"}},
        ]}},
        "track_total_hits": False,
    }


def test_disabled_source_is_not_read():
    service, repository, client = provider(enabled=False)
    with pytest.raises(DomainError) as error:
        service.fields(repository.source.id, None, 10, 0)
    assert error.value.code == "source_disabled"
    assert client.calls == []


def test_missing_external_credential_key_is_a_structured_error():
    repository = Repository()
    client = Client()
    service = OpenSearchEventQueryProvider(repository, client, None)
    with pytest.raises(DomainError) as error:
        service.fields(repository.source.id, None, 10, 0)
    assert error.value.code == "external_key_unavailable"
