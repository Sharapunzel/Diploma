from datetime import UTC, datetime
from unittest.mock import Mock
from uuid import uuid4

import pytest
from pydantic import TypeAdapter

from app.core.errors import DomainError
from app.ecs import PackagedEcsCatalog
from app.schemas.events import (
    EventCard,
    EventFieldDTO,
    EventFieldPage,
    EventSearchRequest,
    EventSearchResponse,
    ExternalEventCard,
    ExternalEventSearchItem,
)
from app.schemas.parsed_logs import ParsedLogBulkDeleteRequest
from app.services.implementations.event_deletion import LocalEventDeletionService
from app.services.implementations.events import EventQueryServiceImpl, PostgreSqlEventQueryProvider
from app.services.implementations.opensearch_events import OpenSearchEventQueryProvider


class Repository:
    def __init__(self, kind="kafka", rows=None):
        self.kind = kind
        self.rows = rows or []

    def source_type(self, source_id):
        return (self.kind, False)

    def snapshot_boundary(self):
        return datetime(2026, 1, 1, tzinfo=UTC)

    def search(
        self,
        source_id,
        filters,
        timestamp_from,
        timestamp_to,
        sort,
        after,
        snapshot_boundary,
        limit,
    ):
        self.seen = (
            source_id,
            filters,
            timestamp_from,
            timestamp_to,
            sort,
            after,
            snapshot_boundary,
            limit,
        )
        rows = self.rows
        if after is not None:
            position = next(
                (index for index, row in enumerate(rows) if (row[1], row[0]) == after), -1
            )
            rows = rows[position + 1 :]
        return rows[:limit]

    def get(self, source_id, event_id):
        return None


def test_event_time_deletion_scope_rejects_mixed_or_incomplete_ranges():
    start = datetime(2026, 1, 1, tzinfo=UTC)
    end = datetime(2026, 1, 2, tzinfo=UTC)
    assert ParsedLogBulkDeleteRequest(timestamp_from=start, timestamp_to=end).timestamp_to == end
    for payload in (
        {"timestamp_from": start},
        {"timestamp_from": end, "timestamp_to": start},
        {"timestamp_from": start.replace(tzinfo=None), "timestamp_to": end},
        {"timestamp_from": start, "timestamp_to": end, "processed_from": start},
    ):
        with pytest.raises(ValueError):
            ParsedLogBulkDeleteRequest(**payload)


def test_opaque_event_delete_is_scoped_and_rolls_back(catalog):
    source_id = uuid4()
    event_id = uuid4()
    repository = Mock()
    repository.source_type.return_value = ("kafka", False)
    repository.get.return_value = object()
    provider = PostgreSqlEventQueryProvider(repository, catalog, "unit-test-key")
    logs = Mock()
    work = Mock()
    service = LocalEventDeletionService(repository, repository, provider.ids, logs, work)
    token = provider._opaque_id(event_id)
    service.delete(source_id, token)
    repository.get.assert_called_once_with(source_id, event_id)
    work.commit.assert_called_once()
    with pytest.raises(DomainError) as invalid:
        service.delete(source_id, "not-an-event-id")
    assert invalid.value.status_code == 404
    repository.get.return_value = None
    with pytest.raises(DomainError) as foreign:
        service.delete(uuid4(), token)
    assert foreign.value.status_code == 404
    repository.source_type.return_value = ("external", True)
    with pytest.raises(DomainError) as external:
        service.delete(source_id, token)
    assert external.value.status_code == 404
    repository.source_type.return_value = ("kafka", True)
    repository.get.return_value = object()
    logs.delete.side_effect = RuntimeError("storage failure")
    with pytest.raises(DomainError) as failure:
        service.delete(source_id, token)
    assert failure.value.status_code == 500
    work.rollback.assert_called()
    logs.delete.side_effect = None
    work.commit.side_effect = RuntimeError("commit failed")
    work.rollback.reset_mock()
    with pytest.raises(DomainError) as commit_failure:
        service.delete(source_id, token)
    assert commit_failure.value.status_code == 500
    work.rollback.assert_called_once()


@pytest.fixture
def catalog():
    return PackagedEcsCatalog.load()


def local_service(repository, catalog):
    provider = PostgreSqlEventQueryProvider(repository, catalog, "unit-test-key")
    return EventQueryServiceImpl(repository, {"kafka": provider})


def test_event_search_uses_opaque_ids_and_fingerprint_bound_cursor(catalog):
    source_id = uuid4()
    identities = [uuid4() for _ in range(2)]
    stamp = datetime(2025, 12, 31, tzinfo=UTC)
    repository = Repository(
        rows=[
            (identities[0], stamp, {"@timestamp": stamp.isoformat()}, "complete"),
            (identities[1], stamp, {"@timestamp": stamp.isoformat()}, "partial"),
        ]
    )
    service = PostgreSqlEventQueryProvider(repository, catalog, "unit-test-key")

    result = service.search(source_id, EventSearchRequest(limit=1))
    assert result.has_more is True
    assert result.items[0].normalization_status == "complete"
    assert result.items[0].id != str(identities[0])
    assert identities[0].hex not in result.items[0].id
    assert repository.seen[0] == source_id
    assert repository.seen[-1] == 2

    next_page = service.search(source_id, EventSearchRequest(limit=1, cursor=result.next_cursor))
    assert repository.seen[5] == (stamp, identities[0])
    assert next_page.items[0].id != result.items[0].id
    assert next_page.items[0].normalization_status == "partial"

    tampered = (
        result.next_cursor[:8]
        + ("A" if result.next_cursor[8] != "A" else "B")
        + result.next_cursor[9:]
    )
    with pytest.raises(DomainError) as error:
        service.search(source_id, EventSearchRequest(limit=1, cursor=tampered))
    assert error.value.code == "invalid_cursor"
    with pytest.raises(DomainError) as error:
        service.search(uuid4(), EventSearchRequest(limit=1, cursor=result.next_cursor))
    assert error.value.code == "invalid_cursor"


def test_external_source_fails_explicitly_and_disabled_kafka_remains_readable(catalog):
    external_repository = Repository(kind="external")
    service = EventQueryServiceImpl(
        external_repository,
        {"kafka": PostgreSqlEventQueryProvider(external_repository, catalog, "unit-test-key")},
    )
    with pytest.raises(DomainError) as error:
        service.fields(uuid4(), None, 20, 0)
    assert error.value.status_code == 409
    assert error.value.code == "source_disabled"

    local_repository = Repository(kind="kafka")
    local = local_service(local_repository, catalog)
    assert local.fields(uuid4(), "source.ip", 20, 0).total >= 1




@pytest.mark.parametrize("value", [True, 3.5, "not-an-ip"])
def test_filter_values_are_checked_against_catalog_type(catalog, value):
    source_id = uuid4()
    repository = Repository()
    service = PostgreSqlEventQueryProvider(repository, catalog, "unit-test-key")
    request = EventSearchRequest(filters=[{"field": "source.ip", "operator": "eq", "value": value}])
    with pytest.raises(DomainError) as error:
        service.search(source_id, request)
    assert error.value.code == "event_filter_invalid"


def test_filter_query_operator_and_source_are_validated(catalog):
    source_id = uuid4()
    repository = Repository()
    service = PostgreSqlEventQueryProvider(repository, catalog, "unit-test-key")
    request = EventSearchRequest(
        filters=[{"field": "source.ip", "operator": "eq", "value": "192.0.2.1"}]
    )
    service.search(source_id, request)
    assert repository.seen[1][0][0].name == "source.ip"

    unknown = EventSearchRequest(
        filters=[{"field": "not.a.real.field", "operator": "eq", "value": "x"}]
    )
    with pytest.raises(DomainError) as error:
        service.search(source_id, unknown)
    assert error.value.status_code == 422


def test_external_wcs_provider_uses_common_contract_and_opaque_external_id():
    source_id = uuid4()
    timestamp = datetime(2026, 1, 1, tzinfo=UTC)

    class ExternalRepository:
        def source_type(self, requested_source):
            assert requested_source == source_id
            return ("external", True)

    class WazuhProvider:
        source_type = "external"

        def fields(self, requested_source, q, limit, offset):
            return EventFieldPage(
                items=[EventFieldDTO(name="wazuh.agent.name", type="keyword", operators=["eq"])],
                total=1,
                limit=limit,
                offset=offset,
            )

        def search(self, requested_source, request):
            return EventSearchResponse(
                items=[ExternalEventSearchItem(
                    source_id=requested_source,
                    source_type="external",
                    id="wazuh-findings-v5-security:document-42",
                    event_timestamp=timestamp,
                    fields={"@timestamp": timestamp.isoformat(), "wazuh": {"agent": {"name": "node-1"}}},
                )],
                has_more=False,
                next_cursor=None,
            )

        def get(self, requested_source, event_id):
            return ExternalEventCard(
                source_id=requested_source,
                source_type="external",
                id=event_id,
                event_timestamp=timestamp,
                fields={"@timestamp": timestamp.isoformat(), "wazuh": {"agent": {"name": "node-1"}}},
            )

    service = EventQueryServiceImpl(ExternalRepository(), {"external": WazuhProvider()})
    page = service.fields(source_id, "wazuh", 10, 0)
    assert page.items[0].name == "wazuh.agent.name"
    result = service.search(source_id, EventSearchRequest())
    assert result.items[0].id == "wazuh-findings-v5-security:document-42"
    assert "normalization_status" not in result.model_dump()["items"][0]
    card = service.get(source_id, result.items[0].id)
    assert card.id == result.items[0].id
    assert card.fields["wazuh"]["agent"]["name"] == "node-1"
    response_card = TypeAdapter(EventCard).validate_python(card.model_dump(mode="json"))
    assert response_card.source_type == "external"


def test_external_date_filters_follow_index_mapping_format():
    assert OpenSearchEventQueryProvider._typed_value(
        "date", "eq", 1_760_000_000_000, "epoch_millis"
    ) == 1_760_000_000_000
    with pytest.raises(ValueError):
        OpenSearchEventQueryProvider._typed_value(
            "date", "eq", "2026-01-01T00:00:00Z", "epoch_millis"
        )
    assert OpenSearchEventQueryProvider._typed_value(
        "date", "eq", "2026-01-01T00:00:00Z", "strict_date_optional_time"
    ) == "2026-01-01T00:00:00+00:00"
