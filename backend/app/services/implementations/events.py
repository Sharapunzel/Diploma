from __future__ import annotations

import base64
import hashlib
import ipaddress
import json
import math
from collections.abc import Mapping
from datetime import datetime
from uuid import UUID

from cryptography.fernet import Fernet, InvalidToken

from ...core.errors import DomainError
from ...ecs import EcsCatalog
from ...repositories.protocols.events import EventQueryRepository, EventSourceRepository
from ...schemas.events import (
    EventCard,
    EventFieldDTO,
    EventFieldPage,
    EventSearchItem,
    EventSearchRequest,
    EventSearchResponse,
    KafkaEventCard,
    LocalEventDetails,
    LocalEventMetadata,
)
from ...services.protocols.events import EventQueryProvider


def _event_operators(field) -> tuple[str, ...]:
    if field.type in {"keyword", "constant_keyword", "wildcard", "text", "match_only_text"}:
        return (
            "eq",
            "neq",
            "in",
            "contains",
            "starts_with",
            "ends_with",
            "exists",
            "not_exists",
        )
    if field.type in {"integer", "long", "float", "double", "scaled_float", "short", "byte"}:
        return ("eq", "neq", "in", "gt", "gte", "lt", "lte", "contains", "exists", "not_exists") if field.is_array else (
            "eq", "neq", "in", "gt", "gte", "lt", "lte", "exists", "not_exists"
        )
    if field.type == "date":
        return ("eq", "in", "contains", "exists", "not_exists") if field.is_array else (
            "eq", "in", "gt", "gte", "lt", "lte", "exists", "not_exists"
        )
    if field.type == "boolean":
        return ("eq", "in", "contains", "exists", "not_exists") if field.is_array else (
            "eq", "in", "exists", "not_exists"
        )
    if field.type == "ip":
        return ("eq", "neq", "in", "contains", "exists", "not_exists") if field.is_array else (
            "eq", "neq", "in", "exists", "not_exists"
        )
    return ()


class PostgreSqlEventQueryProvider:
    source_type = "kafka"

    def __init__(self, repository: EventQueryRepository, catalog: EcsCatalog, secret: str):
        self.repository = repository
        self.catalog = catalog
        secret_bytes = secret.encode("utf-8")
        self.ids = Fernet(
            base64.urlsafe_b64encode(hashlib.sha256(secret_bytes + b"events-id").digest())
        )
        self.cursors = Fernet(
            base64.urlsafe_b64encode(hashlib.sha256(secret_bytes + b"events-cursor").digest())
        )

    def fields(self, source_id: UUID, q: str | None, limit: int, offset: int) -> EventFieldPage:
        matches = [
            field
            for field in self.catalog.fields.values()
            if field.filterable and (q is None or q.casefold() in field.name.casefold())
        ]
        matches.sort(key=lambda item: item.name)
        return EventFieldPage(
            items=[
                EventFieldDTO(
                    name=item.name,
                    type=item.type,
                    operators=list(_event_operators(item)),
                )
                for item in matches[offset : offset + limit]
            ],
            total=len(matches),
            limit=limit,
            offset=offset,
        )

    @staticmethod
    def _typed_value(field, operator: str, value):
        if operator in {"exists", "not_exists"}:
            return None
        values = value if operator == "in" else [value]
        if operator == "in" and (not isinstance(value, list) or not 1 <= len(value) <= 20):
            raise ValueError
        converted = []
        for item in values:
            if field.type in {"integer", "long", "short", "byte"}:
                if isinstance(item, bool) or not isinstance(item, int):
                    raise ValueError
            elif field.type in {"float", "double", "scaled_float", "half_float", "unsigned_long"}:
                if (
                    isinstance(item, bool)
                    or not isinstance(item, (int, float))
                    or not math.isfinite(item)
                ):
                    raise ValueError
            elif field.type == "boolean":
                if not isinstance(item, bool):
                    raise ValueError
            elif field.type == "date":
                if not isinstance(item, str):
                    raise ValueError
                item = datetime.fromisoformat(item)
                if item.tzinfo is None or item.utcoffset() is None:
                    raise ValueError
                item = item.isoformat()
            elif field.type == "ip":
                if not isinstance(item, str):
                    raise ValueError
                item = str(ipaddress.ip_address(item))
            else:
                if not isinstance(item, str) or len(item) > 512:
                    raise ValueError
            converted.append(item)
        return converted if operator == "in" else converted[0]

    @staticmethod
    def _fingerprint(source_id: UUID, request: EventSearchRequest) -> str:
        data = request.model_dump(mode="json", exclude={"cursor", "limit"})
        data["filters"] = sorted(data["filters"], key=lambda item: json.dumps(item, sort_keys=True))
        value = json.dumps(
            {"source": str(source_id), "request": data}, sort_keys=True, separators=(",", ":")
        )
        return hashlib.sha256(value.encode()).hexdigest()

    def _opaque_id(self, identity: UUID) -> str:
        return self.ids.encrypt(str(identity).encode("ascii")).decode("ascii")

    def _decode_id(self, value: str) -> UUID:
        try:
            return UUID(self.ids.decrypt(value.encode("ascii"), ttl=None).decode("ascii"))
        except (InvalidToken, TypeError, ValueError, UnicodeError):
            raise DomainError("event_not_found", "Event not found", 404) from None

    def search(self, source_id: UUID, request: EventSearchRequest) -> EventSearchResponse:
        filters = []
        for index, item in enumerate(request.filters):
            field = self.catalog.get(item.field)
            if field is None or not field.filterable:
                raise DomainError(
                    "event_filter_invalid",
                    "Event filter field is unknown or not searchable",
                    422,
                    {"index": index, "reason": "unknown_field"},
                )
            if item.operator not in _event_operators(field):
                raise DomainError(
                    "event_filter_invalid",
                    "Event filter operator is unsupported",
                    422,
                    {"index": index, "reason": "unsupported_operator"},
                )
            try:
                value = self._typed_value(field, item.operator, item.value)
            except (ValueError, TypeError, OverflowError):
                raise DomainError(
                    "event_filter_invalid",
                    "Event filter value has the wrong type",
                    422,
                    {"index": index, "reason": "invalid_value"},
                ) from None
            filters.append((field, item.operator, value))
        fingerprint = self._fingerprint(source_id, request)
        after = None
        boundary = self.repository.snapshot_boundary()
        if request.cursor is not None:
            try:
                payload = json.loads(self.cursors.decrypt(request.cursor.encode("ascii"), ttl=None))
                if (
                    not isinstance(payload, dict)
                    or set(payload) != {"source", "filters", "timestamp", "id", "snapshot"}
                    or payload["source"] != str(source_id)
                    or payload["filters"] != fingerprint
                ):
                    raise ValueError
                timestamp = datetime.fromisoformat(payload["timestamp"])
                boundary = datetime.fromisoformat(payload["snapshot"])
                if timestamp.tzinfo is None or boundary.tzinfo is None:
                    raise ValueError
                after = (timestamp, UUID(payload["id"]))
            except (InvalidToken, TypeError, ValueError, KeyError, UnicodeError):
                raise DomainError("invalid_cursor", "Cursor is invalid", 422) from None
        rows = self.repository.search(
            source_id,
            filters,
            request.timestamp_from,
            request.timestamp_to,
            request.sort,
            after,
            boundary,
            request.limit + 1,
        )
        more = len(rows) > request.limit
        rows = rows[: request.limit]
        items = [
            EventSearchItem(
                source_id=source_id,
                source_type=self.source_type,
                id=self._opaque_id(identity),
                event_timestamp=timestamp,
                fields=fields,
            )
            for identity, timestamp, fields in rows
        ]
        next_cursor = None
        if more and rows:
            identity, timestamp, _ = rows[-1]
            payload = json.dumps(
                {
                    "source": str(source_id),
                    "filters": fingerprint,
                    "timestamp": timestamp.isoformat(),
                    "id": str(identity),
                    "snapshot": boundary.isoformat(),
                },
                separators=(",", ":"),
            )
            next_cursor = self.cursors.encrypt(payload.encode("utf-8")).decode("ascii")
        return EventSearchResponse(items=items, has_more=more, next_cursor=next_cursor)

    def get(self, source_id: UUID, event_id: str) -> EventCard:
        log = self.repository.get(source_id, self._decode_id(event_id))
        if log is None:
            raise DomainError("event_not_found", "Event not found", 404)
        timestamp = log.ecs_data.get("@timestamp")
        try:
            event_time = datetime.fromisoformat(timestamp)
            if event_time.tzinfo is None:
                raise ValueError
        except (AttributeError, TypeError, ValueError):
            raise DomainError("event_not_found", "Event not found", 404) from None
        return KafkaEventCard(
            source_id=source_id,
            source_type="kafka",
            id=event_id,
            event_timestamp=event_time,
            fields=log.ecs_data,
            details=LocalEventDetails(
                raw=log.raw,
                metadata=LocalEventMetadata(
                    connection_id=log.connection_id,
                    normalizer_id=log.normalizer_id,
                    source_name=log.source_name,
                    connection_name=log.connection_name,
                    normalizer_name=log.normalizer_name,
                    normalizer_version=log.normalizer_version,
                    normalization_status=log.normalization_status,
                    normalization_diagnostics=log.normalization_diagnostics,
                    kafka_topic=log.kafka_topic,
                    kafka_partition=log.kafka_partition,
                    kafka_offset=log.kafka_offset,
                    fluent_bit_collected_at=log.fluent_bit_collected_at,
                    backend_received_at=log.backend_received_at,
                    backend_processed_at=log.backend_processed_at,
                    created_at=log.created_at,
                ),
            ),
        )


class EventQueryServiceImpl:
    def __init__(
        self,
        source_repository: EventSourceRepository,
        providers: Mapping[str, EventQueryProvider],
    ):
        self.source_repository = source_repository
        self.providers = dict(providers)

    def _provider(self, source_id: UUID) -> EventQueryProvider:
        source = self.source_repository.source_type(source_id)
        if source is None:
            raise DomainError("source_not_found", "Source not found", 404)
        source_type, is_enabled = source
        provider = self.providers.get(source_type)
        if provider is None:
            if source_type == "external":
                raise DomainError(
                    "event_source_unavailable", "Reading external sources is not implemented", 501
                )
            raise DomainError("event_source_unsupported", "Source type is not supported", 422)
        if source_type == "external" and not is_enabled:
            raise DomainError("source_disabled", "Source is disabled", 409)
        return provider

    def fields(self, source_id: UUID, q: str | None, limit: int, offset: int) -> EventFieldPage:
        return self._provider(source_id).fields(source_id, q, limit, offset)

    def search(self, source_id: UUID, request: EventSearchRequest) -> EventSearchResponse:
        return self._provider(source_id).search(source_id, request)

    def get(self, source_id: UUID, event_id: str) -> EventCard:
        return self._provider(source_id).get(source_id, event_id)
