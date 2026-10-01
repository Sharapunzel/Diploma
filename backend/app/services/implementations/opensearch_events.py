from __future__ import annotations

import base64
import hashlib
import ipaddress
import json
import math
import time
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from cryptography.fernet import Fernet, InvalidToken

from ...core.errors import DomainError
from ...indexer import IndexerError, matching_indices
from ...models import ExternalConnection, Source
from ...schemas.events import (
    EventFieldDTO,
    EventFieldPage,
    EventSearchItem,
    EventSearchRequest,
    EventSearchResponse,
    ExternalEventCard,
)

_KEYWORD = {"keyword", "constant_keyword", "wildcard"}
_INTEGER = {"integer", "long", "short", "byte"}
_FLOAT = {"float", "double", "scaled_float", "half_float", "unsigned_long"}
_DATE = {"date", "date_nanos"}
_RANGE = {"gt": "gt", "gte": "gte", "lt": "lt", "lte": "lte"}


def _failure(code: str, message: str, status: int, details: dict | None = None):
    raise DomainError(code, message, status, details)


def _operators(field_name: str, field_type: str) -> tuple[str, ...]:
    # OpenSearch mappings do not distinguish scalar values from arrays. These operators
    # have the same any-member behavior as the shared event contract for both shapes.
    if field_type in _KEYWORD:
        return ("eq", "in", "contains", "starts_with", "ends_with")
    if field_type in _INTEGER | _FLOAT:
        return ("eq", "in")
    if field_type in _DATE:
        return ("eq", "in", "gt", "gte", "lt", "lte") if field_name == "@timestamp" else ("eq", "in")
    if field_type == "boolean":
        return ("eq", "in")
    if field_type == "ip":
        return ("eq", "in")
    return ()


class OpenSearchEventQueryProvider:
    """Read-only OpenSearch event provider for the shared event API."""

    source_type = "external"
    CURSOR_TTL_SECONDS = 60
    PIT_KEEP_ALIVE = "2m"
    EVENT_ID_TTL_SECONDS = 86_400
    MAX_TARGETS = 200
    MAX_HITS = 101

    def __init__(self, repository, client, secret: str | None, signing_secret: str | None = None):
        self.repository = repository
        self.client = client
        self.secret = secret
        key = (signing_secret or secret or "development-only-change-me-please-32-bytes").encode("utf-8")
        self._ids = Fernet(base64.urlsafe_b64encode(hashlib.sha256(key + b"external-event-id").digest()))
        self._cursors = Fernet(base64.urlsafe_b64encode(hashlib.sha256(key + b"external-event-cursor").digest()))

    def _connection(self, source_id: UUID) -> tuple[Source, ExternalConnection]:
        row = self.repository.external_source(source_id)
        if row is None:
            _failure("source_not_found", "Source not found", 404)
        source, connection = row
        if source.source_type != "external":
            _failure("event_source_unsupported", "Source type is not supported", 422)
        if not source.is_enabled:
            _failure("source_disabled", "Source is disabled", 409)
        return source, connection

    @staticmethod
    def _target_spec(source: Source) -> tuple[bool, str, str | None]:
        if source.target_type == "index":
            if not source.index_name:
                _failure("event_source_unavailable", "External source target is invalid", 409)
            return False, source.index_name, None
        if source.target_type == "index_pattern":
            if not source.index_pattern:
                _failure("event_source_unavailable", "External source target is invalid", 409)
            return False, None, source.index_pattern
        if source.target_type == "data_stream":
            if not source.data_stream_name:
                _failure("event_source_unavailable", "External source target is invalid", 409)
            return True, source.data_stream_name, None
        if source.target_type == "data_stream_pattern":
            if not source.data_stream_pattern:
                _failure("event_source_unavailable", "External source target is invalid", 409)
            return True, None, source.data_stream_pattern
        _failure("event_source_unavailable", "External source target is invalid", 409)

    @staticmethod
    def _config_fingerprint(source: Source, connection: ExternalConnection) -> str:
        value = {
            "source": str(source.id),
            "source_updated": source.updated_at.isoformat(),
            "connection": str(connection.id),
            "connection_updated": connection.updated_at.isoformat(),
            "url": connection.base_url,
            "username": connection.username,
            "credential": connection.encrypted_password,
            "ca": connection.ca_pem,
            "target_type": source.target_type,
            "index": source.index_name,
            "index_pattern": source.index_pattern,
            "stream": source.data_stream_name,
            "stream_pattern": source.data_stream_pattern,
        }
        return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()

    def _credentials(self, connection: ExternalConnection) -> tuple[str, str, str | None]:
        # The same key material used by TASK-10's ExternalServiceImpl.
        secret = getattr(self, "secret", None)
        if secret is None:
            _failure("external_key_unavailable", "External encryption key is unavailable", 503)
        try:
            password = Fernet(secret.encode("ascii")).decrypt(
                connection.encrypted_password.encode("ascii")
            ).decode("utf-8")
        except (InvalidToken, ValueError, UnicodeError):
            _failure("external_key_mismatch", "External credential cannot be decrypted", 503)
        return password, connection.base_url, connection.ca_pem

    def _call(self, method: str, connection: ExternalConnection, *args):
        try:
            password, base_url, ca_pem = self._credentials(connection)
            return getattr(self.client, method)(
                base_url, connection.username, password, ca_pem, *args
            )
        except IndexerError as error:
            kinds = {
                "access_denied": ("indexer_access_denied", "Indexer authentication or permissions failed", 403),
                "untrusted_certificate": ("indexer_untrusted_certificate", "Indexer certificate is not trusted", 502),
                "hostname_mismatch": ("indexer_hostname_mismatch", "Indexer certificate hostname does not match", 502),
                "timeout": ("indexer_timeout", "Indexer request timed out", 504),
                "unavailable": ("indexer_unavailable", "Indexer is unavailable", 503),
                "response_too_large": ("indexer_response_too_large", "Indexer response exceeds limit", 502),
                "not_found": ("indexer_target_not_found", "Selected indexer target or document was not found", 404),
            }
            code, message, status = kinds.get(
                error.kind, ("indexer_invalid_response", "Invalid indexer response", 502)
            )
            _failure(code, message, status)

    def _targets(self, source: Source, connection: ExternalConnection) -> list[str]:
        is_stream, name, pattern = self._target_spec(source)
        available = self._call("data_streams" if is_stream else "indices", connection)
        targets = matching_indices(available, name, pattern)
        if not targets:
            _failure("indexer_target_not_found", "Selected source target is unavailable", 404)
        if len(targets) > self.MAX_TARGETS:
            _failure("indexer_target_too_broad", "Selected target matches too many indexes", 422)
        return sorted(targets)

    def _field_map(self, source: Source, connection: ExternalConnection, targets: list[str]):
        result = self._call("field_caps", connection, targets)
        if not isinstance(result, dict) or not isinstance(result.get("fields"), dict):
            _failure("indexer_invalid_response", "Indexer returned invalid field metadata", 502)
        mappings = self._call("mappings", connection, targets)
        if not isinstance(mappings, dict):
            _failure("indexer_invalid_response", "Indexer returned invalid mapping metadata", 502)
        definitions: dict[str, list[dict]] = {}

        def collect(properties, prefix=""):
            if not isinstance(properties, dict):
                return
            for part, mapping in properties.items():
                if not isinstance(part, str) or not isinstance(mapping, dict):
                    continue
                name = f"{prefix}.{part}" if prefix else part
                if isinstance(mapping.get("type"), str):
                    definitions.setdefault(name, []).append(mapping)
                collect(mapping.get("properties"), name)
                collect(mapping.get("fields"), name)

        # A data-stream mapping response is keyed by its physical backing indices.
        # Use those returned names directly; some least-privilege roles can inspect
        # mappings but cannot call the separate data-stream administration endpoint.
        mapping_targets = sorted(mappings) if self._target_spec(source)[0] else list(targets)
        for index_mapping in mappings.values():
            if not isinstance(index_mapping, dict):
                _failure("indexer_invalid_response", "Indexer returned invalid mapping metadata", 502)
            mapping_body = index_mapping.get("mappings", {})
            if not isinstance(mapping_body, dict):
                _failure("indexer_invalid_response", "Indexer returned invalid mapping metadata", 502)
            collect(mapping_body.get("properties"))

        target_fields: dict[str, dict[str, dict]] = {}

        def collect_target(properties, prefix="", output=None):
            output = output if output is not None else {}
            if not isinstance(properties, dict):
                return output
            for part, mapping in properties.items():
                if not isinstance(part, str) or not isinstance(mapping, dict):
                    continue
                name = f"{prefix}.{part}" if prefix else part
                if isinstance(mapping.get("type"), str):
                    output[name] = mapping
                collect_target(mapping.get("properties"), name, output)
                collect_target(mapping.get("fields"), name, output)
            return output

        for target, index_mapping in mappings.items():
            if not isinstance(index_mapping, dict):
                _failure("indexer_invalid_response", "Indexer returned invalid mapping metadata", 502)
            mapping_body = index_mapping.get("mappings", {})
            if not isinstance(mapping_body, dict):
                _failure("indexer_invalid_response", "Indexer returned invalid mapping metadata", 502)
            target_fields[target] = collect_target(mapping_body.get("properties"))

        output: dict[str, tuple[str, tuple[str, ...], dict]] = {}
        timestamp_caps = result["fields"].get("@timestamp", {})
        timestamp_types = [kind for kind in timestamp_caps if kind != "unmapped"]
        timestamp_mappings = [target_fields.get(target, {}).get("@timestamp", {})
                              for target in mapping_targets]
        timestamp_formats = [item.get("format", "strict_date_optional_time||epoch_millis")
                             for item in timestamp_mappings]
        if (len(timestamp_types) != 1 or timestamp_types[0] not in _DATE
                or not isinstance(timestamp_caps[timestamp_types[0]], dict)
                or timestamp_caps[timestamp_types[0]].get("searchable") is not True
                or bool(timestamp_caps[timestamp_types[0]].get("non_searchable_indices"))
                or not timestamp_mappings
                or any(item.get("type") not in _DATE for item in timestamp_mappings)
                or any(not isinstance(date_format, str)
                       or not any(part in date_format for part in ("strict_date_optional_time", "date_optional_time"))
                       for date_format in timestamp_formats)
                or len(set(timestamp_formats)) != 1):
            _failure("event_timestamp_unavailable", "Selected targets do not have a compatible searchable @timestamp", 422)
        for name, typed_caps in result["fields"].items():
            if not isinstance(name, str) or not isinstance(typed_caps, dict):
                continue
            caps = [(kind, data) for kind, data in typed_caps.items() if kind != "unmapped"]
            if not caps:
                continue
            types = {kind for kind, _ in caps}
            if len(types) != 1:
                output[name] = ("conflict", ())
                continue
            field_type = next(iter(types))
            operators = _operators(name, field_type)
            if any(not isinstance(data, dict) or data.get("searchable") is not True
                   or bool(data.get("non_searchable_indices")) for _, data in caps):
                operators = ()
            field_definitions = definitions.get(name, [])
            if not field_definitions:
                operators = ()
            else:
                signatures = set()
                for definition in field_definitions:
                    ignore_above = definition.get("ignore_above")
                    if field_type in _KEYWORD:
                        ignore_above_signature = (
                            "too_small"
                            if isinstance(ignore_above, int) and ignore_above < 128
                            else "supports_query_limit"
                        )
                    else:
                        ignore_above_signature = ignore_above
                    signature = {
                        "normalizer": definition.get("normalizer"),
                        "ignore_malformed": definition.get("ignore_malformed", False),
                        "null_value": definition.get("null_value", "__unset__"),
                        "ignore_above": ignore_above_signature,
                        "format": definition.get("format"),
                    }
                    signatures.add(json.dumps(signature, sort_keys=True, default=str))
                    if (
                        definition.get("ignore_malformed") is True
                        or "null_value" in definition
                        or (field_type in _KEYWORD and definition.get("normalizer") is not None)
                        or (field_type in _KEYWORD and ignore_above_signature == "too_small")
                    ):
                        operators = ()
                    if field_type in _DATE and name == "@timestamp":
                        date_format = definition.get("format", "strict_date_optional_time||epoch_millis")
                        if not isinstance(date_format, str) or not any(
                            allowed in date_format for allowed in ("strict_date_optional_time", "date_optional_time")
                        ):
                            operators = ()
                if len(signatures) > 1:
                    operators = ()
            timestamp_definition = next((target_fields[target][name] for target in mapping_targets
                                         if name in target_fields.get(target, {})), {})
            output[name] = (field_type, operators, timestamp_definition)
        return output

    def fields(self, source_id: UUID, q: str | None, limit: int, offset: int) -> EventFieldPage:
        source, connection = self._connection(source_id)
        targets = self._targets(source, connection)
        metadata = self._field_map(source, connection, targets)
        matches = [name for name in metadata if q is None or q.casefold() in name.casefold()]
        matches.sort()
        return EventFieldPage(
            items=[EventFieldDTO(name=name, type=metadata[name][0], operators=list(metadata[name][1]))
                   for name in matches[offset:offset + limit]],
            total=len(matches), limit=limit, offset=offset,
        )

    @staticmethod
    def _typed_value(field_type: str, operator: str, value: Any, date_format: str | None = None):
        if operator == "in":
            if not isinstance(value, list) or not 1 <= len(value) <= 20:
                raise ValueError
            values = value
        else:
            values = [value]
        converted = []
        for item in values:
            if field_type in _INTEGER:
                if isinstance(item, bool) or not isinstance(item, int):
                    raise ValueError
            elif field_type in _FLOAT:
                if isinstance(item, bool) or not isinstance(item, (int, float)) or not math.isfinite(item):
                    raise ValueError
            elif field_type == "boolean":
                if not isinstance(item, bool):
                    raise ValueError
            elif field_type in _DATE:
                if isinstance(item, bool):
                    raise ValueError
                if (isinstance(item, (int, float)) and math.isfinite(item)
                        and date_format is not None and "epoch_millis" in date_format):
                    item = int(item)
                elif (isinstance(item, str) and date_format is not None
                      and any(part in date_format for part in ("strict_date_optional_time", "date_optional_time"))):
                    parsed = datetime.fromisoformat(item)
                    if parsed.tzinfo is None or parsed.utcoffset() is None:
                        raise ValueError
                    item = parsed.isoformat()
                else:
                    raise ValueError
            elif field_type == "ip":
                if not isinstance(item, str):
                    raise ValueError
                item = str(ipaddress.ip_address(item))
            else:
                if not isinstance(item, str) or len(item) > 128:
                    raise ValueError
            converted.append(item)
        return converted if operator == "in" else converted[0]

    @staticmethod
    def _wildcard_literal(value: str) -> str:
        return value.replace("\\", "\\\\").replace("*", "\\*").replace("?", "\\?")

    @staticmethod
    def _query_fingerprint(request: EventSearchRequest) -> str:
        value = request.model_dump(mode="json", exclude={"cursor", "limit"})
        value["filters"] = sorted(value["filters"], key=lambda item: json.dumps(item, sort_keys=True))
        return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()

    def _decode_cursor(self, token: str) -> dict:
        try:
            raw = self._cursors.decrypt(token.encode("ascii"), ttl=self.CURSOR_TTL_SECONDS)
            value = json.loads(raw)
            if not isinstance(value, dict) or set(value) != {
                "source", "configuration", "query", "pit", "targets", "after", "fields"
            }:
                raise ValueError
            return value
        except InvalidToken:
            try:
                issued = self._cursors.extract_timestamp(token.encode("ascii"))
            except (InvalidToken, UnicodeError):
                _failure("invalid_cursor", "Cursor is invalid", 422)
            if time.time() - issued > self.CURSOR_TTL_SECONDS:
                _failure("event_cursor_expired", "Search snapshot expired; start a new search", 409)
            _failure("invalid_cursor", "Cursor is invalid", 422)
        except (TypeError, ValueError, UnicodeError, json.JSONDecodeError):
            _failure("invalid_cursor", "Cursor is invalid", 422)

    def _encode_cursor(self, payload: dict) -> str:
        return self._cursors.encrypt(json.dumps(payload, separators=(",", ":")).encode()).decode("ascii")

    def _event_id(self, source_id: UUID, configuration: str, target: str, index: str, document: str) -> str:
        payload = json.dumps({"source": str(source_id), "configuration": configuration,
                              "target": target, "index": index, "document": document},
                             separators=(",", ":")).encode()
        return self._ids.encrypt(payload).decode("ascii")

    def _decode_event_id(self, token: str) -> dict:
        try:
            value = json.loads(self._ids.decrypt(token.encode("ascii"), ttl=self.EVENT_ID_TTL_SECONDS))
            if not isinstance(value, dict) or set(value) != {
                "source", "configuration", "target", "index", "document"
            }:
                raise ValueError
            return value
        except (InvalidToken, TypeError, ValueError, UnicodeError, json.JSONDecodeError):
            _failure("event_not_found", "Event not found", 404)

    @staticmethod
    def _event_timestamp(fields: dict[str, Any], date_format: str = "strict_date_optional_time||epoch_millis") -> datetime:
        value = fields.get("@timestamp")
        try:
            if isinstance(value, bool):
                raise TypeError
            if isinstance(value, (int, float)) and math.isfinite(value) and "epoch_millis" in date_format:
                result = datetime.fromtimestamp(value / 1000, tz=UTC)
            elif isinstance(value, str) and any(
                allowed in date_format for allowed in ("strict_date_optional_time", "date_optional_time")
            ):
                result = datetime.fromisoformat(value)
            else:
                raise ValueError
            if result.tzinfo is None or result.utcoffset() is None:
                raise ValueError
            return result
        except (AttributeError, TypeError, ValueError, OverflowError, OSError):
            raise ValueError("invalid timestamp")

    def search(self, source_id: UUID, request: EventSearchRequest) -> EventSearchResponse:
        source, connection = self._connection(source_id)
        configuration = self._config_fingerprint(source, connection)
        cursor = self._decode_cursor(request.cursor) if request.cursor else None
        if cursor is not None:
            if (cursor["source"] != str(source_id) or cursor["configuration"] != configuration
                    or cursor["query"] != self._query_fingerprint(request)):
                _failure("invalid_cursor", "Cursor does not match this source or search", 422)
            targets = cursor["targets"]
            fields = cursor["fields"]
        else:
            targets = self._targets(source, connection)
            fields = self._field_map(source, connection, targets)
        filters = []
        for index, item in enumerate(request.filters):
            metadata = fields.get(item.field)
            if metadata is None or not metadata[1]:
                _failure("event_filter_invalid", "Event filter field is unknown or not searchable", 422,
                         {"index": index, "reason": "unknown_field"})
            field_type, allowed = metadata[:2]
            if item.operator not in allowed:
                _failure("event_filter_invalid", "Event filter operator is unsupported", 422,
                         {"index": index, "reason": "unsupported_operator"})
            try:
                value = self._typed_value(
                    field_type, item.operator, item.value,
                    metadata[2].get("format", "strict_date_optional_time||epoch_millis"),
                )
            except (ValueError, TypeError, OverflowError):
                _failure("event_filter_invalid", "Event filter value has the wrong type", 422,
                         {"index": index, "reason": "invalid_value"})
            if item.operator in {"contains", "ends_with"} and len(value) < 3:
                _failure("event_filter_invalid", "Substring filters require at least three characters", 422,
                         {"index": index, "reason": "value_too_short"})
            filters.append((item.field, field_type, item.operator, value))

        timestamp = fields.get("@timestamp")
        if timestamp is None or timestamp[0] not in _DATE:
            _failure("event_timestamp_unavailable", "Selected targets do not have a compatible searchable @timestamp", 422)
        query_fingerprint = self._query_fingerprint(request)
        if cursor is not None:
            pit_id = cursor["pit"]
            targets = cursor["targets"]
            after = cursor["after"]
        else:
            pit = self._call("open_pit", connection, targets)
            if not isinstance(pit, dict) or not isinstance(pit.get("pit_id"), str) or not pit["pit_id"]:
                _failure("indexer_invalid_response", "Indexer returned invalid snapshot metadata", 502)
            pit_id = pit["pit_id"]
            after = None

        clauses: list[dict] = [{"exists": {"field": "@timestamp"}}]
        timestamp_range = {}
        if request.timestamp_from is not None:
            timestamp_range["gte"] = request.timestamp_from.isoformat()
        if request.timestamp_to is not None:
            timestamp_range["lt"] = request.timestamp_to.isoformat()
        if timestamp_range:
            clauses.append({"range": {"@timestamp": timestamp_range}})
        for field, field_type, operator, value in filters:
            if operator == "in":
                clause = {"terms": {field: value}}
            elif operator == "eq":
                clause = {"term": {field: value}}
            elif operator in _RANGE:
                clause = {"range": {field: {operator: value}}}
            else:
                escaped = self._wildcard_literal(value)
                pattern = {"contains": f"*{escaped}*", "starts_with": f"{escaped}*",
                           "ends_with": f"*{escaped}"}[operator]
                clause = {"wildcard": {field: {"value": pattern, "case_insensitive": False}}}
            clauses.append(clause)

        body = {
            "size": min(request.limit + 1, self.MAX_HITS),
            "pit": {"id": pit_id, "keep_alive": self.PIT_KEEP_ALIVE},
            "query": {"bool": {"filter": clauses}},
            "sort": [{"@timestamp": {"order": request.sort, "missing": "_last"}},
                     {"_shard_doc": request.sort}],
            "track_total_hits": False,
        }
        if after is not None:
            body["search_after"] = after
        owns_new_pit = True
        try:
            response = self._call("search", connection, body)
        except DomainError as error:
            self._close_pit(connection, pit_id, owns_new_pit)
            if cursor is not None and error.code in {"indexer_target_not_found", "indexer_invalid_response"}:
                _failure("event_cursor_expired", "Search snapshot expired; start a new search", 409)
            raise
        if not isinstance(response, dict) or not isinstance(response.get("hits"), dict):
            self._close_pit(connection, pit_id, owns_new_pit)
            _failure("indexer_invalid_response", "Indexer returned invalid search response", 502)
        if response.get("timed_out") is not False:
            self._close_pit(connection, pit_id, owns_new_pit)
            _failure("indexer_timeout", "Indexer search timed out", 504)
        shards = response.get("_shards")
        if not isinstance(shards, dict) or not isinstance(shards.get("failed"), int) or shards["failed"]:
            self._close_pit(connection, pit_id, owns_new_pit)
            _failure("indexer_partial_results", "Indexer returned incomplete search results", 502)
        hits = response["hits"].get("hits")
        if not isinstance(hits, list) or len(hits) > request.limit + 1:
            self._close_pit(connection, pit_id, owns_new_pit)
            _failure("indexer_invalid_response", "Indexer returned an invalid number of events", 502)
        more = len(hits) > request.limit
        page_hits = hits[:request.limit]
        items = []
        selected_target = source.data_stream_name or source.data_stream_pattern or source.index_name or source.index_pattern
        new_pit = response.get("pit_id", pit_id)
        for hit in page_hits:
            if not isinstance(hit, dict) or not isinstance(hit.get("_id"), str) or not isinstance(hit.get("_index"), str):
                self._close_pit(connection, pit_id, owns_new_pit)
                _failure("indexer_invalid_response", "Indexer returned an invalid event identity", 502)
            document = hit.get("_source")
            if not isinstance(document, dict):
                self._close_pit(connection, pit_id, owns_new_pit)
                _failure("indexer_invalid_response", "Indexer returned an invalid event document", 502)
            sort_values = hit.get("sort")
            if not isinstance(sort_values, list) or len(sort_values) != 2:
                self._close_pit(connection, pit_id, owns_new_pit)
                _failure("indexer_invalid_response", "Indexer returned an unstable event order", 502)
            try:
                event_time = self._event_timestamp(document, timestamp[2].get("format", "strict_date_optional_time||epoch_millis"))
            except ValueError:
                continue
            items.append(EventSearchItem(
                source_id=source_id,
                source_type="external",
                id=self._event_id(source_id, configuration, selected_target, hit["_index"], hit["_id"]),
                event_timestamp=event_time,
                fields=document,
            ))
        next_cursor = None
        if more and page_hits:
            snapshot_fields = {"@timestamp": fields["@timestamp"]}
            snapshot_fields.update({item.field: fields[item.field] for item in request.filters})
            try:
                next_cursor = self._encode_cursor({
                    "source": str(source_id), "configuration": configuration,
                    "query": query_fingerprint, "pit": new_pit, "targets": targets,
                    "after": page_hits[-1]["sort"], "fields": snapshot_fields,
                })
            except Exception:
                self._close_pit(connection, new_pit, True)
                raise
        else:
            self._close_pit(connection, new_pit, True)
        try:
            return EventSearchResponse(items=items, has_more=more, next_cursor=next_cursor)
        except Exception:
            self._close_pit(connection, new_pit, True)
            raise

    def _close_pit(self, connection, pit_id: str, owned: bool) -> None:
        if not owned:
            return
        try:
            self._call("close_pit", connection, pit_id)
        except DomainError:
            pass

    def get(self, source_id: UUID, event_id: str) -> ExternalEventCard:
        token = self._decode_event_id(event_id)
        source, connection = self._connection(source_id)
        configuration = self._config_fingerprint(source, connection)
        if token["source"] != str(source_id) or token["configuration"] != configuration:
            _failure("event_not_found", "Event not found", 404)
        targets = self._targets(source, connection)
        selected_target = source.data_stream_name or source.data_stream_pattern or source.index_name or source.index_pattern
        is_stream, _, _ = self._target_spec(source)
        if token["target"] != selected_target or (not is_stream and token["index"] not in set(targets)):
            _failure("event_not_found", "Event not found", 404)
        if is_stream:
            response = self._call("find_document", connection, targets, token["index"], token["document"])
            shards = response.get("_shards") if isinstance(response, dict) else None
            if (not isinstance(response, dict) or response.get("timed_out") is not False
                    or not isinstance(shards, dict) or not isinstance(shards.get("failed"), int)
                    or shards["failed"]):
                _failure("indexer_partial_results", "Indexer returned incomplete event-card results", 502)
            hits_block = response.get("hits")
            hits = hits_block.get("hits") if isinstance(hits_block, dict) else None
            if not isinstance(hits, list) or len(hits) > 1:
                _failure("indexer_invalid_response", "Indexer returned invalid event-card results", 502)
            matches = [hit for hit in hits if isinstance(hit, dict)
                       and hit.get("_index") == token["index"]
                       and hit.get("_id") == token["document"]]
            if len(matches) != 1:
                _failure("event_not_found", "Event not found", 404)
            result = matches[0]
            document = result.get("_source")
        else:
            result = self._call("get_document", connection, token["index"], token["document"])
            if not isinstance(result, dict) or result.get("found") is not True:
                _failure("event_not_found", "Event not found", 404)
            if result.get("_index") != token["index"] or result.get("_id") != token["document"]:
                _failure("event_not_found", "Event not found", 404)
            document = result.get("_source")
        if not isinstance(document, dict):
            _failure("indexer_invalid_response", "Indexer returned an invalid event document", 502)
        return ExternalEventCard(
            source_id=source_id, source_type="external", id=event_id,
            event_timestamp=self._card_timestamp(document, connection, selected_target, token["index"]),
            fields=document,
            details={"index": token["index"]},
        )

    def _card_timestamp(self, document, connection, target, index):
        try:
            mappings = self._call("mappings", connection, [target])
            mapped = mappings.get(index, {}).get("mappings", {}).get("properties", {})
            timestamp = mapped.get("@timestamp", {})
            date_format = timestamp.get("format", "strict_date_optional_time||epoch_millis")
            return self._event_timestamp(document, date_format)
        except (AttributeError, TypeError, ValueError):
            _failure("indexer_invalid_response", "Indexer returned an event without valid @timestamp", 502)
