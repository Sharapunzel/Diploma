from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from ...models import ParsedLog, ProcessedKafkaRecord, ProcessingError
from ...normalization.compiler import RuleValidationError
from ...normalization.convert import ConversionError, parse_timestamp
from ...normalization.engine import NormalizationEngine
from ...normalization.schema import Diagnostic
from ...repositories.protocols import UnitOfWork
from ...repositories.protocols.processing import (
    KafkaCoordinates,
    ProcessingClock,
    ProcessingContext,
    ProcessingRepository,
)
from ..protocols.processing import ProcessMessageResult

LOGGER = logging.getLogger("app.durable_processing")


class ProcessingConfigurationError(RuntimeError):
    """The selected source or normalizer cannot currently be processed."""


class ProcessingStorageError(RuntimeError):
    """No durable result was confirmed; the caller may retry the record."""


class ProcessingSystemError(RuntimeError):
    """Unexpected processing failure that must not be recorded as bad input."""


class _InvalidJson(ValueError):
    pass


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise _InvalidJson
        value[key] = item
    return value


def _reject_non_json_constant(_: str) -> None:
    raise _InvalidJson


def _diagnostic(code: str, *, field: str | None = None) -> list[dict[str, Any]]:
    item = Diagnostic(code=code, field=field)
    return [item.model_dump(mode="json", exclude_none=True)]


def _safe_diagnostics(items: list[Diagnostic]) -> list[dict[str, Any]]:
    return [item.model_dump(mode="json", exclude_none=True) for item in items]


def _has_unrepresentable_postgres_text(value: Any) -> bool:
    if isinstance(value, str):
        return "\x00" in value or any("\ud800" <= character <= "\udfff" for character in value)
    if isinstance(value, dict):
        return any(
            _has_unrepresentable_postgres_text(key)
            or _has_unrepresentable_postgres_text(item)
            for key, item in value.items()
        )
    if isinstance(value, list):
        return any(_has_unrepresentable_postgres_text(item) for item in value)
    return False


def _decode_envelope(payload: bytes | None):
    if payload is None:
        return None, "envelope", _diagnostic("null_payload"), None
    try:
        decoded = payload.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        return None, "decode", _diagnostic("invalid_utf8"), None
    try:
        envelope = json.loads(
            decoded,
            object_pairs_hook=_unique_object,
            parse_constant=_reject_non_json_constant,
        )
    except (json.JSONDecodeError, _InvalidJson, RecursionError, ValueError):
        return None, "decode", _diagnostic("invalid_json"), None
    if not isinstance(envelope, dict) or set(envelope) != {"timestamp", "log"}:
        return None, "envelope", _diagnostic("invalid_envelope"), None
    if not isinstance(envelope["log"], str):
        return None, "envelope", _diagnostic("invalid_envelope", field="log"), None
    try:
        collected_at = parse_timestamp(envelope["timestamp"])
    except ConversionError:
        return None, "envelope", _diagnostic("invalid_envelope", field="timestamp"), None
    try:
        envelope["log"].encode("utf-8", errors="strict")
    except UnicodeEncodeError:
        return None, "envelope", _diagnostic("invalid_envelope", field="log"), None
    if "\x00" in envelope["log"]:
        return None, "envelope", _diagnostic("invalid_envelope", field="log"), None
    return envelope, None, [], collected_at


class DurableProcessingServiceImpl:
    def __init__(
        self,
        repository: ProcessingRepository,
        unit_of_work: UnitOfWork,
        engine: NormalizationEngine,
        clock: ProcessingClock | None = None,
    ) -> None:
        self.repository = repository
        self.unit_of_work = unit_of_work
        self.engine = engine
        self.clock = clock or (lambda: datetime.now(UTC))

    def process(
        self,
        source_id: UUID,
        coordinates: KafkaCoordinates,
        payload: bytes | None,
        received_at: datetime,
    ) -> ProcessMessageResult:
        self._validate_call(coordinates, payload, received_at)
        record_id = uuid4()
        operation = "storage"
        try:
            prior_status = self.repository.find_result(coordinates)
            if prior_status is not None:
                self.unit_of_work.commit()
                return ProcessMessageResult("already_processed")

            context = self.repository.resolve_context(source_id)
            if context is None:
                raise ProcessingConfigurationError("source_context_unavailable")
            self._validate_context(context, source_id, coordinates)
            operation = "processing"
            try:
                self.engine.compile(
                    context.rule,
                    normalizer_id=context.normalizer_id,
                    version=context.normalizer_version,
                )
            except RuleValidationError as error:
                raise ProcessingConfigurationError("normalizer_rule_invalid") from error

            envelope, stage, diagnostics, collected_at = _decode_envelope(payload)
            processed_at = self._processed_at(received_at)
            status = "failed"
            normalized = None
            if envelope is not None:
                normalized = self.engine.normalize(
                    context.rule,
                    envelope,
                    normalizer_id=context.normalizer_id,
                    version=context.normalizer_version,
                )
                collected_at = normalized.fluent_bit_collected_at or collected_at
                status = normalized.status
                if status == "failed":
                    stage = "normalization"
                    diagnostics = _safe_diagnostics(normalized.diagnostics)
                    if not diagnostics:
                        diagnostics = _diagnostic("normalization_failed")
                elif (
                    normalized.ecs_data is not None
                    and _has_unrepresentable_postgres_text(normalized.ecs_data)
                ):
                    status = "failed"
                    stage = "normalization"
                    diagnostics = _diagnostic("unrepresentable_ecs_text")

            record = ProcessedKafkaRecord(
                id=record_id,
                connection_id=context.connection_id,
                connection_identity=context.connection_id,
                kafka_topic_identity=context.kafka_topic_identity,
                source_id=context.source_id,
                normalizer_id=context.normalizer_id,
                kafka_topic=coordinates.topic,
                kafka_partition=coordinates.partition,
                kafka_offset=coordinates.offset,
                result_status=status,
                backend_received_at=received_at,
                backend_processed_at=processed_at,
            )
            if status == "failed":
                error = ProcessingError(
                    connection_id=context.connection_id,
                    source_id=context.source_id,
                    normalizer_id=context.normalizer_id,
                    connection_name=context.connection_name,
                    source_name=context.source_name,
                    normalizer_name=context.normalizer_name,
                    normalizer_version=context.normalizer_version,
                    kafka_topic=coordinates.topic,
                    kafka_partition=coordinates.partition,
                    kafka_offset=coordinates.offset,
                    raw_payload=payload if payload is not None else b"",
                    stage=stage or "normalization",
                    diagnostics=diagnostics,
                    fluent_bit_collected_at=collected_at,
                    backend_received_at=received_at,
                    backend_processed_at=processed_at,
                )
                operation = "storage"
                inserted = self.repository.store_error(record, error)
            else:
                if normalized is None or normalized.ecs_data is None:
                    raise ProcessingSystemError("normalizer_returned_no_ecs_data")
                event = ParsedLog(
                    source_id=context.source_id,
                    connection_id=context.connection_id,
                    normalizer_id=context.normalizer_id,
                    normalizer_version=context.normalizer_version,
                    normalizer_name=context.normalizer_name,
                    source_name=context.source_name,
                    connection_name=context.connection_name,
                    kafka_topic=coordinates.topic,
                    kafka_partition=coordinates.partition,
                    kafka_offset=coordinates.offset,
                    deduplication_key=str(record_id),
                    fluent_bit_collected_at=normalized.fluent_bit_collected_at,
                    backend_received_at=received_at,
                    backend_processed_at=processed_at,
                    raw=envelope["log"],
                    ecs_data=normalized.ecs_data,
                    normalization_status=status,
                    normalization_diagnostics=_safe_diagnostics(normalized.diagnostics),
                )
                operation = "storage"
                inserted = self.repository.store_event(record, event)

            self.unit_of_work.commit()
            if not inserted:
                return ProcessMessageResult("already_processed")
            outcome = {
                "complete": "stored_complete",
                "partial": "stored_partial",
                "failed": "stored_failed",
            }[status]
            return ProcessMessageResult(outcome, record_id)
        except ProcessingConfigurationError:
            self.unit_of_work.rollback()
            raise
        except ProcessingSystemError:
            self.unit_of_work.rollback()
            raise
        except Exception as error:  # noqa: BLE001 -- storage failures stay retryable.
            self.unit_of_work.rollback()
            original = getattr(error, "orig", None)
            diagnostics = getattr(original, "diag", None)
            LOGGER.error(
                "durable_processing_failed",
                extra={
                    "error_type": type(error).__name__,
                    "sqlstate": getattr(original, "sqlstate", None),
                    "constraint": getattr(diagnostics, "constraint_name", None),
                    "column": getattr(diagnostics, "column_name", None),
                },
            )
            if operation == "processing":
                raise ProcessingSystemError("processing_runtime_failure") from None
            raise ProcessingStorageError("processing_result_not_committed") from None

    def _processed_at(self, received_at: datetime) -> datetime:
        processed_at = self.clock()
        if processed_at.tzinfo is None or processed_at.utcoffset() is None:
            raise ProcessingSystemError("processing_clock_must_be_timezone_aware")
        return max(processed_at, received_at)

    @staticmethod
    def _validate_call(
        coordinates: KafkaCoordinates, payload: bytes | None, received_at: datetime
    ) -> None:
        if payload is not None and not isinstance(payload, bytes):
            raise ProcessingConfigurationError("payload_must_be_bytes")
        if (
            not coordinates.topic.strip()
            or not isinstance(coordinates.kafka_topic_identity, str)
            or not coordinates.kafka_topic_identity.strip()
            or coordinates.partition < 0
            or coordinates.offset < 0
        ):
            raise ProcessingConfigurationError("kafka_coordinates_invalid")
        if received_at.tzinfo is None or received_at.utcoffset() is None:
            raise ProcessingConfigurationError("received_at_must_be_timezone_aware")

    @staticmethod
    def _validate_context(
        context: ProcessingContext,
        source_id: UUID,
        coordinates: KafkaCoordinates,
    ) -> None:
        if (
            context.source_id != source_id
            or context.connection_id != coordinates.connection_id
            or context.topic != coordinates.topic
            or context.kafka_topic_identity != coordinates.kafka_topic_identity
            or context.normalizer_version < 1
            or not context.connection_name.strip()
            or not context.source_name.strip()
            or not context.normalizer_name.strip()
            or not isinstance(context.rule, dict)
        ):
            raise ProcessingConfigurationError("source_context_mismatch")
