from __future__ import annotations

import hashlib
import json
from datetime import datetime
from uuid import UUID

from itsdangerous import BadSignature, URLSafeSerializer

from ...core.errors import DomainError
from ...schemas.parsed_logs import ParsedLogSearchRequest


class SignedParsedLogCursorCodec:
    def __init__(self, secret: str):
        self.serializer = URLSafeSerializer(
            secret, salt="diploma.parsed-logs.cursor.v1"
        )

    @staticmethod
    def _fingerprint(request: ParsedLogSearchRequest) -> str:
        filters = request.model_dump(mode="json", exclude={"limit", "cursor"})
        for key in ("source_ids", "connection_ids", "normalizer_ids", "kafka_topics", "kafka_partitions"):
            if filters[key] is not None:
                filters[key] = sorted(filters[key])
        filters["ecs_filters"] = sorted(
            filters["ecs_filters"],
            key=lambda item: json.dumps(item, sort_keys=True, separators=(",", ":")),
        )
        serialized = json.dumps(filters, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(serialized.encode("utf-8")).hexdigest()

    def encode(
        self, processed_at: datetime, log_id: UUID, snapshot_boundary: datetime,
        request: ParsedLogSearchRequest,
    ) -> str:
        return self.serializer.dumps({
            "processed_at": processed_at.isoformat(),
            "id": str(log_id),
            "snapshot_boundary": snapshot_boundary.isoformat(),
            "filters": self._fingerprint(request),
        })

    def decode(
        self, token: str | None, request: ParsedLogSearchRequest
    ) -> tuple[datetime, UUID, datetime] | None:
        if token is None:
            return None
        try:
            payload = self.serializer.loads(token)
            if (
                not isinstance(payload, dict)
                or set(payload) != {"processed_at", "id", "snapshot_boundary", "filters"}
                or not isinstance(payload["processed_at"], str)
                or not isinstance(payload["id"], str)
                or not isinstance(payload["snapshot_boundary"], str)
                or payload["filters"] != self._fingerprint(request)
            ):
                raise ValueError
            processed_at = datetime.fromisoformat(payload["processed_at"])
            snapshot_boundary = datetime.fromisoformat(payload["snapshot_boundary"])
            if (
                processed_at.tzinfo is None or processed_at.utcoffset() is None
                or snapshot_boundary.tzinfo is None
                or snapshot_boundary.utcoffset() is None
            ):
                raise ValueError
            return processed_at, UUID(payload["id"]), snapshot_boundary
        except (BadSignature, TypeError, ValueError, KeyError):
            raise DomainError("invalid_cursor", "Cursor is invalid", 422) from None
