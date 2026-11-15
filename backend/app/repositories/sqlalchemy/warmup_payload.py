"""Preflight full canonical rows before loading and copying JSON payloads."""

from sqlalchemy import Text, func, select

from ...detection.contracts import DetectionError
from ...models import WorkingWindowPoint


def warmup_payload_preflight(session, schedule_id, starts, before_read=None):
    if before_read:
        before_read()
    # Include evidence/all fetched columns, encoding/derived-vector allowance,
    # and conservative Python decoding/copying memory. Only a scalar crosses
    # the driver boundary before these checks.
    row = func.octet_length(func.to_jsonb(WorkingWindowPoint.__table__.table_valued()).cast(Text))
    features = func.octet_length(WorkingWindowPoint.features.cast(Text))
    size = session.scalar(select(func.coalesce(func.sum(row + 3 * features + 2048), 0)).where(
        WorkingWindowPoint.schedule_id == schedule_id, WorkingWindowPoint.start_utc.in_(starts)))
    if size > 64 * 1024**2 or size * 8 > 256 * 1024**2:
        raise DetectionError("resource_budget:warmup_input_bytes")
