"""Saved replication progress needed by the capture decision."""

from datetime import datetime
from typing import Protocol


class CaptureStateRepository(Protocol):
    def read_durable_progress(self) -> tuple[int, datetime | None, datetime] | None: ...
