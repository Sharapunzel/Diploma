"""Lifecycle contract for the permanent detector coordinator."""
from typing import Protocol


class DetectorWorkerProtocol(Protocol):
    def run(self) -> None: ...
