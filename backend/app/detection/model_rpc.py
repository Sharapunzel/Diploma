"""Bounded wire contract shared by the runtime adapter and calculation child."""

import json
import struct
from typing import Protocol

from .contracts import DetectionError

MAX_PAYLOAD = 64 * 1024**2
# One checkpoint plus one input, with bounded envelope overhead. No truncation.
MAX_FRAME = 2 * MAX_PAYLOAD + 1024**2


class ModelTransport(Protocol):
    last_timings: dict | None
    def invoke(self, request: dict, cancel=None) -> dict: ...
    def close(self) -> None: ...


def write_frame(stream, value, check=lambda: None):
    payload = bytearray()
    for part in json.JSONEncoder(allow_nan=False, separators=(",", ":")).iterencode(value):
        check()
        payload.extend(part.encode("utf-8"))
        if len(payload) > MAX_FRAME:
            raise DetectionError("resource_budget:model_frame_bytes")
    def write_all(view):
        while view:
            check()
            written = stream.write(view[:65536])
            if not written:
                raise DetectionError("model_process_failed")
            view = view[written:]
    write_all(memoryview(struct.pack("!I", len(payload))))
    view = memoryview(payload)
    write_all(view)
    stream.flush()


def read_frame(stream, check=lambda: None):
    def exact(size):
        result = bytearray()
        while len(result) < size:
            check()
            part = stream.read(min(65536, size - len(result)))
            if not part:
                raise DetectionError("model_process_failed")
            result.extend(part)
        return result
    size = struct.unpack("!I", exact(4))[0]
    if not 0 < size <= MAX_FRAME:
        raise DetectionError("resource_budget:model_frame_bytes")
    try:
        value = json.loads(exact(size))
        if not isinstance(value, dict):
            raise TypeError("envelope")
        return value
    except (ValueError, TypeError, UnicodeError, RecursionError) as error:
        raise DetectionError("model_process_failed") from error
