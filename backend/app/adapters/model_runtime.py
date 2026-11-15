"""One lazy child JVM, one RPC, bounded pipes and cancellable process lifetime."""

import math
import subprocess
import sys
import time
from threading import Event, Lock, Thread
from uuid import uuid4

from ..detection.contracts import DetectionError, StaleCandidate
from ..detection.model_rpc import read_frame, write_frame

MODEL_TIMEOUT = 120


class ModelRuntime:
    def __init__(self):
        self._process = None
        self._gate = Lock()
        self._state = Lock()
        self._closed = Event()
        self.last_timings = None

    @property
    def pid(self):
        with self._state:
            return self._process.pid if self._process is not None else None

    def close(self):
        """Invalidate this owner's transport, including an in-flight blocked RPC."""
        self._closed.set()
        self._discard()

    def _discard(self):
        with self._state:
            process = self._process
            if process is not None:
                if process.poll() is None:
                    process.kill()
                process.wait(timeout=2)
                process.stdin.close()
                process.stdout.close()
                self._process = None
                return process
        return None

    def invoke(self, request, cancel=None):
        # No executor/queue: a programming error cannot enqueue more model work.
        if not self._gate.acquire(blocking=False):
            raise DetectionError("model_request_in_flight")
        started = time.monotonic()
        deadline = started + MODEL_TIMEOUT
        process, thread = None, None
        done, outcome = Event(), []
        self.last_timings = None

        def check():
            if self._closed.is_set() or cancel is not None and cancel.is_set():
                raise StaleCandidate("ownership_lost")
            if time.monotonic() >= deadline:
                raise DetectionError("resource_budget:model_seconds")

        try:
            check()
            identity = str(uuid4())
            with self._state:
                check()
                if self._process is None:
                    self._process = subprocess.Popen(
                        [sys.executable, "-m", "app.detection.model_process", "--serve"],
                        stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                        stderr=subprocess.DEVNULL, bufsize=0)
                process = self._process
            spawned = time.monotonic()

            def exchange():
                try:
                    write_frame(process.stdin, {"id": identity, "action": request["action"],
                        "request": request}, check)
                    response = read_frame(process.stdout, check)
                    if response.get("id") != identity or response.get("action") != request["action"]:
                        raise DetectionError("model_process_failed")
                    if not isinstance(response.get("response"), dict):
                        raise DetectionError("model_process_failed")
                    outcome.append(response)
                except Exception as error:  # noqa: BLE001 - safe RPC failure, no payload disclosure
                    outcome.append(error)
                finally:
                    done.set()

            thread = Thread(target=exchange, name="model-rpc")
            thread.start()
            while not done.wait(.05):
                check()
            check()
            response = outcome.pop()
            if isinstance(response, Exception):
                raise response
            if set(response) != {"id", "action", "response", "timings"}:
                raise DetectionError("model_process_failed")
            result = response["response"]
            if not (set(result) in ({"checkpoint"}, {"result", "checkpoint"},
                                   {"error"}, {"numeric_error", "coordinate"})):
                raise DetectionError("model_process_failed")
            expected = {"checkpoint"} if request["action"] == "warmup" else {"result", "checkpoint"}
            if "error" not in result and "numeric_error" not in result and set(result) != expected:
                raise DetectionError("model_process_failed")
            timings = response.get("timings")
            if (not isinstance(timings, dict) or not set(timings) <= {
                    "init_seconds", "warmup_seconds", "restore_seconds", "process_seconds", "checkpoint_seconds"}
                    or any(type(v) not in (float, int) or not math.isfinite(v) or v < 0 for v in timings.values())):
                raise DetectionError("model_process_failed")
            self.last_timings = {"pid": process.pid, "spawn_seconds": spawned - started,
                "rpc_seconds": time.monotonic() - started, **timings}
            if "error" in result:
                self._discard()
            return result
        except BaseException:
            self._discard()
            raise
        finally:
            if thread is not None:
                thread.join(timeout=2)
                if thread.is_alive():
                    self._discard()
                    thread.join(timeout=2)
                    if thread.is_alive():
                        raise RuntimeError("model_io_shutdown_failed")
            if process is not None and process.poll() is not None:
                process.stdin.close()
                process.stdout.close()
            outcome.clear()
            self._gate.release()
