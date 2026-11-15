"""Real pipe/process fault regressions; no Java/database needed."""

import struct
import sys
import threading
import time

import pytest
from app.adapters import model_runtime
from app.detection.contracts import DetectionError, StaleCandidate
from app.detection.model_rpc import MAX_FRAME, read_frame, write_frame

REPLY = """
from app.detection.model_rpc import read_frame, write_frame
import sys,time,struct
r=read_frame(sys.stdin.buffer)
reply={'id':r['id'],'action':r['action'],'response':{'checkpoint':{}},'timings':{}}
"""


@pytest.mark.parametrize("fault", ["eof", "corrupt", "oversize", "wrong_id", "wrong_action",
    "wrong_payload", "wrong_timings", "timeout", "blocked_write", "cancel", "close"])
def test_fault_reaps_process_and_io_then_valid_request(monkeypatch, fault):
    children = []
    original = model_runtime.subprocess.Popen
    suffix = {
        "eof": "sys.exit(0)",
        "corrupt": "sys.stdout.buffer.write(struct.pack('!I',1)+b'{');sys.stdout.buffer.flush()",
        "oversize": f"sys.stdout.buffer.write(struct.pack('!I',{MAX_FRAME + 1}));sys.stdout.buffer.flush()",
        "wrong_id": "reply['id']='wrong';write_frame(sys.stdout.buffer,reply)",
        "wrong_action": "reply['action']='advance';write_frame(sys.stdout.buffer,reply)",
        "wrong_payload": "reply['response']={'junk':1};write_frame(sys.stdout.buffer,reply)",
        "wrong_timings": "reply['timings']={'secret':'hidden'};write_frame(sys.stdout.buffer,reply)",
        "timeout": "time.sleep(60)", "cancel": "time.sleep(60)", "close": "time.sleep(60)",
    }
    def launch(*args, **kwargs):
        if not children:
            code = "import time;time.sleep(60)" if fault == "blocked_write" else REPLY + suffix[fault]
        else:
            code = REPLY + "write_frame(sys.stdout.buffer,reply);time.sleep(60)"
        child = original([sys.executable, "-B", "-c", code], **kwargs)
        children.append(child)
        return child
    monkeypatch.setattr(model_runtime.subprocess, "Popen", launch)
    monkeypatch.setattr(model_runtime, "MODEL_TIMEOUT", .6)
    runtime = model_runtime.ModelRuntime()
    cancel = threading.Event()
    timer = None
    if fault in {"cancel", "close"}:
        timer = threading.Timer(.25, cancel.set if fault == "cancel" else runtime.close)
        timer.start()
    try:
        started = time.monotonic()
        with pytest.raises((DetectionError, StaleCandidate)):
            runtime.invoke({"action": "warmup", "input": "x" * (2 * 1024**2)}, cancel)
        assert time.monotonic() - started < 4
        assert children and children[0].poll() is not None
        assert children[0].stdin.closed and children[0].stdout.closed
        assert not any(t.name == "model-rpc" for t in threading.enumerate())
        cancel.clear()
        if fault == "close":
            with pytest.raises(StaleCandidate):
                runtime.invoke({"action": "warmup"})
            runtime = model_runtime.ModelRuntime()  # fresh owner only
        monkeypatch.setattr(model_runtime, "MODEL_TIMEOUT", 5)
        assert runtime.invoke({"action": "warmup"}) == {"checkpoint": {}}
    finally:
        runtime.close()
        if timer is not None:
            timer.join()
    assert all(child.poll() is not None for child in children)
    assert not any(t.name == "model-rpc" for t in threading.enumerate())


def test_lazy_cancel_and_only_one_in_flight(monkeypatch):
    original = model_runtime.subprocess.Popen
    children = []
    entered = threading.Event()
    def launch(*args, **kwargs):
        child = original([sys.executable, "-c", "import time;time.sleep(60)"], **kwargs)
        children.append(child)
        entered.set()
        return child
    monkeypatch.setattr(model_runtime.subprocess, "Popen", launch)
    runtime, cancel = model_runtime.ModelRuntime(), threading.Event()
    assert runtime.pid is None and not children
    cancel.set()
    with pytest.raises(StaleCandidate):
        runtime.invoke({"action": "warmup"}, cancel)
    assert not children
    cancel.clear()
    errors = []
    def invoke():
        try:
            runtime.invoke({"action": "warmup"}, cancel)
        except StaleCandidate:
            errors.append("cancelled")
    thread = threading.Thread(target=invoke)
    thread.start()
    try:
        assert entered.wait(5)
        with pytest.raises(DetectionError, match="model_request_in_flight"):
            runtime.invoke({"action": "warmup"})
        runtime.close()
    finally:
        runtime.close()
        thread.join(timeout=3)
    assert errors == ["cancelled"] and not thread.is_alive()
    assert len(children) == 1 and children[0].poll() is not None


def test_framing_partial_io_and_limits(monkeypatch):
    import io

    from app.detection import model_rpc
    class ShortIO(io.BytesIO):
        def write(self, data): return super().write(data[:3])
        def read(self, size): return super().read(min(size, 2))
    stream = ShortIO()
    value = {"id": "a", "text": "\u041f\u0440\u0438\u0432\u0435\u0442"}
    write_frame(stream, value)
    stream.seek(0)
    assert read_frame(stream) == value
    monkeypatch.setattr(model_rpc, "MAX_FRAME", 32)
    with pytest.raises(DetectionError, match="model_frame_bytes"):
        write_frame(io.BytesIO(), {"input": "x" * 33})
    with pytest.raises(DetectionError, match="model_frame_bytes"):
        read_frame(io.BytesIO(struct.pack("!I", 33)))
