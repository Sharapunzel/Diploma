"""Fresh candidate models over an injected runtime; no mutable forest cache."""

import json
import os
import sys
import time
from pathlib import Path

from .contracts import DetectionError, ModelConfig, canonical, digest
from .model_rpc import ModelTransport
from .numeric import NumericQualityError


def checked_response(response):
    if "numeric_error" in response:
        raise NumericQualityError(response["numeric_error"], response["coordinate"])
    if "error" in response:
        raise DetectionError(response["error"])
    return response


class CandidateModel:
    def __init__(self, checkpoint, config, keys, rule_hash, cancel=None, transport=None):
        try:
            if len(canonical(checkpoint).encode()) > 64 * 1024**2:
                raise DetectionError("resource_budget:checkpoint_bytes")
            manifest = json.loads((Path(os.environ.get("RCF_CLASSPATH", "/opt/rcf"))
                                   / "manifest.json").read_text())
            expected_runtime = {"manifest": digest(manifest), "jre": "17.0.17+10",
                                "jpype": "1.6.0", "aws": "4.4.0", "format": 1}
            body = checkpoint["body"]
            if (digest(body) != checkpoint["sha256"] or body["config"] != config
                    or body["keys"] != keys or body["rule_hash"] != rule_hash
                    or body["runtime"] != expected_runtime
                    or body["settings"] != ModelConfig(**config).settings):
                raise DetectionError("checkpoint_envelope_mismatch")
        except (KeyError, TypeError) as error:
            raise DetectionError("checkpoint_corrupt") from error
        self.request = {"action": "advance", "checkpoint": checkpoint, "config": config,
                        "keys": keys, "rule_hash": rule_hash}
        self.candidate = None
        self.cancel = cancel
        self.transport = transport

    def process(self, values):
        response = checked_response(self.transport.invoke({**self.request, "values": values}, self.cancel))
        self.candidate = response["checkpoint"]
        return response["result"]

    def checkpoint(self, boundary):
        if self.candidate is None:
            raise DetectionError("process_required")
        self.candidate["body"]["boundary"] = boundary
        self.candidate["sha256"] = digest(self.candidate["body"])
        if len(canonical(self.candidate).encode()) > 64 * 1024**2:
            raise DetectionError("resource_budget:checkpoint_bytes")
        return self.candidate


class BoundedModelFactory:
    def __init__(self, cancel=None, transport: ModelTransport | None = None):
        self.cancel = cancel
        self.transport = transport

    def warmup(self, config, keys, rule_hash, rows):
        if self.transport is None:
            raise DetectionError("managed_worker_required")
        return checked_response(self.transport.invoke({"action": "warmup", "config": config, "keys": keys,
                       "rule_hash": rule_hash, "rows": rows}, self.cancel))["checkpoint"]

    def restore(self, checkpoint, config, keys, rule_hash):
        if self.transport is None:
            raise DetectionError("managed_worker_required")
        return CandidateModel(checkpoint, config, keys, rule_hash, self.cancel, self.transport)


def calculate(request):
    """Shared calculation for cold references and managed RPC; no retained model."""
    from .aws_rcf import AwsModelFactory, runtime
    timings = {}
    started = time.monotonic()
    factory = AwsModelFactory()
    try:
        runtime()
        initialized = time.monotonic()
        timings["init_seconds"] = initialized - started
        if request["action"] == "warmup":
            checkpoint = factory.warmup(request["config"], request["keys"],
                                        request["rule_hash"], request["rows"])
            timings["warmup_seconds"] = time.monotonic() - initialized
            response = {"checkpoint": checkpoint}
        elif request["action"] == "advance":
            model = factory.restore(request["checkpoint"], request["config"],
                                    request["keys"], request["rule_hash"])
            restored = time.monotonic()
            result = model.process(request["values"])
            processed = time.monotonic()
            checkpoint = model.checkpoint("candidate")
            timings.update(restore_seconds=restored - initialized,
                process_seconds=processed - restored,
                checkpoint_seconds=time.monotonic() - processed)
            response = {"result": result, "checkpoint": checkpoint}
        else:
            raise DetectionError("model_process_failed")
    except NumericQualityError as error:
        response = {"numeric_error": error.reason, "coordinate": error.coordinate}
    except DetectionError as error:
        response = {"error": str(error)}
    return response, timings


def main():
    if sys.argv[1:] != ["--serve"]:
        # Independent fresh-process reference, never used by the worker.
        response, timings = calculate(json.load(sys.stdin))
        print(canonical({"response": response, "timings": timings}))
        return
    from .model_rpc import read_frame, write_frame
    while True:
        envelope = read_frame(sys.stdin.buffer)
        request = envelope["request"]
        if (not isinstance(envelope.get("id"), str) or len(envelope["id"]) != 36
                or envelope.get("action") != request.get("action")):
            raise DetectionError("model_process_failed")
        response, timings = calculate(request)
        write_frame(sys.stdout.buffer, {"id": envelope["id"], "action": envelope["action"],
                    "response": response, "timings": timings})
        # Do not keep the last request/checkpoint/result alive while idle.
        del envelope, request, response, timings


if __name__ == "__main__":
    main()
