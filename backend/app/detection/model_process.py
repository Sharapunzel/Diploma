"""One bounded Python/JVM process per candidate; no reusable mutated cache."""

import json
import os
import subprocess
import sys
from pathlib import Path

from .contracts import DetectionError, ModelConfig, canonical, digest
from .numeric import NumericQualityError


def invoke(request):
    try:
        result = subprocess.run([sys.executable, "-m", "app.detection.model_process"],
            input=canonical(request), capture_output=True, text=True, timeout=120, check=False)
    except subprocess.TimeoutExpired as error:
        raise DetectionError("resource_budget:model_seconds") from error
    if result.returncode:
        raise DetectionError("model_process_failed")
    response = json.loads(result.stdout)
    if "numeric_error" in response:
        raise NumericQualityError(response["numeric_error"], response["coordinate"])
    if "error" in response:
        raise DetectionError(response["error"])
    return response


class CandidateModel:
    def __init__(self, checkpoint, config, keys, rule_hash):
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

    def process(self, values):
        response = invoke({**self.request, "values": values})
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
    def warmup(self, config, keys, rule_hash, rows):
        return invoke({"action": "warmup", "config": config, "keys": keys,
                       "rule_hash": rule_hash, "rows": rows})["checkpoint"]

    def restore(self, checkpoint, config, keys, rule_hash):
        return CandidateModel(checkpoint, config, keys, rule_hash)


def main():
    from .aws_rcf import AwsModelFactory
    request = json.load(sys.stdin)
    factory = AwsModelFactory()
    try:
        if request["action"] == "warmup":
            checkpoint = factory.warmup(request["config"], request["keys"],
                                        request["rule_hash"], request["rows"])
            response = {"checkpoint": checkpoint}
        else:
            model = factory.restore(request["checkpoint"], request["config"],
                                    request["keys"], request["rule_hash"])
            result = model.process(request["values"])
            response = {"result": result, "checkpoint": model.checkpoint("candidate")}
    except NumericQualityError as error:
        response = {"numeric_error": error.reason, "coordinate": error.coordinate}
    except DetectionError as error:
        response = {"error": str(error)}
    print(canonical(response))


if __name__ == "__main__":
    main()
