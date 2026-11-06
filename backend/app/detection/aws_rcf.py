"""Lazy, version-checked JPype adapter for the official AWS thresholded mapper."""

import hashlib
import json
import math
import os
import statistics
import threading
from dataclasses import asdict
from decimal import Decimal
from pathlib import Path

from .contracts import DetectionError, ModelConfig, canonical, digest
from .numeric import Affine

_lock = threading.Lock()
_pid = None
MAX_BYTES = 64 * 1024**2


def runtime():
    global _pid
    import jpype

    with _lock:
        if _pid is not None and _pid != os.getpid():
            raise DetectionError("inherited_jvm_forbidden")
        path = Path(os.environ.get("RCF_CLASSPATH", "/opt/rcf"))
        manifest = json.loads((path / "manifest.json").read_text())
        jars = []
        for item in manifest["artifacts"]:
            artifact = path / item["filename"]
            if hashlib.sha256(artifact.read_bytes()).hexdigest() != item["sha256"]:
                raise DetectionError("runtime_artifact_checksum")
            if artifact.suffix == ".jar":
                jars.append(str(artifact))
        if not jpype.isJVMStarted():
            if _pid is not None:
                raise DetectionError("jvm_restart_forbidden")
            jpype.startJVM("-Xms32m", "-Xmx512m", classpath=jars)
        _pid = os.getpid()
        cls = jpype.JClass
        version = str(cls("java.lang.System").getProperty("java.runtime.version"))
        if version != "17.0.17+10" or jpype.__version__ != "1.6.0":
            raise DetectionError("runtime_version_mismatch")
        identity = {"manifest": digest(manifest), "jre": version, "jpype": jpype.__version__,
                    "aws": "4.4.0", "format": 1}
        return jpype, identity


class AwsModel:
    def __init__(self, config, keys, rule_hash, affine, history, model, identity):
        self.config, self.keys, self.rule_hash = config, keys, rule_hash
        self.affine, self.history, self.model, self.identity = affine, history, model, identity

    def process(self, exact_values):
        import jpype

        prior = [([Decimal(x) for x in row["exact"]], row["ml"]) for row in self.history]
        ml = self.affine.convert(exact_values, prior)
        descriptor = self.model.process(jpype.JArray(jpype.JDouble)(ml),
                                       jpype.JLong(self.model.getForest().getTotalUpdates()))
        score, grade, confidence = (float(descriptor.getRCFScore()),
            float(descriptor.getAnomalyGrade()), float(descriptor.getDataConfidence()))
        if (not all(math.isfinite(x) for x in (score, grade, confidence)) or score < 0
                or not 0 <= grade <= 1 or not 0 <= confidence <= 1):
            raise DetectionError("invalid_model_output")
        expected, attribution, origin = None, None, None
        if grade > 0:
            aws_expected = descriptor.getExpectedValuesList()
            if (aws_expected is not None and len(aws_expected) and aws_expected[0] is not None
                    and len(aws_expected[0]) == len(self.keys)):
                try:
                    expected = [str(x) for x in self.affine.inverse(list(aws_expected[0]))]
                except ValueError as error:
                    # Java has already processed this point: an invalid explanation
                    # is a failed transition, never an input-quality skip.
                    raise DetectionError("invalid_explanation") from error
                origin = "aws_rcf"
            else:
                if not self.history:
                    raise DetectionError("explanation_history_missing")
                expected = [str(statistics.median([Decimal(row["exact"][i])
                    for row in self.history])) for i in range(len(self.keys))]
                origin = "preceding_median"
            vector = descriptor.getAttribution()
            if vector is None or len(vector.high) != len(self.keys):
                raise DetectionError("attribution_missing")
            attribution = [float(vector.high[i] + vector.low[i]) for i in range(len(self.keys))]
            if (not all(math.isfinite(x) and x >= 0 for x in attribution)
                    or not all(Decimal(x).is_finite() for x in expected)):
                raise DetectionError("invalid_explanation")
        self.history = [*self.history, {"exact": [str(x) for x in exact_values], "ml": list(ml)}][
            -self.config.sample_size:]
        return {"score": score, "grade": grade, "confidence": confidence,
                "ml_input": list(ml), "expected": expected, "expected_origin": origin,
                "attribution": attribution, "attribution_origin": "aws_rcf",
                "explanation_version": 1}

    def checkpoint(self, boundary):
        import jpype

        mapper = jpype.JClass(
            "com.amazon.randomcutforest.parkservices.state.ThresholdedRandomCutForestMapper")()
        codec = jpype.JClass("com.fasterxml.jackson.databind.ObjectMapper")()
        body = {"runtime": self.identity, "config": asdict(self.config),
            "settings": self.config.settings, "keys": self.keys,
            "rule_hash": self.rule_hash, "affine": {
                "algorithm": "min_range_decimal_float32_v1",
                "center": [str(x) for x in self.affine.center],
                "scale": [str(x) for x in self.affine.scale]}, "history": self.history,
            "updates": int(self.model.getForest().getTotalUpdates()), "boundary": boundary,
            "mapper": json.loads(str(codec.writeValueAsString(mapper.toState(self.model))))}
        result = {"body": body, "sha256": digest(body)}
        if len(canonical(result).encode()) > MAX_BYTES:
            raise DetectionError("resource_budget:checkpoint_bytes")
        return result


class AwsModelFactory:
    def warmup(self, config, keys, rule_hash, rows):
        jpype, identity = runtime()
        config = ModelConfig(**config)
        affine = Affine.fit(rows)
        if all(same == rows[0] for same in rows):
            raise DetectionError("constant_history")
        builder = jpype.JClass("com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest").builder()
        cls = jpype.JClass
        model = (builder.dimensions(config.dimensions).shingleSize(1)
            .forestMode(cls("com.amazon.randomcutforest.config.ForestMode").STANDARD)
            .transformMethod(cls("com.amazon.randomcutforest.config.TransformMethod").NONE)
            .numberOfTrees(config.trees).sampleSize(config.sample_size).outputAfter(config.output_after)
            .randomSeed(config.seed).timeDecay(1 / config.sample_size)
            .transformDecay(1 / config.sample_size).parallelExecutionEnabled(False)
            .internalShinglingEnabled(True).storeSequenceIndexesEnabled(False)
            .centerOfMassEnabled(False).boundingBoxCacheFraction(1.0).initialAcceptFraction(1.0)
            .anomalyRate(.01).autoAdjust(False).zFactor(3.).scoreDifferencing(.5).alertOnce(False)
            .startNormalization(config.settings["startNormalization"]).stopNormalization(2147483647)
            .weights(jpype.JArray(jpype.JDouble)([1.] * config.dimensions))
            .scoringStrategy(cls("com.amazon.randomcutforest.parkservices.config.ScoringStrategy").EXPECTED_INVERSE_DEPTH)
            .build())
        model.setLowerThreshold(.8)
        model.setInitialThreshold(1.5)
        adapted = AwsModel(config, keys, rule_hash, affine, [], model, identity)
        for row in rows:
            adapted.process(row)
        if model.getForest().getTotalUpdates() < config.minimum_inputs or not model.getForest().isOutputReady():
            raise DetectionError("model_not_ready")
        return adapted.checkpoint("warmup")

    def restore(self, checkpoint, config, keys, rule_hash):
        # Validate envelope and identity before feeding anything into Java.
        if len(canonical(checkpoint).encode()) > MAX_BYTES:
            raise DetectionError("resource_budget:checkpoint_bytes")
        try:
            body = checkpoint["body"]
            if checkpoint["sha256"] != digest(body):
                raise DetectionError("checkpoint_checksum")
            if body["config"] != config or body["keys"] != keys or body["rule_hash"] != rule_hash:
                raise DetectionError("checkpoint_config_mismatch")
            if body["settings"] != ModelConfig(**config).settings:
                raise DetectionError("checkpoint_settings_mismatch")
            if body["runtime"]["format"] != 1:
                raise DetectionError("checkpoint_version")
            forest = body["mapper"]["forestState"]
            if (forest["dimensions"] != config["dimensions"]
                    or forest["numberOfTrees"] != config["trees"]
                    or forest["sampleSize"] != config["sample_size"]
                    or forest["outputAfter"] != config["output_after"]
                    or forest["shingleSize"] != 1
                    or body["mapper"]["forestMode"] != "STANDARD"
                    or body["mapper"]["transformMethod"] != "NONE"):
                raise DetectionError("checkpoint_mapper_config")
            jpype, identity = runtime()
            if identity != body["runtime"]:
                raise DetectionError("checkpoint_runtime_mismatch")
            state_cls = jpype.JClass(
                "com.amazon.randomcutforest.parkservices.state.ThresholdedRandomCutForestState")
            codec = jpype.JClass("com.fasterxml.jackson.databind.ObjectMapper")()
            mapper = jpype.JClass(
                "com.amazon.randomcutforest.parkservices.state.ThresholdedRandomCutForestMapper")()
            model = mapper.toModel(codec.readValue(canonical(body["mapper"]), state_cls.class_),
                                   jpype.JLong(config["seed"]))
            if int(model.getForest().getTotalUpdates()) != body["updates"]:
                raise DetectionError("checkpoint_updates")
            affine = body["affine"]
            if (affine["algorithm"] != "min_range_decimal_float32_v1"
                    or len(affine["center"]) != len(keys) or len(affine["scale"]) != len(keys)):
                raise DetectionError("checkpoint_affine")
            transform = Affine(tuple(Decimal(x) for x in affine["center"]),
                               tuple(Decimal(x) for x in affine["scale"]))
            if (any(not x.is_finite() for x in transform.center)
                    or any(not x.is_finite() or x <= 0 for x in transform.scale)
                    or len(body["history"]) > config["sample_size"]):
                raise DetectionError("checkpoint_affine_history")
            return AwsModel(ModelConfig(**config), keys, rule_hash, transform,
                            body["history"], model, identity)
        except DetectionError:
            raise
        except Exception as error:
            raise DetectionError("checkpoint_corrupt") from error
