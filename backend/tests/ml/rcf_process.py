"""Fresh-process probe of the official mapper, intentionally without adapter fixes."""

import json
import os
import sys
from pathlib import Path

import jpype


def main():
    request = json.load(sys.stdin)
    jars = sorted(Path(os.environ["RCF_CLASSPATH"]).glob("*.jar"))
    if not jars:
        raise RuntimeError("Official RCF JARs are required")
    jpype.startJVM("-Xms32m", "-Xmx512m", classpath=[str(p) for p in jars])
    cls = jpype.JClass
    mapper = cls("com.amazon.randomcutforest.parkservices.state.ThresholdedRandomCutForestMapper")()
    state_class = cls(
        "com.amazon.randomcutforest.parkservices.state.ThresholdedRandomCutForestState"
    )
    codec = cls("com.fasterxml.jackson.databind.ObjectMapper")()
    if request.get("state") is not None:
        state = codec.readValue(json.dumps(request["state"]), state_class.class_)
        model = mapper.toModel(state, jpype.JLong(42))
    else:
        builder = cls("com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest").builder()
        model = (builder.dimensions(request["dimensions"]).shingleSize(1)
                 .forestMode(cls("com.amazon.randomcutforest.config.ForestMode").STANDARD)
                 .transformMethod(cls("com.amazon.randomcutforest.config.TransformMethod").NONE)
                 .numberOfTrees(100).sampleSize(256).outputAfter(256)
                 .randomSeed(42).timeDecay(1 / 256).parallelExecutionEnabled(False).build())
    results = []
    start = request.get("start", 0)
    for index, point in enumerate(request["points"], start):
        descriptor = model.process(jpype.JArray(jpype.JDouble)(point), jpype.JLong(index))
        attribution = descriptor.getAttribution()
        expected = descriptor.getExpectedValuesList()
        results.append({
            "score": float(descriptor.getRCFScore()),
            "grade": float(descriptor.getAnomalyGrade()),
            "confidence": float(descriptor.getDataConfidence()),
            "expected": None if expected is None else [
                None if row is None else [float(x) for x in row] for row in expected
            ],
            "attribution": None if attribution is None else {
                "high": [float(x) for x in attribution.high],
                "low": [float(x) for x in attribution.low],
            },
            "updates": int(model.getForest().getTotalUpdates()),
        })
    state = json.loads(str(codec.writeValueAsString(mapper.toState(model))))
    print(json.dumps({"results": results, "state": state,
                      "java": str(cls("java.lang.System").getProperty("java.runtime.version")),
                      "jpype": jpype.__version__}))


if __name__ == "__main__":
    main()
