"""Current mapper structure for neutral validation tests; no numerical restore.

The ML service regression separately feeds an actual official mapper checkpoint
through this validator and restores it after GC. These small synthetic buffers
exercise structure only and must never be used as a fake Java forest.
"""

import base64
import struct
from copy import deepcopy

from app.detection.runtime_identity import expected_runtime


def pack(values):
    """Current ArrayPacking writer layout, used only to construct parser inputs."""
    if len(values) < 3:
        return list(values)
    low, high = min(values), max(values)
    if low == high:
        return [low, high, len(values)]
    base, count, power = high - low + 1, 0, high - low + 1
    while power < 2**31 - 1:
        power *= base
        count += 1
    count = max(count, 1)
    words = []
    for start in range(0, len(values), count):
        word = 0
        for value in reversed(values[start:start + count]):
            word = word * base + value - low
        words.append(word)
    return [low, high, len(values), *words]


def float_buffer(values):
    return base64.b64encode(struct.pack(">" + "f" * len(values), *values)).decode("ascii")


def mapper_structure(config, updates):
    d, n = config.dimensions, config.sample_size
    deviation = {"discount": 1 / n, "weight": 1., "sumSquared": 1., "sum": 1., "count": 1}
    tree = {"version": "3.0", "dimensions": d, "partialTreeState": True,
        "storeSequenceIndexesEnabled": False, "centerOfMassEnabled": False,
        "boundingBoxCacheFraction": 1., "root": 0, "maxSize": n, "seed": 42, "outputAfter": 1,
        "nodeStoreState": {"version": "3.0", "precision": "FLOAT_32", "compressed": True,
            "partialTreeStateEnabled": True, "capacity": n - 1, "size": 1,
            "canonicalAndNotALeaf": True, "leftIndex": [0], "rightIndex": [0],
            "cutDimension": [0], "cutValueData": float_buffer([.5])}}
    sample = [i % 2 for i in range(n)]
    sampler = {"version": "2.0", "size": n, "capacity": n, "compressed": True,
        "storeSequenceIndicesEnabled": False, "timeDecay": 1 / n, "initialAcceptFraction": 1.,
        "weight": [1.] * n, "pointIndex": pack(sample), "randomSeed": 42,
        "maxSequenceIndex": updates - 1, "sequenceIndexOfMostRecentTimeDecayUpdate": 0,
        "sequenceIndex": None}
    references = [config.trees * sample.count(i) for i in range(2)]
    duplicates = [value for i, count in enumerate(references) if count > config.trees
        for value in (i, count - config.trees)]
    forest = {"version": "4.0", "dimensions": d, "numberOfTrees": config.trees,
        "sampleSize": n, "outputAfter": config.output_after, "shingleSize": 1,
        "totalUpdates": updates, "compressed": True, "partialTreeState": True,
        "compact": True, "internalShinglingEnabled": True, "storeSequenceIndexesEnabled": False,
        "centerOfMassEnabled": False, "saveTreeStateEnabled": True, "saveSamplerStateEnabled": True,
        "saveCoordinatorStateEnabled": True, "precision": "FLOAT_32", "timeDecay": 1 / n,
        "boundingBoxCacheFraction": 1.,
        "executionContext": {"parallelExecutionEnabled": False, "threadPoolSize": 0},
        "pointStoreState": {"version": "3.0", "dimensions": d, "shingleSize": 1,
            "precision": "FLOAT_32", "compressed": True, "internalShinglingEnabled": True,
            "capacity": max(n * config.trees + 1, 2 * n), "currentStoreCapacity": 2 * n,
            "indexCapacity": 2 * n, "rotationEnabled": False, "directLocationMap": False,
            "dynamicResizingEnabled": True, "lastTimeStamp": updates,
            "startOfFreeSegment": 2 * d, "refCount": pack([config.trees] * 2 + [0] * (2 * n - 2)),
            "duplicateRefs": pack(duplicates), "locationList": [0, 1],
            "pointData": float_buffer([0.] * d + [1.] * d), "internalShingle": [0.] * d},
        "compactSamplerStates": [deepcopy(sampler) for _ in range(config.trees)],
        "compactRandomCutTreeStates": [deepcopy(tree) for _ in range(config.trees)]}
    descriptor = {"score": 1., "lastScore": 1., "threshold": 1., "anomalyGrade": 0.,
        "internalTimeStamp": updates, "inputTimeStamp": updates - 1, "relativeIndex": 0,
        "strategy": "EXPECTED_INVERSE_DEPTH", "correctionMode": "NONE", "transformDecay": 1 / n,
        "point": None, "expectedPoint": None, "currentInput": None, "shift": None, "scale": None,
        "postShift": None, "postDeviations": None, "attribution": {"high": None, "low": None}}
    thresholder = {"count": updates, "minimumScores": 1, "autoThreshold": True,
        "zfactor": 3., "lowerThreshold": .8, "absoluteThreshold": .8, "initialThreshold": 1.5,
        "horizon": .5, "deviationStates": [deepcopy(deviation) for _ in range(3)]}
    return {"version": "3.8", "forestState": forest, "forestMode": "STANDARD",
        "transformMethod": "NONE", "scoringStrategy": "EXPECTED_INVERSE_DEPTH",
        "preprocessorStates": [{"version": "4.1", "forestMode": "STANDARD", "transformMethod": "NONE",
            "dimensions": d, "inputLength": d, "shingleSize": 1, "timeDecay": 1 / n,
            "weights": [1.] * (d + 1), "lastShingledPoint": [0.] * d, "lastShingledInput": [0.] * d,
            "valuesSeen": updates, "internalTimeStamp": updates,
            "imputationMethod": "PREVIOUS", "normalizeTime": False, "fastForward": False,
            "startNormalization": config.settings["startNormalization"], "stopNormalization": 2147483647,
            "previousTimeStamps": [updates - 1], "clipFactor": 10., "useImputedFraction": 0.,
            "defaultFill": None, "initialValues": None, "initialTimeStamps": None, "numberOfImputed": 0,
            "deviationStates": [deepcopy(deviation) for _ in range(5 * d)],
            "dataQualityStates": [deepcopy(deviation)],
            "timeStampDeviationStates": [deepcopy(deviation) for _ in range(5)]}],
        "predictorCorrectorState": {"version": "3.8", "baseDimension": d, "autoAdjust": False,
            "lastStrategy": "EXPECTED_INVERSE_DEPTH", "lastScore": [1., 1.], "randomSeed": 42,
            "numberOfAttributors": d, "runLength": 0, "noiseFactor": 1., "samplingSuppport": .1,
            "ignoreDrift": False, "deviationStates": None, "modeInformation": None,
            "ignoreNearExpected": [0.] * (4 * d), "lastDescriptor": None,
            "thresholderStates": [deepcopy(thresholder), deepcopy(thresholder)]},
        "lastDescriptorState": descriptor}


def checkpoint_body(model, config):
    return {"config": model["config"], "keys": list(model["feature_keys"]),
        "rule_hash": model["rule_hash"], "settings": config.settings, "runtime": expected_runtime(),
        "updates": config.minimum_inputs, "boundary": "warmup",
        "mapper": mapper_structure(config, config.minimum_inputs),
        "affine": {"algorithm": "min_range_decimal_float32_v1", "center": ["1"] * config.dimensions,
            "scale": ["1"] * config.dimensions},
        "history": [{"exact": ["1"] * config.dimensions, "ml": [0.] * config.dimensions}]}
