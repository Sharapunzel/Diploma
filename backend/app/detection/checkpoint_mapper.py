"""Structural contract of the official pinned AWS 4.4.0 mapper writer.

Current-writer buffers, references and partial-tree reconstruction are verified
without Java. Unknown versions or unprovable content cannot release history.
"""

from .checkpoint_codec import point_store_content, reference_counts, tree_content
from .checkpoint_values import counts, deviations, integer, number, require, state, vector


def descriptor_content(value, dimensions, check):
    state(value)
    require(value["strategy"] == "EXPECTED_INVERSE_DEPTH" and value["correctionMode"] in {
        "NONE", "NOISE", "MULTI_MODE", "ANOMALY_IN_SHINGLE", "CONDITIONAL_FORECAST",
        "FORECAST", "DATA_DRIFT", "ALERT_ONCE"})
    for key in ("score", "lastScore", "threshold", "anomalyGrade", "transformDecay"):
        number(value[key])
    for key in ("internalTimeStamp", "inputTimeStamp"):
        integer(value[key], 0, 2**63 - 1)
    integer(value["relativeIndex"], -(2**31), 2**31 - 1)
    for key in ("point", "expectedPoint", "currentInput", "shift", "scale", "postShift", "postDeviations"):
        if value[key] is not None:
            vector(value[key], check, length=dimensions)
    attribution = state(value["attribution"])
    require((attribution["high"] is None) == (attribution["low"] is None))
    if attribution["high"] is not None:
        vector(attribution["high"], check, length=dimensions)
        vector(attribution["low"], check, length=dimensions)


def validate_mapper(mapper, config, updates, check):
    check()
    state(mapper, "3.8")
    require(mapper["forestMode"] == "STANDARD" and mapper["transformMethod"] == "NONE"
            and mapper["scoringStrategy"] == "EXPECTED_INVERSE_DEPTH")
    forest = state(mapper["forestState"], "4.0")
    for key, expected in {"dimensions": config.dimensions, "numberOfTrees": config.trees,
            "sampleSize": config.sample_size, "outputAfter": config.output_after,
            "shingleSize": 1, "totalUpdates": updates}.items():
        integer(forest[key])
        require(forest[key] == expected)
    for key, expected in {"compressed": True, "partialTreeState": True, "compact": True,
            "internalShinglingEnabled": True, "storeSequenceIndexesEnabled": False,
            "centerOfMassEnabled": False, "saveTreeStateEnabled": True,
            "saveSamplerStateEnabled": True, "saveCoordinatorStateEnabled": True}.items():
        require(forest[key] is expected)
    require(forest["precision"] == "FLOAT_32" and forest["timeDecay"] == 1 / config.sample_size
            and forest["boundingBoxCacheFraction"] == 1)
    number(forest["timeDecay"])
    number(forest["boundingBoxCacheFraction"])
    context = state(forest["executionContext"])
    require(context["parallelExecutionEnabled"] is False)
    integer(context["threadPoolSize"], 0)
    store = state(forest["pointStoreState"], "3.0")
    counts(store, dimensions=config.dimensions, shingleSize=1)
    require(store["dimensions"] == config.dimensions and store["shingleSize"] == 1
            and store["precision"] == "FLOAT_32" and store["compressed"] is True
            and store["internalShinglingEnabled"] is True)
    for key in ("capacity", "currentStoreCapacity", "indexCapacity"):
        integer(store[key], 1)
    require(store["currentStoreCapacity"] <= store["capacity"]
            and store["indexCapacity"] <= store["capacity"])
    integer(store["startOfFreeSegment"], 0, store["currentStoreCapacity"] * config.dimensions)
    vector(store["internalShingle"], check, length=config.dimensions)
    integer(store["lastTimeStamp"], 0, 2**63 - 1)
    points = point_store_content(store, config, check)
    samplers, trees = forest["compactSamplerStates"], forest["compactRandomCutTreeStates"]
    require(isinstance(samplers, list) and len(samplers) == config.trees
            and isinstance(trees, list) and len(trees) == config.trees)
    for sampler, tree in zip(samplers, trees, strict=True):
        check()
        state(sampler, "2.0")
        counts(sampler, capacity=config.sample_size, size=config.sample_size)
        require(sampler["capacity"] == config.sample_size and sampler["compressed"] is True
                and sampler["storeSequenceIndicesEnabled"] is False
                and sampler["timeDecay"] == 1 / config.sample_size
                and sampler["initialAcceptFraction"] == 1)
        number(sampler["timeDecay"])
        number(sampler["initialAcceptFraction"])
        vector(sampler["weight"], check, length=sampler["size"])
        for weight in sampler["weight"]:
            check()
            require(abs(weight) <= 3.4028235e38)
        for key in ("randomSeed", "maxSequenceIndex", "sequenceIndexOfMostRecentTimeDecayUpdate"):
            integer(sampler[key], -(2**63), 2**63 - 1)
        require(sampler["sequenceIndex"] is None)
        state(tree, "3.0")
        counts(tree, dimensions=config.dimensions)
        number(tree["boundingBoxCacheFraction"])
        require(tree["dimensions"] == config.dimensions and tree["partialTreeState"] is True
                and tree["storeSequenceIndexesEnabled"] is False and tree["centerOfMassEnabled"] is False
                and tree["boundingBoxCacheFraction"] == 1)
        integer(tree["root"], 0)
        integer(tree["maxSize"], 1, config.sample_size)
        nodes = state(tree["nodeStoreState"], "3.0")
        require(nodes["precision"] == "FLOAT_32" and nodes["compressed"] is True
                and nodes["partialTreeStateEnabled"] is True)
        integer(nodes["capacity"], 0, config.sample_size)
        integer(nodes["size"], 0, nodes["capacity"])
        require(type(nodes["canonicalAndNotALeaf"]) is bool)
        integer(tree["seed"], -(2**63), 2**63 - 1)
        integer(tree["outputAfter"], 0)
        tree_content(tree, sampler, points, config, check)
    reference_counts(points, check)
    preprocessors = mapper["preprocessorStates"]
    require(isinstance(preprocessors, list) and len(preprocessors) == 1)
    pre = state(preprocessors[0], "4.1")
    counts(pre, dimensions=config.dimensions, inputLength=config.dimensions, shingleSize=1)
    number(pre["timeDecay"])
    require(pre["forestMode"] == "STANDARD" and pre["transformMethod"] == "NONE"
            and pre["dimensions"] == config.dimensions and pre["inputLength"] == config.dimensions
            and pre["shingleSize"] == 1 and pre["timeDecay"] == 1 / config.sample_size)
    require(pre["imputationMethod"] == "PREVIOUS" and pre["normalizeTime"] is False
            and pre["fastForward"] is False and pre["startNormalization"] == config.settings["startNormalization"]
            and pre["stopNormalization"] == config.settings["stopNormalization"])
    # getWeights() appends weightTime even in STANDARD/shingleSize=1. The
    # configured unit vector has d values; its persisted mapper form has d+1.
    vector(pre["weights"], check, length=config.dimensions + 1)
    for key in ("lastShingledPoint", "lastShingledInput"):
        vector(pre[key], check, length=config.dimensions)
    require(all(value == 1 for value in pre["weights"]))
    require(isinstance(pre["previousTimeStamps"], list) and len(pre["previousTimeStamps"]) == 1)
    integer(pre["previousTimeStamps"][0], 0, 2**63 - 1)
    for key in ("clipFactor", "useImputedFraction"):
        number(pre[key])
    if pre["defaultFill"] is not None:
        vector(pre["defaultFill"], check, length=config.dimensions)
    require(pre["initialValues"] is None and pre["initialTimeStamps"] is None)
    integer(pre["numberOfImputed"], 0)
    for key in ("valuesSeen", "internalTimeStamp"):
        integer(pre[key], 0)
    for key, length in (("deviationStates", 5 * config.dimensions), ("dataQualityStates", 1),
                        ("timeStampDeviationStates", 5)):
        deviations(pre[key], check, length)
    predictor = state(mapper["predictorCorrectorState"], "3.8")
    counts(predictor, baseDimension=config.dimensions)
    require(predictor["baseDimension"] == config.dimensions and predictor["autoAdjust"] is False
            and predictor["lastStrategy"] == "EXPECTED_INVERSE_DEPTH")
    vector(predictor["lastScore"], check, length=2)
    integer(predictor["randomSeed"], -(2**63), 2**63 - 1)
    integer(predictor["numberOfAttributors"], 1)
    integer(predictor["runLength"], 0)
    for key in ("noiseFactor", "samplingSuppport"):
        number(predictor[key])
    require(type(predictor["ignoreDrift"]) is bool and predictor["deviationStates"] is None)
    require(0 <= predictor["samplingSuppport"] < .2)
    if predictor["modeInformation"] is not None:
        vector(predictor["modeInformation"], check)
    vector(predictor["ignoreNearExpected"], check, length=4 * config.dimensions)
    require(all(value >= 0 for value in predictor["ignoreNearExpected"]))
    if predictor["lastDescriptor"] is not None:
        descriptor_content(predictor["lastDescriptor"], config.dimensions, check)
    thresholders = predictor["thresholderStates"]
    require(isinstance(thresholders, list) and len(thresholders) == 2)
    for item in thresholders:
        check()
        state(item)
        integer(item["count"], 0)
        integer(item["minimumScores"], 0)
        require(type(item["autoThreshold"]) is bool)
        # Pinned ObjectMapper uses legacy bean naming: getZFactor -> zfactor.
        # ModelConfig.settings is our separate envelope and retains zFactor.
        for key in ("zfactor", "lowerThreshold", "absoluteThreshold", "initialThreshold", "horizon"):
            number(item[key])
        require(0 <= item["horizon"] <= 1 and item["zfactor"] > 0)
        deviations(item["deviationStates"], check, 3)
    descriptor_content(mapper["lastDescriptorState"], config.dimensions, check)
    check()
