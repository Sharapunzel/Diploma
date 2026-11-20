"""Bounded validation of AWS 4.4.0 ArrayPacking and partial-tree contents.

Format authority: pinned Java/core state/{store,tree,sampler} mappers and
util/ArrayPacking.java in random-cut-forest-by-aws, tag 4.4.0-java. Float buffers
are raw big-endian IEEE float32, not a compressed stream. Integer arrays use the
writer's arithmetic packing. No forest is constructed or evaluated here.
"""

import base64
import math
import sys
from array import array
from dataclasses import dataclass

from .checkpoint_values import integer, require


def packed(values, maximum, check, *, length=None, max_length=None):
    """Decode only a canonical current-writer array, with a proven output cap."""
    require(isinstance(values, list))
    limit = length if length is not None else max_length
    integer(limit, 0)
    size = len(values) if len(values) < 3 else values[2]
    integer(size, 0, limit)
    require(length is None or size == length)
    for value in values:
        check()
        integer(value, 0, 2**31 - 1)
    if len(values) < 3:
        require(all(value <= maximum for value in values))
        return array("I", values)
    low, high = values[:2]
    require(size >= 3 and low <= high <= maximum)
    if low == high:
        require(len(values) == 3)
        result = array("I", [low]) * size
        check()
        return result
    base, per_word, power = high - low + 1, 0, high - low + 1
    while power < 2**31 - 1:
        check()
        power *= base
        per_word += 1
    per_word = max(per_word, 1)
    require(len(values) == 3 + (size + per_word - 1) // per_word)
    result = array("I")
    found_low, found_high = high, low
    for word in values[3:]:
        check()
        count = min(per_word, size - len(result))
        require(word < base**count)
        for _ in range(count):
            check()
            value, word = low + word % base, word // base
            found_low, found_high = min(found_low, value), max(found_high, value)
            result.append(value)
    require(len(result) == size and (found_low, found_high) == (low, high))
    return result


def floats(value, length, check):
    """Decode finite float32 in small chunks, without a Python-float matrix."""
    integer(length, 0)
    require(isinstance(value, str) and len(value) == ((4 * length + 2) // 3) * 4)
    result = array("f")
    require(result.itemsize == 4)
    for offset in range(0, len(value), 65536):
        check()
        block = value[offset:offset + 65536]
        decoded = base64.b64decode(block, validate=True)
        require(base64.b64encode(decoded).decode("ascii") == block and len(decoded) % 4 == 0)
        chunk = array("f")
        chunk.frombytes(decoded)
        if sys.byteorder == "little":
            chunk.byteswap()
        for item in chunk:
            check()
            require(math.isfinite(item))
        result.extend(chunk)
    require(len(result) == length)
    check()
    return result


@dataclass
class SavedPoints:
    values: array
    offsets: array
    references: array
    sampled: array
    dimensions: int


def point_store_content(store, config, check):
    d, total = config.dimensions, config.sample_size * config.trees
    # This is the exact capacity selected by the pinned RandomCutForest builder.
    capacity = max(total + 1, 2 * config.sample_size)
    require(store["capacity"] == capacity and store["rotationEnabled"] is False
            and store["directLocationMap"] is False and store["dynamicResizingEnabled"] is True)
    integer(store["indexCapacity"], 1, capacity)
    integer(store["currentStoreCapacity"], 1, capacity)
    index_capacity = store["indexCapacity"]
    references = packed(store["refCount"], config.trees, check, length=index_capacity)
    duplicates = packed(store["duplicateRefs"], max(total, index_capacity), check,
                        max_length=2 * index_capacity)
    require(len(duplicates) % 2 == 0)
    previous = -1
    for i in range(0, len(duplicates), 2):
        check()
        index, extra = duplicates[i:i + 2]
        require(previous < index < index_capacity and references[index] == config.trees and extra > 0)
        references[index] += extra
        require(references[index] <= total)
        previous = index
    active = 0
    for count in references:
        check()
        active += count > 0
    require(active > 0 and sum(references) == total)
    locations = packed(store["locationList"], store["currentStoreCapacity"] - 1, check, length=active)
    free = store["startOfFreeSegment"]
    # compact() precedes every write, and shingleSize=1 has no overlapping data.
    require(free == active * d and free <= store["currentStoreCapacity"] * d)
    values = floats(store["pointData"], free, check)
    offsets = array("I", [2**32 - 1]) * index_capacity
    next_location = 0
    for index, count in enumerate(references):
        check()
        if count:
            offset = locations[next_location] * d
            require(offset + d <= free)
            offsets[index] = offset
            next_location += 1
    return SavedPoints(values, offsets, references, array("I", [0]) * index_capacity, d)


def tree_content(tree, sampler, points, config, check):
    """Prove that native partial-tree insertion/reconstruction cannot fail.

    BFS bits produce one connected full binary tree. Every sampled vector is
    routed using the native <= cut rule; every leaf is filled and contains only
    identical float32 vectors. Therefore each reconstructed child bounding box
    obeys its cut, without allocating bounding boxes or a model.
    """
    indices = packed(sampler["pointIndex"], len(points.references) - 1, check,
                     length=sampler["size"])
    nodes, capacity = tree["nodeStoreState"], config.sample_size - 1
    require(tree["maxSize"] == config.sample_size and nodes["capacity"] == capacity)
    size = nodes["size"]
    integer(size, 0, capacity)
    children = array("i")
    if size:
        require(tree["root"] == 0 and nodes["canonicalAndNotALeaf"] is True)
        left = packed(nodes["leftIndex"], 1, check, length=size)
        right = packed(nodes["rightIndex"], 1, check, length=size)
        dimensions = packed(nodes["cutDimension"], config.dimensions - 1, check, length=size)
        cuts = floats(nodes["cutValueData"], size, check)
        next_node = 1
        for index in range(size):
            check()
            require(index < next_node)
            for side, flag in enumerate((left[index], right[index])):
                children.append(next_node if flag else -(2 * index + side + 1))
                next_node += bool(flag)
        require(next_node == size)
    else:
        require(nodes["canonicalAndNotALeaf"] is False
                and capacity <= tree["root"] < capacity + len(points.references)
                and all(nodes[key] is None for key in ("leftIndex", "rightIndex", "cutDimension", "cutValueData")))
    leaves = {}
    bits = memoryview(points.values).cast("B").cast("I")
    for point_index in indices:
        check()
        require(points.references[point_index] > 0)
        points.sampled[point_index] += 1
        offset, node = points.offsets[point_index], 0
        while size and node >= 0:
            check()
            side = int(points.values[offset + dimensions[node]] > cuts[node])
            node = children[2 * node + side]
        previous = leaves.get(node)
        if previous is not None and previous != offset:
            for coordinate in range(points.dimensions):
                check()
                require(bits[previous + coordinate] == bits[offset + coordinate])
        leaves[node] = offset
    require(len(leaves) == size + 1)
    if not size:
        require(tree["root"] - capacity in indices)


def reference_counts(points, check):
    for expected, sampled in zip(points.references, points.sampled, strict=True):
        check()
        require(expected == sampled)
