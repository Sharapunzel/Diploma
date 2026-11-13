import { expect, it } from "vitest";
import {
  parseAggregate,
  printAggregate,
  type JsonObject,
} from "../../app/api/aggregateJson";
import { moveDefinition, referenceErrors } from "./rule";

it.each(["count", "derive"])(
  "restores exact coordinates after off/on with %s and preserves dependencies through JSON",
  (op) => {
    const original: JsonObject = {
      features: [
        { key: "a", name: "A", op: "count" },
        {
          key: "b",
          name: "B",
          op,
          ...(op === "derive" ? { expr: { ref: "a" } } : {}),
        },
      ],
    };
    const order = ["a", "b"];
    const off = moveDefinition(original, "a", false);
    expect((off.features as JsonObject[]).map((f) => f.key)).toEqual(["b"]);
    expect((off.auxiliary as JsonObject[]).map((f) => f.key)).toEqual(["a"]);
    expect(referenceErrors(off)).toEqual([]);
    const restored = moveDefinition(off, "a", true);
    expect(restored).toEqual(original);
    expect(referenceErrors(restored)).toEqual([]);
    const roundtrip = parseAggregate(printAggregate(off)) as JsonObject;
    const on = moveDefinition(roundtrip, "a", true, order);
    expect(on).toEqual(original);
    expect(referenceErrors(on)).toEqual([]);
  },
);
