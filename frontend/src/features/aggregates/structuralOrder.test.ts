import { expect, it, vi } from "vitest";
import type {
  AggregatesApi,
  AggregateInput,
  Operator,
} from "../../app/api/aggregates";
import {
  parseAggregate,
  printAggregate,
  type JsonObject,
} from "../../app/api/aggregateJson";
import { DraftStore } from "./DraftStore";
import {
  clone,
  definitionOrder,
  moveDefinition,
  referenceErrors,
  retainDefinitionOrder,
} from "./rule";

const operators = [
  { name: "count", kind: "feature", parameters: [] },
  { name: "derive", kind: "feature", parameters: ["expr"] },
  { name: "formula", kind: "feature", parameters: ["expr"] },
] as unknown as Operator[];
function draft(rule: JsonObject) {
  const store = new DraftStore({} as AggregatesApi);
  store.setOperators(operators);
  store.importText(printAggregate(rule));
  store.switchMode("visual");
  return store;
}
it.each(["auxiliary", "formula", "auxiliary-formula"])(
  "guards deletion of last %s in actual DraftStore and allows corrected wire",
  (marker) => {
    const rule: JsonObject = {
      features: [
        { key: "b", name: "B", op: "derive", expr: { ref: "a" } },
        { key: "a", name: "A", op: "count" },
      ],
    };
    const c: JsonObject = {
      key: "c",
      name: "C",
      op: marker === "auxiliary" ? "count" : "formula",
      ...(marker === "auxiliary"
        ? {}
        : { expr: { value: parseAggregate("1.0") } }),
    };
    if (marker === "formula") (rule.features as JsonObject[]).push(c);
    else rule.auxiliary = [c];
    const store = draft(rule);
    const before = store.text;
    const next = clone(store.rule);
    if (marker === "formula")
      next.features = (next.features as JsonObject[]).filter(
        (f) => f.key !== "c",
      );
    else delete next.auxiliary;
    expect(() => store.setRule(next)).toThrow("Действие изменит");
    expect(store.text).toBe(before);
    expect(referenceErrors(store.rule)).toEqual([]);
    const corrected = clone(rule);
    const list = corrected.features as JsonObject[];
    [list[0], list[1]] = [list[1], list[0]];
    store.importText(printAggregate(corrected));
    store.switchMode("visual");
    const allowed = clone(store.rule);
    if (marker === "formula")
      allowed.features = (allowed.features as JsonObject[]).filter(
        (f) => f.key !== "c",
      );
    else delete allowed.auxiliary;
    store.setRule(allowed);
    expect(referenceErrors(store.rule)).toEqual([]);
    store.dispose();
  },
);
function assertRule(
  store: DraftStore,
  outputs: string[],
  auxiliary: string[] = [],
) {
  const wire = parseAggregate(store.text) as JsonObject;
  expect((wire.features as JsonObject[]).map((f) => f.key)).toEqual(outputs);
  expect(((wire.auxiliary ?? []) as JsonObject[]).map((f) => f.key)).toEqual(
    auxiliary,
  );
  expect(referenceErrors(wire)).toEqual([]);
  expect(
    definitionOrder(store.rule).filter((k) => outputs.includes(k)),
  ).toEqual(outputs);
}
it.each(
  ["count", "derive"].flatMap((op) =>
    ["add", "delete"].map((action) => [op, action]),
  ),
)(
  "restores exact coordinates for %s after isolated visual %s",
  (op, action) => {
    const initial: JsonObject = {
      features: [
        { key: "a", name: "A", op: "count" },
        {
          key: "b",
          name: "B",
          op,
          ...(op === "derive" ? { expr: { ref: "a" } } : {}),
        },
        ...(action === "delete" ? [{ key: "c", name: "C", op: "count" }] : []),
      ],
    };
    const store = draft(initial);
    store.setRule(moveDefinition(store.rule, "a", false));
    assertRule(store, action === "delete" ? ["b", "c"] : ["b"], ["a"]);
    const edited = clone(store.rule);
    if (action === "add")
      (edited.features as JsonObject[]).push({
        key: "c",
        name: "C",
        op: "count",
      });
    else
      edited.features = (edited.features as JsonObject[]).filter(
        (f) => f.key !== "c",
      );
    store.setRule(edited);
    assertRule(store, action === "add" ? ["b", "c"] : ["b"], ["a"]);
    store.setRule(moveDefinition(store.rule, "a", true));
    assertRule(store, action === "add" ? ["a", "b", "c"] : ["a", "b"]);
    store.dispose();
  },
);
it.each(["count", "derive"])(
  "retains %s coordinates through multiple visual additions and deletions",
  (op) => {
    const initial = parseAggregate(
      `{"features":[{"key":"a","name":"A","op":"count"},{"key":"b","name":"B","op":"${op}"${op === "derive" ? ',"expr":{"ref":"a"}' : ""}},{"key":"c","name":"C","op":"count"}]}`,
    ) as JsonObject;
    const store = draft(initial);
    store.setRule(moveDefinition(store.rule, "a", false));
    assertRule(store, ["b", "c"], ["a"]);
    const added = clone(store.rule);
    (added.features as JsonObject[]).push({ key: "d", name: "D", op: "count" });
    retainDefinitionOrder(store.rule, added, true);
    store.setRule(added);
    assertRule(store, ["b", "c", "d"], ["a"]);
    store.setRule(moveDefinition(store.rule, "a", true));
    assertRule(store, ["a", "b", "c", "d"]);
    store.setRule(moveDefinition(store.rule, "a", false));
    const removed = clone(store.rule);
    removed.features = (removed.features as JsonObject[]).filter(
      (f) => f.key !== "c",
    );
    store.setRule(removed);
    assertRule(store, ["b", "d"], ["a"]);
    const more = clone(store.rule);
    (more.features as JsonObject[]).push({ key: "e", name: "E", op: "count" });
    store.setRule(more);
    assertRule(store, ["b", "d", "e"], ["a"]);
    store.switchMode("json");
    store.switchMode("visual");
    store.setOperators(operators);
    store.setRule(moveDefinition(store.rule, "a", true));
    assertRule(store, ["a", "b", "d", "e"]);
    expect((store.rule.features as JsonObject[])[1].expr).toEqual(
      (initial.features as JsonObject[])[1].expr,
    );
    store.dispose();
  },
);
const imported: JsonObject = {
  features: [{ key: "b", name: "B", op: "derive", expr: { ref: "a" } }],
  auxiliary: [{ key: "a", name: "A", op: "count" }],
};
it.each(["import", "copy"])(
  "rejects invalid final auxiliary promotion in a fresh %s before changing the document",
  async (mode) => {
    const donor = draft({
      features: [
        { key: "a", name: "A", op: "count" },
        { key: "b", name: "B", op: "derive", expr: { ref: "a" } },
      ],
    });
    donor.setRule(moveDefinition(donor.rule, "a", false));
    const input = {
      source_id: "source",
      schedule: {
        boundary: "2026-01-01T00:00:00Z",
        timezone: "UTC",
        duration_seconds: 300,
      },
      aggregate_rule: mode === "copy" ? donor.rule : imported,
    } as AggregateInput;
    const store =
      mode === "copy"
        ? new DraftStore(
            {
              copy: vi.fn().mockResolvedValue(input),
            } as unknown as AggregatesApi,
            "source-copy",
          )
        : draft({
            features: [
              { key: "a", name: "A", op: "count" },
              { key: "b", name: "B", op: "count" },
            ],
          });
    if (mode === "copy") {
      store.setOperators(operators);
      await store.loadCopy();
    } else {
      store.setRule(moveDefinition(store.rule, "a", false));
      store.importText(printAggregate(imported));
      store.switchMode("visual");
    }
    assertRule(store, ["b"], ["a"]);
    expect(definitionOrder(donor.rule)).toEqual(["a", "b"]);
    expect(definitionOrder(store.rule)).toEqual(["b", "a"]);
    const before = store.text;
    expect(() => store.setRule(moveDefinition(store.rule, "a", true))).toThrow(
      "Нельзя включить",
    );
    expect(store.text).toBe(before);
    assertRule(store, ["b"], ["a"]);
    store.dispose();
    donor.dispose();
  },
);
it("honors explicit wire reordering and keeps malformed input with its available AST", () => {
  const store = draft({
    features: [
      { key: "a", name: "A", op: "count" },
      { key: "b", name: "B", op: "count" },
    ],
  });
  store.setRule(moveDefinition(store.rule, "a", false));
  const ast = printAggregate(store.rule);
  store.importText('{"features":');
  expect(store.text).toBe('{"features":');
  expect(printAggregate(store.rule)).toBe(ast);
  expect(definitionOrder(store.rule)).toEqual(["a", "b"]);
  store.setText(
    '{"features":[{"key":"b","name":"B","op":"count"},{"key":"a","name":"A","op":"count"}]}',
  );
  store.switchMode("visual");
  assertRule(store, ["b", "a"]);
  const invalid =
    '{"features":[{"key":"b","name":"B","op":"derive","expr":{"ref":"a"}},{"key":"a","name":"A","op":"count"}]}';
  store.setText(invalid);
  expect(store.text).toBe(invalid);
  expect(referenceErrors(store.rule).length).toBeGreaterThan(0);
  store.dispose();
});

it.each(["features", "auxiliary"])(
  "allows independent deletion from %s while another grammar marker remains",
  (group) => {
    const rule: JsonObject = {
      features: [
        { key: "b", name: "B", op: "derive", expr: { ref: "a" } },
        { key: "a", name: "A", op: "count" },
        {
          key: "f",
          name: "F",
          op: "formula",
          expr: { value: parseAggregate("1.0") },
        },
      ],
      auxiliary: [{ key: "c", name: "C", op: "count" }],
    };
    const store = draft(rule);
    const next = clone(store.rule);
    if (group === "features")
      next.features = (next.features as JsonObject[]).filter(
        (f) => f.key !== "f",
      );
    else delete next.auxiliary;
    store.setRule(next);
    expect(printAggregate(store.rule)).toBe(printAggregate(next));
    expect(referenceErrors(store.rule)).toEqual([]);
    store.dispose();
  },
);

it.each([false, true])(
  "does not reject incomplete reference editing without a grammar transition (extended=%s)",
  (extended) => {
    const initial: JsonObject = {
      features: [
        { key: "a", name: "A", op: "count" },
        { key: "b", name: "B", op: "derive", expr: { ref: "a" } },
      ],
      ...(extended
        ? { auxiliary: [{ key: "c", name: "C", op: "count" }] }
        : {}),
    };
    const store = draft(initial);
    const edited = clone(store.rule);
    (edited.features as JsonObject[])[1].expr = { ref: "unfinished" };
    expect(() => store.setRule(edited)).not.toThrow();
    expect(printAggregate(store.rule)).toBe(printAggregate(edited));
    expect(referenceErrors(store.rule).length).toBeGreaterThan(0);
    store.dispose();
  },
);
