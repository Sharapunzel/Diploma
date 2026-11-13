import { expect, it, vi } from "vitest";
import { ApiError } from "../../app/api/client";
import type {
  Aggregate,
  AggregatesApi,
  Operator,
  Preview,
  Validation,
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
} from "./rule";

const boundary = "1900-01-01T00:00:00.123456+05:21:10";
it.each(["count", "derive"])(
  "preserves remembered coordinates through cloned visual edits with %s",
  (op) => {
    const store = new DraftStore({} as AggregatesApi);
    store.setOperators([
      { name: "count", kind: "feature", parameters: [] },
      { name: "derive", kind: "feature", parameters: ["expr"] },
      { name: "add", kind: "expression", parameters: ["args"] },
    ] as unknown as Operator[]);
    const initial = parseAggregate(
      `{"features":[{"key":"a","name":"A","op":"count"},{"key":"b","name":"B","op":"${op}"${op === "derive" ? ',"expr":{"op":"add","args":[{"ref":"a"},{"op":"add","args":[{"value":9007199254740993},{"value":1.0}]}]}' : ""}}]}`,
    ) as JsonObject;
    store.setRule(initial);
    expect(store.localError).toBe("");
    store.setRule(moveDefinition(store.rule, "a", false));
    const edited = clone(store.rule);
    (edited.features as JsonObject[])[0].name = "B2";
    store.setRule(edited);
    store.switchMode("json");
    store.switchMode("visual");
    expect(definitionOrder(store.rule)).toEqual(["a", "b"]);
    store.setRule(moveDefinition(store.rule, "a", true));
    const expected = clone(initial);
    (expected.features as JsonObject[])[1].name = "B2";
    expect(printAggregate(store.rule)).toBe(printAggregate(expected));
    expect(referenceErrors(store.rule)).toEqual([]);
    store.importText(printAggregate(edited));
    expect(definitionOrder(store.rule)).toEqual(["b", "a"]);
    store.dispose();
  },
);
it("preserves legacy output coordinates through visual/JSON transitions and adopts imported order", () => {
  const store = new DraftStore({} as AggregatesApi);
  store.setOperators([
    { name: "count", kind: "feature", parameters: [] },
    { name: "derive", kind: "feature", parameters: ["expr"] },
  ] as unknown as Operator[]);
  const original: JsonObject = {
    features: [
      { key: "a", name: "A", op: "count" },
      { key: "b", name: "B", op: "derive", expr: { ref: "a" } },
    ],
  };
  store.setRule(original);
  store.setRule(moveDefinition(store.rule, "a", false));
  expect(referenceErrors(store.rule)).toEqual([]);
  store.switchMode("json");
  store.switchMode("visual");
  store.setRule(moveDefinition(store.rule, "a", true));
  expect(printAggregate(store.rule)).toBe(printAggregate(original));
  expect(referenceErrors(store.rule)).toEqual([]);
  store.importText(
    '{"features":[{"key":"b","name":"B","op":"count"},{"key":"a","name":"A","op":"count"}]}',
  );
  store.switchMode("visual");
  expect(definitionOrder(store.rule)).toEqual(["b", "a"]);
  store.dispose();
});
const validProcess = {
  name: "Число событий",
  source: "source",
  duration: "300",
  timezone: "Asia/Yekaterinburg",
  boundary,
};
function draft(api: AggregatesApi) {
  const store = new DraftStore(api);
  store.setOperators([
    { name: "count", kind: "feature", parameters: [] },
  ] as unknown as Operator[]);
  for (const [key, value] of Object.entries(validProcess))
    store.setField(key as keyof typeof validProcess, value);
  return store;
}

it.each([
  ["name", ""],
  ["name", "   "],
  ["name", "a".repeat(201)],
  ["source", ""],
  ["duration", "0"],
  ["duration", "604801"],
  ["duration", "1.5"],
  ["timezone", ""],
  ["timezone", "Missing/Zone"],
  ["boundary", ""],
  ["boundary", "2026-02-30T00:00:00Z"],
  ["boundary", "2026-01-01T00:00:00"],
] as const)(
  "requires a valid process %s before checking the rule (%s)",
  async (field, value) => {
    const api = { validate: vi.fn() } as unknown as AggregatesApi;
    const store = draft(api);
    const text = store.text;
    expect(store.processValid).toBe(true);
    store.setField(field, value);
    expect(store.processValid).toBe(false);
    expect(store.processErrors[field]).toBeTruthy();
    await store.validate();
    expect(api.validate).not.toHaveBeenCalled();
    expect(store.localError).toBeTruthy();
    store.setField(field, validProcess[field]);
    expect(store.processValid).toBe(true);
    expect(store.text).toBe(text);
    expect(store.localError).toBe("");
    store.dispose();
  },
);

it("checks the process and rule before choosing a diagnostic window, preserving the exact boundary", async () => {
  const api = {
    validate: vi.fn().mockResolvedValue({ rule_hash: "hash" } as Validation),
    preview: vi.fn(),
  } as unknown as AggregatesApi;
  const store = draft(api);
  expect(store.window).toBe("");
  await store.validate();
  expect(api.validate).toHaveBeenCalledWith(
    expect.objectContaining({
      schedule: {
        boundary,
        timezone: "Asia/Yekaterinburg",
        duration_seconds: 300,
      },
    }),
    expect.any(AbortSignal),
  );
  expect(store.validation).toBeTruthy();
  await store.runPreview();
  expect(api.preview).not.toHaveBeenCalled();
  expect(store.localError).toContain("начало завершённого окна");
  expect(store.canCreate).toBe(false);
  store.dispose();
});

it("replays an unknown request with its exact body/key while the edited process is invalid", async () => {
  const api = {
    validate: vi.fn().mockResolvedValue({ rule_hash: "hash" } as Validation),
    preview: vi.fn().mockResolvedValue({ rule_hash: "hash" } as Preview),
    create: vi
      .fn()
      .mockRejectedValueOnce(new ApiError(503, "http_error"))
      .mockResolvedValue({ id: "confirmed-old" } as Aggregate),
  } as unknown as AggregatesApi;
  const store = draft(api);
  store.setField("window", "1900-01-01T00:00:00.123456+05:21:10");
  await store.validate();
  await store.runPreview();
  await store.create();
  expect(store.unknownCreate).toBe(true);
  store.setField("name", "");
  store.setField("duration", "bad");
  const text =
    '{"features":[{"key":"new_events","name":"Новый признак","op":"count"}]}';
  store.setText(text);
  expect(store.processValid).toBe(false);
  expect(store.canCreate).toBe(true);
  expect(await store.create()).toBeUndefined();
  const calls = vi.mocked(api.create).mock.calls;
  expect(calls).toHaveLength(2);
  expect(printAggregate(calls[1][0])).toBe(printAggregate(calls[0][0]));
  expect(calls[1][1]).toBe(calls[0][1]);
  expect(calls[1][0].schedule.boundary).toBe(boundary);
  expect(store.unknownCreate).toBe(false);
  expect(store.dirty).toBe(true);
  expect(store.name).toBe("");
  expect(store.duration).toBe("bad");
  expect(store.text).toBe(text);
  store.setField("name", "Новый агрегат");
  store.setField("duration", "600");
  expect(store.processValid).toBe(true);
  expect(store.text).toBe(text);
  expect(store.canCreate).toBe(false);
  store.dispose();
});
