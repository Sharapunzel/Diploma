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
  executionFixture,
  executionPolicyFixture,
  previewFixture,
} from "../../app/api/aggregateTestData";
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

function measuredApi(preview: ReturnType<typeof vi.fn>) {
  return {
    validate: vi.fn().mockResolvedValue({
      rule_hash: "a".repeat(64),
      anchor_utc: boundary,
      duration_seconds: 300,
      feature_keys: ["events"],
      budget: { execution_policy: executionPolicyFixture() },
    }),
    preview,
  } as unknown as AggregatesApi;
}
it("keeps preview measurements scoped to the current draft and disposes them", async () => {
  const api = measuredApi(vi.fn().mockResolvedValue(previewFixture()));
  const store = draft(api);
  await store.validate();
  await store.runPreview();
  expect(store.lastExecution?.calculation_status).toBe("computed");
  store.setField("duration", "5");
  expect(store.lastExecution).toBeNull();
  expect(store.preview).toBeNull();
  store.dispose();
  expect(store.lastExecution).toBeNull();
});
it("retains safe server timeout diagnostics without enabling creation or automatic preview retry", async () => {
  const measurement = executionFixture({
    calculation_status: "uncomputed",
    calculation_reason: "execution_budget_exceeded",
    measured_through: "finalization",
  });
  const api = measuredApi(
    vi.fn().mockRejectedValue(
      new ApiError(422, "execution_budget_exceeded", "request", null, {
        execution: measurement,
      }),
    ),
  );
  const store = draft(api);
  await store.validate();
  await store.runPreview();
  expect(store.lastExecution).toEqual(measurement);
  expect(store.preview).toBeNull();
  expect(store.canCreate).toBe(false);
  expect(api.preview).toHaveBeenCalledTimes(1);
  store.setField("source", "other-source");
  expect(store.lastExecution).toBeNull();
  store.dispose();
});
it("does not attach a late timeout fact to a changed preview window", async () => {
  let reject!: (error: unknown) => void;
  const api = measuredApi(
    vi.fn().mockImplementation(
      () =>
        new Promise((_resolve, fail) => {
          reject = fail;
        }),
    ),
  );
  const store = draft(api);
  await store.validate();
  const pending = store.runPreview();
  store.setField("window", "1900-01-02T00:00:00Z");
  reject(
    new ApiError(422, "execution_budget_exceeded", null, null, {
      execution: executionFixture({
        calculation_status: "uncomputed",
        calculation_reason: "execution_budget_exceeded",
      }),
    }),
  );
  await pending;
  expect(store.lastExecution).toBeNull();
  expect(store.error).toBeNull();
  store.dispose();
});
it("does not interpret an expired client wait as server terminal computation", async () => {
  const api = measuredApi(
    vi.fn().mockRejectedValue(new ApiError(0, "preview_wait_timeout")),
  );
  const store = draft(api);
  await store.validate();
  await store.runPreview();
  expect(store.lastExecution).toBeNull();
  expect(store.preview).toBeNull();
  expect(api.preview).toHaveBeenCalledTimes(1);
  store.dispose();
});

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

it("offers the first completed window while preserving the exact original boundary", async () => {
  const api = {
    validate: vi.fn().mockResolvedValue({ rule_hash: "hash" } as Validation),
    preview: vi.fn(),
  } as unknown as AggregatesApi;
  const store = draft(api);
  expect(store.window).toBe("1899-12-31T18:38:50.123456Z");
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
  store.setField("window", "1900-01-01T00:00:01.123456+05:21:10");
  await store.validate();
  await store.runPreview();
  expect(api.preview).not.toHaveBeenCalled();
  expect(store.localError).toContain("выровнено");
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

it("restores only the unresolved create intent for the same actor after navigation", async () => {
  const api = {
    validate: vi.fn().mockResolvedValue({ rule_hash: "hash" } as Validation),
    preview: vi.fn().mockResolvedValue({ rule_hash: "hash" } as Preview),
    create: vi
      .fn()
      .mockRejectedValueOnce(new ApiError(503, "http_error"))
      .mockResolvedValue({ id: "saved" } as Aggregate),
  } as unknown as AggregatesApi;
  const owner = {};
  const first = draft(api);
  first.bindScope(owner, "actor-one");
  await first.validate();
  await first.runPreview();
  await first.create();
  const [body, key] = vi.mocked(api.create).mock.calls[0];
  first.dispose();
  const returned = new DraftStore(api);
  returned.bindScope(owner, "actor-one");
  expect(returned.unknownCreate).toBe(true);
  expect(returned.canCreate).toBe(true);
  await returned.create();
  expect(printAggregate(vi.mocked(api.create).mock.calls[1][0])).toBe(
    printAggregate(body),
  );
  expect(vi.mocked(api.create).mock.calls[1][1]).toBe(key);
  returned.dispose();
  const actor = new DraftStore(api);
  actor.bindScope(owner, "actor-two");
  expect(actor.unknownCreate).toBe(false);
  actor.dispose();
});

it("does not bypass unknown creation from copy A by opening new or copy B", async () => {
  const api = {
    copy: vi.fn().mockResolvedValue({
      source_id: "source-A",
      schedule: {
        boundary: "2026-01-01T00:00:00Z",
        timezone: "UTC",
        duration_seconds: 300,
      },
      aggregate_rule: {
        features: [{ key: "events", name: "Events", op: "count" }],
      },
    }),
    create: vi.fn().mockRejectedValue(new ApiError(0, "network_error")),
  } as unknown as AggregatesApi;
  const owner = {};
  const first = new DraftStore(api, "copy-A");
  first.bindScope(owner, "actor-one");
  first.setOperators([
    { name: "count", kind: "feature", parameters: [] },
  ] as unknown as Operator[]);
  await first.loadCopy();
  first.setField("name", "Копия А");
  first.validation = { rule_hash: "hash" } as Validation;
  first.preview = {} as Preview;
  await first.create();
  const [original, key] = vi.mocked(api.create).mock.calls[0];
  first.dispose();
  for (const copyId of ["", "copy-B"]) {
    const next = new DraftStore(api, copyId);
    next.bindScope(owner, "actor-one");
    expect(next.unknownCreate).toBe(true);
    expect(next.source).toBe("source-A");
    await next.loadCopy();
    await next.create();
    const [body, replayKey] = vi.mocked(api.create).mock.calls.at(-1)!;
    expect(printAggregate(body)).toBe(printAggregate(original));
    expect(replayKey).toBe(key);
    next.dispose();
  }
  expect(api.copy).toHaveBeenCalledTimes(1);
  const changed = new DraftStore(api, "copy-B");
  changed.bindScope(owner, "actor-two");
  expect(changed.unknownCreate).toBe(false);
  changed.dispose();
});
