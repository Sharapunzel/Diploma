import { expect, it, vi } from "vitest";
import {
  AggregatesApi,
  type Aggregate,
  type FieldCatalog,
  type Operator,
  type Preview,
  type Validation,
} from "../../app/api/aggregates";
import { printAggregate } from "../../app/api/aggregateJson";
import { ApiClient, ApiError } from "../../app/api/client";
import {
  definitions,
  formula,
  moveDefinition,
  referenceErrors,
  ruleDocument,
} from "./rule";
import { aggregatePrompt } from "./prompt";
import { calendarInstants, localCalendar } from "./time";
import { CatalogStore } from "./CatalogStore";
import { DraftStore } from "./DraftStore";
import { ListStore } from "./ListStore";

export const operators = [
  ...["count", "sum"].map((name) => ({
    name,
    kind: "feature",
    parameters: name === "count" ? [] : ["field"],
  })),
  ...["formula", "derive"].map((name) => ({
    name,
    kind: "feature",
    parameters: ["expr"],
  })),
  ...["divide", "add", "ratio"].map((name) => ({
    name,
    kind: "expression",
    parameters: ["args"],
    arity: { min: 2, max: 2 },
  })),
  {
    name: "all",
    kind: "condition",
    parameters: ["args"],
    arity: { min: 1, max: null },
  },
  { name: "eq", kind: "condition", parameters: ["field", "value"] },
].map((o) => ({
  inputs: [],
  compatible_fields: [],
  parameter_types: {},
  arity: null,
  result: "number",
  result_nullable: true,
  weight: 1,
  weight_formula: "weight",
  ...o,
})) as Operator[];
const text =
  '{"where":{"op":"eq","field":"user.name","value":"Andrew"},"auxiliary":[{"key":"base","name":"База","op":"count","where":{"op":"eq","field":"event.action","value":"logon"}},{"key":"mid","name":"Средний","op":"formula","expr":{"ref":"base"}}],"features":[{"key":"result","name":"Выход","op":"formula","expr":{"op":"divide","args":[{"aggregate":{"op":"sum","field":"network.bytes","where":{"op":"eq","field":"event.created","value":"1900-01-01T00:00:00.123456+05:21:10"}}},{"ref":"mid"}]}},{"key":"precise","name":"Точное","op":"derive","expr":{"op":"add","args":[{"value":9223372036854775807},{"value":1.0}]}}]}';
it("roundtrips extended/legacy nodes and exact literals without migrating the document", () => {
  const rule = ruleDocument(text, operators);
  expect(printAggregate(rule)).toBe(text);
  expect(referenceErrors(rule)).toEqual([]);
  expect(formula(definitions(rule)[0])).toContain("sum(network.bytes)");
  expect(formula(definitions(rule)[1])).toContain("9223372036854775807 + 1.0");
  const d = new DraftStore({} as AggregatesApi);
  d.setOperators(operators);
  d.setText(text);
  d.switchMode("json");
  d.switchMode("visual");
  expect(printAggregate(d.rule)).toBe(text);
  d.dispose();
});
it("moves definitions without breaking dependencies or inventing enabled fields", () => {
  const rule = ruleDocument(text, operators),
    next = moveDefinition(rule, "result", false);
  expect((next.features as unknown[]).length).toBe(1);
  expect(referenceErrors(next)).toEqual([]);
  const restored = moveDefinition(next, "result", true);
  expect(
    definitions(restored)
      .slice(0, 2)
      .map((f) => f.key),
  ).toEqual(["result", "precise"]);
  expect(printAggregate(restored)).toBe(text);
  expect(printAggregate(restored)).not.toContain('"enabled"');
  expect(printAggregate(rule)).toBe(text);
});
it.each([
  '{"features":[{"key":"a","name":"A","op":"formula","expr":{"ref":"a"}}]}',
  '{"auxiliary":[{"key":"b","name":"B","op":"formula","expr":{"ref":"a"}}],"features":[{"key":"a","name":"A","op":"formula","expr":{"ref":"b"}}]}',
  '{"features":[{"key":"a","name":"A","op":"formula","expr":{"ref":"missing"}}]}',
  '{"features":[]}',
  '{"auxiliary":[{"key":"a","name":"A","op":"count"}],"features":[{"key":"a","name":"A","op":"count"}]}',
])("reports graph errors before creation: %s", (text) => {
  expect(referenceErrors(ruleDocument(text, operators)).length).toBeGreaterThan(
    0,
  );
});
it.each([
  '{"features":[],"auxiliary":null}',
  '{"features":[],"auxiliary":[]}',
  '{"features":[{"key":"a","name":"A","op":"formula","expr":{"aggregate":null}}]}',
  '{"features":[{"key":"a","name":"A","op":"unknown"}]}',
])("preserves unsupported JSON for correction: %s", (text) => {
  const d = new DraftStore({} as AggregatesApi);
  d.setOperators(operators);
  d.setText(text);
  expect(d.text).toBe(text);
  expect(d.localError).not.toBe("");
  d.dispose();
});
it("distinguishes DST gaps, folds and display-only timezone changes", () => {
  expect(calendarInstants("2026-03-08T02:30:00", "America/New_York")).toEqual(
    [],
  );
  expect(calendarInstants("2026-11-01T01:30:00", "America/New_York")).toEqual([
    "2026-11-01T05:30:00.000Z",
    "2026-11-01T06:30:00.000Z",
  ]);
  const iso = "2026-11-01T00:00:00.123456+05:00";
  expect(localCalendar(iso, "UTC")).toBe("2026-10-31T19:00:00");
  expect(iso).toContain("123456+05:00");
  expect(calendarInstants("2026-02-30T12:00:00", "UTC")).toEqual([]);
});
const field = (name: string) => ({
  name,
  type: "long",
  is_array: false,
  description: "Описание ECS",
  supported: true,
  reason: null,
  operators: ["sum"],
  provenance: ["observed"],
  normalizer_origin: [],
});
const page = (offset = 0): FieldCatalog =>
  ({
    items: [field(offset ? "last" : "first")],
    total: 2,
    limit: 100,
    offset,
    snapshot_at: "2026-01-01T00:00:00Z",
    catalog_hash: "a".repeat(64),
    observation: {
      scope: "all_retained_events",
      complete: true,
      rows: offset ? 100 : 1,
    },
    normalizer: null,
  }) as FieldCatalog;
it("copies all catalog pages with stable hash despite different snapshots/counts", async () => {
  const api = {
    fieldCatalog: vi
      .fn()
      .mockImplementation(async (_id, _q, offset) => page(offset)),
  } as unknown as AggregatesApi;
  const s = new CatalogStore(api, "source");
  expect(await s.fullText()).toContain("last");
  expect(s.text).toContain("Описание ECS");
  expect(api.fieldCatalog).toHaveBeenCalledTimes(2);
  s.dispose();
});
it.each(["hash", "duplicate", "empty", "offset"])(
  "rejects partial catalog on %s without copying a previous result",
  async (kind) => {
    const api = {
      fieldCatalog: vi
        .fn()
        .mockResolvedValueOnce(page())
        .mockResolvedValueOnce({
          ...page(1),
          ...(kind === "hash"
            ? { catalog_hash: "b".repeat(64) }
            : kind === "duplicate"
              ? { items: [field("first")] }
              : kind === "empty"
                ? { items: [] }
                : { offset: 0 }),
        }),
    } as unknown as AggregatesApi;
    const s = new CatalogStore(api, "source");
    expect(await s.fullText()).toBeUndefined();
    expect(s.text).toBe("");
    expect(s.error).not.toBeNull();
    s.dispose();
  },
);
it("decodes all catalog metadata, extended opt-in and rejects malformed successful pages", async () => {
  const fetch = vi
    .fn()
    .mockResolvedValueOnce(new Response(printAggregate(page())))
    .mockResolvedValueOnce(new Response('{"items":[],"total":0}'));
  vi.stubGlobal("fetch", fetch);
  try {
    const api = new AggregatesApi(new ApiClient());
    const result = await api.fieldCatalog("source", "%", 0);
    expect(String(result.observation.rows)).toBe("1");
    expect(fetch.mock.calls[0][0]).toContain("q=%25");
    await expect(api.fieldCatalog("source")).rejects.toMatchObject({
      code: "invalid_response",
    });
  } finally {
    vi.unstubAllGlobals();
  }
});
it("keeps prompt independent of catalog/draft and requires questions before JSON", () => {
  const prompt = aggregatePrompt(operators);
  expect(prompt).toContain("До уточнения задавай вопросы");
  expect(prompt).toContain("Не требуй реальные записи");
  expect(prompt).toContain("auxiliary");
  expect(prompt).toContain("validate и preview");
  expect(prompt).not.toContain("compatible_fields");
  expect(prompt.length).toBeLessThan(10000);
});
it("imports invalid JSON independently of an unfinished visual literal", () => {
  const d = new DraftStore({} as AggregatesApi);
  d.setOperators(operators);
  d.setInvalid("literal", true);
  d.importText('{"features":[');
  expect(d.mode).toBe("json");
  expect(d.invalidNodes.size).toBe(0);
  expect(d.text).toBe('{"features":[');
  expect(d.localError).not.toBe("");
  d.dispose();
});
it.each([0, 500, 503, 200])(
  "replays immutable create intent after %s, without discarding edited draft",
  async (status) => {
    const api = {
      validate: vi.fn().mockResolvedValue({ rule_hash: "hash" } as Validation),
      preview: vi.fn().mockResolvedValue({ rule_hash: "hash" } as Preview),
      create: vi
        .fn()
        .mockRejectedValueOnce(
          new ApiError(
            status,
            status === 200 ? "invalid_response" : "http_error",
          ),
        )
        .mockResolvedValue({ id: "old" } as Aggregate),
    } as unknown as AggregatesApi;
    const d = new DraftStore(api);
    d.setOperators(operators);
    for (const [key, v] of Object.entries({
      source: "source",
      name: "Old",
      boundary: "2026-01-01T00:00:00.123456Z",
      window: "2026-01-01T00:00:00.123456Z",
    }))
      d.setField(key as "name", v);
    await d.validate();
    await d.runPreview();
    await d.create();
    expect(d.unknownCreate).toBe(true);
    d.setField("name", "New");
    d.setText('{"features":[] }');
    expect(d.unknownCreate).toBe(true);
    expect(await d.create()).toBeUndefined();
    expect(api.create).toHaveBeenCalledTimes(2);
    const calls = vi.mocked(api.create).mock.calls;
    expect(printAggregate(calls[1][0])).toBe(printAggregate(calls[0][0]));
    expect(calls[1][1]).toBe(calls[0][1]);
    expect(d.dirty).toBe(true);
    expect(d.name).toBe("New");
    d.dispose();
  },
);
it("only commits server filters after success, keeping previous rows on failure", async () => {
  const api = {
    list: vi
      .fn()
      .mockResolvedValueOnce({ items: [{ id: "old" }], total: 60 })
      .mockRejectedValueOnce(new ApiError(503, "http_error")),
  } as unknown as AggregatesApi;
  const s = new ListStore(api);
  await s.load();
  expect(await s.load(0, { q: "beyond first page", state: "archived" })).toBe(
    false,
  );
  expect(s.items[0].id).toBe("old");
  expect(s.filters.q).toBe("");
  expect(vi.mocked(api.list).mock.calls[1][2]).toMatchObject({
    q: "beyond first page",
    state: "archived",
  });
  s.dispose();
});
