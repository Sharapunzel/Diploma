import { describe, expect, it, vi } from "vitest";
import { ApiClient, ApiError } from "../../app/api/client";
import {
  AggregatesApi,
  type Aggregate,
  type Operator,
  type Point,
  type Preview,
  type Validation,
} from "../../app/api/aggregates";
import {
  exact,
  parseAggregate,
  printAggregate,
  type JsonObject,
} from "../../app/api/aggregateJson";
import { DraftStore } from "./DraftStore";
import { referenceErrors, ruleDocument, aggregatePrompt } from "./rule";
import { chartSeries } from "./chart";
import { displayTime } from "./time";
import { PromptStore } from "./PromptStore";
import { CollectionStore } from "./CollectionStore";
import { CardStore } from "./CardStore";

const operators = [
  { name: "count", kind: "feature", parameters: [] },
  { name: "derive", kind: "feature", parameters: ["expr"] },
  { name: "all", kind: "condition", parameters: ["args"] },
  { name: "in", kind: "condition", parameters: ["field", "value"] },
  { name: "eq", kind: "condition", parameters: ["field", "value"] },
  { name: "ratio", kind: "expression", parameters: ["args"] },
] as Operator[];
const validation = {
  rule_hash: "a".repeat(64),
  anchor_utc: "2026-01-01T00:00:00Z",
  duration_seconds: 300,
  feature_keys: ["events"],
  budget: {},
} as Validation;
const preview = {
  ...validation,
  start_utc: validation.anchor_utc,
  end_utc: "2026-01-01T00:05:00Z",
  scheduled_at: "2026-01-01T00:05:00Z",
  features: [],
  event_count: 0,
  observed_rows: 0,
  completeness: "unverified",
  completeness_reason: "no evidence",
  time_basis: "availability_at",
} as unknown as Preview;
function draft(api: AggregatesApi) {
  const d = new DraftStore(api);
  d.operators = operators;
  d.setField("source", "source");
  d.setField("name", "Тест");
  d.setField("boundary", validation.anchor_utc);
  d.setField("window", validation.anchor_utc);
  return d;
}
function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((r) => {
    resolve = r;
  });
  return { promise, resolve };
}

describe("aggregate transport and draft", () => {
  it("locks a historical calculation until completion and does not submit it twice", async () => {
    const pending = deferred<Point>();
    const api = {
      historical: vi.fn().mockReturnValue(pending.promise),
    } as unknown as AggregatesApi;
    const card = new CardStore(api, "aggregate");
    const first = card.history("2026-01-01T00:00:00Z");
    expect(card.lifecycle.isPending("history")).toBe(true);
    await card.history("2026-01-01T00:00:00Z");
    expect(api.historical).toHaveBeenCalledTimes(1);
    pending.resolve({} as Point);
    await first;
    expect(card.lifecycle.isPending("history")).toBe(false);
    card.dispose();
  });
  it("rejects malformed successful DTOs and preserves the response request ID", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response('{"items":[],"total":-1}', {
          status: 200,
          headers: { "X-Request-ID": "request-dto" },
        }),
      ),
    );
    try {
      await expect(
        new AggregatesApi(new ApiClient()).list(),
      ).rejects.toMatchObject({
        code: "invalid_response",
        requestId: "request-dto",
      });
    } finally {
      vi.unstubAllGlobals();
    }
  });
  it("loads every catalogue page for the prompt and cancels a source transition", async () => {
    const api = {
      fields: vi.fn().mockImplementation(async (_source, _query, offset) => ({
        total: 3,
        items:
          offset === 0
            ? [{ name: "first" }, { name: "second" }]
            : [{ name: "last" }],
      })),
    } as unknown as AggregatesApi;
    const prompt = new PromptStore(api, "source");
    await prompt.load(operators);
    expect(prompt.text).toContain('"last"');
    expect(vi.mocked(api.fields).mock.calls.map((c) => c[2])).toEqual([0, 2]);
    prompt.dispose();
  });
  it("keeps page loading visible and ignores an old page after a newer request", async () => {
    const old = deferred<{ items: string[]; total: number }>();
    const next = deferred<{ items: string[]; total: number }>();
    const load = vi
      .fn()
      .mockReturnValueOnce(old.promise)
      .mockReturnValueOnce(next.promise);
    const store = new CollectionStore<string>(load);
    const p1 = store.load(0);
    expect(store.loading).toBe(true);
    const p2 = store.load(50);
    expect(load.mock.calls[0][1].aborted).toBe(true);
    old.resolve({ items: ["old"], total: 1 });
    await p1;
    expect(store.loading).toBe(true);
    next.resolve({ items: ["new"], total: 51 });
    await p2;
    expect(store.items).toEqual(["new"]);
    expect(store.loading).toBe(false);
    store.dispose();
  });
  it("round trips nested numeric lexemes, int64, typed lists and exact date strings in both modes", () => {
    expect(() => parseAggregate('{"value":1e10000}')).toThrow();
    expect(() => parseAggregate('{"value":NaN}')).toThrow();
    const text =
      '{"where":{"op":"all","args":[{"op":"in","field":"network.bytes","value":[9007199254740993,9223372036854775807,-9223372036854775808,1,1.0]},{"op":"eq","field":"event.created","value":"1900-01-01T00:00:00.123456+05:21:10"}]},"features":[{"key":"n","name":"N","op":"count"},{"key":"r","name":"R","op":"derive","expr":{"op":"ratio","args":[{"ref":"n"},{"value":1.0}]}}]}';
    const d = draft({} as AggregatesApi);
    d.setText(text);
    d.switchMode("visual");
    d.switchMode("json");
    expect(printAggregate(d.rule)).toBe(text);
    expect(d.localError).toBe("");
    expect(printAggregate(ruleDocument(d.text, operators))).toBe(text);
    d.setText('{"features":[');
    d.switchMode("visual");
    expect(d.mode).toBe("json");
    expect(d.text).toBe('{"features":[');
    expect(d.localError).not.toBe("");
    d.dispose();
  });
  it("does not discard unknown nodes and reports references after reordering", () => {
    expect(() =>
      ruleDocument(
        '{"features":[{"key":"a","name":"A","op":"future"}]}',
        operators,
      ),
    ).toThrow("неизвестная");
    expect(
      referenceErrors(
        parseAggregate(
          '{"features":[{"key":"r","expr":{"ref":"n"}},{"key":"n","op":"count"}]}',
        ) as JsonObject,
      ),
    ).toEqual([expect.stringContaining("features[0].expr.ref")]);
    expect(aggregatePrompt(operators)).toContain('"ratio"');
  });
  it("decodes responses losslessly and sends numeric wire values through the existing CSRF client", async () => {
    const card = {
      id: "id",
      name: "N",
      source_id: "s",
      source_name: "S",
      source_type: "kafka",
      rule_hash: "a",
      anchor_utc: "t",
      original_boundary: "t",
      timezone: "UTC",
      created_at: "t",
      created_by: "u",
      state: "active",
      next_scheduled_at: "t",
      duration_seconds: 300,
      skipped_ranges: [],
      aggregate_rule: parseAggregate(
        '{"features":[{"key":"x","op":"derive","expr":{"value":9007199254740993}}]}',
      ),
    };
    const fetcher = vi
      .fn()
      .mockResolvedValue(new Response(printAggregate(card), { status: 201 }));
    vi.stubGlobal("fetch", fetcher);
    try {
      const client = new ApiClient();
      client.setCsrf("csrf");
      const api = new AggregatesApi(client);
      const body = {
        source_id: "s",
        aggregate_rule: card.aggregate_rule,
        schedule: { boundary: "t", timezone: "UTC", duration_seconds: 300 },
        name: "N",
        window_start_utc: "t",
        time_basis: "availability_at",
        rule_hash: "a",
      } as never;
      const result = await api.create(body, "key");
      expect(printAggregate(result.aggregate_rule)).toContain(
        "9007199254740993",
      );
      const opts = fetcher.mock.calls[0][1];
      expect(opts.body).toContain('"value":9007199254740993');
      expect(opts.headers.get("X-CSRF-Token")).toBe("csrf");
      expect(opts.headers.get("Idempotency-Key")).toBe("key");
      expect(opts.decodeResponse).toBeUndefined();
    } finally {
      vi.unstubAllGlobals();
    }
  });
  it("rejects stale validation/preview and aborts on dispose", async () => {
    const old = deferred<Validation>();
    const api = {
      validate: vi.fn().mockReturnValue(old.promise),
      preview: vi.fn().mockResolvedValue(preview),
    } as unknown as AggregatesApi;
    const d = draft(api);
    const pending = d.validate();
    const signal = vi.mocked(api.validate).mock.calls[0][1]!;
    d.setField("source", "other");
    old.resolve(validation);
    await pending;
    expect(signal.aborted).toBe(true);
    expect(d.validation).toBeNull();
    vi.mocked(api.validate).mockResolvedValue(validation);
    await d.validate();
    const p = deferred<Preview>();
    vi.mocked(api.preview).mockReturnValue(p.promise);
    const checking = d.runPreview();
    d.setField("window", "other");
    p.resolve(preview);
    await checking;
    expect(d.preview).toBeNull();
    const next = deferred<Validation>();
    vi.mocked(api.validate).mockReturnValue(next.promise);
    const disposing = d.validate();
    const last = vi.mocked(api.validate).mock.calls.at(-1)![1]!;
    d.dispose();
    next.resolve(validation);
    await disposing;
    expect(last.aborted).toBe(true);
    expect(d.validation).toBeNull();
  });
  it("locks double submit, preserves idempotent retry after unknown success, and renews the key for changed bodies", async () => {
    const first = deferred<Aggregate>();
    const api = {
      validate: vi.fn().mockResolvedValue(validation),
      preview: vi.fn().mockResolvedValue(preview),
      create: vi.fn().mockReturnValue(first.promise),
    } as unknown as AggregatesApi;
    const d = draft(api);
    await d.validate();
    await d.runPreview();
    const creating = d.create();
    await d.create();
    expect(api.create).toHaveBeenCalledTimes(1);
    first.resolve(
      Promise.reject(new ApiError(201, "invalid_response")) as never,
    );
    await creating;
    expect(d.unknownCreate).toBe(true);
    vi.mocked(api.create).mockRejectedValue(new ApiError(0, "network_error"));
    await d.create();
    const calls = vi.mocked(api.create).mock.calls;
    expect(calls[1][1]).toBe(calls[0][1]);
    expect(printAggregate(calls[1][0])).toBe(printAggregate(calls[0][0]));
    d.setField("name", "Другое имя");
    await d.validate();
    await d.runPreview();
    await d.create();
    expect(vi.mocked(api.create).mock.calls[2][1]).not.toBe(calls[0][1]);
    d.dispose();
  });
  it("blocks incomplete literals, permits correction and leaves display time outside calculation context", async () => {
    const api = {
      validate: vi.fn().mockResolvedValue(validation),
    } as unknown as AggregatesApi;
    const d = draft(api);
    d.setInvalid("literal", true);
    await d.validate();
    expect(api.validate).not.toHaveBeenCalled();
    d.setInvalid("literal", false);
    d.setField("duration", "0");
    await d.validate();
    expect(d.localError).not.toBe("");
    d.setField("duration", "300");
    await d.validate();
    expect(d.validation).toBeTruthy();
    const before = d.context;
    expect(displayTime("2026-10-25T00:30:00Z", "Europe/Berlin")).toContain(
      "GMT+02:00",
    );
    expect(displayTime("2026-10-25T01:30:00Z", "Europe/Berlin")).toContain(
      "GMT+01:00",
    );
    expect(displayTime("1900-01-01T00:00:00.123456+05:21:10", "UTC")).toBe(
      "1900-01-01T00:00:00.123456+05:21:10",
    );
    expect(d.context).toBe(before);
    d.dispose();
  });
  it("breaks gaps, null, unknown quality and separates historical points while retaining exact values", () => {
    const make = (
      id: string,
      start: number,
      value: string,
      completeness = "complete",
      kind = "working",
    ) =>
      ({
        id,
        kind,
        start_utc: `2026-01-01T00:0${start}:00Z`,
        end_utc: `2026-01-01T00:0${start + 1}:00Z`,
        completeness,
        features: [{ key: "n", value: parseAggregate(value), reason: null }],
      }) as Point;
    const series = chartSeries(
      [
        make("a", 0, "9007199254740993"),
        make("b", 1, "9007199254740992"),
        make("c", 3, "0", "unknown"),
        make("d", 4, "null"),
        make("h", 0, "1", "complete", "historical"),
      ],
      "n",
    );
    expect(series[0].values.map((v) => v.join)).toEqual([
      false,
      true,
      false,
      false,
    ]);
    expect(series[0].values[2].valid).toBe(false);
    expect(series[0].values[3].value).toBeNull();
    expect(series[1].values).toHaveLength(1);
    expect(exact(series[0].values[0].point.features![0].value)).toBe(
      "9007199254740993",
    );
  });
});
