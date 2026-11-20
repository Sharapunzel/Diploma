import { describe, expect, it } from "vitest";
import { decodeCard, decodePage, decodeReceipt } from "../../app/api/detectors";
import { exact, printAggregate } from "../../app/api/aggregateJson";
import { at, fixture, id, receipt } from "./testData";
import { reasonText } from "./labels";
describe("detector DTO", () => {
  it("uses safe contextual advice for an accepted start and past failure", () => {
    const reason = {
      code: "warmup_required",
      message: "private traceback",
      factor: null,
    };
    expect(reasonText(reason, "requested")).not.toContain("Запустите");
    expect(reasonText(reason, "created")).toContain("Запустите");
    expect(
      reasonText({ ...reason, code: "history_insufficient" }, "history"),
    ).not.toContain("вручную");
    expect(reasonText({ ...reason, code: "unknown" })).not.toContain(
      "private traceback",
    );
  });
  it("keeps consumed/skipped and evaluated windows distinct with exact indices", () => {
    const window = {
      index: 1,
      kind: "skipped",
      reason: null,
      start_utc: at,
      end_utc: at,
      scheduled_at: at,
      aggregation_started_at: at,
      aggregation_completed_at: at,
      evaluation_started_at: null,
      evaluation_completed_at: null,
      late: false,
      time_basis: "event_time",
      origin: "online",
      close_not_before_at: "2026-11-10T00:00:02Z",
    };
    const body = JSON.stringify({
      ...fixture(),
      last_consumed: window,
      last_evaluated: {
        ...window,
        index: 0,
        kind: "evaluated",
        evaluation_started_at: at,
        evaluation_completed_at: at,
      },
    }).replace('"index":1', '"index":9007199254740993');
    const c = decodeCard(body, id);
    expect(exact(c.last_consumed?.index)).toBe("9007199254740993");
    expect(c.last_consumed?.kind).toBe("skipped");
    expect(c.last_evaluated?.kind).toBe("evaluated");
    expect(exact(c.last_evaluated?.index)).toBe("0");
    expect(c.last_consumed?.time_basis).toBe("event_time");
    expect(c.last_consumed?.close_not_before_at).toBe("2026-11-10T00:00:02Z");
  });
  it("preserves int64 and large counters in cards/receipts without rounding", () => {
    for (const seed of ["-9223372036854775808", "9223372036854775807"]) {
      const text = JSON.stringify(fixture())
        .replace('"seed":42', `"seed":${seed}`)
        .replace('"revision":0', '"revision":9007199254740993');
      const c = decodeCard(text, id);
      expect(exact(c.config.seed)).toBe(seed);
      expect(exact(c.revision)).toBe("9007199254740993");
      expect(printAggregate(c)).toContain(seed);
    }
    expect(
      exact(
        decodeReceipt(
          JSON.stringify(receipt()).replace(
            '"execution_generation":1',
            '"execution_generation":9007199254740993',
          ),
          "create",
        ).execution_generation,
      ),
    ).toBe("9007199254740993");
  });
  it.each(["", "null", "{}", '{"items":[]}', "[]"])(
    "rejects incomplete successful responses %s",
    (body) => {
      expect(() => decodeCard(body, id)).toThrow();
      expect(() => decodeReceipt(body, "start", id)).toThrow();
      expect(() => decodePage(body)).toThrow();
    },
  );
  it("checks enums, dimensions, nullable fields and target identity", () => {
    for (const change of [
      { execution_state: "healthy" },
      { aggregate_semantic_basis: "processing" },
      { aggregate_execution_mode: "paused" },
      { aggregate_paused: "true" },
      { aggregate_paused: undefined },
      { next_close_not_before_at: undefined },
      { warmup: {} },
      { last_consumed: {} },
      { feature_names: [] },
      { waiting_for_point: undefined },
      { config: { ...fixture().config, seed: "42" } },
    ])
      expect(() =>
        decodeCard(JSON.stringify({ ...fixture(), ...change }), id),
      ).toThrow();
    expect(() =>
      decodeCard(JSON.stringify(fixture()), id.replace("123", "124")),
    ).toThrow();
    expect(() =>
      decodeReceipt(JSON.stringify(receipt("stop")), "start", id),
    ).toThrow();
    expect(
      decodePage(
        JSON.stringify({
          items: [fixture()],
          total: 101,
          limit: 25,
          offset: 25,
        }),
      ).total,
    ).toBe(101);
  });
  it("reads retired provenance and aggregate-resume without relabelling old windows", () => {
    const window = {
      index: 4,
      kind: "evaluated",
      reason: null,
      start_utc: at,
      end_utc: at,
      scheduled_at: at,
      aggregation_started_at: at,
      aggregation_completed_at: at,
      evaluation_started_at: at,
      evaluation_completed_at: at,
      late: false,
      time_basis: "availability_at",
      origin: "legacy",
      close_not_before_at: at,
    };
    const old = {
      ...fixture(),
      aggregate_semantic_basis: "availability_v1",
      aggregate_execution_mode: "retired",
      execution_state: "terminated",
      last_evaluated: window,
      recovery: {
        reason: "aggregate_resume",
        first_index: 4,
        last_index: 6,
        started_at: at,
      },
    };
    const card = decodeCard(JSON.stringify(old), id);
    expect(card.aggregate_execution_mode).toBe("retired");
    expect(card.last_evaluated?.time_basis).toBe("availability_at");
    expect(card.last_evaluated?.origin).toBe("legacy");
    expect(card.recovery?.reason).toBe("aggregate_resume");
    for (const change of [
      { time_basis: undefined },
      { origin: undefined },
      { close_not_before_at: undefined },
      { close_not_before_at: "2026-11-10T00:00:00" },
      { time_basis: "browser" },
      { origin: "historical" },
    ])
      expect(() =>
        decodeCard(
          JSON.stringify({ ...old, last_evaluated: { ...window, ...change } }),
          id,
        ),
      ).toThrow();
  });
  it.each([
    ["waiting_close_deadline", "штатное ожидание"],
    ["aggregate_paused", "сам не запустится"],
    ["cleanup_in_progress", "Модель и позиция"],
    ["aggregate_time_semantics_retired", "отдельный детектор"],
  ])("explains %s independently of server text", (code, expected) => {
    expect(reasonText({ code, message: "private", factor: null })).toContain(
      expected,
    );
  });
});
