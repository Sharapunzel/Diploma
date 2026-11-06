import { expect, it, vi } from "vitest";
import {
  AggregatesApi,
  type Aggregate,
  type AggregateInput,
  type Operator,
} from "../../app/api/aggregates";
import { ApiError } from "../../app/api/client";
import { DraftStore } from "./DraftStore";
import { CardStore } from "./CardStore";
import { chartScale } from "./chart";

function deferred<T>() {
  let resolve!: (v: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((yes, no) => {
    resolve = yes;
    reject = no;
  });
  return { promise, resolve, reject };
}
const original: AggregateInput = {
  source_id: "source",
  schedule: {
    boundary: "2026-01-01T00:00:00Z",
    timezone: "UTC",
    duration_seconds: 300,
  },
  aggregate_rule: {
    features: [{ key: "original", name: "Original", op: "count" }],
  },
};
const ops = [
  { name: "count", kind: "feature", parameters: [] },
] as unknown as Operator[];
it("keeps copy editing closed through delay/failure/retry, then never reloads over user input", async () => {
  const first = deferred<AggregateInput>(),
    retry = deferred<AggregateInput>();
  const api = {
    copy: vi
      .fn()
      .mockReturnValueOnce(first.promise)
      .mockReturnValueOnce(retry.promise),
  } as unknown as AggregatesApi;
  const d = new DraftStore(api, "original-id");
  d.setOperators(ops);
  const loading = d.loadCopy();
  d.setField("source", "edited");
  d.setField("boundary", "edited");
  d.setText('{"features":[{"key":"edited","name":"E","op":"count"}]}');
  d.switchMode("json");
  expect(d.editable).toBe(false);
  expect(d.source).toBe("");
  expect(d.mode).toBe("visual");
  expect(d.text).not.toContain("edited");
  first.reject(new ApiError(503, "http_error"));
  await loading;
  expect(d.copyState).toBe("error");
  expect(d.editable).toBe(false);
  const again = d.loadCopy();
  expect(d.copyState).toBe("loading");
  retry.resolve(original);
  await again;
  expect(d.editable).toBe(true);
  expect(d.source).toBe("source");
  expect(d.text).toContain("original");
  d.setText('{"features":[{"key":"edited","name":"E","op":"count"}]}');
  await d.loadCopy();
  expect(api.copy).toHaveBeenCalledTimes(2);
  expect(d.text).toContain("edited");
  d.dispose();
});
it("ignores a copy arriving after route/session disposal", async () => {
  const pending = deferred<AggregateInput>();
  const api = {
    copy: vi.fn().mockReturnValue(pending.promise),
  } as unknown as AggregatesApi;
  const d = new DraftStore(api, "original-id");
  const waiting = d.loadCopy();
  const signal = vi.mocked(api.copy).mock.calls[0][1]!;
  d.dispose();
  pending.resolve(original);
  await waiting;
  expect(signal.aborted).toBe(true);
  expect(d.source).toBe("");
  expect(d.text).toBe("");
  expect(d.copyError).toBeNull();
});
it.each(["archive", "delete", "resume"] as const)(
  "serializes refresh with %s and ignores an older read",
  async (action) => {
    const oldRead = deferred<Aggregate>(),
      mutation = deferred<Aggregate>();
    const api = {
      card: vi.fn().mockReturnValue(oldRead.promise),
      lifecycle: vi.fn().mockReturnValue(mutation.promise),
    } as unknown as AggregatesApi;
    const store = new CardStore(api, "aggregate");
    const reading = store.load();
    const readSignal = vi.mocked(api.card).mock.calls[0][1]!;
    const changing = store.action(action);
    const signal = vi.mocked(api.lifecycle).mock.calls[0][2]!;
    await store.load();
    expect(api.card).toHaveBeenCalledTimes(1);
    expect(signal.aborted).toBe(false);
    expect(readSignal.aborted).toBe(true);
    const state =
      action === "delete"
        ? "deleted"
        : action === "archive"
          ? "archived"
          : "active";
    mutation.resolve({ id: "aggregate", state } as Aggregate);
    await changing;
    oldRead.resolve({ id: "aggregate", state: "stale" } as Aggregate);
    await reading;
    expect(store.card?.state).toBe(state);
    expect(store.lifecycle.pending.size).toBe(0);
    expect(store.error).toBeNull();
    store.dispose();
  },
);
it.each([
  new ApiError(0, "network_error"),
  new ApiError(200, "invalid_response"),
  new ApiError(500, "internal_error"),
  new ApiError(503, "aggregate_database_error"),
  new ApiError(502, "http_error"),
  new ApiError(503, "http_error"),
])(
  "requires a successful explicit read after an unknown mutation",
  async (error) => {
    const api = {
      lifecycle: vi.fn().mockRejectedValue(error),
      card: vi
        .fn()
        .mockRejectedValueOnce(new ApiError(503, "aggregate_database_error"))
        .mockRejectedValueOnce(new ApiError(200, "invalid_response"))
        .mockResolvedValue({ state: "deleted" }),
    } as unknown as AggregatesApi;
    const store = new CardStore(api, "aggregate");
    store.card = { state: "active" } as Aggregate;
    await store.action("delete");
    expect(store.mutationOutcome).toBe("unknown");
    expect(store.canMutate).toBe(false);
    await store.action("delete");
    expect(api.lifecycle).toHaveBeenCalledTimes(1);
    for (let attempt = 0; attempt < 2; attempt++) {
      await store.load();
      expect(store.card?.state).toBe("active");
      expect(store.mutationOutcome).toBe("unknown");
      expect(store.canMutate).toBe(false);
      await store.action("delete");
      expect(api.lifecycle).toHaveBeenCalledTimes(1);
    }
    await store.load();
    expect(store.card?.state).toBe("deleted");
    expect(store.mutationOutcome).toBeNull();
    expect(store.canMutate).toBe(true);
    expect(api.card).toHaveBeenCalledTimes(3);
    expect(store.lifecycle.pending.size).toBe(0);
    store.dispose();
  },
);
it("preserves a confirmed refusal and aborts mutation expectation on disposal", async () => {
  const api = {
    lifecycle: vi
      .fn()
      .mockRejectedValueOnce(new ApiError(409, "aggregate_inactive")),
  } as unknown as AggregatesApi;
  const store = new CardStore(api, "aggregate");
  store.card = { state: "active" } as Aggregate;
  await store.action("archive");
  expect(store.mutationOutcome).toBe("refused");
  expect(store.card.state).toBe("active");
  const result = deferred<Aggregate>();
  vi.mocked(api.lifecycle).mockReturnValue(result.promise);
  const changing = store.action("delete");
  const signal = vi.mocked(api.lifecycle).mock.calls[1][2]!;
  store.dispose();
  result.resolve({ state: "deleted" } as Aggregate);
  await changing;
  expect(signal.aborted).toBe(true);
  expect(store.card).toBeNull();
  expect(store.lifecycle.pending.size).toBe(0);
});
it.each([
  [0.1, 0.2],
  [-0.2, -0.1],
  [-0.1, 0.2],
  [-1e308, 1e308],
  [Number.MIN_VALUE, Number.MIN_VALUE * 2],
  [9007199254740992, 9007199254740994],
])("maps the actual labelled domain %j to finite endpoints", (a, b) => {
  const scale = chartScale([a, b])!;
  expect(scale.low).toBe(a);
  expect(scale.high).toBe(b);
  expect(scale.position(a)).toBe(170);
  expect(scale.position(b)).toBe(30);
  for (const value of [a, b, a / 2 + b / 2]) {
    const y = scale.position(value);
    expect(Number.isFinite(y)).toBe(true);
    expect(y).toBeGreaterThanOrEqual(30);
    expect(y).toBeLessThanOrEqual(170);
  }
});
it.each([0, 0.1, -5, 1e308])(
  "explicitly represents a constant %s domain at the middle",
  (value) => {
    const scale = chartScale([value, value])!;
    expect(scale.constant).toBe(true);
    expect(scale.low).toBe(value);
    expect(scale.high).toBe(value);
    expect(scale.position(value)).toBe(100);
  },
);
it("does not invent a domain when no numeric values are loaded", () => {
  expect(chartScale([])).toBeNull();
});
