import { afterEach, expect, it, vi } from "vitest";
import { ApiClient, ApiError } from "./client";
import {
  AggregatesApi,
  previewExecutionFailure,
  type PreviewInput,
} from "./aggregates";
import {
  aggregateId,
  aggregateFixture,
  executionFixture,
  pointFixture,
  previewFixture,
  uncomputedPointFixture,
} from "./aggregateTestData";
import { printAggregate, exact } from "./aggregateJson";

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

function input(duration: number): PreviewInput {
  return {
    source_id: "00000000-0000-4000-8000-000000000001",
    aggregate_rule: { features: [{ key: "events", op: "count" }] },
    schedule: {
      boundary: "2026-01-01T00:00:00Z",
      timezone: "UTC",
      duration_seconds: duration,
    },
    window_start_utc: "2026-01-01T00:00:00Z",
    time_basis: "event_time",
  };
}

function pendingFetch() {
  let requestSignal: AbortSignal | null = null;
  const fetch = vi.fn((_path: string, options: RequestInit) => {
    requestSignal = options.signal as AbortSignal;
    return new Promise<Response>((_resolve, reject) => {
      requestSignal!.addEventListener("abort", () => {
        reject(new DOMException("Aborted", "AbortError"));
      });
    });
  });
  vi.stubGlobal("fetch", fetch);
  return { fetch, signal: () => requestSignal };
}

it.each([
  [1, 10800],
  [5, 14000],
  [37, 39600],
  [300, 40000],
])(
  "bounds preview waiting for D=%s without declaring a terminal server failure",
  async (duration, waitMs) => {
    vi.useFakeTimers();
    const request = pendingFetch();
    const pending = new AggregatesApi(new ApiClient()).preview(input(duration));
    const result = expect(pending).rejects.toMatchObject({
      code: "preview_wait_timeout",
      status: 0,
      details: null,
    });
    await vi.advanceTimersByTimeAsync(waitMs - 1);
    expect(request.signal()?.aborted).toBe(false);
    expect(request.fetch).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(1);
    await result;
    expect(request.signal()?.aborted).toBe(true);
    expect(request.fetch).toHaveBeenCalledTimes(1);
    expect(vi.getTimerCount()).toBe(0);
  },
);

it("preserves caller cancellation and does not wait for or repeat a preview", async () => {
  vi.useFakeTimers();
  const request = pendingFetch();
  const caller = new AbortController();
  const pending = new AggregatesApi(new ApiClient()).preview(
    input(300),
    caller.signal,
  );
  const result = expect(pending).rejects.toMatchObject({ name: "AbortError" });
  caller.abort();
  await result;
  expect(request.signal()?.aborted).toBe(true);
  expect(vi.getTimerCount()).toBe(0);
  await vi.advanceTimersByTimeAsync(40000);
  expect(request.fetch).toHaveBeenCalledTimes(1);
});

it("releases preview wait resources after a real decoded response", async () => {
  vi.useFakeTimers();
  const request = vi.fn().mockResolvedValue(
    new Response(printAggregate(previewFixture()), {
      headers: { "Content-Type": "application/json" },
    }),
  );
  vi.stubGlobal("fetch", request);
  const result = await new AggregatesApi(new ApiClient()).preview(input(300));
  expect(exact(result.event_count)).toBe("1");
  expect(vi.getTimerCount()).toBe(0);
  await vi.advanceTimersByTimeAsync(40000);
  expect(request).toHaveBeenCalledTimes(1);
});

function respond(value: unknown) {
  return new Response(printAggregate(value), {
    headers: { "X-Request-ID": "execution-response" },
  });
}
it("reads one terminal diagnostic window through list and both lookups", async () => {
  const terminal = uncomputedPointFixture();
  const request = vi
    .fn()
    .mockResolvedValueOnce(respond({ items: [terminal], total: 1 }))
    .mockResolvedValueOnce(respond(terminal))
    .mockResolvedValueOnce(respond(terminal));
  vi.stubGlobal("fetch", request);
  const api = new AggregatesApi(new ApiClient());
  const list = await api.points(aggregateId);
  const byIndex = await api.pointAt(aggregateId, "0");
  const byId = await api.pointById(aggregateId, terminal.id);
  for (const point of [list.items[0], byIndex, byId]) {
    expect(point.calculation_status).toBe("uncomputed");
    expect(point.event_count).toBeNull();
    expect(point.features).toBeNull();
    expect(point.snapshot_reference_at).toBeNull();
    expect(point.payload_state).toBe("present");
  }
  expect(request).toHaveBeenCalledTimes(3);
});
it.each([
  ["event_count", 0],
  ["features", []],
  ["snapshot_metadata", {}],
  ["snapshot_reference_at", "2026-01-01T00:10:00Z"],
  ["completeness", "complete"],
  ["evidence_refs", {}],
  ["payload_state", "cleared"],
  ["execution", null],
])("rejects uncomputed with invented %s", async (key, value) => {
  vi.stubGlobal(
    "fetch",
    vi
      .fn()
      .mockResolvedValue(
        respond({ ...uncomputedPointFixture(), [key]: value }),
      ),
  );
  await expect(
    new AggregatesApi(new ApiClient()).pointAt(aggregateId, "0"),
  ).rejects.toMatchObject({
    code: "invalid_response",
    requestId: "execution-response",
  });
});
it.each([
  ["event_count", null],
  ["completeness", null],
  ["snapshot_metadata", null],
  ["calculation_status", undefined],
])(
  "keeps computed %s mandatory instead of weakening the existing branch",
  async (key, value) => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(respond({ ...pointFixture(), [key]: value })),
    );
    await expect(
      new AggregatesApi(new ApiClient()).pointAt(aggregateId, "0"),
    ).rejects.toMatchObject({ code: "invalid_response" });
  },
);
it("rejects a different execution index through by-ID without relying on a lookup URL index", async () => {
  const point = uncomputedPointFixture();
  vi.stubGlobal(
    "fetch",
    vi.fn().mockResolvedValue(
      respond({
        ...point,
        execution: { ...point.execution, window_index: 1 },
      }),
    ),
  );
  await expect(
    new AggregatesApi(new ApiClient()).pointById(aggregateId, point.id),
  ).rejects.toMatchObject({
    code: "invalid_response",
    requestId: "execution-response",
  });
});

it("keeps unmeasured existing event-time computed points valid without inventing an attempt", async () => {
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue(respond(pointFixture())));
  const point = await new AggregatesApi(new ApiClient()).pointAt(
    aggregateId,
    "0",
  );
  expect(point.calculation_status).toBe("computed");
  expect(point.execution).toBeNull();
});
it.each([
  { calculation_status: "computed" },
  { calculation_reason: null },
  { attempt_id: "secret" },
  { queue_seconds: -1 },
  { elapsed_seconds: null },
  { measured_through: "after_ack" },
  { timings: { raw: "secret" } },
  { timings: { selection: -1 } },
])(
  "rejects inconsistent or unsafe execution diagnostics %j",
  async (change) => {
    const point = uncomputedPointFixture();
    vi.stubGlobal(
      "fetch",
      vi
        .fn()
        .mockResolvedValue(
          respond({ ...point, execution: { ...point.execution, ...change } }),
        ),
    );
    await expect(
      new AggregatesApi(new ApiClient()).pointAt(aggregateId, "0"),
    ).rejects.toMatchObject({ code: "invalid_response" });
  },
);
it("accepts a measured partial duration above hard without inventing a commit ACK", async () => {
  const point = uncomputedPointFixture({
    execution: executionFixture({
      calculation_status: "uncomputed",
      calculation_reason: "execution_budget_exceeded",
      origin: "online",
      window_index: 0,
      elapsed_seconds: 31,
      measured_through: "finalization",
    }),
  });
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue(respond(point)));
  const decoded = await new AggregatesApi(new ApiClient()).pointAt(
    aggregateId,
    "0",
  );
  expect(decoded.execution?.online_latency_seconds).toBeNull();
  expect(decoded.execution?.soft_latency_exceeded).toBeNull();
});
it("only exposes validated server preview refusal as a measured fact", () => {
  const execution = executionFixture({
    calculation_status: "uncomputed",
    calculation_reason: "execution_budget_exceeded",
    measured_through: "finalization",
  });
  expect(
    previewExecutionFailure(
      new ApiError(422, "execution_budget_exceeded", null, "private", {
        execution,
      }),
    ),
  ).toEqual(execution);
  expect(
    previewExecutionFailure(
      new ApiError(0, "preview_wait_timeout", null, null, { execution }),
    ),
  ).toBeNull();
  expect(
    previewExecutionFailure(
      new ApiError(422, "execution_budget_exceeded", null, null, {
        execution: { ...execution, origin: "online" },
      }),
    ),
  ).toBeNull();
});
it("rejects a stale or malformed card policy while retaining strict DTO checks", async () => {
  const card = aggregateFixture({
    duration_seconds: 5,
    execution_policy: {
      policy_version: "window_execution_v1",
      soft_latency_seconds: 5,
      hard_budget_seconds: 30,
    },
  });
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue(respond(card)));
  await expect(
    new AggregatesApi(new ApiClient()).card(aggregateId),
  ).rejects.toMatchObject({ code: "invalid_response" });
});
