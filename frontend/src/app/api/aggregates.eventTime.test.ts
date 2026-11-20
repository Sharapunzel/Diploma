import { afterEach, expect, it, vi } from "vitest";
import { ApiClient } from "./client";
import { AggregatesApi, keepLastDecimal } from "./aggregates";
import { exact, parseAggregate, printAggregate } from "./aggregateJson";
import {
  aggregateFixture,
  aggregateId,
  cleanupFixture,
  cleanupId,
  pointFixture,
  previewFixture,
  protectionFixture,
  receiptFixture,
} from "./aggregateTestData";

afterEach(() => vi.unstubAllGlobals());
function response(value: unknown, status = 200) {
  return new Response(printAggregate(value), {
    status,
    headers: { "X-Request-ID": "event-time-response" },
  });
}
it("decodes event-time and read-only retired cards and preserved legacy points without changing exact numbers", async () => {
  const card = aggregateFixture({
    semantic_basis: "availability_v1",
    execution_mode: "retired",
    execution_reason: "aggregate_time_semantics_retired",
    online_start_index: null,
  });
  const point = pointFixture({
    origin: "legacy",
    time_basis: "availability_at",
    window_index: null,
    features: [
      {
        key: "n",
        value: parseAggregate("9007199254740993"),
        reason: null,
        valid_count: 1,
        invalid_count: 0,
      },
    ],
  });
  vi.stubGlobal(
    "fetch",
    vi
      .fn()
      .mockResolvedValueOnce(response(card))
      .mockResolvedValueOnce(response(point)),
  );
  const api = new AggregatesApi(new ApiClient());
  expect(await api.card(aggregateId)).toMatchObject({
    semantic_basis: "availability_v1",
    execution_mode: "retired",
  });
  expect(
    exact((await api.pointById(aggregateId, point.id)).features![0].value),
  ).toBe("9007199254740993");
});
it("preserves the exact previously accepted legacy create replay body and key", async () => {
  const retired = aggregateFixture({
    semantic_basis: "availability_v1",
    execution_mode: "retired",
    execution_reason: "aggregate_time_semantics_retired",
  });
  const request = vi.fn().mockResolvedValue(response(retired, 201));
  vi.stubGlobal("fetch", request);
  const body = {
    source_id: retired.source_id,
    aggregate_rule: retired.aggregate_rule,
    schedule: {
      boundary: retired.original_boundary,
      timezone: "UTC",
      duration_seconds: 300,
    },
    window_start_utc: retired.anchor_utc,
    time_basis: "availability_at",
    name: retired.name,
    rule_hash: retired.rule_hash,
  };
  const result = await new AggregatesApi(new ApiClient()).create(
    body,
    "accepted-old-key",
  );
  expect(result.execution_mode).toBe("retired");
  expect(request.mock.calls[0][1].body).toBe(printAggregate(body));
  expect(request.mock.calls[0][1].headers.get("Idempotency-Key")).toBe(
    "accepted-old-key",
  );
});
it.each([
  ["semantic_basis", "processing"],
  ["execution_mode", "paused"],
  ["paused", "false"],
  ["execution_generation", 1.5],
  ["active_cleanup_id", "not-uuid"],
  ["next_close_not_before_at", "tomorrow"],
  ["online_start_index", undefined],
  ["diagnostics", null],
  ["next_index", -1],
])("rejects malformed required card field %s", async (key, value) => {
  vi.stubGlobal(
    "fetch",
    vi
      .fn()
      .mockResolvedValue(response({ ...aggregateFixture(), [key]: value })),
  );
  await expect(
    new AggregatesApi(new ApiClient()).card(aggregateId),
  ).rejects.toMatchObject({
    code: "invalid_response",
    requestId: "event-time-response",
  });
});
it.each([
  ["origin", "historical"],
  ["in_process", undefined],
  ["snapshot_reference_at", "broken"],
  ["snapshot_metadata", null],
  ["close_not_before_at", false],
])("checks required preview field %s", async (key, value) => {
  vi.stubGlobal(
    "fetch",
    vi.fn().mockResolvedValue(response({ ...previewFixture(), [key]: value })),
  );
  await expect(
    new AggregatesApi(new ApiClient()).preview({} as never),
  ).rejects.toMatchObject({ code: "invalid_response" });
});
it("transports exact int64 keep_last as a number, uses the cleanup identity and keeps command CSRF/header contracts", async () => {
  const receipt = receiptFixture({
    action: "cleanup",
    cleanup_operation_id: cleanupId,
  });
  const request = vi
    .fn()
    .mockResolvedValueOnce(
      response(
        protectionFixture({
          requested_keep: parseAggregate("9223372036854775807"),
        }),
      ),
    )
    .mockResolvedValueOnce(response(receipt, 202))
    .mockResolvedValueOnce(response(cleanupFixture()))
    .mockResolvedValueOnce(
      response(
        receiptFixture({
          action: "cleanup_retry",
          cleanup_operation_id: cleanupId,
        }),
        202,
      ),
    )
    .mockResolvedValueOnce(response(receiptFixture()));
  vi.stubGlobal("fetch", request);
  const client = new ApiClient();
  client.setCsrf("csrf");
  const api = new AggregatesApi(client);
  const plan = await api.cleanupPreview(aggregateId, "9223372036854775807");
  expect(exact(plan.requested_keep)).toBe("9223372036854775807");
  const accepted = await api.cleanup(
    aggregateId,
    "9223372036854775807",
    "cleanup-key",
  );
  expect(accepted.cleanup_operation_id).toBe(cleanupId);
  await api.cleanupOperation(aggregateId, accepted.cleanup_operation_id!);
  await api.retryCleanup(aggregateId, cleanupId, "retry-key");
  await api.command(aggregateId, "pause", "pause-key");
  expect(request.mock.calls[0][0]).toContain("keep_last=9223372036854775807");
  expect(request.mock.calls[1][1].body).toBe(
    '{"keep_last":9223372036854775807}',
  );
  expect(request.mock.calls[2][0]).toContain(
    `/cleanup-operations/${cleanupId}`,
  );
  expect(request.mock.calls[3][1].headers.get("Idempotency-Key")).toBe(
    "retry-key",
  );
  expect(request.mock.calls[4][1].headers.get("Idempotency-Key")).toBe(
    "pause-key",
  );
  expect(request.mock.calls[4][1].headers.get("X-CSRF-Token")).toBe("csrf");
  expect(request.mock.calls[4][1].body).toBeUndefined();
});
it.each(["", "-1", "+1", "1.0", "1e3", "true", " 1", "9223372036854775808"])(
  "rejects invalid keep_last input %j before transport",
  (value) => {
    expect(() => keepLastDecimal(value)).toThrow();
  },
);
it.each(["0", "1", "999", "9007199254740993", "9223372036854775807"])(
  "preserves legal decimal %s",
  (value) => {
    expect(keepLastDecimal(value)).toBe(value);
  },
);
it("rejects malformed command, blocker and operation instead of accepting a successful status", async () => {
  vi.stubGlobal(
    "fetch",
    vi
      .fn()
      .mockResolvedValueOnce(
        response(
          receiptFixture({ action: "cleanup", cleanup_operation_id: null }),
          202,
        ),
      )
      .mockResolvedValueOnce(
        response(
          protectionFixture({
            blockers: [
              {
                detector_id: aggregateId,
                name: "D",
                reason: "running",
                minimum_keep: 1,
                protected_start_index: null,
              } as never,
            ],
          }),
        ),
      )
      .mockResolvedValueOnce(
        response({ ...cleanupFixture(), deleted_bytes_if_measured: "0" }),
      ),
  );
  const api = new AggregatesApi(new ApiClient());
  await expect(api.cleanup(aggregateId, "0", "key")).rejects.toMatchObject({
    code: "invalid_response",
  });
  await expect(api.cleanupPreview(aggregateId, "0")).rejects.toMatchObject({
    code: "invalid_response",
  });
  await expect(
    api.cleanupOperation(aggregateId, cleanupId),
  ).rejects.toMatchObject({ code: "invalid_response" });
});
