import { afterEach, expect, it, vi } from "vitest";
import { ApiClient } from "./client";
import { AggregatesApi } from "./aggregates";
import { exact, parseAggregate, printAggregate } from "./aggregateJson";
import {
  aggregateFixture,
  aggregateId,
  autoCleanupFixture,
  missingAutoCleanupFixture,
} from "./aggregateTestData";

afterEach(() => vi.unstubAllGlobals());
function client(summary: unknown) {
  vi.stubGlobal(
    "fetch",
    vi.fn().mockResolvedValue(
      new Response(
        printAggregate({ ...aggregateFixture(), auto_cleanup: summary }),
        {
          status: 200,
          headers: { "X-Request-ID": "auto-cleanup-dto" },
        },
      ),
    ),
  );
  return new AggregatesApi(new ApiClient());
}
it("keeps missing retention state explicit instead of fabricating initial revision or counters", async () => {
  const expected = missingAutoCleanupFixture();
  const result = await client(expected).card(aggregateId);
  expect(result.auto_cleanup).toEqual(expected);
  expect(result.auto_cleanup.history_revision).toBeNull();
  expect(result.auto_cleanup.auto_cleared_total).toBeNull();
});
it("preserves int64 revisions, counters and protected ranges through the real lossless adapter", async () => {
  const maximum = parseAggregate("9223372036854775807");
  const result = await client(
    autoCleanupFixture({
      status: "protected",
      reserve_windows: 512,
      auto_cleared_total: parseAggregate("9007199254740993"),
      history_revision: maximum,
      protected_total: 1,
      protected_from_index: parseAggregate("9007199254740993"),
      protected_by: [
        {
          detector_id: aggregateId,
          name: "Отстающая модель",
          reason: "unconsumed_points",
          protected_from_index: parseAggregate("9007199254740993"),
          protected_to_index: maximum,
        },
      ],
    }),
  ).card(aggregateId);
  expect(exact(result.auto_cleanup.auto_cleared_total)).toBe(
    "9007199254740993",
  );
  expect(exact(result.auto_cleanup.history_revision)).toBe(
    "9223372036854775807",
  );
  expect(exact(result.auto_cleanup.protected_by[0].protected_to_index)).toBe(
    "9223372036854775807",
  );
});
it.each([
  "scheduled",
  "idle",
  "compacting",
  "protected",
  "deferred",
  "error",
  "inactive",
] as const)("decodes the mandatory %s summary", async (status) => {
  expect(
    (await client(autoCleanupFixture({ status })).card(aggregateId))
      .auto_cleanup.status,
  ).toBe(status);
});
it.each([
  undefined,
  null,
  {},
  autoCleanupFixture({ policy_version: null }),
  { ...autoCleanupFixture(), policy_version: "other" },
  { ...autoCleanupFixture(), status: "succeeded" },
  { ...autoCleanupFixture(), history_revision: undefined },
  { ...autoCleanupFixture(), history_revision: "1" },
  autoCleanupFixture({ history_revision: -1 }),
  autoCleanupFixture({
    history_revision: parseAggregate("9223372036854775808"),
  }),
  autoCleanupFixture({ history_revision: parseAggregate("1.0") }),
  autoCleanupFixture({ auto_cleared_total: 1, history_revision: 0 }),
  autoCleanupFixture({ reserve_windows: 0 }),
  autoCleanupFixture({ reserve_windows: 511 }),
  autoCleanupFixture({ reserve_windows: 4097 }),
  autoCleanupFixture({ retained_from_index: 5, retained_to_index: null }),
  autoCleanupFixture({ retained_from_index: 5, retained_to_index: 4 }),
  { ...autoCleanupFixture(), checked_at: "2026-02-30T00:00:00Z" },
  { ...autoCleanupFixture(), protected_by: null },
  autoCleanupFixture({
    protected_total: 0,
    protected_by: [
      {
        detector_id: aggregateId,
        name: "Модель",
        reason: "planned_warmup",
        protected_from_index: 0,
        protected_to_index: 1,
      },
    ],
  }),
  autoCleanupFixture({
    protected_total: 1,
    protected_by: [
      {
        detector_id: "bad",
        name: "Модель",
        reason: "planned_warmup",
        protected_from_index: 0,
        protected_to_index: 1,
      },
    ],
  }),
  autoCleanupFixture({
    protected_total: 1,
    protected_by: [
      {
        detector_id: aggregateId,
        name: "Модель",
        reason: "planned_warmup",
        protected_from_index: 2,
        protected_to_index: 1,
      },
    ],
  }),
  {
    ...autoCleanupFixture(),
    protected_total: 1,
    protected_by: [
      {
        detector_id: aggregateId,
        name: "Модель",
        reason: "planned_warmup",
        protected_from_index: null,
        protected_to_index: null,
      },
    ],
  },
  {
    ...autoCleanupFixture(),
    protected_total: 1,
    protected_by: [
      {
        detector_id: aggregateId,
        name: "Модель",
        reason: "unknown",
        protected_from_index: 0,
        protected_to_index: null,
      },
    ],
  },
  { ...missingAutoCleanupFixture(), history_revision: 0 },
  { ...missingAutoCleanupFixture(), protected_total: 0 },
])(
  "rejects an incomplete, unsafe or fabricated retention summary %#",
  async (summary) => {
    await expect(client(summary).card(aggregateId)).rejects.toMatchObject({
      code: "invalid_response",
      requestId: "auto-cleanup-dto",
    });
  },
);
it("accepts a future unread suffix without treating it as a nonempty retained range", async () => {
  const result = await client(
    autoCleanupFixture({
      status: "protected",
      protected_total: 1,
      retained_from_index: 8,
      retained_to_index: 519,
      protected_by: [
        {
          detector_id: aggregateId,
          name: "Модель",
          reason: "unconsumed_points",
          protected_from_index: 1000,
          protected_to_index: null,
        },
      ],
    }),
  ).card(aggregateId);
  expect(exact(result.auto_cleanup.protected_by[0].protected_from_index)).toBe(
    "1000",
  );
  expect(result.auto_cleanup.protected_by[0].protected_to_index).toBeNull();
  expect(exact(result.auto_cleanup.retained_to_index)).toBe("519");
});
