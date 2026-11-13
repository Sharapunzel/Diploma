import { expect, it, vi } from "vitest";
import { runInAction } from "mobx";
import type { AggregatesApi, Point, Operator } from "../../app/api/aggregates";
import { exact, parseAggregate } from "../../app/api/aggregateJson";
import { ApiError } from "../../app/api/client";
import { CardStore } from "./CardStore";

const start = "2026-01-01T00:00:00Z";
it("treats an empty operator response as a retryable failure", async () => {
  const operators = vi.fn().mockResolvedValue([]);
  const store = new CardStore(
    { operators } as unknown as AggregatesApi,
    "empty",
  );
  await store.loadOperators();
  expect(store.operators).toEqual([]);
  expect(store.operatorsError).not.toBeNull();
  expect(store.lifecycle.isPending("operators")).toBe(false);
  store.dispose();
});
const point: Point = {
  id: "historical-1",
  aggregate_id: "aggregate",
  kind: "historical",
  start_utc: start,
  end_utc: "2026-01-01T00:05:00Z",
  scheduled_at: "2026-01-01T00:05:00Z",
  actual_started_at: "2026-01-01T01:00:00Z",
  actual_completed_at: "2026-01-01T01:00:01Z",
  event_count: 1,
  features: [
    {
      key: "n",
      value: parseAggregate("9007199254740993"),
      valid_count: 1,
      invalid_count: 0,
      reason: null,
    },
  ],
  completeness: "complete",
  completeness_reason: "verified_input_and_kafka_coverage",
  time_basis: "availability_at",
  event_time_min: start,
  event_time_max: start,
};
function setup() {
  const historical = vi.fn().mockResolvedValue(point);
  const store = new CardStore(
    { historical } as unknown as AggregatesApi,
    "aggregate",
  );
  return { historical, store };
}
it("retries operator failures explicitly, locks concurrent retries and discards late disposed metadata", async () => {
  const ops = [
    { name: "count", kind: "feature", parameters: [] },
  ] as unknown as Operator[];
  const operators = vi
    .fn()
    .mockRejectedValue(new ApiError(503, "internal_error"));
  const store = new CardStore({ operators } as unknown as AggregatesApi, "one");
  await store.loadOperators();
  expect(store.operatorsError).not.toBeNull();
  await store.loadOperators();
  expect(operators).toHaveBeenCalledTimes(2);
  let resolve!: (ops: Operator[]) => void;
  operators.mockImplementation(
    () =>
      new Promise<Operator[]>((done) => {
        resolve = done;
      }),
  );
  const pending = store.loadOperators();
  await store.loadOperators();
  expect(operators).toHaveBeenCalledTimes(3);
  expect(store.lifecycle.isPending("operators")).toBe(true);
  resolve(ops);
  await pending;
  expect(store.operatorsError).toBeNull();
  expect(store.operators.map((o) => o.name)).toEqual(["count"]);
  const stale = store.loadOperators();
  const signal = operators.mock.calls.at(-1)![0] as AbortSignal;
  store.dispose();
  expect(signal.aborted).toBe(true);
  resolve(ops);
  await stale;
  expect(store.operators).toEqual([]);
  expect(store.lifecycle.isPending("operators")).toBe(false);
});
it("shows only the confirmed selected historical window and clears it when its date is cleared", async () => {
  const { store, historical } = setup();
  expect(store.historicalResult).toBeNull();
  expect(await store.history(start)).toBe(true);
  expect(store.historicalResult?.start_utc).toBe(start);
  expect(exact(store.historicalResult!.features![0].value)).toBe(
    "9007199254740993",
  );
  expect(store.success).toContain("выбранного окна");
  store.clearHistoricalResult();
  expect(store.historicalResult).toBeNull();
  expect(store.success).toBe("");
  expect(store.historyError).toBeNull();
  expect(historical).toHaveBeenCalledTimes(1);
  store.dispose();
});
it("does not restore an old historical response after the selection changes, or send another POST", async () => {
  const { store, historical } = setup();
  let resolve!: (value: Point) => void;
  historical.mockReturnValue(
    new Promise<Point>((done) => {
      resolve = done;
    }),
  );
  const request = store.history(start);
  store.clearHistoricalResult();
  resolve(point);
  expect(await request).toBe(true);
  expect(store.historicalResult).toBeNull();
  expect(store.success).toBe("");
  expect(historical).toHaveBeenCalledTimes(1);
  expect(store.lifecycle.isPending("history")).toBe(false);
  store.dispose();
});
it("removes a previous historical result on failure and scopes its error to that date", async () => {
  const { store, historical } = setup();
  await store.history(start);
  const failure = new ApiError(422, "historical_time_basis_unknown");
  historical.mockRejectedValueOnce(failure);
  expect(await store.history("2026-01-01T00:05:00Z")).toBe(false);
  expect(store.historicalResult).toBeNull();
  expect(store.success).toBe("");
  expect(store.historyError).toBe(failure);
  const mutationError = new ApiError(503, "aggregate_database_error");
  runInAction(() => {
    store.error = mutationError;
    store.mutationOutcome = "unknown";
  });
  store.clearHistoricalResult();
  expect(store.historyError).toBeNull();
  expect(store.error).toBe(mutationError);
  expect(store.mutationOutcome).toBe("unknown");
  expect(store.canMutate).toBe(false);
  store.dispose();
});
it("discards a response after the aggregate scope is disposed", async () => {
  const { store, historical } = setup();
  let resolve!: (value: Point) => void;
  historical.mockReturnValue(
    new Promise<Point>((done) => {
      resolve = done;
    }),
  );
  const request = store.history(start);
  const signal = historical.mock.calls[0][2] as AbortSignal;
  store.dispose();
  resolve(point);
  await request;
  expect(signal.aborted).toBe(true);
  expect(store.historicalResult).toBeNull();
  expect(store.success).toBe("");
});
