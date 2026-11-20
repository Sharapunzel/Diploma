import { afterEach, expect, it, vi } from "vitest";
import { AggregatesApi, type Aggregate } from "../../app/api/aggregates";
import { ApiError } from "../../app/api/client";
import { exact, parseAggregate } from "../../app/api/aggregateJson";
import {
  aggregateFixture,
  aggregateId,
  autoCleanupFixture,
  missingAutoCleanupFixture,
  pointFixture,
  receiptFixture,
} from "../../app/api/aggregateTestData";
import { CardStore } from "./CardStore";

const owned: CardStore[] = [];
afterEach(() => {
  owned.splice(0).forEach((store) => store.dispose());
  vi.useRealTimers();
});
function setup() {
  const api = {
    card: vi.fn().mockResolvedValue(aggregateFixture()),
    command: vi.fn(),
    cleanup: vi.fn(),
  };
  const store = new CardStore(api as unknown as AggregatesApi, aggregateId);
  owned.push(store);
  return { api, store };
}
function revised(revision: string) {
  return aggregateFixture({
    auto_cleanup: autoCleanupFixture({
      history_revision: parseAggregate(revision),
      auto_cleared_total: parseAggregate(revision),
    }),
  });
}
it("polls visible cards no faster than five seconds, coalesces long reads, preserves exact revision and invalidates cached history", async () => {
  vi.useFakeTimers();
  const { api, store } = setup();
  store.setVisible(true);
  await store.load();
  store.historicalResult = pointFixture();
  let resolve!: (value: Aggregate) => void;
  api.card.mockReturnValueOnce(
    new Promise<Aggregate>((done) => {
      resolve = done;
    }),
  );
  await vi.advanceTimersByTimeAsync(4999);
  expect(api.card).toHaveBeenCalledTimes(1);
  await vi.advanceTimersByTimeAsync(1);
  expect(api.card).toHaveBeenCalledTimes(2);
  expect(store.cardLoading).toBe(false);
  await store.load();
  await vi.advanceTimersByTimeAsync(10000);
  expect(api.card).toHaveBeenCalledTimes(2);
  const updated = revised("9007199254740993");
  api.card.mockResolvedValue(updated);
  resolve(updated);
  await vi.advanceTimersByTimeAsync(0);
  expect(exact(store.card!.auto_cleanup.history_revision)).toBe(
    "9007199254740993",
  );
  expect(store.pointsVersion).toBe(1);
  expect(store.historicalResult).toBeNull();
  expect(store.success).toBe("");
  expect(api.command).not.toHaveBeenCalled();
  expect(api.cleanup).not.toHaveBeenCalled();
  store.setVisible(false);
  await vi.advanceTimersByTimeAsync(10000);
  expect(api.card).toHaveBeenCalledTimes(2);
  store.dispose();
  expect(vi.getTimerCount()).toBe(0);
});
it("stops polling after a background error and requires an explicit read without resolving unknown commands", async () => {
  vi.useFakeTimers();
  const { api, store } = setup();
  store.setVisible(true);
  await store.load();
  store.commandOutcome = "unknown";
  store.commandNeedsRead = true;
  store.commandIntent = Object.freeze({
    target: aggregateId,
    action: "pause",
    key: "unchanged-intent",
  });
  store.mutationOutcome = "unknown";
  api.card.mockRejectedValueOnce(new ApiError(503, "aggregate_database_error"));
  await vi.advanceTimersByTimeAsync(5000);
  expect(store.autoCleanupError).toBeInstanceOf(ApiError);
  expect(store.error).toBeNull();
  expect(store.commandOutcome).toBe("unknown");
  expect(store.commandNeedsRead).toBe(true);
  expect(store.mutationOutcome).toBe("unknown");
  await vi.advanceTimersByTimeAsync(15000);
  expect(api.card).toHaveBeenCalledTimes(2);
  await store.load();
  expect(store.autoCleanupError).toBeNull();
  expect(store.commandOutcome).toBe("unknown");
  expect(store.commandIntent?.key).toBe("unchanged-intent");
  await vi.advanceTimersByTimeAsync(5000);
  expect(api.card).toHaveBeenCalledTimes(4);
  expect(api.command).not.toHaveBeenCalled();
});
it("aborts a hidden background read and refuses its late revision before resuming the same scoped card", async () => {
  vi.useFakeTimers();
  const { api, store } = setup();
  store.setVisible(true);
  await store.load();
  let resolve!: (value: Aggregate) => void;
  api.card.mockReturnValueOnce(
    new Promise<Aggregate>((done) => {
      resolve = done;
    }),
  );
  await vi.advanceTimersByTimeAsync(5000);
  const signal = api.card.mock.calls[1][1] as AbortSignal;
  store.setVisible(false);
  expect(signal.aborted).toBe(true);
  resolve(revised("99"));
  await vi.advanceTimersByTimeAsync(10000);
  expect(exact(store.card!.auto_cleanup.history_revision)).toBe("0");
  expect(store.pointsVersion).toBe(0);
  expect(api.card).toHaveBeenCalledTimes(2);
  api.card.mockResolvedValue(revised("1"));
  store.setVisible(true);
  await vi.advanceTimersByTimeAsync(0);
  expect(store.pointsVersion).toBe(1);
  expect(api.card).toHaveBeenCalledTimes(3);
  store.dispose();
  await vi.advanceTimersByTimeAsync(10000);
  expect(store.card).toBeNull();
  expect(vi.getTimerCount()).toBe(0);
});
it("keeps a missing state's null revision unknown and refreshes when a valid revision becomes available", async () => {
  vi.useFakeTimers();
  const { api, store } = setup();
  api.card.mockResolvedValue(
    aggregateFixture({ auto_cleanup: missingAutoCleanupFixture() }),
  );
  store.setVisible(true);
  await store.load();
  expect(store.card!.auto_cleanup.history_revision).toBeNull();
  expect(store.pointsVersion).toBe(0);
  api.card.mockResolvedValue(revised("0"));
  await vi.advanceTimersByTimeAsync(5000);
  expect(store.pointsVersion).toBe(1);
  expect(exact(store.card!.auto_cleanup.history_revision)).toBe("0");
});

it.each([null, "confirmed"] as const)(
  "moves a failed explicit read out of background and an old %s command context while keeping polling stopped until a successful manual read",
  async (previousCommand) => {
    vi.useFakeTimers();
    const { api, store } = setup();
    store.setVisible(true);
    await store.load();
    store.commandOutcome = previousCommand;
    store.commandReceipt = previousCommand ? receiptFixture() : null;
    store.commandNeedsRead = false;
    store.mutationOutcome = "unknown";
    const backgroundError = new ApiError(503, "aggregate_database_error");
    api.card.mockRejectedValueOnce(backgroundError);
    await vi.advanceTimersByTimeAsync(5000);
    expect(store.autoCleanupError).toBe(backgroundError);
    const manualError = new ApiError(500, "internal_error");
    api.card.mockRejectedValueOnce(manualError);
    await store.load();
    expect(store.error).toBe(manualError);
    expect(store.autoCleanupError).toBeNull();
    expect(store.commandReadError).toBeNull();
    expect(store.mutationOutcome).toBe("unknown");
    await vi.advanceTimersByTimeAsync(15000);
    store.setVisible(false);
    store.setVisible(true);
    await vi.advanceTimersByTimeAsync(15000);
    expect(api.card).toHaveBeenCalledTimes(3);
    await store.load();
    expect(store.error).toBeNull();
    expect(store.mutationOutcome).toBeNull();
    await vi.advanceTimersByTimeAsync(5000);
    expect(api.card).toHaveBeenCalledTimes(5);
    expect(api.command).not.toHaveBeenCalled();
    expect(api.cleanup).not.toHaveBeenCalled();
  },
);

it("presents an accepted command's failed GET only in its command context and never retries the command or read automatically", async () => {
  vi.useFakeTimers();
  const { api, store } = setup();
  store.setVisible(true);
  await store.load();
  api.command.mockResolvedValueOnce(receiptFixture());
  const error = new ApiError(503, "aggregate_database_error");
  api.card.mockRejectedValueOnce(error);
  await store.sendCommand("pause");
  expect(store.commandOutcome).toBe("confirmed");
  expect(store.commandNeedsRead).toBe(true);
  expect(store.commandReadError).toBe(error);
  expect(store.error).toBeNull();
  expect(store.autoCleanupError).toBeNull();
  expect(store.canCommand).toBe(false);
  await vi.advanceTimersByTimeAsync(15000);
  store.setVisible(false);
  store.setVisible(true);
  await vi.advanceTimersByTimeAsync(15000);
  expect(api.card).toHaveBeenCalledTimes(2);
  expect(api.command).toHaveBeenCalledTimes(1);
  await store.refreshCommandCard();
  expect(store.commandReadError).toBeNull();
  expect(store.canCommand).toBe(true);
  await vi.advanceTimersByTimeAsync(5000);
  expect(api.card).toHaveBeenCalledTimes(4);
  expect(api.command).toHaveBeenCalledTimes(1);
});
