import { afterEach, expect, it, vi } from "vitest";
import type { AggregatesApi } from "../../app/api/aggregates";
import { ApiError } from "../../app/api/client";
import { exact, parseAggregate } from "../../app/api/aggregateJson";
import {
  aggregateFixture,
  aggregateId,
  aggregateTime,
  autoCleanupFixture,
  cleanupFixture,
  cleanupId,
  pointFixture,
  protectionFixture,
  receiptFixture,
} from "../../app/api/aggregateTestData";
import { CardStore } from "./CardStore";

afterEach(() => vi.useRealTimers());
function setup() {
  const api = {
    card: vi.fn().mockResolvedValue(aggregateFixture()),
    command: vi.fn().mockResolvedValue(receiptFixture()),
    cleanupPreview: vi.fn().mockResolvedValue(protectionFixture()),
    cleanup: vi
      .fn()
      .mockResolvedValue(
        receiptFixture({ action: "cleanup", cleanup_operation_id: cleanupId }),
      ),
    cleanupOperation: vi.fn().mockResolvedValue(cleanupFixture()),
    retryCleanup: vi.fn().mockResolvedValue(
      receiptFixture({
        action: "cleanup_retry",
        cleanup_operation_id: cleanupId,
      }),
    ),
  };
  const store = new CardStore(api as unknown as AggregatesApi, aggregateId);
  return { api, store };
}
it("keeps the exact unknown pause intent until explicit replay, blocks the opposite command, and separately reports failed GET after receipt", async () => {
  const { api, store } = setup();
  api.command.mockRejectedValueOnce(new ApiError(503, "http_error"));
  await store.sendCommand("pause");
  expect(store.commandOutcome).toBe("unknown");
  const intent = store.commandIntent;
  await store.sendCommand("unpause");
  await store.load();
  expect(store.commandOutcome).toBe("unknown");
  expect(api.command).toHaveBeenCalledTimes(1);
  api.card.mockRejectedValueOnce(new ApiError(503, "card_failed"));
  await store.replayCommand();
  expect(api.command.mock.calls[1].slice(0, 3)).toEqual([
    aggregateId,
    "pause",
    intent!.key,
  ]);
  expect(store.commandOutcome).toBe("confirmed");
  expect(store.commandReceipt?.action).toBe("pause");
  expect(store.commandReadError).not.toBeNull();
  expect(store.commandNeedsRead).toBe(true);
  await store.sendCommand("unpause");
  expect(api.command).toHaveBeenCalledTimes(2);
  await store.refreshCommandCard();
  expect(store.commandReadError).toBeNull();
  expect(store.canCommand).toBe(true);
  store.dispose();
});
it("locks a pending POST and a receipt for a different target becomes unknown, never current success", async () => {
  const { api, store } = setup();
  let resolve!: (value: ReturnType<typeof receiptFixture>) => void;
  api.command.mockReturnValue(
    new Promise((done) => {
      resolve = done;
    }),
  );
  const pending = store.sendCommand("pause");
  await store.sendCommand("pause");
  expect(api.command).toHaveBeenCalledTimes(1);
  expect(store.commandBusy).toBe(true);
  resolve(receiptFixture({ aggregate_id: cleanupId }));
  await pending;
  expect(store.commandOutcome).toBe("unknown");
  expect(store.commandReceipt).toBeNull();
  expect(store.success).toBe("");
  store.dispose();
});
it("does not let a previous receipt confirm an unknown new command", async () => {
  const { api, store } = setup();
  await store.sendCommand("pause");
  api.command.mockRejectedValueOnce(new ApiError(0, "network_error"));
  await store.sendCommand("unpause");
  expect(store.commandReceipt).toBeNull();
  expect(store.commandOutcome).toBe("unknown");
  expect(store.commandIntent?.action).toBe("unpause");
  store.dispose();
});
it("retains unknown intent across navigation only for its actor and target, aborting old reads", async () => {
  const { api, store } = setup();
  const owner = {};
  store.bindScope(owner, "actor-one");
  api.command.mockRejectedValueOnce(new ApiError(0, "network_error"));
  await store.sendCommand("pause");
  const key = store.commandIntent!.key;
  store.dispose();
  const other = new CardStore(api as unknown as AggregatesApi, cleanupId);
  other.bindScope(owner, "actor-one");
  expect(other.commandIntent).toBeNull();
  other.dispose();
  const back = new CardStore(api as unknown as AggregatesApi, aggregateId);
  back.bindScope(owner, "actor-one");
  expect(back.commandIntent?.key).toBe(key);
  expect(back.commandOutcome).toBe("unknown");
  back.dispose();
  const changed = new CardStore(api as unknown as AggregatesApi, aggregateId);
  changed.bindScope(owner, "actor-two");
  expect(changed.commandIntent).toBeNull();
  changed.dispose();
});
it("invalidates protection for a changed N, refuses stale responses and retains exact int64", async () => {
  const { api, store } = setup();
  let resolve!: (value: ReturnType<typeof protectionFixture>) => void;
  api.cleanupPreview.mockReturnValueOnce(
    new Promise((done) => {
      resolve = done;
    }),
  );
  const pending = store.loadCleanupPlan();
  store.setCleanupKeepLast("1");
  resolve(protectionFixture());
  await pending;
  expect(store.cleanupPlan).toBeNull();
  expect(store.canCleanup).toBe(false);
  await store.sendCommand("cleanup");
  expect(api.cleanup).not.toHaveBeenCalled();
  store.setCleanupKeepLast("9223372036854775807");
  api.cleanupPreview.mockResolvedValue(
    protectionFixture({
      requested_keep: parseAggregate("9223372036854775807"),
      planned_delete: 0,
    }),
  );
  await store.loadCleanupPlan();
  expect(exact(store.cleanupPlan!.requested_keep)).toBe("9223372036854775807");
  await store.sendCommand("cleanup");
  expect(api.cleanup.mock.calls[0][1]).toBe("9223372036854775807");
  expect(store.commandReceipt?.cleanup_operation_id).toBe(cleanupId);
  expect(api.cleanupOperation.mock.calls[0][1]).toBe(cleanupId);
  expect(store.cleanupOperation?.status).toBe("pending");
  store.dispose();
});
it("uses all detector blockers and never turns unknown protection or N below minimum into permission", async () => {
  const { api, store } = setup();
  api.cleanupPreview.mockResolvedValue(
    protectionFixture({
      minimum_keep: 3,
      maximum_delete: 7,
      can_cleanup: false,
      reason: "detector_not_stopped",
      blockers: [
        {
          detector_id: aggregateId,
          name: "Первый",
          reason: "detector_not_stopped",
          minimum_keep: 1,
          protected_start_index: null,
          protected_end_index: null,
        },
        {
          detector_id: cleanupId,
          name: "Второй",
          reason: "protected_history",
          minimum_keep: 3,
          protected_start_index: 7,
          protected_end_index: 9,
        },
      ],
    }),
  );
  await store.loadCleanupPlan();
  await store.sendCommand("cleanup");
  expect(store.cleanupPlan!.blockers).toHaveLength(2);
  expect(api.cleanup).not.toHaveBeenCalled();
  api.cleanupPreview.mockRejectedValue(
    new ApiError(503, "cleanup_protection_unavailable"),
  );
  await store.loadCleanupPlan();
  expect(store.cleanupPlan).toBeNull();
  expect(store.canCleanup).toBe(false);
  store.dispose();
});
it("refreshes points/card and clears selection only after succeeded, preserving unknown bytes", async () => {
  const { api, store } = setup();
  await store.loadCleanupPlan();
  await store.sendCommand("cleanup");
  expect(store.pointsVersion).toBe(0);
  expect(store.cleanupOperation!.deleted_bytes_if_measured).toBeNull();
  api.cleanupOperation.mockResolvedValue(
    cleanupFixture({
      status: "succeeded",
      deleted_points: 10,
      completed_at: "2026-01-01T00:10:00Z",
    }),
  );
  await store.loadCleanupOperation();
  expect(store.pointsVersion).toBe(1);
  expect(store.cleanupPlan).toBeNull();
  expect(store.historicalResult).toBeNull();
  await store.loadCleanupOperation();
  expect(store.pointsVersion).toBe(1);
  store.dispose();
});
it("resumes a failed operation with a new key and replays an unknown retry with that same key", async () => {
  const { api, store } = setup();
  api.cleanupOperation.mockResolvedValue(
    cleanupFixture({ status: "failed", safe_error: "temporary_unavailable" }),
  );
  await store.loadCleanupPlan();
  await store.sendCommand("cleanup");
  const originalKey = api.cleanup.mock.calls[0][2];
  api.retryCleanup.mockRejectedValueOnce(new ApiError(0, "network_error"));
  await store.sendCommand("cleanup_retry");
  const retry = store.commandIntent!;
  expect(retry.key).not.toBe(originalKey);
  expect(retry.operationId).toBe(cleanupId);
  await store.replayCommand();
  expect(api.retryCleanup.mock.calls.map((call) => call.slice(0, 3))).toEqual([
    [aggregateId, cleanupId, retry.key],
    [aggregateId, cleanupId, retry.key],
  ]);
  store.dispose();
});

it("keeps conflicting lifecycle mutations fenced while active cleanup remains readable and retryable", async () => {
  const { api, store } = setup();
  api.card.mockResolvedValue(
    aggregateFixture({ active_cleanup_id: cleanupId }),
  );
  await store.load();
  expect(store.canMutate).toBe(false);
  expect(store.canCommand).toBe(true);
  await store.sendCommand("pause");
  expect(api.command).not.toHaveBeenCalled();
  store.dispose();
});
it("restores active cleanup after reload, polls at two seconds only while visible and stops on error", async () => {
  vi.useFakeTimers();
  const { api, store } = setup();
  api.card.mockResolvedValue(
    aggregateFixture({ active_cleanup_id: cleanupId }),
  );
  store.setVisible(true);
  await store.load();
  await vi.advanceTimersByTimeAsync(0);
  expect(api.cleanupOperation).toHaveBeenCalledTimes(1);
  await vi.advanceTimersByTimeAsync(1999);
  expect(api.cleanupOperation).toHaveBeenCalledTimes(1);
  await vi.advanceTimersByTimeAsync(1);
  expect(api.cleanupOperation).toHaveBeenCalledTimes(2);
  store.setVisible(false);
  await vi.advanceTimersByTimeAsync(6000);
  expect(api.cleanupOperation).toHaveBeenCalledTimes(2);
  api.cleanupOperation.mockRejectedValue(new ApiError(503, "http_error"));
  store.setVisible(true);
  await vi.advanceTimersByTimeAsync(0);
  expect(api.cleanupOperation).toHaveBeenCalledTimes(3);
  await vi.advanceTimersByTimeAsync(6000);
  expect(api.cleanupOperation).toHaveBeenCalledTimes(3);
  expect(api.cleanup).not.toHaveBeenCalled();
  expect(api.retryCleanup).not.toHaveBeenCalled();
  store.dispose();
  expect(vi.getTimerCount()).toBe(0);
});

const nextCleanupId = "00000000-0000-4000-8000-000000000044";
it("replaces a completed cleanup with the newly active identity and refreshes each completed result once", async () => {
  vi.useFakeTimers();
  const { api, store } = setup();
  store.card = aggregateFixture({ active_cleanup_id: cleanupId });
  api.cleanupOperation.mockResolvedValueOnce(
    cleanupFixture({
      status: "succeeded",
      deleted_points: 10,
      completed_at: aggregateTime,
    }),
  );
  await store.loadCleanupOperation();
  expect(store.cleanupOperation?.id).toBe(cleanupId);
  expect(store.pointsVersion).toBe(1);
  store.setVisible(true);
  store.historicalResult = pointFixture();
  api.card.mockResolvedValue(
    aggregateFixture({ active_cleanup_id: nextCleanupId }),
  );
  api.cleanupOperation.mockResolvedValue(
    cleanupFixture({ id: nextCleanupId, status: "running" }),
  );
  await store.load();
  await vi.advanceTimersByTimeAsync(0);
  expect(store.cleanupOperation?.id).toBe(nextCleanupId);
  expect(store.cleanupOperation?.status).toBe("running");
  expect(api.cleanupOperation.mock.calls.at(-1)?.[1]).toBe(nextCleanupId);
  const reads = api.cleanupOperation.mock.calls.length;
  await vi.advanceTimersByTimeAsync(1999);
  expect(api.cleanupOperation).toHaveBeenCalledTimes(reads);
  api.card.mockResolvedValue(aggregateFixture());
  api.cleanupOperation.mockResolvedValue(
    cleanupFixture({
      id: nextCleanupId,
      status: "succeeded",
      deleted_points: 10,
      completed_at: aggregateTime,
    }),
  );
  await vi.advanceTimersByTimeAsync(1);
  expect(store.pointsVersion).toBe(2);
  expect(store.historicalResult).toBeNull();
  await store.loadCleanupOperation();
  expect(store.pointsVersion).toBe(2);
  expect(api.cleanup).not.toHaveBeenCalled();
  store.dispose();
  expect(vi.getTimerCount()).toBe(0);
});

it.each(["succeeded", "error"])(
  "switches cleanup while the previous read is pending and ignores its late %s",
  async (late) => {
    vi.useFakeTimers();
    const { api, store } = setup();
    let resolve!: (value: ReturnType<typeof cleanupFixture>) => void;
    let reject!: (error: Error) => void;
    api.cleanupOperation.mockReturnValueOnce(
      new Promise((done, fail) => {
        resolve = done;
        reject = fail;
      }),
    );
    store.card = aggregateFixture({ active_cleanup_id: cleanupId });
    const previous = store.loadCleanupOperation();
    const oldSignal = api.cleanupOperation.mock.calls[0][2] as AbortSignal;
    store.setVisible(true);
    api.card.mockResolvedValue(
      aggregateFixture({ active_cleanup_id: nextCleanupId }),
    );
    api.cleanupOperation.mockResolvedValue(
      cleanupFixture({ id: nextCleanupId, status: "running" }),
    );
    await store.load();
    await vi.advanceTimersByTimeAsync(0);
    expect(oldSignal.aborted).toBe(true);
    expect(api.cleanupOperation.mock.calls[1][1]).toBe(nextCleanupId);
    expect(store.cleanupOperation?.id).toBe(nextCleanupId);
    if (late === "error") reject(new ApiError(503, "http_error"));
    else
      resolve(
        cleanupFixture({
          status: "succeeded",
          completed_at: aggregateTime,
          deleted_points: 10,
        }),
      );
    await previous;
    expect(store.cleanupOperation?.id).toBe(nextCleanupId);
    expect(store.cleanupOperationError).toBeNull();
    expect(store.pointsVersion).toBe(0);
    await vi.advanceTimersByTimeAsync(2000);
    expect(api.cleanupOperation.mock.calls.at(-1)?.[1]).toBe(nextCleanupId);
    expect(api.cleanupOperation).toHaveBeenCalledTimes(3);
    store.dispose();
    expect(vi.getTimerCount()).toBe(0);
  },
);

it.each([false, true])(
  "finishes observing an external cleanup after its card fence disappears, ignoring an old receipt (%s)",
  async (oldReceipt) => {
    vi.useFakeTimers();
    const { api, store } = setup();
    if (oldReceipt)
      store.commandReceipt = receiptFixture({
        action: "cleanup",
        cleanup_operation_id: cleanupId,
      });
    store.card = aggregateFixture({ active_cleanup_id: cleanupId });
    api.cleanupOperation.mockResolvedValueOnce(
      cleanupFixture({
        status: "succeeded",
        deleted_points: 10,
        completed_at: aggregateTime,
      }),
    );
    await store.loadCleanupOperation();
    expect(store.pointsVersion).toBe(1);
    store.setVisible(true);
    api.card.mockResolvedValue(
      aggregateFixture({ active_cleanup_id: nextCleanupId }),
    );
    api.cleanupOperation.mockResolvedValue(
      cleanupFixture({ id: nextCleanupId, status: "running" }),
    );
    await store.load();
    await vi.advanceTimersByTimeAsync(0);
    const firstBRead = api.cleanupOperation.mock.calls.length - 1;
    api.card.mockResolvedValue(aggregateFixture());
    await vi.advanceTimersByTimeAsync(5000);
    expect(store.card?.active_cleanup_id).toBeNull();
    expect(store.cleanupOperation?.status).toBe("running");
    api.cleanupOperation.mockResolvedValue(
      cleanupFixture({
        id: nextCleanupId,
        status: "succeeded",
        deleted_points: 10,
        completed_at: aggregateTime,
      }),
    );
    await vi.advanceTimersByTimeAsync(1000);
    expect(store.cleanupOperation?.id).toBe(nextCleanupId);
    expect(store.cleanupOperation?.status).toBe("succeeded");
    expect(store.pointsVersion).toBe(2);
    expect(
      api.cleanupOperation.mock.calls
        .slice(firstBRead)
        .every((call) => call[1] === nextCleanupId),
    ).toBe(true);
    store.dispose();
    expect(vi.getTimerCount()).toBe(0);
  },
);

it("clears cleaned values immediately and combines terminal receipt and revision into one points refresh", async () => {
  vi.useFakeTimers();
  const { api, store } = setup();
  store.card = aggregateFixture({ active_cleanup_id: cleanupId });
  store.historicalResult = pointFixture();
  api.cleanupOperation.mockResolvedValue(
    cleanupFixture({
      status: "succeeded",
      deleted_points: 10,
      completed_at: aggregateTime,
    }),
  );
  let resolve!: (value: ReturnType<typeof aggregateFixture>) => void;
  api.card.mockReturnValueOnce(
    new Promise((done) => {
      resolve = done;
    }),
  );
  const completed = store.loadCleanupOperation();
  await vi.advanceTimersByTimeAsync(0);
  expect(store.historicalResult).toBeNull();
  expect(store.pointsClearVersion).toBe(1);
  expect(store.pointsVersion).toBe(0);
  resolve(
    aggregateFixture({
      auto_cleanup: autoCleanupFixture({ history_revision: 10 }),
    }),
  );
  await completed;
  expect(store.pointsVersion).toBe(1);
  expect(store.pointsClearVersion).toBe(1);
  await store.loadCleanupOperation();
  expect(store.pointsVersion).toBe(1);
  expect(api.card).toHaveBeenCalledTimes(1);
  store.dispose();
});

it("does not refresh or discard current points twice when the final card revision precedes the terminal receipt", async () => {
  const { api, store } = setup();
  store.card = aggregateFixture({ active_cleanup_id: cleanupId });
  await store.loadCleanupOperation();
  store.historicalResult = pointFixture();
  api.card.mockResolvedValue(
    aggregateFixture({
      auto_cleanup: autoCleanupFixture({ history_revision: 10 }),
    }),
  );
  await store.load(true);
  expect(store.pointsVersion).toBe(1);
  expect(store.historicalResult).toBeNull();
  const selectedAfterCleanup = pointFixture();
  store.historicalResult = selectedAfterCleanup;
  api.cleanupOperation.mockResolvedValue(
    cleanupFixture({
      status: "succeeded",
      deleted_points: 10,
      completed_at: aggregateTime,
    }),
  );
  await store.loadCleanupOperation();
  expect(store.cleanupOperation?.status).toBe("succeeded");
  expect(store.pointsVersion).toBe(1);
  expect(store.pointsClearVersion).toBe(0);
  expect(store.historicalResult).toBe(selectedAfterCleanup);
  expect(api.card).toHaveBeenCalledTimes(1);
  store.dispose();
});

it("requires the final refresh when only a partial revision was seen under the cleanup fence", async () => {
  const { api, store } = setup();
  store.card = aggregateFixture({ active_cleanup_id: cleanupId });
  await store.loadCleanupOperation();
  api.card.mockResolvedValueOnce(
    aggregateFixture({
      active_cleanup_id: cleanupId,
      auto_cleanup: autoCleanupFixture({ history_revision: 5 }),
    }),
  );
  await store.load(true);
  expect(store.pointsVersion).toBe(1);
  api.card.mockResolvedValue(
    aggregateFixture({
      auto_cleanup: autoCleanupFixture({ history_revision: 10 }),
    }),
  );
  api.cleanupOperation.mockResolvedValue(
    cleanupFixture({
      status: "succeeded",
      deleted_points: 10,
      completed_at: aggregateTime,
    }),
  );
  await store.loadCleanupOperation();
  expect(store.pointsVersion).toBe(2);
  expect(store.pointsClearVersion).toBe(1);
  expect(api.card).toHaveBeenCalledTimes(2);
  store.dispose();
});

it("recognizes an already refreshed revision as final only after its same cleanup fence disappears", async () => {
  const { api, store } = setup();
  store.card = aggregateFixture({ active_cleanup_id: cleanupId });
  await store.loadCleanupOperation();
  api.card.mockResolvedValueOnce(
    aggregateFixture({
      active_cleanup_id: cleanupId,
      auto_cleanup: autoCleanupFixture({ history_revision: 10 }),
    }),
  );
  await store.load(true);
  api.card.mockResolvedValue(
    aggregateFixture({
      auto_cleanup: autoCleanupFixture({ history_revision: 10 }),
    }),
  );
  await store.load(true);
  expect(store.pointsVersion).toBe(1);
  api.cleanupOperation.mockResolvedValue(
    cleanupFixture({
      status: "succeeded",
      deleted_points: 10,
      completed_at: aggregateTime,
    }),
  );
  await store.loadCleanupOperation();
  expect(store.pointsVersion).toBe(1);
  expect(store.pointsClearVersion).toBe(0);
  expect(api.card).toHaveBeenCalledTimes(2);
  store.dispose();
});
