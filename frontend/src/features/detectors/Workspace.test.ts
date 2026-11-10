import { describe, expect, it, vi } from "vitest";
import { ApiClient, ApiError } from "../../app/api/client";
import { SessionStore } from "../../app/stores/SessionStore";
import type { DetectorsApi } from "../../app/api/detectors";
import type { AggregatesApi } from "../../app/api/aggregates";
import {
  CommandStore,
  DetectorWorkspace,
  DraftStore,
  ListStore,
  strictInteger,
} from "./Workspace";
import { exact } from "../../app/api/aggregateJson";
import { fixture, id, receipt } from "./testData";
function service() {
  return {
    command: vi.fn().mockResolvedValue(receipt()),
    card: vi.fn().mockResolvedValue(fixture()),
    list: vi
      .fn()
      .mockResolvedValue({ items: [], total: 0, limit: 25, offset: 0 }),
  };
}
function deferred<T>() {
  let resolve!: (v: T) => void;
  const promise = new Promise<T>((r) => {
    resolve = r;
  });
  return { promise, resolve };
}
describe("detector command intents", () => {
  it("locks duplicate sends, freezes unknown body/key, and retries only explicitly", async () => {
    const api = service();
    const pending = deferred<never>();
    api.command.mockReturnValueOnce(pending.promise);
    const s = new CommandStore(api as unknown as DetectorsApi, null);
    const first = s.send("create", {});
    await s.send("create", {});
    expect(api.command).toHaveBeenCalledTimes(1);
    pending.resolve(Promise.reject(new ApiError(0, "network_error")) as never);
    await first;
    const old = api.command.mock.calls[0];
    expect(s.outcome).toBe("unknown");
    await s.send("stop");
    expect(api.command).toHaveBeenCalledTimes(1);
    await s.retry();
    expect(api.command.mock.calls[1].slice(0, 4)).toEqual(old.slice(0, 4));
    expect(s.card?.id).toBe(id);
    s.dispose();
  });
  it("receipt confirms command even when following GET fails; later read needs no POST", async () => {
    const api = service();
    api.card.mockRejectedValueOnce(new ApiError(503, "http_error"));
    const s = new CommandStore(api as unknown as DetectorsApi, id);
    await s.send("start");
    expect(s.outcome).toBe("confirmed");
    expect(s.receipt).not.toBeNull();
    expect(s.readError).not.toBeNull();
    await s.send("stop");
    expect(api.command).toHaveBeenCalledTimes(1);
    await s.load();
    expect(api.command).toHaveBeenCalledTimes(1);
    expect(s.card).not.toBeNull();
    s.dispose();
  });
  it("invalid/5xx stays unknown, valid GET resolves it; failed-start gets a new intent", async () => {
    const api = service();
    api.command.mockRejectedValueOnce(new ApiError(200, "invalid_response"));
    const s = new CommandStore(api as unknown as DetectorsApi, id);
    await s.send("start");
    const old = s.intent?.key;
    await s.load();
    expect(s.outcome).toBeNull();
    await s.send("start");
    expect(api.command.mock.calls[1][3]).not.toBe(old);
    s.dispose();
  });
  it("late GET cannot overwrite a command or disposed context", async () => {
    const api = service(),
      d = deferred<ReturnType<typeof fixture>>();
    api.card.mockReturnValueOnce(d.promise);
    const s = new CommandStore(api as unknown as DetectorsApi, id);
    const load = s.load();
    await s.send("stop");
    d.resolve({ ...fixture(), name: "stale" });
    await load;
    expect(s.card?.name).toBe("RCF");
    const next = deferred<ReturnType<typeof fixture>>();
    api.card.mockReturnValueOnce(next.promise);
    const read = s.load();
    s.dispose();
    next.resolve(fixture());
    await read;
    expect(s.card).toBeNull();
    expect(s.lifecycle.pending.size).toBe(0);
  });
  it("definitive refusal permits a corrected command", async () => {
    const api = service();
    api.command.mockRejectedValueOnce(
      new ApiError(409, "idempotency_conflict"),
    );
    const s = new CommandStore(api as unknown as DetectorsApi, id);
    await s.send("start");
    expect(s.outcome).toBeNull();
    expect(s.intent).toBeNull();
    s.dispose();
  });
});
describe("draft and list", () => {
  it("destroys actor intents and draft when session identity changes", () => {
    const client = new ApiClient(),
      session = new SessionStore(client),
      workspace = new DetectorWorkspace(client, session);
    const old = workspace.draft;
    old.change("name", "Чужой черновик");
    old.command.intent = {
      action: "create",
      target: null,
      body: {},
      key: "actor-key",
    };
    session.accept(
      {
        authenticated: true,
        user: { id, username: "admin", email: null, display_name: "Admin" },
        role: "admin",
        permissions: ["detectors.read", "detectors.write"],
        authentication_method: "local",
        csrf_token: "synthetic",
      },
      session.version,
    );
    expect(old.command.intent).toBeNull();
    expect(workspace.draft).not.toBe(old);
    expect(workspace.draft.name).toBe("");
    workspace.dispose();
    session.dispose();
  });
  it("validates strict int64, defaults, accelerated step and output before POST", async () => {
    expect(
      exact(strictInteger("9223372036854775807", undefined, 2n ** 63n - 1n)),
    ).toBe("9223372036854775807");
    for (const bad of ["1.5", "1e2", "true", " 1"])
      expect(() => strictInteger(bad)).toThrow();
    const api = service();
    const aggregates = {
      list: vi.fn().mockResolvedValue({
        items: [{ id, state: "active", duration_seconds: 2 }],
        total: 101,
      }),
    };
    const s = new DraftStore(
      api as unknown as DetectorsApi,
      aggregates as unknown as AggregatesApi,
    );
    await s.catalog();
    expect(aggregates.list.mock.calls[0][0]).toBe(0);
    expect(aggregates.list.mock.calls[0][2]).toEqual({
      limit: 10,
      state: "active",
    });
    s.change("aggregateId", id);
    s.change("name", " RCF ");
    expect(s.payload()?.name).toBe("RCF");
    expect(s.payload()?.step_seconds).toBeNull();
    s.mode(true);
    s.change("step", "3");
    expect(s.payload()).toBeNull();
    s.change("step", "1");
    s.change("seed", "9223372036854775808");
    expect(s.payload()).toBeNull();
    s.change("seed", "-9223372036854775808");
    s.change("output", "2");
    expect(s.payload()).toBeNull();
    s.change("sample", "2");
    expect(s.payload()).not.toBeNull();
    s.change("name", "🚀".repeat(200));
    expect(s.payload()).not.toBeNull();
    s.change("name", "🚀".repeat(201));
    expect(s.payload()).toBeNull();
    s.change("name", "RCF");
    await s.submit();
    expect(api.command).toHaveBeenCalledTimes(1);
    expect(api.command.mock.calls[0][0]).toBe("create");
    s.dispose();
  });
  it("filter resets pagination and ignores obsolete response", async () => {
    const api = service(),
      d = deferred<{ items: never[]; total: number }>();
    api.list.mockReturnValueOnce(d.promise);
    const s = new ListStore(api as unknown as DetectorsApi);
    const old = s.load(50);
    await s.filter(id, "running");
    d.resolve({ items: [], total: 99 });
    await old;
    expect(s.offset).toBe(0);
    expect(s.total).toBe(0);
    s.dispose();
  });
  it("searches the whole active catalog, ignores late q, and retains the selected snapshot", async () => {
    const api = service(),
      old = deferred<{ items: never[]; total: number }>();
    const selected = {
      id,
      state: "active",
      name: "Выбранный агрегат",
      source_name: "Источник A",
      duration_seconds: 2,
    };
    const aggregates = {
      list: vi
        .fn()
        .mockResolvedValueOnce({ items: [selected], total: 60 })
        .mockReturnValueOnce(old.promise)
        .mockResolvedValueOnce({ items: [], total: 0 })
        .mockRejectedValueOnce(new ApiError(503, "http_error"))
        .mockResolvedValueOnce({ items: [selected], total: 1 }),
    };
    const s = new DraftStore(
      api as unknown as DetectorsApi,
      aggregates as unknown as AggregatesApi,
    );
    await s.catalog();
    s.change("aggregateId", id);
    s.change("name", "RCF");
    s.search("старый");
    const first = s.catalog();
    s.search("новый");
    await s.catalog();
    old.resolve({ items: [], total: 99 });
    await first;
    expect(s.total).toBe(0);
    expect(s.selected).toEqual(selected);
    expect(s.payload()?.aggregate_id).toBe(id);
    s.search("ошибка");
    await s.catalog();
    expect(s.catalogError).toBeInstanceOf(ApiError);
    expect(s.selected).toEqual(selected);
    await s.catalog();
    expect(s.catalogError).toBeNull();
    expect(aggregates.list.mock.calls[4][2]).toEqual({
      limit: 10,
      state: "active",
      q: "ошибка",
    });
    s.command.outcome = "unknown";
    const count = aggregates.list.mock.calls.length;
    s.search("обход");
    await s.catalog();
    s.change("aggregateId", "other");
    expect(s.query).toBe("ошибка");
    expect(s.aggregateId).toBe(id);
    expect(aggregates.list).toHaveBeenCalledTimes(count);
    s.dispose();
  });
});
