import { describe, expect, it, vi } from "vitest";
import { ApiError } from "../../app/api/client";
import type { Agent, AgentsApi } from "../../app/api/agents";
import { AgentStore } from "./AgentStore";

const agent = {
  id: "agent",
  operation_id: "operation",
  state: "provisioning",
  topics: [],
} as unknown as Agent;

describe("AgentStore", () => {
  it("keeps the idempotency key after an unknown result and reuses it", async () => {
    const keys: string[] = [];
    let count = 0;
    const api = {
      operation: vi.fn(async () => ({ state: "succeeded" })),
      get: vi.fn(async () => ({ ...agent, operation_id: null })),
    } as unknown as AgentsApi;
    const store = new AgentStore(api);
    const action = vi.fn(async (key: string) => {
      keys.push(key);
      if (++count === 1) throw new ApiError(0, "network_error");
      return agent;
    });
    expect(await store.run(action)).toEqual({ status: "unknown" });
    expect(store.pending).toBe(false);
    expect(await store.retryUnknown()).toEqual({
      status: "succeeded",
      agentId: "agent",
    });
    expect(keys).toHaveLength(2);
    expect(keys[0]).toBe(keys[1]);
    expect(store.error).toBeNull();
    expect(store.canRetryMutation).toBe(false);
  });
  it("retries observing an accepted operation after operation polling loses the network", async () => {
    const api = {
      operation: vi
        .fn()
        .mockRejectedValueOnce(new ApiError(0, "network_error"))
        .mockResolvedValue({ id: "operation", state: "succeeded" }),
      get: vi.fn(async () => ({ ...agent, operation_id: "operation" })),
    } as unknown as AgentsApi;
    const store = new AgentStore(api);
    const post = vi.fn(async () => ({
      id: "operation",
      agent_id: "agent",
      state: "pending",
    }));
    expect(await store.run(post as never)).toEqual({
      status: "observation_error",
      agentId: "agent",
    });
    expect(store.observationOperationId).toBe("operation");
    expect(store.observationFailed).toBe(true);
    expect(store.canRetryMutation).toBe(false);
    expect(api.operation).toHaveBeenCalledTimes(1);
    await store.retryObservation();
    expect(api.operation).toHaveBeenCalledTimes(2);
    expect(post).toHaveBeenCalledTimes(1);
    expect(store.operation?.state).toBe("succeeded");
    expect(store.observationFailed).toBe(false);
    expect(store.error).toBeNull();
  });
  it("retries observing an accepted operation after agent GET loses the network", async () => {
    const api = {
      operation: vi.fn(async () => ({ id: "operation", state: "succeeded" })),
      get: vi
        .fn()
        .mockRejectedValueOnce(new ApiError(0, "network_error"))
        .mockResolvedValue({ ...agent, operation_id: "operation" }),
    } as unknown as AgentsApi;
    const store = new AgentStore(api);
    const post = vi.fn(async () => ({
      id: "operation",
      agent_id: "agent",
      state: "pending",
    }));
    expect(await store.run(post as never)).toEqual({
      status: "observation_error",
      agentId: "agent",
    });
    expect(store.observationFailed).toBe(true);
    expect(store.canRetryMutation).toBe(false);
    await store.retryObservation();
    expect(post).toHaveBeenCalledTimes(1);
    expect(api.get).toHaveBeenCalledTimes(2);
    expect(store.operation?.state).toBe("succeeded");
    expect(store.observationFailed).toBe(false);
  });
  it("does not expose mutation retry after an ordinary agent GET error", async () => {
    const api = {
      get: vi.fn(async () => {
        throw new ApiError(0, "network_error");
      }),
    } as unknown as AgentsApi;
    const store = new AgentStore(api);
    await store.get("agent");
    expect(store.error).toBeInstanceOf(ApiError);
    expect(store.observationFailed).toBe(false);
    expect(store.canRetryMutation).toBe(false);
  });
  it("keeps the unknown action available after reconciliation", async () => {
    const keys: string[] = [];
    const api = {
      list: vi.fn(async () => ({ items: [], total: 0 })),
      get: vi.fn(async () => ({ ...agent, operation_id: null })),
      operation: vi.fn(async () => ({ state: "succeeded" })),
    } as unknown as AgentsApi;
    const store = new AgentStore(api);
    const action = vi.fn(async (key: string) => {
      keys.push(key);
      if (keys.length === 1) throw new ApiError(0, "network_error");
      return agent;
    });
    expect((await store.run(action))?.status).toBe("unknown");
    await store.reconcile();
    expect(store.error).toBeInstanceOf(ApiError);
    expect(await store.retryUnknown()).toEqual({
      status: "succeeded",
      agentId: "agent",
    });
    expect(keys[0]).toBe(keys[1]);
  });
  it("does not report success while a provisioning operation is pending", async () => {
    const api = {
      operation: vi.fn(async () => ({ state: "pending" })),
      get: vi.fn(async () => agent),
    } as unknown as AgentsApi;
    const store = new AgentStore(api);
    vi.useFakeTimers();
    const work = store.run(async () => agent);
    await vi.runAllTimersAsync();
    await work;
    expect(store.operation?.state).toBe("pending");
    expect(store.notice).toMatch(/ещё выполняется/);
    vi.useRealTimers();
  });
  it("keeps a failed provisioning operation visible without reporting success", async () => {
    const api = {
      operation: vi.fn(async () => ({
        state: "failed",
        safe_error: "Не удалось настроить Kafka",
      })),
      get: vi.fn(async () => agent),
    } as unknown as AgentsApi;
    const store = new AgentStore(api);
    const result = await store.run(async () => agent);
    expect(result).toEqual({ status: "failed", agentId: "agent" });
    expect(store.operation?.state).toBe("failed");
    expect(store.error).toBeInstanceOf(ApiError);
    expect(store.notice).toBe("");
    expect(store.pending).toBe(false);
  });
  it("reports a synchronous metadata PATCH as successful despite an old failed operation", async () => {
    const updated = { ...agent, name: "renamed", operation_id: null };
    const api = {
      operation: vi.fn(async () => ({
        state: "failed",
        safe_error: "Old provisioning failure",
      })),
      get: vi.fn(async () => ({ ...agent, operation_id: "old-operation" })),
    } as unknown as AgentsApi;
    const store = new AgentStore(api);
    store.operation = {
      id: "old-operation",
      agent_id: "agent",
      kind: "register",
      state: "failed",
      attempts: 1,
      safe_error: "Old provisioning failure",
    };
    expect(await store.run(async () => updated, "agent", false)).toEqual({
      status: "succeeded",
      agentId: "agent",
    });
    expect(store.agent?.name).toBe("renamed");
    expect(store.notice).toBe("Изменения сохранены.");
    expect(api.operation).not.toHaveBeenCalled();
    expect(api.get).not.toHaveBeenCalled();
  });
  it("blocks a duplicate mutation while the first request is in flight", async () => {
    let release!: (value: Agent) => void;
    const api = {
      get: vi.fn(async () => ({ ...agent, operation_id: null })),
    } as unknown as AgentsApi;
    const store = new AgentStore(api);
    const action = vi.fn(
      () =>
        new Promise<Agent>((resolve) => {
          release = resolve;
        }),
    );
    const first = store.run(action, "agent", false);
    expect(await store.run(action, "agent", false)).toBeNull();
    expect(action).toHaveBeenCalledTimes(1);
    release(agent);
    await first;
    expect(store.pending).toBe(false);
  });
  it("ignores a stale response from the previously opened agent", async () => {
    const resolvers = new Map<string, (value: Agent) => void>();
    const api = {
      get: vi.fn(
        (id: string) =>
          new Promise<Agent>((resolve) => resolvers.set(id, resolve)),
      ),
    } as unknown as AgentsApi;
    const store = new AgentStore(api);
    const first = store.get("old");
    const second = store.get("new");
    resolvers.get("old")!({ ...agent, id: "old", operation_id: null });
    await first;
    expect(store.agent).toBeNull();
    resolvers.get("new")!({ ...agent, id: "new", operation_id: null });
    await second;
    expect(store.agent?.id).toBe("new");
    expect(store.loading).toBe(false);
  });
});
