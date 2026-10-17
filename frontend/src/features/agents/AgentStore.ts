import { makeAutoObservable, runInAction } from "mobx";
import { ApiError } from "../../app/api/client";
import {
  AgentsApi,
  type Agent,
  type AgentOperation,
} from "../../app/api/agents";

export type MutationResult =
  | { status: "succeeded"; agentId: string }
  | { status: "pending" | "observation_error"; agentId: string }
  | { status: "failed" | "unknown"; agentId?: string };

function isUnknown(error: unknown) {
  return (
    error instanceof ApiError &&
    (error.status === 0 || error.code === "invalid_response")
  );
}

export class AgentStore {
  page: Agent[] = [];
  total = 0;
  agent: Agent | null = null;
  operation: AgentOperation | null = null;
  loading = false;
  pending = false;
  error: unknown = null;
  notice = "";
  observationAgentId: string | null = null;
  observationOperationId: string | null = null;
  observationFailed = false;
  uncertainMutation = false;
  private requestKey: string | null = null;
  private pendingAction: (() => Promise<Agent | AgentOperation>) | null = null;
  private loadedId: string | null = null;
  private loadVersion = 0;
  constructor(private api: AgentsApi) {
    makeAutoObservable(this, { api: false, pendingAction: false } as never);
  }
  get canRetryMutation() {
    return this.uncertainMutation && this.pendingAction !== null;
  }

  async list(offset = 0) {
    this.loading = true;
    this.error = null;
    try {
      const page = await this.api.list(offset);
      runInAction(() => {
        this.page = page.items;
        this.total = page.total;
      });
    } catch (error) {
      runInAction(() => {
        this.error = error;
      });
    } finally {
      runInAction(() => {
        this.loading = false;
      });
    }
  }
  async get(id: string) {
    const version = ++this.loadVersion;
    if (this.loadedId !== id) {
      runInAction(() => {
        this.agent = null;
        this.operation = null;
        this.loadedId = id;
      });
    }
    this.loading = true;
    this.error = null;
    try {
      const agent = await this.api.get(id);
      if (version !== this.loadVersion) return;
      runInAction(() => {
        this.agent = agent;
        const operationId =
          agent.operation_id ??
          (this.observationAgentId === id ? this.observationOperationId : null);
        this.observationAgentId = operationId ? id : null;
        this.observationOperationId = operationId;
      });
      if (this.observationOperationId) {
        await this.poll(id, this.observationOperationId, false, version);
        if (version === this.loadVersion) {
          runInAction(() => {
            this.observationFailed = false;
          });
        }
      } else {
        runInAction(() => {
          this.observationFailed = false;
          this.observationAgentId = null;
          this.observationOperationId = null;
        });
      }
    } catch (error) {
      if (version !== this.loadVersion) return;
      runInAction(() => {
        this.error = error;
        this.observationFailed = this.observationAgentId === id;
      });
    } finally {
      if (version === this.loadVersion)
        runInAction(() => {
          this.loading = false;
        });
    }
  }
  async run(
    action: (key: string) => Promise<Agent | AgentOperation>,
    id?: string,
    trackOperation = true,
    retry = false,
    keyOverride?: string,
  ): Promise<MutationResult | null> {
    if (this.pending || (this.pendingAction && !retry)) return null;
    this.pending = true;
    this.error = null;
    this.notice = "";
    this.uncertainMutation = false;
    const key = keyOverride ?? this.requestKey ?? crypto.randomUUID();
    this.requestKey = key;
    this.pendingAction = () => action(key);
    let acceptedId: string | null = null;
    let acceptedOperationId: string | null = null;
    let accepted = false;
    try {
      const result = await action(key);
      accepted = true;
      runInAction(() => {
        this.requestKey = null;
        this.pendingAction = null;
        this.uncertainMutation = false;
      });
      const agentId =
        id ?? ("agent_id" in result ? result.agent_id : result.id);
      acceptedId = agentId;
      if (!trackOperation) {
        runInAction(() => {
          this.loadedId = agentId;
          this.agent = result as Agent;
          this.observationAgentId = null;
          this.observationOperationId = null;
          this.observationFailed = false;
          this.error = null;
          this.notice = "Изменения сохранены.";
        });
        return { status: "succeeded", agentId };
      }
      if (this.loadedId !== agentId) {
        runInAction(() => {
          this.loadedId = agentId;
          this.agent = null;
          this.operation = null;
        });
      }
      const operationId =
        "operation_id" in result ? result.operation_id : result.id;
      acceptedOperationId = operationId || null;
      runInAction(() => {
        this.observationAgentId = agentId;
        this.observationOperationId = acceptedOperationId;
        this.observationFailed = false;
      });
      if (trackOperation && operationId)
        await this.poll(agentId, operationId, true);
      await this.get(agentId);
      const state = trackOperation ? this.operation?.state : "succeeded";
      if (state === "failed") return { status: "failed", agentId };
      if (this.error)
        return {
          status: this.observationFailed ? "observation_error" : "pending",
          agentId,
        };
      if (state !== "succeeded") return { status: "pending", agentId };
      runInAction(() => {
        this.notice = "Операция выполнена.";
      });
      return { status: "succeeded", agentId };
    } catch (error) {
      runInAction(() => {
        this.error = error;
        if (accepted) {
          this.observationAgentId = acceptedId;
          this.observationOperationId = acceptedOperationId;
          this.observationFailed = true;
        } else if (!isUnknown(error)) {
          this.requestKey = null;
          this.pendingAction = null;
          this.uncertainMutation = false;
        } else {
          this.uncertainMutation = true;
        }
      });
      if (accepted && acceptedId)
        return { status: "observation_error", agentId: acceptedId };
      return {
        status: isUnknown(error) ? "unknown" : "failed",
        agentId: acceptedId ?? undefined,
      };
    } finally {
      runInAction(() => {
        this.pending = false;
      });
    }
  }
  async reconcile(id?: string) {
    if (id) await this.get(id);
    else await this.list();
    if (this.pendingAction && !this.error)
      runInAction(() => {
        this.error = new ApiError(0, "network_error");
      });
  }
  async retryObservation(id = this.observationAgentId ?? undefined) {
    if (!id || this.pending) return;
    await this.get(id);
  }
  async retryUnknown(id?: string) {
    if (!this.pendingAction || this.pending) return null;
    const action = this.pendingAction;
    return this.run(() => action(), id, true, true);
  }
  private async poll(
    id: string,
    operationId: string,
    wait: boolean,
    version?: number,
  ) {
    for (let attempt = 0; attempt < (wait ? 30 : 1); attempt++) {
      const operation = await this.api.operation(id, operationId);
      if (version !== undefined && version !== this.loadVersion) return;
      runInAction(() => {
        this.operation = operation;
      });
      if (["succeeded", "failed"].includes(operation.state)) {
        if (operation.state === "failed")
          runInAction(() => {
            this.error = new ApiError(
              409,
              "agent_operation_failed",
              null,
              operation.safe_error,
            );
          });
        return;
      }
      if (wait) await new Promise((resolve) => setTimeout(resolve, 1000));
    }
    if (wait)
      runInAction(() => {
        this.notice =
          "Операция ещё выполняется. Состояние можно обновить позже.";
      });
  }
}
