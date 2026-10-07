import { makeAutoObservable, runInAction } from "mobx";
import type {
  ExternalConnection,
  ExternalConnectionInput,
  ExternalConnectionPatch,
  ExternalSource,
  KafkaConnection,
  KafkaConnectionInput,
  KafkaConnectionPatch,
  KafkaSource,
  ResourcesApi,
} from "../../app/api/resources";
import { RequestLifecycle } from "./RequestLifecycle";

export type ConnectionKind = "kafka" | "external";
export type ConnectionRecord = KafkaConnection | ExternalConnection;
export type OperationResult<T> =
  | { value: T; stale: false }
  | { error: unknown; stale: false }
  | { stale: true };
type DiscoveryKind = "topics" | "indices" | "data-streams";

export class ConnectionsStore {
  detail: { kind: ConnectionKind; item: ConnectionRecord } | null = null;
  discoveryKind: DiscoveryKind = "topics";
  includeInternal = false;
  discovery = {
    names: [] as string[],
    total: 0,
    offset: 0,
    loading: false,
    error: null as unknown,
  };
  topicStatus: Record<string, { registered: boolean; partitions: number }> = {};
  links: (KafkaSource | ExternalSource)[] = [];
  linksLoading = false;
  linksComplete = false;
  linksError: unknown = null;
  private precheckSequence = 0;

  constructor(
    private resources: ResourcesApi,
    private lifecycle: RequestLifecycle,
  ) {
    makeAutoObservable(this, { resources: false, lifecycle: false } as never, {
      autoBind: true,
    });
  }

  openDetail(
    kind: ConnectionKind,
    item: ConnectionRecord,
    canWrite: boolean,
    canReadSources: boolean,
  ) {
    this.closeDetail();
    this.detail = { kind, item };
    this.discoveryKind = kind === "kafka" ? "topics" : "indices";
    void this.refresh(canWrite);
    if (canReadSources) void this.loadLinks();
  }

  closeDetail() {
    this.lifecycle.cancel("connection-discovery");
    this.lifecycle.cancel("connection-discovery-more");
    this.lifecycle.cancel("connection-links");
    this.detail = null;
    this.links = [];
    this.linksComplete = false;
    this.linksLoading = false;
  }

  setDiscoveryKind(kind: "indices" | "data-streams", canWrite: boolean) {
    if (this.discoveryKind === kind) return;
    this.discoveryKind = kind;
    void this.refresh(canWrite);
  }

  setIncludeInternal(value: boolean, canWrite: boolean) {
    this.includeInternal = value;
    void this.refresh(canWrite);
  }

  async refresh(canWrite: boolean) {
    const detail = this.detail;
    if (!detail || (detail.kind === "external" && !canWrite)) return;
    const includeInternal = this.includeInternal;
    this.lifecycle.cancel("connection-discovery-more");
    const request = this.lifecycle.begin("connection-discovery");
    runInAction(() => {
      this.discovery = {
        names: [],
        total: 0,
        offset: 0,
        loading: true,
        error: null,
      };
    });
    try {
      if (detail.kind === "kafka") {
        const page = await this.resources.topics(
          detail.item.id,
          includeInternal,
          request.signal,
        );
        if (!request.current() || this.detail?.item.id !== detail.item.id)
          return;
        const topics = page.items.filter(
          (topic) => topic.name.startsWith("__") === includeInternal,
        );
        runInAction(() => {
          this.topicStatus = Object.fromEntries(
            topics.map((item) => [
              item.name,
              {
                registered: item.is_registered,
                partitions: item.partition_count,
              },
            ]),
          );
          this.discovery = {
            names: topics.map((item) => item.name),
            total: topics.length,
            offset: 0,
            loading: false,
            error: null,
          };
        });
      } else {
        const page = await this.resources.discovered(
          detail.item.id,
          this.discoveryKind === "topics" ? "indices" : this.discoveryKind,
          0,
          request.signal,
        );
        if (!request.current() || this.detail?.item.id !== detail.item.id)
          return;
        runInAction(() => {
          this.discovery = {
            names: page.items,
            total: page.total,
            offset: page.offset,
            loading: false,
            error: null,
          };
        });
      }
    } catch (error) {
      if (request.current())
        runInAction(() => {
          this.discovery = { ...this.discovery, loading: false, error };
        });
    }
  }

  async loadMoreDiscovery() {
    const detail = this.detail;
    if (!detail || detail.kind !== "external") return;
    await this.lifecycle.once("connection-discovery-more", async () => {
      const request = this.lifecycle.begin("connection-discovery-more:request");
      const offset = this.discovery.offset + 50;
      runInAction(() => {
        this.discovery = { ...this.discovery, loading: true, error: null };
      });
      try {
        const page = await this.resources.discovered(
          detail.item.id,
          this.discoveryKind === "topics" ? "indices" : this.discoveryKind,
          offset,
          request.signal,
        );
        if (!request.current() || this.detail?.item.id !== detail.item.id)
          return;
        runInAction(() => {
          this.discovery = {
            ...this.discovery,
            names: [...new Set([...this.discovery.names, ...page.items])],
            offset: page.offset,
            loading: false,
          };
        });
      } catch (error) {
        if (request.current())
          runInAction(() => {
            this.discovery = { ...this.discovery, loading: false, error };
          });
      }
    });
  }

  async loadLinks() {
    const detail = this.detail;
    if (!detail) return;
    const request = this.lifecycle.begin("connection-links");
    runInAction(() => {
      this.links = [];
      this.linksLoading = true;
      this.linksComplete = false;
      this.linksError = null;
    });
    try {
      const all: (KafkaSource | ExternalSource)[] = [];
      for (let offset = 0; ; offset += 50) {
        if (detail.kind === "kafka") {
          const page = await this.resources.kafkaSources(
            offset,
            detail.item.id,
            request.signal,
          );
          all.push(...page.items);
          if (offset + page.items.length >= page.total) break;
        } else {
          const page = await this.resources.externalSources(
            offset,
            request.signal,
          );
          all.push(
            ...page.items.filter(
              (source) => source.external_connection_id === detail.item.id,
            ),
          );
          if (offset + page.items.length >= page.total) break;
        }
      }
      if (!request.current() || this.detail?.item.id !== detail.item.id) return;
      runInAction(() => {
        this.links = all;
        this.linksLoading = false;
        this.linksComplete = true;
      });
    } catch (error) {
      if (request.current())
        runInAction(() => {
          this.linksLoading = false;
          this.linksError = error;
        });
    }
  }

  private async operation<T>(
    key: string,
    context: string,
    work: (signal: AbortSignal) => Promise<T>,
  ): Promise<OperationResult<T>> {
    const result = await this.lifecycle.once(key, async () => {
      const request = this.lifecycle.begin(context);
      try {
        const value = await work(request.signal);
        return request.current()
          ? ({ value, stale: false } as const)
          : ({ stale: true } as const);
      } catch (error) {
        return request.current()
          ? ({ error, stale: false } as const)
          : ({ stale: true } as const);
      }
    });
    return result ?? { stale: true };
  }

  saveKafka(
    id: string | undefined,
    data: KafkaConnectionInput | KafkaConnectionPatch,
  ) {
    return this.operation(
      id ? `connection:${id}` : "connection-save:mutation",
      "connection-save",
      (signal) =>
        id
          ? this.resources.patchKafkaConnection(
              id,
              data as KafkaConnectionPatch,
              signal,
            )
          : this.resources.createKafkaConnection(
              data as KafkaConnectionInput,
              signal,
            ),
    );
  }

  saveExternal(
    id: string | undefined,
    data: ExternalConnectionInput | ExternalConnectionPatch,
  ) {
    return this.operation(
      id ? `connection:${id}` : "connection-save:mutation",
      "connection-save",
      (signal) =>
        id
          ? this.resources.patchExternalConnection(
              id,
              data as ExternalConnectionPatch,
              signal,
            )
          : this.resources.createExternalConnection(
              data as ExternalConnectionInput,
              signal,
            ),
    );
  }

  deleteConnection(kind: ConnectionKind, id: string) {
    return this.operation(
      `connection:${id}`,
      `connection-delete:${id}`,
      (signal) => this.resources.deleteConnection(kind, id, signal),
    );
  }

  testConnection(kind: ConnectionKind, id: string) {
    return this.operation(
      `connection:${id}`,
      `connection-test:${id}`,
      (signal) => this.resources.testConnection(kind, id, signal),
    );
  }

  uploadCa(id: string, pem: string) {
    return this.operation(`connection:${id}`, "ca-upload", (signal) =>
      this.resources.uploadCa(id, pem, signal),
    );
  }

  deleteCa(id: string) {
    return this.operation(
      `connection:${id}`,
      `connection-ca-delete:${id}`,
      (signal) => this.resources.deleteCa(id, signal),
    );
  }

  async activeExternalCount(connectionId: string, canReadSources: boolean) {
    const token = ++this.precheckSequence;
    if (!canReadSources) return { stale: false as const, count: null, token };
    const request = this.lifecycle.begin("connection-active-sources");
    try {
      let count = 0;
      for (let offset = 0; ; offset += 50) {
        const page = await this.resources.externalSources(
          offset,
          request.signal,
        );
        count += page.items.filter(
          (source) =>
            source.external_connection_id === connectionId && source.is_enabled,
        ).length;
        if (offset + page.items.length >= page.total)
          return request.current()
            ? { stale: false as const, count, token }
            : { stale: true as const, count: null, token };
      }
    } catch {
      return request.current()
        ? { stale: false as const, count: null, token }
        : { stale: true as const, count: null, token };
    }
  }

  isCurrentPrecheck(token: number) {
    return token === this.precheckSequence;
  }

  dispose() {
    this.closeDetail();
  }
}
