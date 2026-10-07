import { makeAutoObservable } from "mobx";
import type {
  ExternalConnection,
  ExternalSource,
  ExternalSourceInput,
  ExternalSourcePatch,
  KafkaConnection,
  KafkaSource,
  KafkaSourceInput,
  KafkaSourcePatch,
  Normalizer,
  ResourcesApi,
  TargetType,
} from "../../app/api/resources";
import { RequestLifecycle } from "./RequestLifecycle";
import type { OperationResult } from "./ConnectionsStore";

export type SourceKind = "kafka" | "external";
export type SourceRecord = KafkaSource | ExternalSource;

export class SourcesStore {
  focused: SourceRecord | null = null;
  focusedError: unknown = null;
  knownConnections: Record<
    string,
    KafkaConnection | ExternalConnection | null
  > = {};
  knownNormalizers: Record<string, Normalizer | null> = {};
  topics: string[] = [];
  targets: string[] = [];
  targetTotal = 0;
  targetOffset = 0;
  targetLoading = false;
  targetError: unknown = null;
  selectedNormalizerError: unknown = null;

  constructor(
    private resources: ResourcesApi,
    private lifecycle: RequestLifecycle,
  ) {
    makeAutoObservable(this, { resources: false, lifecycle: false } as never, {
      autoBind: true,
    });
  }

  clear() {
    this.lifecycle.cancel("sources");
    this.focused = null;
    this.focusedError = null;
    this.knownConnections = {};
    this.knownNormalizers = {};
    this.clearTargets();
    this.selectedNormalizerError = null;
  }

  async loadFocused(id: string | null, kind: SourceKind | null) {
    this.lifecycle.cancel("sources-focused");
    this.focused = null;
    this.focusedError = null;
    if (!id || !kind) return;
    const request = this.lifecycle.begin("sources-focused");
    try {
      const source =
        kind === "kafka"
          ? await this.resources.kafkaSource(id, request.signal)
          : await this.resources.externalSource(id, request.signal);
      if (request.current()) this.focused = source;
    } catch (error) {
      if (request.current()) this.focusedError = error;
    }
  }

  async loadKnown(
    kafkaSources: KafkaSource[],
    externalSources: ExternalSource[],
    canReadConnections: boolean,
    canReadNormalizers: boolean,
  ) {
    if (!canReadConnections && !canReadNormalizers) return;
    const request = this.lifecycle.begin("sources-known");
    const operations: Promise<void>[] = [];
    if (canReadConnections) {
      const kafkaIds = new Set(
        kafkaSources.map((source) => source.connection_id),
      );
      const externalIds = new Set(
        externalSources.map((source) => source.external_connection_id),
      );
      for (const id of kafkaIds)
        if (!(id in this.knownConnections))
          operations.push(
            this.resources.kafkaConnection(id, request.signal).then(
              (item) => {
                if (request.current())
                  this.knownConnections = {
                    ...this.knownConnections,
                    [id]: item,
                  };
              },
              () => {
                if (request.current())
                  this.knownConnections = {
                    ...this.knownConnections,
                    [id]: null,
                  };
              },
            ),
          );
      for (const id of externalIds)
        if (!(id in this.knownConnections))
          operations.push(
            this.resources.externalConnection(id, request.signal).then(
              (item) => {
                if (request.current())
                  this.knownConnections = {
                    ...this.knownConnections,
                    [id]: item,
                  };
              },
              () => {
                if (request.current())
                  this.knownConnections = {
                    ...this.knownConnections,
                    [id]: null,
                  };
              },
            ),
          );
    }
    if (canReadNormalizers)
      for (const id of new Set(
        kafkaSources
          .map((source) => source.normalizer_id)
          .filter((id): id is string => !!id),
      ))
        if (!(id in this.knownNormalizers))
          operations.push(
            this.resources.normalizer(id, request.signal).then(
              (item) => {
                if (request.current())
                  this.knownNormalizers = {
                    ...this.knownNormalizers,
                    [id]: item,
                  };
              },
              () => {
                if (request.current())
                  this.knownNormalizers = {
                    ...this.knownNormalizers,
                    [id]: null,
                  };
              },
            ),
          );
    await Promise.allSettled(operations);
  }

  clearTargets() {
    this.lifecycle.cancel("sources-targets");
    this.targets = [];
    this.topics = [];
    this.targetTotal = 0;
    this.targetOffset = 0;
    this.targetLoading = false;
    this.targetError = null;
  }

  connectionPage(kind: SourceKind, offset: number, signal: AbortSignal) {
    return kind === "kafka"
      ? this.resources.kafkaConnections(offset, "", signal)
      : this.resources.externalConnections(offset, signal);
  }

  normalizerPage(offset: number, signal: AbortSignal) {
    return this.resources.normalizers(offset, "", signal);
  }

  async loadTargets(
    kind: SourceKind,
    connectionId: string | undefined,
    targetType: TargetType | undefined,
    canDiscover: boolean,
  ) {
    this.clearTargets();
    if (!connectionId) return;
    const request = this.lifecycle.begin("sources-targets");
    this.targetLoading = true;
    try {
      if (kind === "kafka") {
        const page = await this.resources.topics(
          connectionId,
          false,
          request.signal,
        );
        if (request.current())
          this.topics = page.items.map((item) => item.name);
      } else if (canDiscover && targetType && !targetType.endsWith("pattern")) {
        const page = await this.resources.discovered(
          connectionId,
          targetType === "index" ? "indices" : "data-streams",
          0,
          request.signal,
        );
        if (request.current()) {
          this.targets = page.items;
          this.targetTotal = page.total;
          this.targetOffset = page.offset;
        }
      }
    } catch (error) {
      if (request.current()) this.targetError = error;
    } finally {
      if (request.current()) this.targetLoading = false;
    }
  }

  async loadMoreTargets(connectionId: string, targetType: TargetType) {
    await this.lifecycle.once("sources-targets:more", async () => {
      const request = this.lifecycle.begin("sources-targets:page");
      this.targetLoading = true;
      this.targetError = null;
      try {
        const page = await this.resources.discovered(
          connectionId,
          targetType === "index" ? "indices" : "data-streams",
          this.targetOffset + 50,
          request.signal,
        );
        if (request.current()) {
          this.targets = [...new Set([...this.targets, ...page.items])];
          this.targetOffset = page.offset;
        }
      } catch (error) {
        if (request.current()) this.targetError = error;
      } finally {
        if (request.current()) this.targetLoading = false;
      }
    });
  }

  async readSelectedNormalizer(id: string) {
    this.selectedNormalizerError = null;
    const request = this.lifecycle.begin("sources-selected-normalizer");
    try {
      const item = await this.resources.normalizer(id, request.signal);
      if (request.current())
        this.knownNormalizers = { ...this.knownNormalizers, [id]: item };
    } catch (error) {
      if (request.current()) this.selectedNormalizerError = error;
    }
  }

  retrySelectedNormalizer(id: string) {
    return this.readSelectedNormalizer(id);
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

  saveKafka(id: string | undefined, data: KafkaSourceInput | KafkaSourcePatch) {
    return this.operation(
      id ? `source:${id}` : "source-create",
      "source-save",
      (signal) =>
        id
          ? this.resources.patchKafkaSource(
              id,
              data as KafkaSourcePatch,
              signal,
            )
          : this.resources.createKafkaSource(data as KafkaSourceInput, signal),
    );
  }

  saveExternal(
    id: string | undefined,
    data: ExternalSourceInput | ExternalSourcePatch,
  ) {
    return this.operation(
      id ? `source:${id}` : "source-create",
      "source-save",
      (signal) =>
        id
          ? this.resources.patchExternalSource(
              id,
              data as ExternalSourcePatch,
              signal,
            )
          : this.resources.createExternalSource(
              data as ExternalSourceInput,
              signal,
            ),
    );
  }

  action(
    kind: SourceKind,
    id: string,
    action: "enable" | "disable" | "delete",
  ) {
    return this.operation(`source:${id}`, `source-action:${id}`, (signal) =>
      this.resources.sourceAction(kind, id, action, signal),
    );
  }

  assignNormalizer(id: string, normalizerId: string | null) {
    return this.operation(`source:${id}`, "normalizer-mutation", (signal) =>
      this.resources.assignNormalizer(id, normalizerId, signal),
    );
  }
}
