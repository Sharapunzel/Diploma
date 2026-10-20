import { makeAutoObservable, runInAction } from "mobx";
import {
  DiagnosticsApi,
  type KafkaEvent,
  type ProcessingError,
  type SourceState,
  type SourceTopology,
} from "../../app/api/diagnostics";
import type { Page } from "../../app/api/resources";

export class DiagnosticsStore {
  sources: Page<SourceState> | null = null;
  source: SourceState | null = null;
  topology: SourceTopology | null = null;
  errors: Page<ProcessingError> | null = null;
  events: Page<KafkaEvent> | null = null;
  loading = {
    sources: false,
    source: false,
    topology: false,
    errors: false,
    events: false,
  };
  failures: Record<string, unknown> = {};
  selectedId = "";
  private controllers: Record<string, AbortController | undefined> = {};

  constructor(private api: DiagnosticsApi) {
    makeAutoObservable(this, { api: false, controllers: false } as never, {
      autoBind: true,
    });
  }
  dispose() {
    Object.values(this.controllers).forEach((controller) =>
      controller?.abort(),
    );
  }
  async loadSources(offset: number, q: string) {
    await this.load(
      "sources",
      (signal) => this.api.sources(offset, q, signal),
      (result) => {
        this.sources = result;
      },
    );
  }
  select(id: string) {
    this.selectedId = id;
    this.source = null;
    this.topology = null;
    this.controllers.source?.abort();
    this.controllers.topology?.abort();
    if (!id) return;
    void this.refreshSource();
  }
  refreshSource() {
    const id = this.selectedId;
    if (!id) return;
    void this.load(
      "source",
      (signal) => this.api.source(id, signal),
      (result) => {
        if (this.selectedId === id) this.source = result;
      },
    );
    void this.load(
      "topology",
      (signal) => this.api.topology(id, signal),
      (result) => {
        if (this.selectedId === id) this.topology = result;
      },
    );
  }
  loadErrors(
    sourceId: string | undefined,
    type: string | undefined,
    start: string,
    end: string,
    offset: number,
  ) {
    this.errors = null;
    return this.load(
      "errors",
      (signal) =>
        this.api.errors({ sourceId, type, start, end, offset }, signal),
      (result) => {
        this.errors = result;
      },
    );
  }
  loadEvents(
    sourceId: string | undefined,
    type: string | undefined,
    start: string,
    end: string,
    offset: number,
  ) {
    this.events = null;
    return this.load(
      "events",
      (signal) =>
        this.api.events({ sourceId, type, start, end, offset }, signal),
      (result) => {
        this.events = result;
      },
    );
  }
  private async load<T>(
    key: keyof DiagnosticsStore["loading"],
    request: (signal: AbortSignal) => Promise<T>,
    accept: (result: T) => void,
  ) {
    this.controllers[key]?.abort();
    const controller = new AbortController();
    this.controllers[key] = controller;
    this.loading[key] = true;
    this.failures[key] = null;
    try {
      const result = await request(controller.signal);
      if (this.controllers[key] !== controller) return;
      runInAction(() => accept(result));
    } catch (error) {
      if (this.controllers[key] !== controller || controller.signal.aborted)
        return;
      runInAction(() => {
        this.failures[key] = error;
      });
    } finally {
      if (this.controllers[key] === controller)
        runInAction(() => {
          this.loading[key] = false;
        });
    }
  }
}
