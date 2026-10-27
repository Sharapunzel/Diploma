import { makeAutoObservable, runInAction } from "mobx";
import {
  EventsApi,
  type EventCard,
  type EventField,
  type EventFilter,
  type EventResults,
  type EventSearch,
  type EventSource,
} from "../../app/api/events";

const WEEK_MS = 7 * 24 * 60 * 60 * 1000;

export class EventsStore {
  sources: EventSource[] = [];
  source: EventSource | null = null;
  fields: EventField[] = [];
  columns: string[] = [];
  filters: EventFilter[] = [];
  from = "";
  to = "";
  sort: "asc" | "desc" = "desc";
  defaultWindow = true;
  page = 0;
  cursors: (string | undefined)[] = [undefined];
  pages: EventResults[] = [];
  sourceOffsets = { kafka: 0, external: 0 };
  sourceTotals = { kafka: 0, external: 0 };
  catalogError: unknown = null;
  catalogReady = false;
  results: EventResults | null = null;
  card: EventCard | null = null;
  loading = false;
  cardLoading = false;
  error: unknown = null;
  cardError: unknown = null;
  sourceError: unknown = null;
  loadingSources = false;
  private generation = 0;
  private controller: AbortController | null = null;
  private cardController: AbortController | null = null;
  private sourceController: AbortController | null = null;
  private disposed = false;

  constructor(private api: EventsApi) {
    makeAutoObservable(this);
  }

  async loadSources() {
    this.sourceController?.abort();
    this.sourceController = new AbortController();
    const controller = this.sourceController;
    this.loadingSources = true;
    this.sourceError = null;
    this.sources = [];
    this.sourceOffsets = { kafka: 0, external: 0 };
    this.sourceTotals = { kafka: 0, external: 0 };
    await Promise.all(
      (["kafka", "external"] as const).map(async (kind) => {
        try {
          const page = await this.api.sources(kind, 0, controller.signal);
          if (this.disposed || this.sourceController !== controller) return;
          runInAction(() => {
            this.sources.push(
              ...page.items.filter(
                (item) => item.source_type === "kafka" || item.is_enabled,
              ),
            );
            this.sourceOffsets[kind] = page.items.length;
            this.sourceTotals[kind] = page.total;
          });
        } catch (error) {
          if (!this.disposed && this.sourceController === controller)
            runInAction(() => {
              this.sourceError = error;
            });
        }
      }),
    );
    if (this.sourceController === controller)
      runInAction(() => {
        this.loadingSources = false;
      });
  }

  async loadMoreSources(requestedKind?: "kafka" | "external") {
    if (this.loadingSources || this.disposed) return;
    const kind =
      requestedKind ??
      (["kafka", "external"] as const).find(
        (item) => this.sourceOffsets[item] < this.sourceTotals[item],
      );
    if (!kind || this.sourceOffsets[kind] >= this.sourceTotals[kind]) return;
    const offset = this.sourceOffsets[kind];
    this.loadingSources = true;
    this.sourceError = null;
    const controller = this.sourceController;
    try {
      const page = await this.api.sources(kind, offset, controller?.signal);
      if (this.disposed || this.sourceController !== controller) return;
      runInAction(() => {
        this.sourceOffsets[kind] += page.items.length;
        this.sourceTotals[kind] = page.total;
        this.sources.push(
          ...page.items.filter(
            (item) => item.source_type === "kafka" || item.is_enabled,
          ),
        );
      });
    } catch (error) {
      if (!this.disposed && this.sourceController === controller)
        runInAction(() => {
          this.sourceError = error;
        });
    } finally {
      if (this.sourceController === controller)
        runInAction(() => {
          this.loadingSources = false;
        });
    }
  }

  async select(id: string | null) {
    if (this.disposed) return;
    this.generation++;
    this.controller?.abort();
    this.cardController?.abort();
    const generation = this.generation;
    this.source = this.sources.find((item) => item.id === id) ?? null;
    this.fields = [];
    this.columns = [];
    this.filters = [];
    this.card = null;
    this.cardError = null;
    this.cardLoading = false;
    this.results = null;
    this.error = null;
    this.loading = false;
    this.cursors = [undefined];
    this.pages = [];
    this.catalogError = null;
    this.catalogReady = false;
    this.page = 0;
    if (!this.source) return;
    this.loading = true;
    const now = Date.now();
    this.defaultWindow = true;
    this.from = new Date(now - WEEK_MS).toISOString();
    this.to = new Date(now).toISOString();
    await this.loadCatalog(generation);
  }

  async loadCatalog(generation = this.generation) {
    if (!this.source || this.disposed) return;
    this.controller?.abort();
    const sourceId = this.source.id;
    this.catalogError = null;
    this.catalogReady = false;
    this.loading = true;
    this.controller = new AbortController();
    try {
      const fields: EventField[] = [];
      let offset = 0;
      while (true) {
        const page = await this.api.fields(
          sourceId,
          offset,
          "",
          this.controller.signal,
        );
        fields.push(...page.items);
        offset += page.items.length;
        if (!page.items.length || offset >= page.total) break;
      }
      if (this.disposed || generation !== this.generation) return;
      runInAction(() => {
        this.fields = fields;
        this.catalogReady = true;
        this.columns = ["message", "event.action"].filter((name) =>
          fields.some((field) => field.name === name),
        );
      });
      await this.search();
    } catch (error) {
      if (
        generation === this.generation &&
        !(error instanceof DOMException && error.name === "AbortError")
      ) {
        runInAction(() => {
          this.catalogError = error;
          this.loading = false;
        });
      }
    }
  }

  private request(cursor?: string): EventSearch {
    return {
      timestamp_from: this.from,
      timestamp_to: this.to,
      sort: this.sort,
      filters: this.filters,
      limit: 50,
      ...(cursor ? { cursor } : {}),
    };
  }

  async search(page = 0) {
    if (!this.source || this.disposed) return;
    if (this.pages[page]) {
      this.results = this.pages[page];
      this.page = page;
      this.error = null;
      return;
    }
    const generation = ++this.generation;
    this.controller?.abort();
    this.controller = new AbortController();
    const sourceId = this.source.id;
    this.loading = true;
    this.error = null;
    try {
      const result = await this.api.search(
        sourceId,
        this.request(this.cursors[page]),
        this.controller.signal,
      );
      if (this.disposed || generation !== this.generation) return;
      runInAction(() => {
        this.results = result;
        this.pages[page] = result;
        this.page = page;
        if (result.next_cursor) this.cursors[page + 1] = result.next_cursor;
      });
    } catch (error) {
      if (
        generation === this.generation &&
        !(error instanceof DOMException && error.name === "AbortError")
      ) {
        runInAction(() => {
          this.error = error;
        });
      }
    } finally {
      if (generation === this.generation)
        runInAction(() => {
          this.loading = false;
        });
    }
  }

  async applySearch(
    from: string,
    to: string,
    sort: "asc" | "desc",
    filters: EventFilter[],
    columns = this.columns,
  ): Promise<boolean> {
    if (!this.source || this.disposed || this.loading) return false;
    const generation = ++this.generation;
    this.controller?.abort();
    this.controller = new AbortController();
    const sourceId = this.source.id;
    const request: EventSearch = {
      timestamp_from: from,
      timestamp_to: to,
      sort,
      filters,
      limit: 50,
    };
    this.loading = true;
    this.error = null;
    try {
      const result = await this.api.search(
        sourceId,
        request,
        this.controller.signal,
      );
      if (
        this.disposed ||
        generation !== this.generation ||
        this.source?.id !== sourceId
      )
        return false;
      runInAction(() => {
        this.defaultWindow = false;
        this.from = from;
        this.to = to;
        this.sort = sort;
        this.filters = filters;
        this.columns = columns;
        this.cursors = [undefined];
        this.pages = [result];
        this.page = 0;
        this.results = result;
        if (result.next_cursor) this.cursors[1] = result.next_cursor;
      });
      return true;
    } catch (error) {
      if (
        generation === this.generation &&
        !(error instanceof DOMException && error.name === "AbortError")
      ) {
        runInAction(() => {
          this.error = error;
        });
      }
      return false;
    } finally {
      if (generation === this.generation)
        runInAction(() => {
          this.loading = false;
        });
    }
  }

  refresh() {
    if (!this.catalogReady) return Promise.resolve();
    if (this.defaultWindow) {
      const now = Date.now();
      this.from = new Date(now - WEEK_MS).toISOString();
      this.to = new Date(now).toISOString();
    }
    this.cursors = [undefined];
    this.pages = [];
    this.page = 0;
    this.results = null;
    return this.search();
  }

  async openCard(eventId: string) {
    if (!this.source) return;
    this.cardController?.abort();
    const sourceId = this.source.id;
    const controller = new AbortController();
    this.cardController = controller;
    this.card = null;
    this.cardError = null;
    this.cardLoading = true;
    try {
      const card = await this.api.card(sourceId, eventId, controller.signal);
      if (
        !this.disposed &&
        this.source?.id === sourceId &&
        this.cardController === controller
      )
        runInAction(() => {
          this.card = card;
        });
    } catch (error) {
      if (
        this.cardController === controller &&
        !(error instanceof DOMException && error.name === "AbortError")
      )
        runInAction(() => {
          this.cardError = error;
        });
    } finally {
      if (this.cardController === controller)
        runInAction(() => {
          this.cardLoading = false;
        });
    }
  }

  closeCard() {
    this.cardController?.abort();
    this.cardController = null;
    this.card = null;
    this.cardError = null;
    this.cardLoading = false;
  }

  dispose() {
    this.disposed = true;
    this.generation++;
    this.sourceController?.abort();
    this.controller?.abort();
    this.cardController?.abort();
    this.sourceController = null;
    this.controller = null;
    this.cardController = null;
  }
}
