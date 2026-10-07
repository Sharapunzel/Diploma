import { makeAutoObservable, runInAction } from "mobx";
import { ResourcesApi, type Page } from "../../app/api/resources";

export class ResourceStore<T extends { id: string }> {
  items: T[] = [];
  total = 0;
  offset = 0;
  loading = false;
  error: unknown = null;
  private controller: AbortController | null = null;
  private version = 0;
  constructor(
    public resources: ResourcesApi,
    private list: (offset: number, signal: AbortSignal) => Promise<Page<T>>,
  ) {
    makeAutoObservable(
      this,
      { resources: false, list: false, controller: false } as never,
      { autoBind: true },
    );
  }
  async load(offset = this.offset): Promise<boolean> {
    this.controller?.abort();
    const controller = new AbortController();
    this.controller = controller;
    const version = ++this.version;
    this.loading = true;
    this.error = null;
    try {
      const page = await this.list(offset, controller.signal);
      if (version !== this.version) return false;
      runInAction(() => {
        this.items = page.items;
        this.total = page.total;
        this.offset = offset;
      });
      if (offset > 0 && page.items.length === 0 && page.total > 0)
        return await this.load(
          Math.max(0, Math.ceil(page.total / 50 - 1) * 50),
        );
      return true;
    } catch (error) {
      if (version !== this.version || controller.signal.aborted) return false;
      runInAction(() => {
        this.error = error;
      });
      return false;
    } finally {
      if (version === this.version)
        runInAction(() => {
          this.loading = false;
        });
    }
  }
  dispose() {
    this.version++;
    this.controller?.abort();
    this.items = [];
  }
}
