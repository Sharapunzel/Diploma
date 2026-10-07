import { makeAutoObservable, runInAction } from "mobx";
import type { Page } from "../../app/api/resources";
import { RequestLifecycle } from "./RequestLifecycle";

type Loader<T> = (offset: number, signal: AbortSignal) => Promise<Page<T>>;

/** Server-backed selectable options with context-safe initial and incremental loading. */
export class PagedOptionsStore<T> {
  items: T[] = [];
  total = 0;
  offset = 0;
  loading = false;
  error: unknown = null;
  failedOffset: number | null = null;
  private context = "";
  private loader: Loader<T> | null = null;

  constructor(
    private lifecycle: RequestLifecycle,
    private identity: (item: T) => string,
  ) {
    makeAutoObservable(
      this,
      { lifecycle: false, identity: false, loader: false } as never,
      { autoBind: true },
    );
  }

  configure(context: string, loader: Loader<T>) {
    this.cancel();
    this.context = context;
    this.loader = loader;
    this.items = [];
    this.total = 0;
    this.offset = 0;
    this.error = null;
    this.failedOffset = null;
  }

  async load(offset = 0) {
    const context = this.context;
    const loader = this.loader;
    if (!context || !loader) return;
    await this.lifecycle.once(`${context}:lock`, async () => {
      const request = this.lifecycle.begin(`${context}:request`);
      runInAction(() => {
        this.loading = true;
        this.error = null;
      });
      try {
        const page = await loader(offset, request.signal);
        if (!request.current() || this.context !== context) return;
        runInAction(() => {
          const values = offset ? [...this.items, ...page.items] : page.items;
          this.items = [
            ...new Map(
              values.map((item) => [this.identity(item), item]),
            ).values(),
          ];
          this.total = page.total;
          this.offset = page.offset;
          this.failedOffset = null;
        });
      } catch (error) {
        if (request.current() && this.context === context)
          runInAction(() => {
            this.error = error;
            this.failedOffset = offset;
          });
      } finally {
        if (request.current() && this.context === context)
          runInAction(() => {
            this.loading = false;
          });
      }
    });
  }

  loadMore() {
    return this.load(this.offset + 50);
  }

  retry() {
    return this.load(this.failedOffset ?? 0);
  }

  cancel() {
    if (this.context) this.lifecycle.cancel(this.context);
    this.context = "";
    this.loader = null;
    this.loading = false;
    this.failedOffset = null;
  }
}
