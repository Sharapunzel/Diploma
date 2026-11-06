import { makeAutoObservable, observable, runInAction } from "mobx";
import { RequestLifecycle } from "../resources/RequestLifecycle";
import type { AggregatePage } from "../../app/api/aggregates";

export class CollectionStore<T> {
  items: T[] = [];
  total = 0;
  offset = 0;
  loaded = false;
  error: unknown = null;
  lifecycle = new RequestLifecycle();
  constructor(
    private fetchPage: (
      offset: number,
      signal: AbortSignal,
    ) => Promise<AggregatePage<T>>,
  ) {
    makeAutoObservable(
      this,
      { fetchPage: false, items: observable.ref } as never,
      { autoBind: true },
    );
  }
  get loading() {
    return this.lifecycle.isPending("load");
  }
  async load(offset = this.offset) {
    this.lifecycle.cancel("load");
    return this.lifecycle.once("load", async () => {
      const r = this.lifecycle.begin("page");
      this.error = null;
      try {
        const data = await this.fetchPage(offset, r.signal);
        if (r.current())
          runInAction(() => {
            this.items = data.items;
            this.total = data.total;
            this.offset = offset;
            this.loaded = true;
          });
      } catch (e) {
        if (r.current())
          runInAction(() => {
            this.error = e;
          });
      }
    });
  }
  dispose() {
    this.lifecycle.dispose();
    this.items = [];
    this.total = 0;
    this.error = null;
  }
}
