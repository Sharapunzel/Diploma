import { makeAutoObservable, observable, runInAction } from "mobx";
import type { Aggregate, AggregatesApi } from "../../app/api/aggregates";
import { RequestLifecycle } from "../resources/RequestLifecycle";
export type AggregateFilters = { q: string; state: "" | "active" | "archived" };
export class ListStore {
  items: Aggregate[] = [];
  total = 0;
  offset = 0;
  filters: AggregateFilters = { q: "", state: "" };
  error: unknown = null;
  lifecycle = new RequestLifecycle();
  constructor(private api: AggregatesApi) {
    makeAutoObservable(this, { api: false, items: observable.ref } as never, {
      autoBind: true,
    });
  }
  get loading() {
    return this.lifecycle.isPending("load");
  }
  async load(
    offset = this.offset,
    filters = this.filters,
  ): Promise<boolean | undefined> {
    this.lifecycle.cancel("load");
    return this.lifecycle.once("load", async () => {
      const r = this.lifecycle.begin("read");
      this.error = null;
      try {
        const page = await this.api.list(offset, r.signal, {
          q: filters.q,
          ...(filters.state ? { state: filters.state } : {}),
        });
        if (r.current()) {
          runInAction(() => {
            this.items = page.items;
            this.total = page.total;
            this.offset = offset;
            this.filters = { ...filters };
          });
          return true;
        }
      } catch (error) {
        if (r.current())
          runInAction(() => {
            this.error = error;
          });
      }
      return false;
    });
  }
  dispose() {
    this.lifecycle.dispose();
    this.items = [];
    this.total = 0;
    this.error = null;
  }
}
