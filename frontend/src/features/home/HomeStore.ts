import { makeAutoObservable, runInAction } from "mobx";
import type { ApiClient } from "../../app/api/client";
import type { HomeOverview } from "../../app/api/home";

export class HomeStore {
  overview: HomeOverview | null = null;
  loading = false;
  error: unknown = null;
  private controller: AbortController | null = null;

  constructor(
    private api: ApiClient,
    readonly accessKey: string,
  ) {
    makeAutoObservable(this, { api: false, controller: false } as never, {
      autoBind: true,
    });
  }

  cancel() {
    this.controller?.abort();
    this.controller = null;
  }

  async load() {
    this.cancel();
    const controller = new AbortController();
    this.controller = controller;
    this.loading = true;
    this.error = null;
    try {
      const data = await this.api.request<HomeOverview>("/home/overview", {
        signal: controller.signal,
      });
      if (this.controller === controller && !controller.signal.aborted) {
        runInAction(() => {
          this.overview = data;
        });
      }
    } catch (cause) {
      if (this.controller === controller && !controller.signal.aborted) {
        runInAction(() => {
          this.error = cause;
        });
      }
    } finally {
      if (this.controller === controller && !controller.signal.aborted) {
        runInAction(() => {
          this.loading = false;
          this.controller = null;
        });
      }
    }
  }
}
