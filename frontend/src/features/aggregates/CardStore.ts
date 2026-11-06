import { makeAutoObservable, observable, runInAction } from "mobx";
import { AggregatesApi, type Aggregate } from "../../app/api/aggregates";
import { RequestLifecycle } from "../resources/RequestLifecycle";
import { ApiError } from "../../app/api/client";

export class CardStore {
  card: Aggregate | null = null;
  error: unknown = null;
  success = "";
  mutationOutcome: "unknown" | "refused" | null = null;
  lifecycle = new RequestLifecycle();
  constructor(
    private api: AggregatesApi,
    public id: string,
  ) {
    makeAutoObservable(this, { api: false, card: observable.ref } as never, {
      autoBind: true,
    });
  }
  async load() {
    if (this.lifecycle.isPending("action")) return;
    return this.lifecycle.once("load", async () => {
      const r = this.lifecycle.begin("read");
      try {
        const card = await this.api.card(this.id, r.signal);
        if (r.current())
          runInAction(() => {
            this.card = card;
            this.error = null;
            this.mutationOutcome = null;
          });
      } catch (e) {
        if (r.current())
          runInAction(() => {
            this.error = e;
          });
      }
    });
  }
  async action(action: "archive" | "resume" | "delete") {
    if (!this.canMutate) return;
    this.lifecycle.cancel("read");
    this.lifecycle.cancel("load");
    return this.lifecycle.once("action", async () => {
      const r = this.lifecycle.begin("mutation");
      this.error = null;
      this.success = "";
      this.mutationOutcome = null;
      try {
        const card = await this.api.lifecycle(this.id, action, r.signal);
        if (r.current())
          runInAction(() => {
            this.card = card;
            this.success = "Состояние агрегата обновлено.";
          });
        return r.current();
      } catch (e) {
        if (r.current())
          runInAction(() => {
            this.error = e;
            this.mutationOutcome =
              !(e instanceof ApiError) ||
              e.status === 0 ||
              e.code === "invalid_response" ||
              e.status >= 500
                ? "unknown"
                : "refused";
          });
        return false;
      }
    });
  }
  async history(start: string) {
    if (!this.canMutate) return;
    return this.lifecycle.once("history", async () => {
      const r = this.lifecycle.begin("history-request");
      this.error = null;
      this.success = "";
      try {
        await this.api.historical(this.id, start, r.signal);
        if (r.current())
          runInAction(() => {
            this.success =
              "Историческое окно сохранено. Рабочий курсор не изменён.";
          });
        return r.current();
      } catch (e) {
        if (r.current())
          runInAction(() => {
            this.error = e;
          });
        return false;
      }
    });
  }
  get canMutate() {
    return (
      !this.lifecycle.isPending("action") && this.mutationOutcome !== "unknown"
    );
  }
  dispose() {
    this.lifecycle.dispose();
    this.card = null;
    this.error = null;
    this.success = "";
    this.mutationOutcome = null;
  }
}
