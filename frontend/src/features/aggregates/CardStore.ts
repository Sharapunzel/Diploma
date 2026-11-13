import { makeAutoObservable, observable, runInAction } from "mobx";
import {
  AggregatesApi,
  type Aggregate,
  type Point,
  type Operator,
} from "../../app/api/aggregates";
import { RequestLifecycle } from "../resources/RequestLifecycle";
import { ApiError } from "../../app/api/client";

const historicalSuccess =
  "Расчёт выбранного окна сохранён. Автоматическое расписание не изменено.";

export class CardStore {
  card: Aggregate | null = null;
  error: unknown = null;
  success = "";
  historicalResult: Point | null = null;
  historyError: unknown = null;
  operators: Operator[] = [];
  operatorsError: unknown = null;
  private historyRevision = 0;
  mutationOutcome: "unknown" | "refused" | null = null;
  lifecycle = new RequestLifecycle();
  constructor(
    private api: AggregatesApi,
    public id: string,
  ) {
    makeAutoObservable(
      this,
      {
        api: false,
        card: observable.ref,
        historicalResult: observable.ref,
      } as never,
      {
        autoBind: true,
      },
    );
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
  async loadOperators() {
    return this.lifecycle.once("operators", async () => {
      const request = this.lifecycle.begin("operators-request");
      this.operatorsError = null;
      try {
        const operators = await this.api.operators(request.signal, true);
        if (!operators.length) throw new Error("empty_operator_catalog");
        if (request.current())
          runInAction(() => {
            this.operators = operators;
          });
      } catch (error) {
        if (request.current())
          runInAction(() => {
            this.operatorsError = error;
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
  clearHistoricalResult() {
    this.historyRevision++;
    this.historicalResult = null;
    this.historyError = null;
    if (this.success === historicalSuccess) this.success = "";
  }
  async history(start: string) {
    if (!this.canMutate) return;
    return this.lifecycle.once("history", async () => {
      const r = this.lifecycle.begin("history-request");
      const revision = this.historyRevision;
      this.historyError = null;
      this.success = "";
      this.historicalResult = null;
      try {
        const point = await this.api.historical(this.id, start, r.signal);
        if (r.current() && revision === this.historyRevision)
          runInAction(() => {
            this.historicalResult = point ?? null;
            this.success = historicalSuccess;
          });
        return r.current();
      } catch (e) {
        if (r.current() && revision === this.historyRevision)
          runInAction(() => {
            this.historyError = e;
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
    this.operators = [];
    this.operatorsError = null;
    this.clearHistoricalResult();
    this.error = null;
    this.success = "";
    this.mutationOutcome = null;
  }
}
