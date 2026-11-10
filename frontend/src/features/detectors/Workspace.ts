import { makeAutoObservable, observable, reaction, runInAction } from "mobx";
import { AggregatesApi, type Aggregate } from "../../app/api/aggregates";
import { ApiClient, ApiError } from "../../app/api/client";
import { exact, LosslessNumber, type Json } from "../../app/api/aggregateJson";
import {
  DetectorsApi,
  type Action,
  type DetectorCard,
  type DetectorInput,
  type DetectorSummary,
  type Receipt,
} from "../../app/api/detectors";
import type { SessionStore } from "../../app/stores/SessionStore";
import { RequestLifecycle } from "../resources/RequestLifecycle";

export type Intent = {
  action: Action;
  target: string | null;
  body: DetectorInput | Record<string, never>;
  key: string;
};
export function unknownOutcome(e: unknown) {
  return (
    !(e instanceof ApiError) ||
    e.status === 0 ||
    e.status >= 500 ||
    e.code === "invalid_response" ||
    e.code === "detector_command_unconfirmed"
  );
}
export function strictInteger(text: string, min?: bigint, max?: bigint): Json {
  if (!/^-?\d+$/.test(text))
    throw new Error("Введите целое число без дроби или экспоненты.");
  const n = BigInt(text);
  if ((min !== undefined && n < min) || (max !== undefined && n > max))
    throw new Error("Число вне допустимого диапазона.");
  return new LosslessNumber(n.toString());
}
export class CommandStore {
  lifecycle = new RequestLifecycle();
  intent: Intent | null = null;
  receipt: Receipt | null = null;
  outcome: "unknown" | "confirmed" | null = null;
  error: unknown = null;
  readError: unknown = null;
  card: DetectorCard | null = null;
  success = "";
  needsRead = false;
  readVersion = 0;
  constructor(
    private api: DetectorsApi,
    public id: string | null,
  ) {
    makeAutoObservable(
      this,
      {
        api: false,
        intent: observable.ref,
        receipt: observable.ref,
        card: observable.ref,
      } as never,
      { autoBind: true },
    );
  }
  get busy() {
    return this.lifecycle.isPending("command");
  }
  async load() {
    if (!this.id || this.busy) return;
    return this.lifecycle.once("load", async () => {
      const r = this.lifecycle.begin("read");
      try {
        const card = await this.api.card(this.id!, r.signal);
        if (r.current())
          runInAction(() => {
            this.card = card;
            this.readError = null;
            this.needsRead = false;
            this.readVersion++;
            if (this.outcome === "unknown") {
              this.outcome = null;
              this.intent = null;
            }
          });
      } catch (e) {
        if (r.current())
          runInAction(() => {
            this.readError = e;
          });
      }
    });
  }
  async send(action: Action, body: Intent["body"] = {}, retry = false) {
    if (
      this.busy ||
      (!retry && (this.outcome === "unknown" || this.needsRead)) ||
      (retry && !this.intent)
    )
      return;
    this.lifecycle.cancel("read");
    this.lifecycle.cancel("load");
    if (!retry) {
      this.intent = { action, target: this.id, body, key: crypto.randomUUID() };
      this.receipt = null;
      this.outcome = null;
    }
    const intent = this.intent!;
    await this.lifecycle.once("command", async () => {
      const r = this.lifecycle.begin("mutation");
      this.error = null;
      this.readError = null;
      this.success = "";
      try {
        const receipt = await this.api.command(
          intent.action,
          intent.target,
          intent.body,
          intent.key,
          r.signal,
        );
        if (r.current())
          runInAction(() => {
            this.receipt = receipt;
            this.id = receipt.detector_id;
            this.outcome = "confirmed";
            this.needsRead = true;
            this.intent = null;
            this.success =
              intent.action === "create"
                ? "Детектор создан без запуска."
                : intent.action === "start"
                  ? "Команда запуска подтверждена; это ещё не подтверждение готовности модели."
                  : "Команда остановки подтверждена. Текущее состояние читается отдельно.";
          });
      } catch (e) {
        if (r.current())
          runInAction(() => {
            this.error = e;
            this.outcome = unknownOutcome(e) ? "unknown" : null;
            if (!this.outcome) this.intent = null;
          });
      }
    });
    if (this.outcome === "confirmed") await this.load();
  }
  retry() {
    return this.intent
      ? this.send(this.intent.action, this.intent.body, true)
      : Promise.resolve();
  }
  pause() {
    this.lifecycle.cancel("read");
    this.lifecycle.cancel("load");
  }
  dispose() {
    this.lifecycle.dispose();
    this.intent = null;
    this.receipt = null;
    this.card = null;
    this.error = null;
    this.readError = null;
    this.id = null;
    this.outcome = null;
    this.needsRead = false;
    this.success = "";
  }
}
export class DraftStore {
  name = "";
  aggregateId = "";
  accelerated = false;
  step = "1";
  trees = "100";
  sample = "256";
  output = "256";
  seed = "42";
  dirty = false;
  errors: Record<string, string> = {};
  aggregates: Aggregate[] = [];
  selectedAggregate: Aggregate | null = null;
  query = "";
  catalogQuery: string | null = null;
  total = 0;
  offset = 0;
  catalogError: unknown = null;
  lifecycle = new RequestLifecycle();
  command: CommandStore;
  constructor(
    api: DetectorsApi,
    private aggregatesApi: AggregatesApi,
  ) {
    this.command = new CommandStore(api, null);
    makeAutoObservable(
      this,
      {
        aggregatesApi: false,
        aggregates: observable.ref,
        selectedAggregate: observable.ref,
      } as never,
      { autoBind: true },
    );
  }
  get selected() {
    return this.selectedAggregate?.id === this.aggregateId
      ? this.selectedAggregate
      : this.aggregates.find((a) => a.id === this.aggregateId);
  }
  get frozen() {
    return (
      this.command.busy ||
      this.command.outcome === "unknown" ||
      !!this.command.receipt
    );
  }
  change(
    key:
      | "name"
      | "aggregateId"
      | "step"
      | "trees"
      | "sample"
      | "output"
      | "seed",
    value: string,
  ) {
    if (!this.frozen) {
      this[key] = value;
      if (key === "aggregateId")
        this.selectedAggregate =
          this.aggregates.find((a) => a.id === value) ?? null;
      this.dirty = true;
      this.errors = {};
    }
  }
  mode(value: boolean) {
    if (!this.frozen) {
      this.accelerated = value;
      this.dirty = true;
    }
  }
  search(value: string) {
    if (this.frozen || value === this.query) return;
    this.query = value;
    this.lifecycle.cancel("catalog-read");
    this.lifecycle.cancel("catalog");
    this.aggregates = [];
    this.total = 0;
    this.catalogError = null;
    this.catalogQuery = null;
  }
  async catalog(query = this.query) {
    if (this.frozen) return;
    this.lifecycle.cancel("catalog-read");
    this.lifecycle.cancel("catalog");
    return this.lifecycle.once("catalog", async () => {
      const r = this.lifecycle.begin("catalog-read");
      try {
        const page = await this.aggregatesApi.list(0, r.signal, {
          limit: 10,
          state: "active",
          ...(query.trim() ? { q: query.trim() } : {}),
        });
        if (r.current())
          runInAction(() => {
            this.aggregates = page.items;
            this.total = page.total;
            this.offset = 0;
            this.catalogError = null;
            this.catalogQuery = query;
          });
      } catch (e) {
        if (r.current())
          runInAction(() => {
            this.catalogError = e;
            this.catalogQuery = query;
            this.aggregates = [];
            this.total = 0;
          });
      }
    });
  }
  payload(): DetectorInput | null {
    this.errors = {};
    if (!this.name.trim() || [...this.name.trim()].length > 200)
      this.errors.name = "Имя: от 1 до 200 символов после удаления пробелов.";
    if (!this.selected || this.selected.state !== "active")
      this.errors.aggregateId = "Выберите активный сохранённый агрегат.";
    const read = (key: string, value: string, min?: bigint, max?: bigint) => {
      try {
        return strictInteger(value, min, max);
      } catch (e) {
        this.errors[key] = (e as Error).message;
        return null;
      }
    };
    const trees = read("trees", this.trees, 1n),
      sample_size = read("sample", this.sample, 2n),
      output_after = read("output", this.output, 2n),
      seed = read("seed", this.seed, -(2n ** 63n), 2n ** 63n - 1n);
    if (
      sample_size &&
      output_after &&
      BigInt(exact(output_after)) < BigInt(exact(sample_size))
    )
      this.errors.output =
        "Порог начала оценки должен быть не меньше размера выборки.";
    const step_seconds = this.accelerated
      ? read(
          "step",
          this.step,
          1n,
          BigInt(this.selected?.duration_seconds ?? 1),
        )
      : null;
    return Object.keys(this.errors).length
      ? null
      : {
          aggregate_id: this.aggregateId,
          name: this.name.trim(),
          accelerated: this.accelerated,
          step_seconds,
          config: { trees, sample_size, output_after, seed },
        };
  }
  async submit() {
    const body = this.payload();
    if (!body || this.frozen) return;
    await this.command.send("create", body);
    if (this.command.receipt)
      runInAction(() => {
        this.dirty = false;
      });
  }
  dispose() {
    this.lifecycle.dispose();
    this.command.dispose();
    this.name = "";
    this.seed = "42";
    this.aggregateId = "";
    this.aggregates = [];
    this.selectedAggregate = null;
    this.query = "";
    this.dirty = false;
  }
}
export class ListStore {
  lifecycle = new RequestLifecycle();
  items: DetectorSummary[] = [];
  total = 0;
  offset = 0;
  limit = 25;
  aggregate = "";
  execution = "";
  error: unknown = null;
  constructor(private api: DetectorsApi) {
    makeAutoObservable(this, { api: false, items: observable.ref } as never, {
      autoBind: true,
    });
  }
  async load(
    offset = this.offset,
    filters = { aggregate: this.aggregate, execution: this.execution },
  ) {
    this.lifecycle.cancel("read");
    this.lifecycle.cancel("load");
    this.error = null;
    return this.lifecycle.once("load", async () => {
      const r = this.lifecycle.begin("read");
      try {
        const p = await this.api.list(
          {
            limit: this.limit,
            offset,
            ...(filters.aggregate ? { aggregate_id: filters.aggregate } : {}),
            ...(filters.execution
              ? { execution_state: filters.execution }
              : {}),
          },
          r.signal,
        );
        if (r.current())
          runInAction(() => {
            this.items = p.items;
            this.total = p.total;
            this.offset = offset;
            this.aggregate = filters.aggregate;
            this.execution = filters.execution;
            this.error = null;
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
  filter(aggregate: string, execution: string) {
    this.items = [];
    this.total = 0;
    return this.load(0, { aggregate, execution });
  }
  dispose() {
    this.lifecycle.dispose();
    this.items = [];
  }
}
export class DetectorWorkspace {
  draft: DraftStore;
  list: ListStore;
  cards = new Map<string, CommandStore>();
  private api: DetectorsApi;
  private aggregates: AggregatesApi;
  private stop: () => void;
  constructor(client: ApiClient, session: SessionStore) {
    this.api = new DetectorsApi(client);
    this.aggregates = new AggregatesApi(client);
    this.draft = new DraftStore(this.api, this.aggregates);
    this.list = new ListStore(this.api);
    makeAutoObservable(
      this,
      {
        api: false,
        aggregates: false,
        stop: false,
        cards: false,
      } as never,
      { autoBind: true },
    );
    this.stop = reaction(
      () => `${session.version}|${session.data?.permissions.join("|")}`,
      () => this.reset(),
    );
  }
  card(id: string) {
    if (this.draft.command.id === id) return this.draft.command;
    let c = this.cards.get(id);
    if (!c) {
      c = new CommandStore(this.api, id);
      this.cards.set(id, c);
    }
    return c;
  }
  newDraft() {
    if (this.draft.command.receipt) {
      if (this.draft.command.id)
        this.cards.set(this.draft.command.id, this.draft.command);
      this.draft.lifecycle.dispose();
      this.draft = new DraftStore(this.api, this.aggregates);
    }
  }
  reset() {
    this.draft.dispose();
    this.list.dispose();
    this.cards.forEach((c) => c.dispose());
    this.cards.clear();
    this.draft = new DraftStore(this.api, this.aggregates);
    this.list = new ListStore(this.api);
  }
  dispose() {
    this.stop();
    this.reset();
  }
}
