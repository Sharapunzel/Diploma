import { makeAutoObservable, runInAction } from "mobx";
import {
  AggregatesApi,
  type AggregateField,
  type Operator,
} from "../../app/api/aggregates";
import { printAggregate } from "../../app/api/aggregateJson";
import { RequestLifecycle } from "../resources/RequestLifecycle";
import { aggregatePrompt } from "./rule";

export class PromptStore {
  text = "";
  error: unknown = null;
  lifecycle = new RequestLifecycle();
  constructor(
    private api: AggregatesApi,
    private source: string,
  ) {
    makeAutoObservable(this, { api: false } as never, { autoBind: true });
  }
  async load(operators: Operator[]) {
    return this.lifecycle.once("load", async () => {
      const r = this.lifecycle.begin("catalog");
      this.error = null;
      this.text = "";
      try {
        const fields: AggregateField[] = [];
        if (this.source) {
          let offset = 0;
          let total = 1;
          while (offset < total) {
            const page = await this.api.fields(
              this.source,
              "",
              offset,
              r.signal,
            );
            if (!r.current()) return;
            if (!page.items.length && offset < page.total)
              throw new Error("Неполный каталог");
            fields.push(...page.items);
            offset += page.items.length;
            total = page.total;
          }
        }
        if (r.current())
          runInAction(() => {
            this.text =
              aggregatePrompt(operators) +
              (this.source
                ? `\nПолный каталог полей (не наблюдавшиеся данные):\n${printAggregate(fields, true)}`
                : "\nИсточник не выбран: используйте полный серверный каталог ECS при выборе поля.");
          });
      } catch (error) {
        if (r.current())
          runInAction(() => {
            this.error = error;
          });
      }
    });
  }
  dispose() {
    this.lifecycle.dispose();
    this.text = "";
    this.error = null;
  }
}
