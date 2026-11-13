import { makeAutoObservable, observable, runInAction } from "mobx";
import { ApiError } from "../../app/api/client";
import {
  AggregatesApi,
  type AggregateInput,
  type Aggregate,
  type CreateInput,
  type Operator,
  type Preview,
  type Validation,
} from "../../app/api/aggregates";
import { printAggregate, type JsonObject } from "../../app/api/aggregateJson";
import { RequestLifecycle } from "../resources/RequestLifecycle";
import {
  clone,
  initialRule,
  referenceErrors,
  ruleDocument,
  retainDefinitionOrder,
  assertVisualReferences,
  resetDefinitionOrder,
} from "./rule";
import { processIssues, validInstant } from "./processValidation";

export class DraftStore {
  name = "";
  source = "";
  boundary = "";
  timezone = "UTC";
  duration = "300";
  window = "";
  timeBasis = "availability_at";
  text = printAggregate(initialRule(), true);
  mode: "visual" | "json" = "visual";
  rule: JsonObject = initialRule();
  operators: Operator[] = [];
  validation: Validation | null = null;
  preview: Preview | null = null;
  error: unknown = null;
  localError = "";
  success = "";
  dirty = false;
  unknownCreate = false;
  invalidNodes = new Set<string>();
  lifecycle = new RequestLifecycle();
  copyState: "none" | "loading" | "error" | "ready";
  copyError: unknown = null;
  private retry: { body: CreateInput; key: string; signature: string } | null =
    null;
  constructor(
    private api: AggregatesApi,
    private copyId = "",
  ) {
    this.copyState = copyId ? "loading" : "none";
    makeAutoObservable(
      this,
      {
        api: false,
        rule: observable.ref,
        validation: observable.ref,
        preview: observable.ref,
        retry: false,
      } as never,
      { autoBind: true },
    );
  }
  get busy() {
    return this.lifecycle.pending.size > 0;
  }
  get editable() {
    return this.copyState === "none" || this.copyState === "ready";
  }
  async loadCopy() {
    if (!this.copyId || this.copyState === "ready") return;
    return this.lifecycle.once("copy-load", async () => {
      const request = this.lifecycle.begin("copy-request");
      this.copyState = "loading";
      this.copyError = null;
      try {
        const input = await this.api.copy(this.copyId, request.signal);
        if (request.current())
          runInAction(() => {
            this.copyState = "ready";
            this.applyCopy(input);
          });
      } catch (error) {
        if (request.current())
          runInAction(() => {
            this.copyError = error;
            this.copyState = "error";
          });
      }
    });
  }
  get context() {
    return printAggregate([
      this.source,
      this.text,
      this.boundary,
      this.timezone,
      this.duration,
      this.window,
      this.timeBasis,
    ]);
  }
  get errors() {
    return referenceErrors(this.rule);
  }
  get processErrors() {
    return processIssues(this);
  }
  get processValid() {
    return !Object.keys(this.processErrors).length;
  }
  get canCreate() {
    if (this.unknownCreate) return !!this.retry && this.editable && !this.busy;
    return (
      !!this.validation &&
      this.editable &&
      this.processValid &&
      !!this.preview &&
      !this.unknownCreate &&
      !this.localError &&
      !this.invalidNodes.size &&
      !this.errors.length &&
      !this.busy
    );
  }
  invalidate() {
    this.dirty = true;
    this.validation = null;
    this.preview = null;
    this.error = null;
    this.success = "";
    this.lifecycle.cancel("check");
  }
  setField(
    key:
      | "name"
      | "source"
      | "boundary"
      | "timezone"
      | "duration"
      | "window"
      | "timeBasis",
    value: string,
  ) {
    if (!this.editable || this.lifecycle.isPending("create")) return;
    if (this[key] === value) return;
    this.invalidate();
    this[key] = value;
    try {
      ruleDocument(this.text, this.operators);
      this.localError = "";
    } catch {
      /* Keep a JSON syntax error until the document is corrected. */
    }
  }
  setInvalid(id: string, invalid: boolean) {
    if (invalid) {
      this.invalidNodes.add(id);
      this.invalidate();
    } else {
      this.invalidNodes.delete(id);
      if (!this.invalidNodes.size) {
        try {
          ruleDocument(this.text, this.operators);
          this.localError = "";
        } catch {
          /* Keep invalid JSON available for correction. */
        }
      }
    }
  }
  setText(text: string) {
    if (!this.editable || this.lifecycle.isPending("create")) return;
    const previous = this.rule;
    this.invalidate();
    this.text = text;
    this.localError = "";
    try {
      this.rule = ruleDocument(text, this.operators);
      retainDefinitionOrder(previous, this.rule);
    } catch (e) {
      this.localError = e instanceof Error ? e.message : "Некорректный JSON";
    }
  }
  setOperators(operators: Operator[]) {
    this.operators = operators;
    try {
      const previous = this.rule;
      this.rule = ruleDocument(this.text, operators);
      retainDefinitionOrder(previous, this.rule);
      this.localError = "";
    } catch (e) {
      this.localError = e instanceof Error ? e.message : "Проверьте JSON";
    }
  }
  setRule(rule: JsonObject) {
    assertVisualReferences(this.rule, rule);
    retainDefinitionOrder(this.rule, rule, true);
    this.setText(printAggregate(rule, true));
    retainDefinitionOrder(rule, this.rule);
  }
  importText(text: string) {
    if (!this.editable || this.lifecycle.isPending("create")) return;
    this.invalidNodes.clear();
    this.mode = "json";
    this.setText(text);
    if (!this.localError) resetDefinitionOrder(this.rule);
  }
  switchMode(mode: "visual" | "json") {
    if (!this.editable) return;
    if (this.invalidNodes.size) {
      this.localError =
        "Исправьте незавершённые значения перед переключением режима.";
      return;
    }
    if (mode === "visual") {
      try {
        const previous = this.rule;
        this.rule = ruleDocument(this.text, this.operators);
        retainDefinitionOrder(previous, this.rule);
        this.localError = "";
      } catch (e) {
        this.localError = e instanceof Error ? e.message : "Некорректный JSON";
        return;
      }
    }
    this.mode = mode;
  }
  private applyCopy(input: AggregateInput) {
    this.invalidate();
    this.source = input.source_id;
    this.boundary = input.schedule.boundary;
    this.timezone = input.schedule.timezone;
    this.duration = String(input.schedule.duration_seconds);
    this.rule = clone(input.aggregate_rule);
    this.setRule(clone(input.aggregate_rule));
  }
  input(): AggregateInput {
    const processError = Object.values(this.processErrors)[0];
    if (processError) throw new Error(processError);
    if (this.invalidNodes.size)
      throw new Error(
        "Исправьте незавершённые значения в визуальном редакторе.",
      );
    if (this.localError || this.errors.length)
      throw new Error(this.localError || this.errors.join("; "));
    return {
      source_id: this.source,
      schedule: {
        boundary: this.boundary,
        timezone: this.timezone,
        duration_seconds: Number(this.duration),
      },
      aggregate_rule: ruleDocument(this.text, this.operators),
    };
  }
  async validate() {
    if (!this.editable || this.busy || this.invalidNodes.size) return;
    this.validation = null;
    this.preview = null;
    this.error = null;
    let body: AggregateInput;
    try {
      body = this.input();
    } catch (e) {
      this.localError = e instanceof Error ? e.message : "Проверьте поля";
      return;
    }
    const context = this.context;
    return this.lifecycle.once("validate", async () => {
      const r = this.lifecycle.begin("check");
      try {
        const result = await this.api.validate(body, r.signal);
        if (r.current() && this.context === context)
          runInAction(() => {
            this.validation = result;
            this.success = "Правило и расписание проверены сервером.";
          });
      } catch (e) {
        if (r.current())
          runInAction(() => {
            this.error = e;
          });
      }
    });
  }
  async runPreview() {
    if (!this.validation || this.busy) return;
    if (!validInstant(this.window)) {
      this.localError =
        "Выберите начало завершённого окна для предварительного расчёта.";
      return;
    }
    const context = this.context;
    const body = {
      ...this.input(),
      window_start_utc: this.window,
      time_basis: this.timeBasis,
    };
    const hash = this.validation.rule_hash;
    this.error = null;
    this.preview = null;
    return this.lifecycle.once("preview", async () => {
      const r = this.lifecycle.begin("check");
      try {
        const result = await this.api.preview(body, r.signal);
        if (
          r.current() &&
          this.context === context &&
          result.rule_hash === hash
        )
          runInAction(() => {
            this.preview = result;
            this.success =
              "Предварительный расчёт завершён. Данные не сохранены.";
          });
      } catch (e) {
        if (r.current())
          runInAction(() => {
            this.error = e;
          });
      }
    });
  }
  async create(): Promise<Aggregate | undefined> {
    if (this.busy || !this.editable || (!this.unknownCreate && !this.canCreate))
      return;
    const body: CreateInput =
      this.unknownCreate && this.retry
        ? this.retry.body
        : {
            ...this.input(),
            name: this.name,
            window_start_utc: this.window,
            time_basis: this.timeBasis,
            rule_hash: this.validation!.rule_hash,
          };
    const signature = printAggregate(body);
    if (!this.unknownCreate && this.retry?.signature !== signature)
      this.retry = { body: clone(body), signature, key: crypto.randomUUID() };
    const retry = this.retry!;
    const submittedContext = this.context;
    const submittedName = this.name;
    this.error = null;
    return this.lifecycle.once("create", async () => {
      const r = this.lifecycle.begin("mutation");
      try {
        const result = await this.api.create(retry.body, retry.key, r.signal);
        if (!r.current()) return;
        runInAction(() => {
          // A replay can confirm an older intent after the user edited the draft.
          const currentBody = (() => {
            try {
              return printAggregate({
                ...this.input(),
                name: this.name,
                window_start_utc: this.window,
                time_basis: this.timeBasis,
                rule_hash: this.validation?.rule_hash ?? retry.body.rule_hash,
              });
            } catch {
              return "";
            }
          })();
          this.dirty = currentBody !== retry.signature;
          this.unknownCreate = false;
          this.retry = null;
          if (this.dirty)
            this.success = `Прежний запрос подтверждён: ${result.id}. Изменённый черновик сохранён на экране.`;
        });
        if (
          this.dirty ||
          submittedContext !== this.context ||
          submittedName !== this.name
        )
          return;
        return result;
      } catch (e) {
        if (r.current())
          runInAction(() => {
            this.error = e;
            this.unknownCreate =
              e instanceof ApiError &&
              (e.status === 0 ||
                e.status >= 500 ||
                e.code === "invalid_response");
          });
      }
    });
  }
  dispose() {
    this.lifecycle.dispose();
    this.copyError = null;
    this.copyState = "none";
    this.name = "";
    this.source = "";
    this.boundary = "";
    this.window = "";
    this.error = null;
    this.success = "";
    this.localError = "";
    this.unknownCreate = false;
    this.invalidNodes.clear();
    this.operators = [];
    this.text = "";
    this.rule = initialRule();
    this.retry = null;
    this.validation = null;
    this.preview = null;
  }
}
