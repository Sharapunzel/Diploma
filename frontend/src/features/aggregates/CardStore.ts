import { makeAutoObservable, observable, runInAction } from "mobx";
import {
  AggregatesApi,
  type Aggregate,
  type Point,
  type Operator,
  type AggregateAction,
  type AggregateCommandReceipt,
  type CleanupProtection,
  type CleanupOperation,
  keepLastDecimal,
} from "../../app/api/aggregates";
import { exact } from "../../app/api/aggregateJson";
import { RequestLifecycle } from "../resources/RequestLifecycle";
import { ApiError } from "../../app/api/client";

const historicalSuccess =
  "Сохранённое окно загружено. Новый расчёт не выполнялся.";

export type AggregateCommandIntent = Readonly<{
  action: AggregateAction;
  target: string;
  keepLast?: string;
  operationId?: string;
  key: string;
}>;
type CommandContext = {
  intent: AggregateCommandIntent | null;
  receipt: AggregateCommandReceipt | null;
  outcome: "unknown" | "confirmed" | null;
  needsRead: boolean;
};
const commandScopes = new WeakMap<
  object,
  { actor: string; contexts: Map<string, CommandContext> }
>();

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
  commandIntent: AggregateCommandIntent | null = null;
  commandReceipt: AggregateCommandReceipt | null = null;
  commandOutcome: "unknown" | "confirmed" | null = null;
  commandError: unknown = null;
  commandReadError: unknown = null;
  commandNeedsRead = false;
  cleanupKeepLast = "0";
  cleanupPlan: CleanupProtection | null = null;
  cleanupPlanError: unknown = null;
  cleanupOperation: CleanupOperation | null = null;
  cleanupOperationError: unknown = null;
  autoCleanupError: unknown = null;
  pointsVersion = 0;
  pointsClearVersion = 0;
  private cleanupRefreshPending = false;
  private commandContexts: Map<string, CommandContext> | null = null;
  private visible = false;
  private poll: ReturnType<typeof setTimeout> | null = null;
  private cardPoll: ReturnType<typeof setTimeout> | null = null;
  private cardReadAt = 0;
  private cardReadFailed = false;
  private backgroundLoading = false;
  private observedSucceeded = "";
  private operationReadAt = 0;
  private operationReadId = "";
  private cleanupRevision: {
    id: string;
    revision: string;
    final: boolean;
  } | null = null;
  constructor(
    private api: AggregatesApi,
    public id: string,
  ) {
    this.id = id.toLowerCase();
    makeAutoObservable(
      this,
      {
        api: false,
        card: observable.ref,
        historicalResult: observable.ref,
        commandIntent: observable.ref,
        commandReceipt: observable.ref,
        cleanupPlan: observable.ref,
        cleanupOperation: observable.ref,
        commandContexts: false,
        poll: false,
        cardPoll: false,
      } as never,
      {
        autoBind: true,
      },
    );
  }
  /** Memory only, shared by route mounts for one actor. Never reassign an unresolved target. */
  bindScope(owner: object, actor: string) {
    let scope = commandScopes.get(owner);
    if (!scope || scope.actor !== actor) {
      scope = { actor, contexts: new Map() };
      commandScopes.set(owner, scope);
    }
    this.commandContexts = scope.contexts;
    const context = scope.contexts.get(this.id);
    if (context) {
      this.commandIntent = context.intent;
      this.commandReceipt = context.receipt;
      this.commandOutcome = context.outcome;
      this.commandNeedsRead = context.needsRead;
    }
  }
  private saveCommandContext() {
    this.commandContexts?.set(this.id, {
      intent: this.commandIntent,
      receipt: this.commandReceipt,
      outcome:
        this.commandIntent && this.commandBusy
          ? "unknown"
          : this.commandOutcome,
      needsRead: this.commandNeedsRead,
    });
  }
  get cardLoading() {
    return this.lifecycle.isPending("load") && !this.backgroundLoading;
  }
  private acceptCard(card: Aggregate) {
    const previousRevision = this.card?.auto_cleanup.history_revision;
    const revision = card.auto_cleanup.history_revision;
    const cleanupTarget = card.active_cleanup_id || this.cleanupTarget;
    const revisionChanged =
      !!this.card &&
      revision !== null &&
      (previousRevision === null ||
        exact(previousRevision) !== exact(revision));
    if (this.cleanupRefreshPending || revisionChanged) {
      this.pointsVersion++;
      this.cleanupPlan = null;
      if (!this.cleanupRefreshPending) this.clearHistoricalResult();
    }
    this.cleanupRefreshPending = false;
    this.card = card;
    this.cardReadFailed = false;
    this.autoCleanupError = null;
    this.cardReadAt = Date.now();
    const changed = card.active_cleanup_id
      ? this.observeCleanup(card.active_cleanup_id)
      : false;
    if (revisionChanged && cleanupTarget && revision !== null)
      this.cleanupRevision = {
        id: cleanupTarget,
        revision: exact(revision),
        final: !card.active_cleanup_id,
      };
    else if (
      !card.active_cleanup_id &&
      this.cleanupRevision &&
      cleanupTarget === this.cleanupRevision?.id &&
      revision !== null &&
      exact(revision) === this.cleanupRevision.revision
    )
      this.cleanupRevision.final = true;
    if (
      this.visible &&
      card.active_cleanup_id &&
      (changed || (!this.cleanupOperation && !this.cleanupOperationError))
    )
      void this.loadCleanupOperation();
    this.scheduleCardRead();
  }
  async load(background = false) {
    if (this.lifecycle.isPending("action") || this.commandBusy) return;
    return this.lifecycle.once("load", async () => {
      const r = this.lifecycle.begin("read");
      this.backgroundLoading = background;
      this.cardReadAt = Date.now();
      try {
        const card = await this.api.card(this.id, r.signal);
        if (r.current())
          runInAction(() => {
            this.acceptCard(card);
            if (!background) {
              this.error = null;
              this.mutationOutcome = null;
              this.commandReadError = null;
              this.commandNeedsRead = false;
              this.saveCommandContext();
            }
          });
      } catch (e) {
        if (r.current())
          runInAction(() => {
            // Stop automatic reads independently of the one context presenting
            // this failure. A manual/command GET must not also appear as an
            // automatic-storage error or silently schedule its own retry.
            this.cardReadFailed = true;
            if (background) this.autoCleanupError = e;
            else {
              this.autoCleanupError = null;
              if (
                this.commandOutcome === "confirmed" &&
                this.commandNeedsRead
              ) {
                this.error = null;
                this.commandReadError = e;
              } else {
                this.error = e;
                this.commandReadError = null;
              }
            }
          });
      } finally {
        if (r.current())
          runInAction(() => {
            this.backgroundLoading = false;
            if (this.cleanupRefreshPending) {
              this.cleanupRefreshPending = false;
              this.pointsVersion++;
            }
            this.scheduleCardRead();
          });
      }
    });
  }
  refreshCommandCard() {
    return this.load();
  }
  get commandBusy() {
    return this.lifecycle.isPending("command");
  }
  get canCommand() {
    return (
      !this.commandBusy &&
      !this.lifecycle.isPending("action") &&
      this.mutationOutcome !== "unknown" &&
      this.commandOutcome !== "unknown" &&
      !this.commandNeedsRead
    );
  }
  get cleanupInputError() {
    try {
      keepLastDecimal(this.cleanupKeepLast);
      return "";
    } catch (error) {
      return error instanceof Error
        ? error.message
        : "Проверьте число сохраняемых точек.";
    }
  }
  get cleanupPlanLoading() {
    return this.lifecycle.isPending("cleanup-plan");
  }
  setCleanupKeepLast(value: string) {
    if (this.cleanupKeepLast === value) return;
    this.cleanupKeepLast = value;
    this.cleanupPlan = null;
    this.cleanupPlanError = null;
    this.lifecycle.cancel("cleanup-plan");
    this.lifecycle.cancel("cleanup-plan-request");
  }
  async loadCleanupPlan() {
    if (this.cleanupInputError || this.cleanupPlanLoading) return;
    const requested = keepLastDecimal(this.cleanupKeepLast);
    return this.lifecycle.once("cleanup-plan", async () => {
      const r = this.lifecycle.begin("cleanup-plan-request");
      this.cleanupPlan = null;
      this.cleanupPlanError = null;
      try {
        const plan = await this.api.cleanupPreview(
          this.id,
          requested,
          r.signal,
        );
        if (
          plan.aggregate_id !== this.id ||
          exact(plan.requested_keep) !== requested
        )
          throw new ApiError(200, "invalid_response");
        if (r.current() && keepLastDecimal(this.cleanupKeepLast) === requested)
          runInAction(() => {
            this.cleanupPlan = plan;
          });
      } catch (error) {
        if (r.current())
          runInAction(() => {
            this.cleanupPlanError = error;
          });
      }
    });
  }
  get canCleanup() {
    return (
      this.canCommand &&
      !this.cleanupInputError &&
      !!this.cleanupPlan?.can_cleanup &&
      this.cleanupPlan.aggregate_id === this.id &&
      exact(this.cleanupPlan.requested_keep) ===
        keepLastDecimal(this.cleanupKeepLast) &&
      !this.card?.active_cleanup_id
    );
  }
  async sendCommand(action: AggregateAction, replay = false) {
    if (
      this.commandBusy ||
      (!replay && !this.canCommand) ||
      (replay && !this.commandIntent)
    )
      return;
    if (!replay) {
      if (this.card?.active_cleanup_id && action !== "cleanup_retry") return;
      if (action === "cleanup" && !this.canCleanup) return;
      if (
        action === "cleanup_retry" &&
        (this.cleanupOperation?.status !== "failed" ||
          this.cleanupOperation.aggregate_id !== this.id)
      )
        return;
      this.commandIntent = Object.freeze({
        action,
        target: this.id,
        key: crypto.randomUUID(),
        ...(action === "cleanup"
          ? { keepLast: keepLastDecimal(this.cleanupKeepLast) }
          : {}),
        ...(action === "cleanup_retry"
          ? { operationId: this.cleanupOperation!.id }
          : {}),
      });
      this.commandReceipt = null;
      this.commandOutcome = null;
      this.commandNeedsRead = false;
    }
    const intent = this.commandIntent!;
    if (intent.target !== this.id) return;
    this.lifecycle.cancel("read");
    this.lifecycle.cancel("load");
    await this.lifecycle.once("command", async () => {
      const r = this.lifecycle.begin("command-request");
      this.commandError = null;
      this.commandReadError = null;
      this.success = "";
      this.saveCommandContext();
      try {
        const receipt =
          intent.action === "cleanup"
            ? await this.api.cleanup(
                intent.target,
                intent.keepLast!,
                intent.key,
                r.signal,
              )
            : intent.action === "cleanup_retry"
              ? await this.api.retryCleanup(
                  intent.target,
                  intent.operationId!,
                  intent.key,
                  r.signal,
                )
              : await this.api.command(
                  intent.target,
                  intent.action,
                  intent.key,
                  r.signal,
                );
        if (
          receipt.aggregate_id !== intent.target ||
          receipt.action !== intent.action ||
          (intent.operationId &&
            receipt.cleanup_operation_id !== intent.operationId)
        )
          throw new ApiError(200, "invalid_response");
        if (r.current())
          runInAction(() => {
            this.commandReceipt = receipt;
            if (receipt.cleanup_operation_id)
              this.observeCleanup(receipt.cleanup_operation_id);
            this.commandIntent = null;
            this.commandOutcome = "confirmed";
            this.commandNeedsRead = true;
            this.cleanupOperationError = null;
            this.success =
              intent.action === "cleanup" || intent.action === "cleanup_retry"
                ? "Очистка принята. Завершение подтверждается состоянием операции."
                : "Команда подтверждена. Текущее состояние читается отдельно.";
            this.saveCommandContext();
          });
      } catch (error) {
        if (r.current())
          runInAction(() => {
            this.commandError = error;
            const unknown =
              !(error instanceof ApiError) ||
              error.status === 0 ||
              error.status >= 500 ||
              error.code === "invalid_response";
            this.commandOutcome = unknown ? "unknown" : null;
            if (!unknown) this.commandIntent = null;
            if (error instanceof ApiError && error.status === 409)
              this.cleanupPlan = null;
            this.saveCommandContext();
          });
      }
    });
    if (this.commandOutcome === "confirmed") {
      await this.load();
      if (this.commandReceipt?.cleanup_operation_id)
        await this.loadCleanupOperation();
    }
  }
  replayCommand() {
    return this.commandIntent
      ? this.sendCommand(this.commandIntent.action, true)
      : Promise.resolve();
  }
  setVisible(value: boolean) {
    this.visible = value;
    if (!value) {
      if (this.cardPoll) clearTimeout(this.cardPoll);
      this.cardPoll = null;
      if (this.backgroundLoading) {
        this.lifecycle.cancel("read");
        this.lifecycle.cancel("load");
        this.backgroundLoading = false;
      }
      if (this.poll) clearTimeout(this.poll);
      this.poll = null;
      this.lifecycle.cancel("cleanup-operation");
      this.lifecycle.cancel("cleanup-operation-request");
      return;
    }
    this.scheduleCardRead();
    const cleanupTarget = this.cleanupTarget;
    if (
      cleanupTarget &&
      !this.cleanupOperationError &&
      (!this.cleanupOperation ||
        this.cleanupOperation.id !== cleanupTarget ||
        ["pending", "running"].includes(this.cleanupOperation.status))
    ) {
      const wait = Math.max(0, this.operationReadAt + 2000 - Date.now());
      if (wait)
        this.poll = setTimeout(() => {
          this.poll = null;
          void this.loadCleanupOperation();
        }, wait);
      else void this.loadCleanupOperation();
    }
  }
  private get cleanupTarget() {
    return (
      this.card?.active_cleanup_id ||
      this.operationReadId ||
      this.commandReceipt?.cleanup_operation_id
    );
  }
  private scheduleCardRead() {
    if (this.cardPoll) clearTimeout(this.cardPoll);
    this.cardPoll = null;
    if (!this.visible || !this.card || this.cardReadFailed) return;
    this.cardPoll = setTimeout(
      async () => {
        this.cardPoll = null;
        this.cardReadAt = Date.now();
        await this.load(true);
        this.scheduleCardRead();
      },
      Math.max(0, this.cardReadAt + 5000 - Date.now()),
    );
  }
  private scheduleCleanupRead() {
    if (this.poll) clearTimeout(this.poll);
    this.poll = null;
    if (
      !this.visible ||
      this.cleanupOperationError ||
      !this.cleanupOperation ||
      !["pending", "running"].includes(this.cleanupOperation.status)
    )
      return;
    this.poll = setTimeout(() => {
      this.poll = null;
      void this.loadCleanupOperation();
    }, 2000);
  }
  private observeCleanup(id: string) {
    if (this.operationReadId === id) return false;
    if (this.poll) clearTimeout(this.poll);
    this.poll = null;
    this.lifecycle.cancel("cleanup-operation");
    this.lifecycle.cancel("cleanup-operation-request");
    this.operationReadId = id;
    this.cleanupRevision = null;
    this.operationReadAt = 0;
    this.cleanupOperation = null;
    this.cleanupOperationError = null;
    return true;
  }
  async loadCleanupOperation() {
    const id = this.cleanupTarget;
    if (!id) return;
    this.observeCleanup(id);
    if (this.lifecycle.isPending("cleanup-operation")) return;
    this.operationReadAt = Date.now();
    let current = () => false;
    await this.lifecycle.once("cleanup-operation", async () => {
      const r = this.lifecycle.begin("cleanup-operation-request");
      current = r.current;
      try {
        const operation = await this.api.cleanupOperation(
          this.id,
          id,
          r.signal,
        );
        if (operation.aggregate_id !== this.id || operation.id !== id)
          throw new ApiError(200, "invalid_response");
        if (r.current())
          runInAction(() => {
            this.cleanupOperation = operation;
            this.cleanupOperationError = null;
          });
      } catch (error) {
        if (r.current())
          runInAction(() => {
            this.cleanupOperationError = error;
          });
      }
    });
    if (!current() || this.operationReadId !== id) return;
    if (
      this.cleanupOperation?.status === "succeeded" &&
      this.observedSucceeded !== this.cleanupOperation.id
    ) {
      const refreshed =
        this.cleanupRevision?.id === this.cleanupOperation.id &&
        this.cleanupRevision.final;
      runInAction(() => {
        this.observedSucceeded = this.cleanupOperation!.id;
        this.cleanupPlan = null;
        if (!refreshed) {
          this.pointsClearVersion++;
          this.cleanupRefreshPending = true;
          this.clearHistoricalResult();
        }
      });
      if (!refreshed) {
        this.lifecycle.cancel("read");
        this.lifecycle.cancel("load");
        this.backgroundLoading = false;
        await this.load(true);
      }
    }
    this.scheduleCleanupRead();
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
            this.acceptCard(card);
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
    return this.canCommand && !this.card?.active_cleanup_id;
  }
  dispose() {
    if (this.commandBusy) this.commandOutcome = "unknown";
    this.saveCommandContext();
    this.setVisible(false);
    this.lifecycle.dispose();
    this.card = null;
    this.cardReadFailed = false;
    this.operators = [];
    this.operatorsError = null;
    this.clearHistoricalResult();
    this.error = null;
    this.success = "";
    this.mutationOutcome = null;
    this.commandContexts = null;
    this.commandIntent = null;
    this.commandReceipt = null;
    this.commandError = null;
    this.commandReadError = null;
    this.cleanupOperation = null;
    this.cleanupPlan = null;
  }
}
