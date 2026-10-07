import { makeAutoObservable, runInAction } from "mobx";

/** Owns abortable request contexts and keyed UI locks for one mounted feature. */
export class RequestLifecycle {
  pending = new Set<string>();
  private generations = new Map<string, number>();
  private controllers = new Map<string, AbortController>();
  private locks = new Map<string, symbol>();
  private disposed = false;

  constructor() {
    makeAutoObservable(
      this,
      {
        generations: false,
        controllers: false,
        locks: false,
        disposed: false,
      } as never,
      { autoBind: true },
    );
  }

  begin(context: string): { signal: AbortSignal; current: () => boolean } {
    this.cancel(context);
    const generation = (this.generations.get(context) ?? 0) + 1;
    const controller = new AbortController();
    this.generations.set(context, generation);
    this.controllers.set(context, controller);
    return {
      signal: controller.signal,
      current: () =>
        !this.disposed &&
        !controller.signal.aborted &&
        this.generations.get(context) === generation,
    };
  }

  cancel(context: string) {
    for (const [key, controller] of this.controllers)
      if (key === context || key.startsWith(`${context}:`)) {
        controller.abort();
        this.controllers.delete(key);
        this.generations.set(key, (this.generations.get(key) ?? 0) + 1);
      }
    for (const key of [...this.pending])
      if (key === context || key.startsWith(`${context}:`)) {
        this.pending.delete(key);
        this.locks.delete(key);
      }
  }

  async once<T>(key: string, task: () => Promise<T>): Promise<T | undefined> {
    if (this.disposed || this.pending.has(key)) return undefined;
    const token = Symbol(key);
    this.locks.set(key, token);
    runInAction(() => this.pending.add(key));
    try {
      return await task();
    } finally {
      if (this.locks.get(key) === token)
        runInAction(() => {
          this.locks.delete(key);
          this.pending.delete(key);
        });
    }
  }

  isPending(key: string) {
    return this.pending.has(key);
  }

  dispose() {
    this.disposed = true;
    for (const controller of this.controllers.values()) controller.abort();
    this.controllers.clear();
    this.generations.clear();
    this.locks.clear();
    this.pending.clear();
  }
}
