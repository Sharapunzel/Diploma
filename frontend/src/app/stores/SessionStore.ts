import { makeAutoObservable, runInAction } from "mobx";
import { ApiClient, ApiError } from "../api/client";
import type { Session } from "../api/dto";

export type SessionState = "loading" | "anonymous" | "authenticated" | "error";

export class SessionStore {
  state: SessionState = "loading";
  data: Session | null = null;
  error: unknown = null;
  loggingOut = false;
  private generation = 0;
  private identityVersion = 0;
  private controller: AbortController | null = null;
  private csrfCheck: Promise<void> | null = null;
  private disposed = false;

  constructor(public api: ApiClient) {
    makeAutoObservable(
      this,
      {
        api: false,
        controller: false,
        csrfCheck: false,
        disposed: false,
      } as never,
      { autoBind: true },
    );
    api.setAuthHandlers({
      unauthorized: this.expire,
      csrfInvalid: this.recheckCsrf,
    });
  }

  get version() {
    return this.identityVersion;
  }

  async refresh(showLoading = false, duringLogout = false): Promise<void> {
    if (this.disposed || (this.loggingOut && !duringLogout)) return;
    let generation = ++this.generation;
    this.controller?.abort();
    const controller = new AbortController();
    this.controller = controller;
    if (showLoading) this.state = "loading";
    try {
      const data = await this.api.session(controller.signal);
      if (generation !== this.generation || this.disposed) return;
      const next = data.authenticated ? data : null;
      const identityChanged =
        this.data?.user?.id !== next?.user?.id || !!this.data !== !!next;
      if (identityChanged) {
        this.identityVersion++;
        generation = ++this.generation;
        this.api.cancelPending(controller.signal);
      }
      runInAction(() => {
        this.data = next;
        this.state = next ? "authenticated" : "anonymous";
        this.error = null;
        this.api.setCsrf(next?.csrf_token || null);
      });
    } catch (error) {
      if (generation !== this.generation || this.disposed || isAbort(error))
        return;
      runInAction(() => {
        this.error = error;
        this.state = "error";
      });
    }
  }

  accept(data: Session, expectedVersion: number): boolean {
    if (
      this.disposed ||
      this.loggingOut ||
      expectedVersion !== this.identityVersion
    )
      return false;
    this.generation++;
    this.identityVersion++;
    this.controller?.abort();
    this.api.cancelPending();
    this.data = data;
    this.api.setCsrf(data.csrf_token);
    this.state = "authenticated";
    this.error = null;
    return true;
  }

  expire() {
    if (this.disposed || (!this.data && this.state !== "authenticated")) return;
    this.clear();
  }

  private clear() {
    this.generation++;
    this.identityVersion++;
    this.controller?.abort();
    this.api.cancelPending();
    this.data = null;
    this.error = null;
    this.api.setCsrf(null);
    this.state = "anonymous";
  }

  async recheckCsrf(): Promise<void> {
    if (this.disposed) return;
    if (!this.csrfCheck) {
      this.csrfCheck = this.refresh(false, true).finally(() => {
        this.csrfCheck = null;
      });
    }
    await this.csrfCheck;
  }

  async logout(): Promise<void> {
    if (this.loggingOut) return;
    this.loggingOut = true;
    this.generation++;
    this.identityVersion++;
    this.controller?.abort();
    this.api.cancelPending();
    try {
      await this.api.logout();
      runInAction(() => this.clear());
    } catch (error) {
      if (error instanceof ApiError && error.status === 401) {
        runInAction(() => this.clear());
      } else {
        throw error;
      }
    } finally {
      runInAction(() => {
        this.loggingOut = false;
      });
    }
  }

  has(permission: string): boolean {
    return this.data?.permissions.includes(permission) || false;
  }

  dispose() {
    this.disposed = true;
    this.generation++;
    this.controller?.abort();
    this.api.cancelPending();
    this.api.setAuthHandlers(null);
  }
}

function isAbort(error: unknown): boolean {
  return error instanceof DOMException && error.name === "AbortError";
}
