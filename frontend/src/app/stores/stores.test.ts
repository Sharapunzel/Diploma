import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiClient, ApiError } from "../api/client";
import type { Session } from "../api/dto";
import { SessionStore } from "./SessionStore";
import { ThemeStore } from "./ThemeStore";

export const authenticated: Session = {
  authenticated: true,
  user: { id: "1", username: "u", email: null, display_name: "User" },
  role: "renamed",
  permissions: ["events.read"],
  authentication_method: "local",
  csrf_token: "csrf",
};

const anonymous: Session = {
  authenticated: false,
  user: null,
  role: null,
  permissions: [],
  authentication_method: null,
  csrf_token: null,
};

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((done, fail) => {
    resolve = done;
    reject = fail;
  });
  return { promise, resolve, reject };
}

afterEach(() => {
  vi.unstubAllGlobals();
  localStorage.clear();
  document.documentElement.removeAttribute("data-theme");
});

describe("ThemeStore", () => {
  it("tracks OS only in system mode and removes listener", () => {
    let listener: (() => void) | undefined;
    const remove = vi.fn();
    const media = {
      matches: false,
      addEventListener: vi.fn((_event: string, callback: () => void) => {
        listener = callback;
      }),
      removeEventListener: remove,
    };
    vi.stubGlobal("matchMedia", () => media);
    const store = new ThemeStore();
    expect(store.choice).toBe("system");
    expect(document.documentElement.dataset.theme).toBe("light");
    media.matches = true;
    listener?.();
    expect(store.dark).toBe(true);
    expect(document.documentElement.dataset.theme).toBe("dark");
    store.choose("light");
    expect(document.documentElement.dataset.theme).toBe("light");
    listener?.();
    expect(store.dark).toBe(false);
    expect(localStorage.getItem("theme")).toBe("light");
    store.dispose();
    expect(remove).toHaveBeenCalledOnce();
  });

  it("survives invalid and unavailable storage", () => {
    localStorage.setItem("theme", "invalid");
    const invalid = new ThemeStore();
    expect(invalid.choice).toBe("system");
    invalid.dispose();
    vi.stubGlobal("localStorage", {
      getItem: () => {
        throw new Error("blocked");
      },
      setItem: () => {
        throw new Error("blocked");
      },
    });
    const blocked = new ThemeStore();
    blocked.choose("dark");
    expect(blocked.dark).toBe(true);
    blocked.dispose();
  });
});

describe("SessionStore", () => {
  it("aborts a private request and blocks its late success after anonymous refresh", async () => {
    const privateResponse = deferred<Response>();
    let privateSignal: AbortSignal | undefined;
    const fetchMock = vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
      if (String(input).endsWith("/auth/session"))
        return Promise.resolve(new Response(JSON.stringify(anonymous)));
      privateSignal = init?.signal ?? undefined;
      return privateResponse.promise;
    });
    vi.stubGlobal("fetch", fetchMock);
    const api = new ApiClient();
    const store = new SessionStore(api);
    store.accept(authenticated, store.version);

    const request = api.request<{ secret: string }>("/synthetic-private-data");
    expect(privateSignal?.aborted).toBe(false);
    await store.refresh();

    expect(privateSignal?.aborted).toBe(true);
    expect(store.state).toBe("anonymous");
    expect(store.data).toBeNull();
    privateResponse.resolve(new Response(JSON.stringify({ secret: "old" })));
    await expect(request).rejects.toMatchObject({ name: "AbortError" });
  });

  it("aborts old identity requests and ignores a late 401 after user switch", async () => {
    const body = deferred<string>();
    const bodyStarted = deferred<void>();
    let oldSignal: AbortSignal | undefined;
    const nextUser: Session = {
      ...authenticated,
      user: { ...authenticated.user!, id: "2", username: "next" },
      role: "new role",
      permissions: ["admin.read"],
      csrf_token: "next-csrf",
    };
    const oldResponse = {
      status: 401,
      ok: false,
      headers: new Headers({ "X-Request-ID": "late-401" }),
      text: () => {
        bodyStarted.resolve();
        return body.promise;
      },
    } as Response;
    const fetchMock = vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
      if (String(input).endsWith("/auth/session"))
        return Promise.resolve(new Response(JSON.stringify(nextUser)));
      oldSignal = init?.signal ?? undefined;
      return Promise.resolve(oldResponse);
    });
    vi.stubGlobal("fetch", fetchMock);
    const api = new ApiClient();
    const store = new SessionStore(api);
    store.accept(authenticated, store.version);

    const oldRequest = api.request("/synthetic-private-data");
    await bodyStarted.promise;
    await store.refresh();
    expect(oldSignal?.aborted).toBe(true);
    expect(store.data?.user?.id).toBe("2");
    expect(store.has("admin.read")).toBe(true);
    expect(store.has("events.read")).toBe(false);

    body.resolve(
      JSON.stringify({
        code: "expired",
        message: "expired",
        request_id: "late-401",
        details: null,
      }),
    );
    await expect(oldRequest).rejects.toMatchObject({ name: "AbortError" });
    expect(store.state).toBe("authenticated");
    expect(store.data?.user?.id).toBe("2");
  });

  it("preserves same-user requests on refresh and retains identity on network failure", async () => {
    const privateResponse = deferred<Response>();
    let privateSignal: AbortSignal | undefined;
    const fetchMock = vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
      if (String(input).endsWith("/auth/session"))
        return Promise.resolve(
          new Response(
            JSON.stringify({ ...authenticated, csrf_token: "refreshed" }),
          ),
        );
      privateSignal = init?.signal ?? undefined;
      return privateResponse.promise;
    });
    vi.stubGlobal("fetch", fetchMock);
    const api = new ApiClient();
    const store = new SessionStore(api);
    store.accept(authenticated, store.version);

    const request = api.request<{ current: boolean }>(
      "/synthetic-current-data",
    );
    await store.refresh();
    expect(privateSignal?.aborted).toBe(false);
    expect(store.data?.csrf_token).toBe("refreshed");
    privateResponse.resolve(new Response(JSON.stringify({ current: true })));
    await expect(request).resolves.toEqual({ current: true });

    vi.stubGlobal("fetch", vi.fn().mockRejectedValue(new Error("offline")));
    await store.refresh();
    expect(store.state).toBe("error");
    expect(store.data?.user?.id).toBe("1");
  });

  it("ignores stale refresh after logout and checks permissions rather than role name", async () => {
    let resolve!: (value: Session) => void;
    const api = new ApiClient();
    api.session = vi.fn(
      () =>
        new Promise<Session>((done) => {
          resolve = done;
        }),
    );
    api.logout = vi.fn().mockResolvedValue({ status: "ok" });
    const store = new SessionStore(api);
    const pending = store.refresh(true);
    expect(store.accept(authenticated, store.version)).toBe(true);
    expect(store.has("events.read")).toBe(true);
    expect(store.has("users.read")).toBe(false);
    await store.logout();
    resolve(authenticated);
    await pending;
    expect(store.state).toBe("anonymous");
    expect(store.data).toBeNull();
  });

  it("blocks refresh started while logout is pending and rejects stale login", async () => {
    let finishLogout!: (value: { status: string }) => void;
    const api = new ApiClient();
    api.logout = vi.fn(
      () =>
        new Promise<{ status: string }>((resolve) => {
          finishLogout = resolve;
        }),
    );
    api.session = vi.fn().mockResolvedValue(authenticated);
    const store = new SessionStore(api);
    store.accept(authenticated, store.version);
    const oldVersion = store.version;
    const leaving = store.logout();
    await store.refresh();
    expect(api.session).not.toHaveBeenCalled();
    finishLogout({ status: "ok" });
    await leaving;
    expect(store.accept(authenticated, oldVersion)).toBe(false);
    expect(store.state).toBe("anonymous");
  });

  it("expires on arbitrary 401, retains session on permission 403 and checks CSRF once", async () => {
    const api = new ApiClient();
    const store = new SessionStore(api);
    store.accept(authenticated, store.version);
    api.session = vi.fn().mockResolvedValue(authenticated);
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            code: "permission_denied",
            message: "forbidden",
            request_id: "r",
            details: null,
          }),
          { status: 403 },
        ),
      )
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            code: "csrf_invalid",
            message: "stale",
            request_id: "r",
            details: null,
          }),
          { status: 403 },
        ),
      )
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            code: "unauthorized",
            message: "expired",
            request_id: "r",
            details: null,
          }),
          { status: 401 },
        ),
      );
    vi.stubGlobal("fetch", fetchMock);
    await expect(api.request("/x")).rejects.toMatchObject({ status: 403 });
    expect(store.state).toBe("authenticated");
    await expect(api.request("/x", { method: "POST" })).rejects.toMatchObject({
      code: "csrf_invalid",
    });
    expect(api.session).toHaveBeenCalledTimes(1);
    expect(fetchMock).toHaveBeenCalledTimes(2);
    await expect(api.request("/x")).rejects.toMatchObject({ status: 401 });
    expect(store.state).toBe("anonymous");
    expect(store.data).toBeNull();
  });

  it("keeps session on logout CSRF error and exposes refreshed CSRF", async () => {
    const api = new ApiClient();
    const store = new SessionStore(api);
    store.accept(authenticated, store.version);
    api.session = vi
      .fn()
      .mockResolvedValue({ ...authenticated, csrf_token: "new-csrf" });
    api.logout = vi.fn().mockImplementation(async () => {
      await store.recheckCsrf();
      throw new ApiError(403, "csrf_invalid", "request-1");
    });
    await expect(store.logout()).rejects.toMatchObject({
      code: "csrf_invalid",
    });
    expect(store.state).toBe("authenticated");
    expect(store.data?.csrf_token).toBe("new-csrf");
  });
});
