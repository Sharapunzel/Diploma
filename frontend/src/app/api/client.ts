import {
  isErrorBody,
  isMethods,
  isSession,
  type Methods,
  type Session,
} from "./dto";

export class ApiError extends Error {
  constructor(
    public status: number,
    public code: string,
    public requestId: string | null = null,
    public serverMessage: string | null = null,
    public details: unknown = null,
  ) {
    super(code);
    this.name = "ApiError";
  }
}

type AuthEventHandlers = {
  unauthorized: () => void;
  csrfInvalid: () => Promise<void>;
};

type ApiRequestOptions = RequestInit & { ignoreUnauthorized?: boolean };

export class ApiClient {
  private csrf: string | null = null;
  private handlers: AuthEventHandlers | null = null;
  private pending = new Set<AbortController>();

  setCsrf(token: string | null) {
    this.csrf = token;
  }

  setAuthHandlers(handlers: AuthEventHandlers | null) {
    this.handlers = handlers;
  }

  cancelPending(except?: AbortSignal) {
    for (const controller of this.pending) {
      if (controller.signal !== except) controller.abort();
    }
  }

  async request<T>(path: string, options: ApiRequestOptions = {}): Promise<T> {
    const { ignoreUnauthorized = false, ...fetchOptions } = options;
    const method = (fetchOptions.method || "GET").toUpperCase();
    const headers = new Headers(fetchOptions.headers);
    if (!["GET", "HEAD", "OPTIONS"].includes(method) && this.csrf) {
      headers.set("X-CSRF-Token", this.csrf);
    }

    const controller = new AbortController();
    const onAbort = () => controller.abort();
    if (fetchOptions.signal?.aborted) controller.abort();
    else
      fetchOptions.signal?.addEventListener("abort", onAbort, { once: true });
    this.pending.add(controller);

    try {
      let response: Response;
      try {
        response = await fetch(`/api/v1${path}`, {
          ...fetchOptions,
          credentials: "same-origin",
          headers,
          signal: controller.signal,
        });
      } catch (error) {
        if (controller.signal.aborted) throw abortError();
        if (isAbort(error)) throw error;
        throw new ApiError(0, "network_error");
      }
      throwIfAborted(controller.signal);

      if (response.status === 204) {
        throwIfAborted(controller.signal);
        return undefined as T;
      }

      let payload: unknown = null;
      try {
        const body = await response.text();
        throwIfAborted(controller.signal);
        if (body) payload = JSON.parse(body) as unknown;
      } catch (error) {
        if (controller.signal.aborted) throw abortError();
        if (isAbort(error)) throw error;
        if (response.ok)
          throw new ApiError(
            response.status,
            "invalid_response",
            response.headers.get("X-Request-ID"),
          );
      }

      if (!response.ok) {
        throwIfAborted(controller.signal);
        const body = isErrorBody(payload) ? payload : null;
        const apiError = new ApiError(
          response.status,
          body?.code || "http_error",
          body?.request_id || response.headers.get("X-Request-ID"),
          body?.message || null,
          body?.details ?? null,
        );
        if (response.status === 401 && !ignoreUnauthorized)
          this.handlers?.unauthorized();
        if (
          apiError.code === "csrf_invalid" &&
          !["GET", "HEAD", "OPTIONS"].includes(method)
        ) {
          await this.handlers?.csrfInvalid();
          throwIfAborted(controller.signal);
        }
        throw apiError;
      }
      throwIfAborted(controller.signal);
      return payload as T;
    } finally {
      this.pending.delete(controller);
      fetchOptions.signal?.removeEventListener("abort", onAbort);
    }
  }

  async session(signal?: AbortSignal): Promise<Session> {
    const data = await this.request<unknown>("/auth/session", { signal });
    if (!isSession(data)) throw new ApiError(200, "invalid_response");
    return data;
  }

  async methods(signal?: AbortSignal): Promise<Methods> {
    const data = await this.request<unknown>("/auth/methods", { signal });
    if (!isMethods(data)) throw new ApiError(200, "invalid_response");
    return data;
  }

  async login(
    username: string,
    password: string,
    signal?: AbortSignal,
  ): Promise<Session> {
    const data = await this.request<unknown>("/auth/local/login", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ username, password }),
      signal,
      ignoreUnauthorized: true,
    });
    if (!isSession(data) || !data.authenticated)
      throw new ApiError(200, "invalid_response");
    return data;
  }

  logout(signal?: AbortSignal) {
    return this.request<{ status: string }>("/auth/logout", {
      method: "POST",
      signal,
    });
  }
}

function isAbort(error: unknown): boolean {
  return error instanceof DOMException && error.name === "AbortError";
}

function throwIfAborted(signal: AbortSignal) {
  if (signal.aborted) throw abortError();
}

function abortError() {
  return new DOMException("The operation was aborted", "AbortError");
}

export function errorText(error: unknown): string {
  if (!(error instanceof ApiError)) return "Не удалось выполнить запрос.";
  let message: string;
  if (error.code === "invalid_response")
    message = "Получен некорректный ответ сервера. Повторите попытку.";
  else if (error.code === "csrf_invalid")
    message = "Сессия изменилась. Повторите действие.";
  else if (error.code === "invalid_credentials")
    message = "Неверное имя пользователя или пароль.";
  else if (error.status === 0) message = "Нет соединения с сервером.";
  else if (error.status === 401)
    message = "Сессия истекла. Войдите в систему снова.";
  else if (error.status === 403) message = "Недостаточно прав для действия.";
  else if (error.status === 409)
    message = "Конфликт данных. Обновите страницу.";
  else if (error.status === 422) message = "Проверьте введённые данные.";
  else if (error.status >= 500) message = "Сервер временно недоступен.";
  else message = "Не удалось выполнить запрос.";
  return message;
}
