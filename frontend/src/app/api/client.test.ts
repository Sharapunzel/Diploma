import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiClient, ApiError, errorText } from "./client";

afterEach(() => vi.unstubAllGlobals());

describe("ApiClient", () => {
  it("uses same-origin cookies and CSRF for all unsafe methods, including search", async () => {
    const fetchMock = vi
      .fn()
      .mockImplementation(() => Promise.resolve(new Response("{}")));
    vi.stubGlobal("fetch", fetchMock);
    const client = new ApiClient();
    client.setCsrf("token");
    for (const method of ["POST", "PUT", "PATCH", "DELETE"]) {
      await client.request("/events/search", { method, body: "{}" });
    }
    expect(fetchMock).toHaveBeenCalledTimes(4);
    for (const [url, options] of fetchMock.mock.calls) {
      expect(url).toBe("/api/v1/events/search");
      expect(options.credentials).toBe("same-origin");
      expect(options.headers.get("X-CSRF-Token")).toBe("token");
    }
  });

  it("keeps caller media type for non-JSON body", async () => {
    const fetchMock = vi.fn().mockResolvedValue(new Response("{}"));
    vi.stubGlobal("fetch", fetchMock);
    await new ApiClient().request("/upload", {
      method: "POST",
      headers: { "Content-Type": "application/x-pem-file" },
      body: "PEM data",
    });
    expect(fetchMock.mock.calls[0][1].headers.get("Content-Type")).toBe(
      "application/x-pem-file",
    );
  });

  it("handles 204, preserves server error fields and does not retry changes", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(new Response(null, { status: 204 }))
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            code: "csrf_invalid",
            message: "Private backend detail",
            request_id: "req-1",
            details: { field: "x" },
          }),
          { status: 403 },
        ),
      );
    vi.stubGlobal("fetch", fetchMock);
    const client = new ApiClient();
    expect(
      await client.request("/auth/logout", { method: "POST" }),
    ).toBeUndefined();
    const error = await client
      .request("/auth/logout", { method: "POST" })
      .catch((value: unknown) => value);
    expect(error).toMatchObject({
      status: 403,
      code: "csrf_invalid",
      requestId: "req-1",
      serverMessage: "Private backend detail",
      details: { field: "x" },
    });
    expect(errorText(error)).not.toContain("Private backend detail");
    expect(errorText(new ApiError(401, "invalid_credentials"))).toBe(
      "Неверное имя пользователя или пароль.",
    );
    expect(errorText(new ApiError(401, "expired"))).toBe(
      "Сессия истекла. Войдите в систему снова.",
    );
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it.each([401, 403, 409, 422, 503])(
    "keeps HTTP status %s and request ID",
    async (status) => {
      vi.stubGlobal(
        "fetch",
        vi.fn().mockResolvedValue(
          new Response("proxy error", {
            status,
            headers: { "X-Request-ID": "proxy-id" },
          }),
        ),
      );
      await expect(new ApiClient().request("/x")).rejects.toMatchObject({
        status,
        code: "http_error",
        requestId: "proxy-id",
      });
    },
  );

  it("rejects malformed successful auth responses", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(new Response("not json"))
      .mockResolvedValueOnce(new Response("{}"))
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ authenticated: false })),
      );
    vi.stubGlobal("fetch", fetchMock);
    const client = new ApiClient();
    await expect(client.methods()).rejects.toMatchObject({
      code: "invalid_response",
    });
    await expect(client.methods()).rejects.toMatchObject({
      code: "invalid_response",
    });
    await expect(client.session()).rejects.toMatchObject({
      code: "invalid_response",
    });
  });

  it("distinguishes network failures and abort during fetch or body read", async () => {
    const client = new ApiClient();
    vi.stubGlobal("fetch", vi.fn().mockRejectedValue(new Error("offline")));
    await expect(client.session()).rejects.toMatchObject({
      status: 0,
      code: "network_error",
    });
    vi.stubGlobal(
      "fetch",
      vi.fn().mockRejectedValue(new DOMException("aborted", "AbortError")),
    );
    await expect(client.session()).rejects.toMatchObject({
      name: "AbortError",
    });
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        status: 200,
        ok: true,
        text: () => Promise.reject(new DOMException("aborted", "AbortError")),
      }),
    );
    await expect(client.session()).rejects.toMatchObject({
      name: "AbortError",
    });
    expect(ApiError).toBeDefined();
  });

  it("localizes the exact resource domain error codes", () => {
    const codes = [
      "source_normalizer_required",
      "source_topic_conflict",
      "topic_identity_changed",
      "topic_identity_history_unknown",
      "source_configuration_changed",
      "external_configuration_changed",
    ];
    for (const code of codes) {
      const text = errorText(new ApiError(409, code));
      expect(text).not.toBe("Конфликт данных. Обновите страницу.");
      expect(text.length).toBeGreaterThan(20);
    }
    expect(errorText(new ApiError(409, "source_exists"))).toContain(
      "зарегистрированные источники",
    );
    expect(errorText(new ApiError(403, "indexer_access_denied"))).toContain(
      "Индексер",
    );
  });
});
