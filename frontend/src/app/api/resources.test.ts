import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiClient, ApiError } from "./client";
import {
  externalTargetPayload,
  matchPattern,
  ResourcesApi,
  targetField,
  validationFields,
} from "./resources";

afterEach(() => vi.unstubAllGlobals());

describe("resource contracts", () => {
  it("forms each external target payload with exactly one target field", () => {
    expect(externalTargetPayload("index", "logs")).toEqual({
      target_type: "index",
      index_name: "logs",
    });
    expect(externalTargetPayload("index_pattern", "logs-*")).toEqual({
      target_type: "index_pattern",
      index_pattern: "logs-*",
    });
    expect(externalTargetPayload("data_stream", "security")).toEqual({
      target_type: "data_stream",
      data_stream_name: "security",
    });
    expect(externalTargetPayload("data_stream_pattern", "security-*")).toEqual({
      target_type: "data_stream_pattern",
      data_stream_pattern: "security-*",
    });
  });
  it("sends raw PEM with the required media type and accepts 204", async () => {
    const fetch = vi
      .fn()
      .mockResolvedValue(new Response(null, { status: 204 }));
    vi.stubGlobal("fetch", fetch);
    const api = new ApiClient();
    api.setCsrf("csrf");
    await new ResourcesApi(api).uploadCa(
      "id",
      "-----BEGIN CERTIFICATE-----\nabc\n-----END CERTIFICATE-----",
    );
    expect(fetch).toHaveBeenCalledWith(
      "/api/v1/external-connections/id/ca",
      expect.objectContaining({
        method: "PUT",
        body: "-----BEGIN CERTIFICATE-----\nabc\n-----END CERTIFICATE-----",
      }),
    );
    const headers = new Headers(fetch.mock.calls[0][1].headers);
    expect(headers.get("Content-Type")).toBe("application/x-pem-file");
    expect(headers.get("X-CSRF-Token")).toBe("csrf");
  });

  it("keeps discovery kinds and pages distinct", async () => {
    const fetch = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          items: ["logs"],
          total: 101,
          limit: 50,
          offset: 50,
        }),
        { status: 200 },
      ),
    );
    vi.stubGlobal("fetch", fetch);
    const result = await new ResourcesApi(new ApiClient()).discovered(
      "id",
      "data-streams",
      50,
    );
    expect(fetch.mock.calls[0][0]).toBe(
      "/api/v1/external-connections/id/data-streams?limit=50&offset=50",
    );
    expect(result.total).toBe(101);
  });

  it("sends a target change with only its selected target field", async () => {
    const fetch = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          id: "source",
          source_type: "external",
          name: "Source",
          external_connection_id: "connection",
          target_type: "data_stream_pattern",
          index_name: null,
          index_pattern: null,
          data_stream_name: null,
          data_stream_pattern: "logs-*",
          is_enabled: false,
        }),
        { status: 200 },
      ),
    );
    vi.stubGlobal("fetch", fetch);

    await new ResourcesApi(new ApiClient()).patchExternalSource("source", {
      target_type: "data_stream_pattern",
      data_stream_pattern: "logs-*",
    });

    expect(fetch.mock.calls[0][0]).toBe("/api/v1/external-sources/source");
    expect(JSON.parse(String(fetch.mock.calls[0][1].body))).toEqual({
      target_type: "data_stream_pattern",
      data_stream_pattern: "logs-*",
    });
  });

  it.each([
    ["index", "index_name", "new-index"],
    ["index_pattern", "index_pattern", "new-*"],
    ["data_stream", "data_stream_name", "new-stream"],
    ["data_stream_pattern", "data_stream_pattern", "new-stream-*"],
  ] as const)(
    "PATCHes a changed %s target with only its matching field",
    async (targetType, field, value) => {
      const fetch = vi
        .fn()
        .mockResolvedValue(
          new Response(JSON.stringify({ id: "source" }), { status: 200 }),
        );
      vi.stubGlobal("fetch", fetch);
      const payload = externalTargetPayload(targetType, value);
      await new ResourcesApi(new ApiClient()).patchExternalSource("source", {
        ...payload,
      });

      expect(JSON.parse(String(fetch.mock.calls[0][1].body))).toEqual({
        target_type: targetType,
        [field]: value,
      });
    },
  );

  it("matches masks only against names supplied for the right target type", () => {
    expect(targetField.data_stream_pattern).toBe("data_stream_pattern");
    expect(matchPattern("logs-?", "logs-a")).toBe(true);
    expect(matchPattern("logs-?", "logs-aa")).toBe(false);
    expect(matchPattern("logs.*", "logsXone")).toBe(false);
  });

  it("binds server validation to known fields without displaying server detail", () => {
    const error = new ApiError(422, "validation_error", "request", null, {
      errors: [
        { loc: ["body", "index_pattern"], msg: "internal detail" },
        { loc: ["body", "unknown"], msg: "hidden" },
      ],
    });
    expect(validationFields(error, { index_pattern: "target" })).toEqual([
      { name: "target", errors: ["Проверьте значение поля."] },
    ]);
  });
});
