import { afterEach, expect, it, vi } from "vitest";
import { ApiClient } from "./client";
import { NormalizersApi } from "./normalizers";
import { emptyRule } from "../../pages/normalizers/rules";

afterEach(() => vi.unstubAllGlobals());

it("sends exact create, versioned update, delete and preview payloads", async () => {
  const fetch = vi
    .fn()
    .mockImplementation(async (_path: string, options: RequestInit) =>
      options.method === "DELETE"
        ? new Response(null, { status: 204 })
        : new Response(JSON.stringify({ id: "n", version: 1 }), {
            status: 200,
          }),
    );
  vi.stubGlobal("fetch", fetch);
  const client = new ApiClient();
  client.setCsrf("csrf");
  const service = new NormalizersApi(client);
  const payload = { name: "Demo", description: null, rule: emptyRule() };
  await service.create(payload);
  await service.update("n", { ...payload, version: 4 });
  await service.preview(payload.rule, {
    timestamp: "2026-01-01T00:00:00Z",
    log: "synthetic log",
  });
  await service.remove("n", 4);
  expect(JSON.parse(fetch.mock.calls[0][1].body)).toEqual(payload);
  expect(JSON.parse(fetch.mock.calls[1][1].body)).toEqual({
    ...payload,
    version: 4,
  });
  expect(JSON.parse(fetch.mock.calls[2][1].body)).toEqual({
    rule: payload.rule,
    sample: { timestamp: "2026-01-01T00:00:00Z", log: "synthetic log" },
  });
  expect(fetch.mock.calls[3][0]).toBe("/api/v1/normalizers/n?version=4");
  expect(fetch.mock.calls.map((call) => call[1].method)).toEqual([
    "POST",
    "PATCH",
    "POST",
    "DELETE",
  ]);
});
