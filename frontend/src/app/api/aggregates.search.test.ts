import { afterEach, expect, it, vi } from "vitest";
import { ApiClient } from "./client";
import { AggregatesApi } from "./aggregates";

afterEach(() => vi.restoreAllMocks());
it("preserves the old list defaults and encodes server search without local filtering", async () => {
  const client = new ApiClient();
  const request = vi
    .spyOn(client, "request")
    .mockResolvedValue({ items: [], total: 0 });
  const api = new AggregatesApi(client);
  await api.list();
  expect(request.mock.calls[0][0]).toBe("/aggregates?limit=50&offset=0");
  await api.list(0, undefined, {
    limit: 10,
    state: "active",
    q: "  Поиск %_\\'  ",
  });
  const url = new URL(request.mock.calls[1][0], "https://example.test");
  expect([...url.searchParams]).toEqual([
    ["limit", "10"],
    ["offset", "0"],
    ["q", "Поиск %_\\'"],
    ["state", "active"],
  ]);
  await api.list(50, undefined, { q: "   " });
  expect(request.mock.calls[2][0]).toBe("/aggregates?limit=50&offset=50");
});
