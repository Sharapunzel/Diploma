import { expect, it, vi } from "vitest";
import { AggregatesApi } from "./aggregates";
import { ApiClient } from "./client";

it("preserves the fifty-point default while allowing a three-row history page", async () => {
  const request = vi.fn().mockResolvedValue({ items: [], total: 0 });
  const api = new AggregatesApi({ request } as unknown as ApiClient);
  const signal = new AbortController().signal;
  await api.points("aggregate", 0, signal);
  expect(request).toHaveBeenLastCalledWith(
    "/aggregates/aggregate/points?limit=50&offset=0",
    expect.objectContaining({ signal }),
  );
  await api.points("aggregate", 3, signal, 3);
  expect(request).toHaveBeenLastCalledWith(
    "/aggregates/aggregate/points?limit=3&offset=3",
    expect.objectContaining({ signal }),
  );
});
