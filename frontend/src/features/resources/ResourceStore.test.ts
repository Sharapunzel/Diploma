import { describe, expect, it, vi } from "vitest";
import { ResourceStore } from "./ResourceStore";
import type { ResourcesApi } from "../../app/api/resources";

describe("resource list state", () => {
  it("ignores a late page after a newer request", async () => {
    let first!: (value: {
      items: { id: string }[];
      total: number;
      limit: number;
      offset: number;
    }) => void;
    const old = new Promise<{
      items: { id: string }[];
      total: number;
      limit: number;
      offset: number;
    }>((resolve) => {
      first = resolve;
    });
    const list = vi.fn().mockImplementation((offset: number) =>
      offset === 0
        ? old
        : Promise.resolve({
            items: [{ id: "new" }],
            total: 51,
            limit: 50,
            offset,
          }),
    );
    const store = new ResourceStore<{ id: string }>({} as ResourcesApi, list);
    const prior = store.load(0);
    await store.load(50);
    first({ items: [{ id: "old" }], total: 1, limit: 50, offset: 0 });
    await prior;
    expect(store.items.map((item) => item.id)).toEqual(["new"]);
    expect(store.offset).toBe(50);
  });
});
