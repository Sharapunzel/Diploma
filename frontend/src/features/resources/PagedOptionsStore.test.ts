import { describe, expect, it } from "vitest";
import { PagedOptionsStore } from "./PagedOptionsStore";
import { RequestLifecycle } from "./RequestLifecycle";

type Item = { id: string; name: string };
const page = (items: Item[], total = items.length, offset = 0) => ({
  items,
  total,
  limit: 50,
  offset,
});

describe("PagedOptionsStore", () => {
  it("ignores a late page after the form context changes", async () => {
    const lifecycle = new RequestLifecycle();
    const store = new PagedOptionsStore<Item>(lifecycle, (item) => item.id);
    let release!: (value: ReturnType<typeof page>) => void;
    store.configure(
      "options:kafka",
      () => new Promise((resolve) => (release = resolve)),
    );
    const old = store.load(50);
    store.configure("options:external", async () =>
      page([{ id: "external", name: "Current" }]),
    );
    await store.load();
    release(page([{ id: "kafka", name: "Old" }], 51, 50));
    await old;

    expect(store.items).toEqual([{ id: "external", name: "Current" }]);
    expect(store.loading).toBe(false);
  });

  it("deduplicates load-more and keeps page totals independent", async () => {
    const lifecycle = new RequestLifecycle();
    const store = new PagedOptionsStore<Item>(lifecycle, (item) => item.id);
    let calls = 0;
    store.configure("options", async (offset) => {
      calls += 1;
      return page([{ id: String(offset), name: String(offset) }], 101, offset);
    });
    await Promise.all([store.loadMore(), store.loadMore()]);

    expect(calls).toBe(1);
    expect(store.items).toHaveLength(1);
    expect(store.total).toBe(101);
  });

  it("retries the failed page instead of replacing the last successful one", async () => {
    const lifecycle = new RequestLifecycle();
    const store = new PagedOptionsStore<Item>(lifecycle, (item) => item.id);
    const offsets: number[] = [];
    let fail = true;
    store.configure("options", async (offset) => {
      offsets.push(offset);
      if (offset === 50 && fail) {
        fail = false;
        throw new Error("temporary");
      }
      return page([{ id: String(offset), name: String(offset) }], 101, offset);
    });

    await store.load();
    await store.loadMore();
    expect(store.failedOffset).toBe(50);
    await store.retry();

    expect(offsets).toEqual([0, 50, 50]);
    expect(store.items.map((item) => item.id)).toEqual(["0", "50"]);
  });
});
