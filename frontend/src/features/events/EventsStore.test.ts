import { describe, expect, it, vi } from "vitest";
import { EventsStore } from "./EventsStore";
import type { EventsApi, EventSource } from "../../app/api/events";

const local: EventSource = {
  id: "local",
  source_type: "kafka",
  name: "Local",
  is_enabled: false,
} as EventSource;
const external: EventSource = {
  id: "external",
  source_type: "external",
  name: "External",
  is_enabled: true,
} as EventSource;
const disabled: EventSource = {
  id: "disabled",
  source_type: "external",
  name: "Disabled",
  is_enabled: false,
} as EventSource;

describe("EventsStore", () => {
  it("loads first source pages immediately and more on demand", async () => {
    const api = {
      sources: vi.fn(async (kind: string, offset: number) => ({
        items: kind === "kafka" ? [local] : offset ? [disabled] : [external],
        total: kind === "kafka" ? 1 : 2,
        limit: 1,
        offset,
      })),
    } as unknown as EventsApi;
    const store = new EventsStore(api);
    await store.loadSources();
    expect(store.sources.map((source) => source.id)).toEqual([
      "local",
      "external",
    ]);
    expect((api.sources as ReturnType<typeof vi.fn>).mock.calls.length).toBe(2);
    await store.loadMoreSources();
    expect((api.sources as ReturnType<typeof vi.fn>).mock.calls.length).toBe(3);
    expect(store.sources.map((source) => source.id)).toEqual([
      "local",
      "external",
    ]);
  });

  it("shows the first completed source page while the other source request is pending", async () => {
    let finishExternal!: (page: {
      items: EventSource[];
      total: number;
      limit: number;
      offset: number;
    }) => void;
    const api = {
      sources: vi.fn((kind: string) =>
        kind === "kafka"
          ? Promise.resolve({ items: [local], total: 1, limit: 50, offset: 0 })
          : new Promise((resolve) => {
              finishExternal = resolve;
            }),
      ),
    } as unknown as EventsApi;
    const store = new EventsStore(api);
    const loading = store.loadSources();
    await Promise.resolve();
    expect(store.sources.map((item) => item.id)).toEqual(["local"]);
    finishExternal({ items: [external], total: 1, limit: 50, offset: 0 });
    await loading;
    expect(store.sources.map((item) => item.id)).toEqual(["local", "external"]);
  });

  it("uses one seven-day window across cursor pages and resets it for a new query", async () => {
    const api = {
      fields: vi.fn(async () => ({
        items: [],
        total: 0,
        limit: 100,
        offset: 0,
      })),
      search: vi.fn(async () => ({
        items: [],
        has_more: true,
        next_cursor: "next",
      })),
    } as unknown as EventsApi;
    const store = new EventsStore(api);
    store.sources = [local];
    await store.select("local");
    const first = (api.search as ReturnType<typeof vi.fn>).mock.calls[0][1];
    expect(
      new Date(first.timestamp_to).getTime() -
        new Date(first.timestamp_from).getTime(),
    ).toBe(7 * 86400000);
    await store.search(1);
    const second = (api.search as ReturnType<typeof vi.fn>).mock.calls[1][1];
    expect(second).toMatchObject({
      timestamp_from: first.timestamp_from,
      timestamp_to: first.timestamp_to,
      cursor: "next",
    });
    store.applySearch(first.timestamp_from, first.timestamp_to, "asc", []);
    expect(store.cursors).toEqual([undefined]);
    expect(store.page).toBe(0);
  });

  it("ignores responses belonging to a previously selected source", async () => {
    let finish!: (value: {
      items: [];
      total: number;
      limit: number;
      offset: number;
    }) => void;
    const api = {
      fields: vi.fn((id: string) =>
        id === "local"
          ? new Promise((resolve) => {
              finish = resolve;
            })
          : Promise.resolve({ items: [], total: 0, limit: 100, offset: 0 }),
      ),
      search: vi.fn(async () => ({
        items: [],
        has_more: false,
        next_cursor: null,
      })),
    } as unknown as EventsApi;
    const store = new EventsStore(api);
    store.sources = [local, external];
    const old = store.select("local");
    await store.select("external");
    finish({ items: [], total: 0, limit: 100, offset: 0 });
    await old;
    expect(store.source?.id).toBe("external");
    expect(
      (api.search as ReturnType<typeof vi.fn>).mock.calls.map(
        (call) => call[0],
      ),
    ).toEqual(["external"]);
  });

  it("ignores pending source-page responses after leaving the page", async () => {
    const finishers: ((page: {
      items: EventSource[];
      total: number;
      limit: number;
      offset: number;
    }) => void)[] = [];
    const api = {
      sources: vi.fn(
        () =>
          new Promise((resolve) => {
            finishers.push(resolve);
          }),
      ),
    } as unknown as EventsApi;
    const store = new EventsStore(api);
    const loading = store.loadSources();
    store.dispose();
    finishers.forEach((finish) =>
      finish({ items: [local], total: 1, limit: 50, offset: 0 }),
    );
    await loading;
    expect(store.sources).toEqual([]);
  });

  it("keeps the table loading while the selected source catalog is pending", async () => {
    let finish!: (value: {
      items: [];
      total: number;
      limit: number;
      offset: number;
    }) => void;
    const api = {
      fields: vi.fn(
        () =>
          new Promise((resolve) => {
            finish = resolve;
          }),
      ),
      search: vi.fn(async () => ({
        items: [],
        has_more: false,
        next_cursor: null,
      })),
    } as unknown as EventsApi;
    const store = new EventsStore(api);
    store.sources = [local];
    const selected = store.select("local");
    expect(store.loading).toBe(true);
    finish({ items: [], total: 0, limit: 100, offset: 0 });
    await selected;
    expect(store.loading).toBe(false);
  });

  it("keeps the first page of one snapshot when moving back", async () => {
    const first = {
      items: [{ id: "old" }],
      has_more: true,
      next_cursor: "cursor",
    };
    const second = {
      items: [{ id: "next" }],
      has_more: false,
      next_cursor: null,
    };
    const api = {
      fields: vi.fn(async () => ({
        items: [],
        total: 0,
        limit: 100,
        offset: 0,
      })),
      search: vi
        .fn()
        .mockResolvedValueOnce(first)
        .mockResolvedValueOnce(second),
    } as unknown as EventsApi;
    const store = new EventsStore(api);
    store.sources = [external];
    await store.select("external");
    await store.search(1);
    await store.search(0);
    expect(store.results).toEqual(first);
    expect((api.search as ReturnType<typeof vi.fn>).mock.calls).toHaveLength(2);
    expect(
      (api.search as ReturnType<typeof vi.fn>).mock.calls[1][1].cursor,
    ).toBe("cursor");
    await store.refresh();
    expect((api.search as ReturnType<typeof vi.fn>).mock.calls).toHaveLength(3);
  });

  it("retries the catalog rather than searching after a field page fails", async () => {
    const api = {
      fields: vi
        .fn()
        .mockResolvedValueOnce({
          items: [{ name: "a", type: "keyword", operators: ["eq"] }],
          total: 2,
          limit: 1,
          offset: 0,
        })
        .mockRejectedValueOnce(new Error("catalog"))
        .mockResolvedValueOnce({
          items: [{ name: "a", type: "keyword", operators: ["eq"] }],
          total: 1,
          limit: 1,
          offset: 0,
        }),
      search: vi.fn(async () => ({
        items: [],
        has_more: false,
        next_cursor: null,
      })),
    } as unknown as EventsApi;
    const store = new EventsStore(api);
    store.sources = [local];
    await store.select("local");
    expect(store.catalogError).toBeTruthy();
    expect(store.catalogReady).toBe(false);
    expect(api.search).not.toHaveBeenCalled();
    await store.loadCatalog();
    expect(store.catalogError).toBeNull();
    expect(store.catalogReady).toBe(true);
    expect(api.search).toHaveBeenCalledTimes(1);
  });

  it("keeps the visible page on an invalid cursor and starts a fresh walk on refresh", async () => {
    const first = {
      items: [{ id: "old" }],
      has_more: true,
      next_cursor: "expired",
    };
    const api = {
      fields: vi.fn(async () => ({
        items: [],
        total: 0,
        limit: 100,
        offset: 0,
      })),
      search: vi
        .fn()
        .mockResolvedValueOnce(first)
        .mockRejectedValueOnce(new Error("cursor expired"))
        .mockResolvedValueOnce({
          items: [{ id: "fresh" }],
          has_more: false,
          next_cursor: null,
        }),
    } as unknown as EventsApi;
    const store = new EventsStore(api);
    store.sources = [external];
    await store.select("external");
    await store.search(1);
    expect(store.page).toBe(0);
    expect(store.results?.items[0].id).toBe("old");
    expect(store.error).toBeTruthy();
    await store.refresh();
    expect(store.results?.items[0].id).toBe("fresh");
    expect(
      (api.search as ReturnType<typeof vi.fn>).mock.calls[2][1].cursor,
    ).toBeUndefined();
  });
});
