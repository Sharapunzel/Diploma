import { describe, expect, it, vi } from "vitest";
import { ConnectionsStore } from "./ConnectionsStore";
import { RequestLifecycle } from "./RequestLifecycle";
import type { ResourcesApi } from "../../app/api/resources";

describe("ConnectionsStore", () => {
  it("shows internal topics instead of appending them to regular topics", async () => {
    const allTopics = [
      { name: "application-events", partition_count: 3, is_registered: false },
      { name: "__consumer_offsets", partition_count: 50, is_registered: false },
    ];
    const api = {
      topics: vi.fn(async (_id: string, includeInternal: boolean) => ({
        items: includeInternal ? allTopics : [allTopics[0]],
        total: includeInternal ? allTopics.length : 1,
      })),
    } as unknown as ResourcesApi;
    const store = new ConnectionsStore(api, new RequestLifecycle());
    store.detail = {
      kind: "kafka",
      item: {
        id: "connection",
        name: "Kafka",
        bootstrap_servers: ["kafka:9092"],
        security_protocol: "PLAINTEXT",
        created_at: "",
        updated_at: "",
      },
    };

    await store.refresh(true);
    expect(store.discovery.names).toEqual(["application-events"]);
    store.setIncludeInternal(true, true);
    await expect
      .poll(() => store.discovery.names)
      .toEqual(["__consumer_offsets"]);
    expect(store.discovery.total).toBe(1);
    expect(Object.keys(store.topicStatus)).toEqual(["__consumer_offsets"]);
  });

  it("keeps only the current discovery kind after a late page", async () => {
    let release!: (value: {
      items: string[];
      total: number;
      limit: number;
      offset: number;
    }) => void;
    const api = {
      discovered: vi.fn((_: string, kind: string) =>
        kind === "indices"
          ? new Promise((resolve) => (release = resolve))
          : Promise.resolve({
              items: ["stream"],
              total: 1,
              limit: 50,
              offset: 0,
            }),
      ),
    } as unknown as ResourcesApi;
    const store = new ConnectionsStore(api, new RequestLifecycle());
    const item = {
      id: "connection",
      name: "Indexer",
      base_url: "https://indexer",
      username: "reader",
      has_password: true,
      has_ca: false,
      created_at: "",
      updated_at: "",
    };

    store.openDetail("external", item, true, false);
    store.setDiscoveryKind("data-streams", true);
    await Promise.resolve();
    release({ items: ["old-index"], total: 1, limit: 50, offset: 0 });
    await Promise.resolve();

    expect(store.discovery.names).toEqual(["stream"]);
  });

  it("marks an active-source precheck stale when its context is cancelled", async () => {
    let release!: (page: {
      items: never[];
      total: number;
      limit: number;
      offset: number;
    }) => void;
    const api = {
      externalSources: vi.fn(
        () => new Promise((resolve) => (release = resolve)),
      ),
    } as unknown as ResourcesApi;
    const lifecycle = new RequestLifecycle();
    const store = new ConnectionsStore(api, lifecycle);
    const resultPromise = store.activeExternalCount("connection", true);
    lifecycle.cancel("connection-active-sources");
    release({ items: [], total: 0, limit: 50, offset: 0 });

    const result = await resultPromise;
    expect(result.stale).toBe(true);
    expect(result.count).toBeNull();
  });
});
