import { describe, expect, it, vi } from "vitest";
import { DiagnosticsStore } from "./DiagnosticsStore";
import type {
  DiagnosticsApi,
  SourceState,
  SourceTopology,
} from "../../app/api/diagnostics";

const source = (id: string): SourceState => ({
  source_id: id,
  source_name: id,
  connection_id: null,
  connection_name: null,
  topic: id,
  topic_id: null,
  is_enabled: true,
  is_archived: false,
  worker_status: "running",
  reason_code: null,
  reason_description: null,
  captured_at: "2026-01-01T00:00:00Z",
  partitions: [],
});
const topology = (id: string): SourceTopology => ({
  source_id: id,
  normalizer_id: null,
  normalizer_name: null,
  agent_id: null,
  agent_name: null,
  agent_state: null,
  assignment_state: null,
});
describe("DiagnosticsStore", () => {
  it("keeps the newest source after a fast switch", async () => {
    let completeFirst!: (value: SourceState) => void;
    const api = {
      source: vi.fn((id: string) =>
        id === "first"
          ? new Promise<SourceState>((resolve) => {
              completeFirst = resolve;
            })
          : Promise.resolve(source(id)),
      ),
      topology: vi.fn((id: string) => Promise.resolve(topology(id))),
    } as unknown as DiagnosticsApi;
    const store = new DiagnosticsStore(api);
    store.select("first");
    store.select("second");
    completeFirst(source("first"));
    await vi.waitFor(() => expect(store.source?.source_id).toBe("second"));
    expect(store.topology?.source_id).toBe("second");
    store.dispose();
  });
  it("does not retain another source's error history while reading a new page", async () => {
    let complete!: (value: {
      items: [];
      total: number;
      limit: number;
      offset: number;
    }) => void;
    const api = {
      errors: vi.fn(
        () =>
          new Promise((resolve) => {
            complete = resolve;
          }),
      ),
    } as unknown as DiagnosticsApi;
    const store = new DiagnosticsStore(api);
    store.errors = { items: [], total: 9, limit: 20, offset: 0 };
    const request = store.loadErrors("second", undefined, "", "", 0);
    expect(store.errors).toBeNull();
    complete({ items: [], total: 0, limit: 20, offset: 0 });
    await request;
    expect(store.errors?.total).toBe(0);
    store.dispose();
  });
});
