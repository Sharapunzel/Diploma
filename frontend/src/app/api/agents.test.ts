import { describe, expect, it, vi } from "vitest";
import type { ApiClient } from "./client";
import { AgentsApi } from "./agents";

describe("AgentsApi security contract", () => {
  it("keeps a caller-supplied idempotency key on provisioning and prevents claim caching", async () => {
    const request = vi.fn(async () => ({}));
    const api = new AgentsApi({ request } as unknown as ApiClient);
    const key = "11111111-1111-4111-8111-111111111111";
    await api.create(
      {
        name: "edge",
        description: null,
        topics: [
          {
            connection_id: "connection",
            topic_name: "edge.logs",
            action: "create",
          },
        ],
      },
      key,
    );
    expect(request).toHaveBeenCalledWith(
      "/agents",
      expect.objectContaining({
        method: "POST",
        cache: "no-store",
        headers: expect.objectContaining({ "Idempotency-Key": key }),
      }),
    );
    await api.claim("agent");
    expect(request).toHaveBeenCalledWith(
      "/agents/agent/credentials/claim",
      expect.objectContaining({ method: "POST", cache: "no-store" }),
    );
    await api.transport();
    expect(request).toHaveBeenCalledWith(
      "/agents/transport",
      expect.objectContaining({ cache: "no-store" }),
    );
  });
});
