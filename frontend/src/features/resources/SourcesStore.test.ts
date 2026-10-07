import { describe, expect, it, vi } from "vitest";
import { SourcesStore } from "./SourcesStore";
import { RequestLifecycle } from "./RequestLifecycle";
import type { ResourcesApi } from "../../app/api/resources";

describe("SourcesStore", () => {
  it("retries selected normalizer metadata with a new GET", async () => {
    const normalizer = vi
      .fn()
      .mockRejectedValueOnce(new Error("offline"))
      .mockResolvedValueOnce({
        id: "normalizer",
        name: "Ready rule",
        version: 3,
        rule_status: "ready",
      });
    const store = new SourcesStore(
      { normalizer } as unknown as ResourcesApi,
      new RequestLifecycle(),
    );
    await store.readSelectedNormalizer("normalizer");
    expect(store.selectedNormalizerError).toBeTruthy();
    await store.retrySelectedNormalizer("normalizer");

    expect(normalizer).toHaveBeenCalledTimes(2);
    expect(store.knownNormalizers.normalizer?.name).toBe("Ready rule");
    expect(store.selectedNormalizerError).toBeNull();
  });

  it("does not publish a delayed action after its lifecycle is disposed", async () => {
    let release!: () => void;
    const sourceAction = vi.fn(
      () => new Promise<void>((resolve) => (release = resolve)),
    );
    const lifecycle = new RequestLifecycle();
    const store = new SourcesStore(
      { sourceAction } as unknown as ResourcesApi,
      lifecycle,
    );
    const action = store.action("kafka", "source", "enable");
    lifecycle.dispose();
    release();

    await expect(action).resolves.toEqual({ stale: true });
  });

  it("keeps a normalizer mutation failure separate from the selected-card retry", async () => {
    const normalizer = vi.fn().mockRejectedValue(new Error("metadata failed"));
    const assignNormalizer = vi
      .fn()
      .mockRejectedValue(new Error("write failed"));
    const store = new SourcesStore(
      { normalizer, assignNormalizer } as unknown as ResourcesApi,
      new RequestLifecycle(),
    );

    await store.readSelectedNormalizer("normalizer");
    const result = await store.assignNormalizer("source", "normalizer");

    expect(store.selectedNormalizerError).toBeTruthy();
    expect(result).toMatchObject({ error: expect.any(Error), stale: false });
    await store.retrySelectedNormalizer("normalizer");
    expect(normalizer).toHaveBeenCalledTimes(2);
    expect(assignNormalizer).toHaveBeenCalledTimes(1);
  });
});
