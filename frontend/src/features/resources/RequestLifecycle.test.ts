import { describe, expect, it, vi } from "vitest";
import { RequestLifecycle } from "./RequestLifecycle";

describe("RequestLifecycle", () => {
  it("invalidates the previous context and protects late completion", () => {
    const lifecycle = new RequestLifecycle();
    const first = lifecycle.begin("options");
    const second = lifecycle.begin("options");

    expect(first.signal.aborted).toBe(true);
    expect(first.current()).toBe(false);
    expect(second.current()).toBe(true);
  });

  it("deduplicates one operation while independent entities continue", async () => {
    const lifecycle = new RequestLifecycle();
    let release!: () => void;
    const waiting = new Promise<void>((resolve) => (release = resolve));
    const firstTask = vi.fn(() => waiting);
    const duplicateTask = vi.fn(async () => undefined);
    const otherTask = vi.fn(async () => "done");

    const first = lifecycle.once("source:a", firstTask);
    expect(await lifecycle.once("source:a", duplicateTask)).toBeUndefined();
    expect(await lifecycle.once("source:b", otherTask)).toBe("done");
    expect(lifecycle.isPending("source:a")).toBe(true);
    expect(lifecycle.isPending("source:b")).toBe(false);
    release();
    await first;

    expect(firstTask).toHaveBeenCalledOnce();
    expect(duplicateTask).not.toHaveBeenCalled();
    expect(otherTask).toHaveBeenCalledOnce();
    expect(lifecycle.isPending("source:a")).toBe(false);
  });

  it("aborts all contexts and clears pending on dispose", async () => {
    const lifecycle = new RequestLifecycle();
    const request = lifecycle.begin("normalizers");
    void lifecycle.once("source:a", () => new Promise(() => undefined));
    lifecycle.dispose();

    expect(request.signal.aborted).toBe(true);
    expect(request.current()).toBe(false);
    expect(lifecycle.pending.size).toBe(0);
  });

  it("does not let an old finally unlock a newer operation after cancel", async () => {
    const lifecycle = new RequestLifecycle();
    let releaseOld!: () => void;
    let releaseNew!: () => void;
    const old = lifecycle.once(
      "options:page",
      () => new Promise<void>((resolve) => (releaseOld = resolve)),
    );
    lifecycle.cancel("options");
    const current = lifecycle.once(
      "options:page",
      () => new Promise<void>((resolve) => (releaseNew = resolve)),
    );

    releaseOld();
    await old;
    expect(lifecycle.isPending("options:page")).toBe(true);
    releaseNew();
    await current;
    expect(lifecycle.isPending("options:page")).toBe(false);
  });
});
