import { expect, test } from "@playwright/test";
import { Deadline, withPhaseEnd } from "./deadline";

test("auto720 is bounded by current body and parent global remainder", () => {
  let now = 10_000;
  const global = new Deadline(120_000, () => now);
  now += 90_000;
  const auto = global.child(720_000);
  expect(auto.remaining()).toBe(30_000);
  const generation = auto.child(300_000);
  now += 12_000;
  expect(generation.remaining()).toBe(18_000);
  expect(auto.remaining()).toBe(18_000);
  now += 18_000;
  expect(() => generation.remaining()).toThrow("budget exhausted");
  expect(() => auto.remaining()).toThrow("budget exhausted");
});

test("generation starts before the sole jump and progress cannot renew300", async () => {
  let now = 500_000;
  const body = new Deadline(720_000, () => now);
  const generation = body.child(300_000);
  await generation.run(async () => {
    now += 10_000; // Clock-file write and its synchronous completion.
    expect(generation.remaining()).toBe(290_000);
    now += 200_000; // Observed progress does not create another deadline.
    expect(generation.remaining()).toBe(90_000);
  });
  now += 90_000;
  expect(() => generation.remaining()).toThrow("budget exhausted");
  expect(body.remaining()).toBe(420_000);
});

test("both resumed models share300 including the first start request and UI wait", async () => {
  let now = 0;
  const body = new Deadline(720_000, () => now);
  const resume = body.child(300_000);
  const firstStart = resume.child(45_000);
  await firstStart.run(async () => {
    now += 10_000; // Navigation and explicit start request.
    now += 20_000; // UI response and running state.
    expect(firstStart.remaining()).toBe(15_000);
  });
  now += 250_000; // Ordinary catchup consumed this part of the same tail.
  expect(resume.remaining()).toBe(20_000);
  expect(resume.child(45_000).remaining()).toBe(20_000);
  now += 20_000;
  expect(() => resume.remaining()).toThrow("budget exhausted");
});

test("a phase cannot outlive the remaining auto body", () => {
  let now = 0;
  const auto = new Deadline(720_000, () => now);
  now = 700_000;
  const resume = auto.child(300_000);
  expect(resume.remaining()).toBe(20_000);
  expect(resume.child(45_000).remaining()).toBe(20_000);
});

test("late successful work fails before more assertions and still reaches finally", async () => {
  let now = 0;
  const phase = new Deadline(300_000, () => now);
  const events: string[] = [];
  async function scenario() {
    try {
      await phase.run(async () => {
        now = 300_000;
        return "late ready";
      });
      events.push("continued assertions");
    } finally {
      events.push("diagnostic", "cleanup");
    }
  }
  await expect(scenario()).rejects.toThrow("budget exhausted");
  expect(events).toEqual(["diagnostic", "cleanup"]);
});

test("an exhausted phase never starts more work", async () => {
  let now = 0;
  const phase = new Deadline(300_000, () => now);
  now = 300_001;
  let calls = 0;
  await expect(
    phase.run(async () => {
      calls += 1;
    }),
  ).rejects.toThrow("budget exhausted");
  expect(calls).toBe(0);
  expect(phase.snapshot()).toEqual({ elapsed_ms: 300_001, remaining_ms: 0 });
});

test("phase end is exactly once before resume after successful work", async () => {
  const events: string[] = [];
  const result = await withPhaseEnd(
    async () => {
      events.push("begin+jump", "work");
      return 42;
    },
    (outcome) => events.push(`end:${outcome}`),
    () => events.push("unavailable"),
  );
  events.push("resume");
  expect(result).toBe(42);
  expect(events).toEqual(["begin+jump", "work", "end:complete", "resume"]);
});

test("failed work retains the exact assertion when end marker and diagnostic emit both fail", async () => {
  const assertion = new Error("original generation assertion");
  const events: string[] = [];
  const result = withPhaseEnd(
    async () => {
      throw assertion;
    },
    (outcome) => {
      events.push(`end:${outcome}`);
      throw new Error("marker write failed");
    },
    () => {
      events.push("unavailable");
      throw new Error("emit failed");
    },
  );
  try {
    await result;
    events.push("resume");
  } catch (error) {
    expect(error).toBe(assertion);
  } finally {
    events.push("cleanup");
  }
  expect(events).toEqual(["end:failed", "unavailable", "cleanup"]);
});

test("end failure preserves successful result without extending a spent deadline", async () => {
  let now = 0;
  const phase = new Deadline(300_000, () => now);
  const errors: unknown[] = [];
  const marker = new Error("marker unavailable");
  const result = await withPhaseEnd(
    () =>
      phase.run(async () => {
        now = 299_999;
        return "ready";
      }),
    () => {
      now = 300_001;
      throw marker;
    },
    (error) => errors.push(error),
  );
  expect(result).toBe("ready");
  expect(errors).toEqual([marker]);
  expect(() => phase.remaining()).toThrow("budget exhausted");
});

test("unfinished work has no fictitious end marker", async () => {
  let release!: () => void;
  const pending = new Promise<void>((resolve) => {
    release = resolve;
  });
  const outcomes: string[] = [];
  const result = withPhaseEnd(
    () => pending,
    (outcome) => outcomes.push(outcome),
    () => {},
  );
  await Promise.resolve();
  expect(outcomes).toEqual([]);
  release();
  await result;
  expect(outcomes).toEqual(["complete"]);
});
