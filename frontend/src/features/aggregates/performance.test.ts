import { describe, expect, it } from "vitest";
import { executionThresholds } from "./performance";

describe("execution thresholds", () => {
  it.each([
    [1, 0.8],
    [5, 4],
    [20, 16],
    [37, 29.6],
    [38, 30],
    [100, 30],
    [300, 30],
    [604800, 30],
  ])(
    "keeps the independent soft target for a %s-second window",
    (duration, hard) => {
      expect(executionThresholds(duration)).toEqual({
        softSeconds: 5,
        hardSeconds: hard,
      });
      expect(executionThresholds(String(duration))).toEqual(
        executionThresholds(duration),
      );
    },
  );

  it.each([
    "",
    " ",
    "1.5",
    "1e2",
    "+5",
    "-1",
    "604801",
    0,
    -1,
    1.5,
    NaN,
    Infinity,
  ])("does not present a policy for invalid duration %s", (duration) => {
    expect(executionThresholds(duration)).toBeNull();
  });
});
