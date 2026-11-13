import { expect, it } from "vitest";
import type { Point } from "../../app/api/aggregates";
import { parseAggregate } from "../../app/api/aggregateJson";
import { chartSeries, chartTimeScale } from "./chart";

function point(
  id: string,
  start: string,
  end: string,
  value = "1",
  completeness = "complete",
  kind = "working",
) {
  return {
    id,
    start_utc: start,
    end_utc: end,
    kind,
    completeness,
    features: [
      {
        key: "n",
        value: parseAggregate(value),
        reason: value === "null" ? "no_values" : null,
      },
    ],
  } as Point;
}

it("positions points and ticks by elapsed time across an actual gap", () => {
  const timeline = chartTimeScale(
    [
      point("late", "2026-01-01T00:09:00Z", "2026-01-01T00:10:00Z"),
      point("early", "2026-01-01T05:00:00+05:00", "2026-01-01T05:01:00+05:00"),
    ],
    0,
    100,
  );
  expect(timeline).not.toBeNull();
  expect(timeline!.position(Date.parse("2026-01-01T00:01:00Z"))).toBe(10);
  expect(timeline!.position(Date.parse("2026-01-01T00:09:00Z"))).toBe(90);
  expect(timeline!.ticks.map((tick) => [tick.iso, tick.x])).toEqual([
    ["2026-01-01T00:00:00.000Z", 0],
    ["2026-01-01T00:05:00.000Z", 50],
    ["2026-01-01T00:10:00.000Z", 100],
  ]);
});

it("sorts the chart chronologically when equivalent source dates use different offsets", () => {
  const series = chartSeries(
    [
      point(
        "earlier",
        "2026-01-01T05:00:00+05:00",
        "2026-01-01T05:01:00+05:00",
      ),
      point("later", "2026-01-01T01:00:00Z", "2026-01-01T01:01:00Z"),
    ],
    "n",
  );
  expect(series[0].values.map((value) => value.point.id)).toEqual([
    "earlier",
    "later",
  ]);
});

it("connects only adjacent complete values without crossing missing points, nulls or kinds", () => {
  const minute = (n: number) =>
    `2026-01-01T00:${String(n).padStart(2, "0")}:00Z`;
  const series = chartSeries(
    [
      point("a", minute(0), minute(1)),
      point("b", minute(1), minute(2)),
      point("null", minute(2), minute(3), "null"),
      point("c", minute(3), minute(4)),
      point("unknown", minute(4), minute(5), "0", "unknown"),
      point("d", minute(5), minute(6)),
      point("gap", minute(8), minute(9)),
      point("historical", minute(1), minute(2), "1", "complete", "historical"),
    ],
    "n",
  );
  expect(series[0].values.map((value) => value.join)).toEqual([
    false,
    true,
    false,
    false,
    false,
    false,
    false,
  ]);
  expect(series[1].values.map((value) => value.join)).toEqual([false]);
});

it("has no invented time axis for empty or malformed timestamps", () => {
  expect(chartTimeScale([])).toBeNull();
  expect(chartTimeScale([point("bad", "unknown", "unknown")])).toBeNull();
});
