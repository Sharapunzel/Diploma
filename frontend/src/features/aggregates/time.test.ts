import { expect, it } from "vitest";
import { calendarInstants, scheduleFraction } from "./time";

it.each([
  ["2026-01-01T00:00:00.123456Z", ".123456"],
  ["2026-01-01T00:00:00.123456+00:00", ".123456"],
  ["2026-01-01T00:00:00.123456+05:00", ".123456"],
  ["2026-01-01T00:00:00.123456+05:00:03.654321", ".469135"],
  ["2026-01-01T00:00:00.123456-05:00:03.654321", ".777777"],
  ["2026-01-01T00:00:00.900000-05:00:03.200000", ".100000"],
  ["2026-01-01T00:00:00+05:00:03.654321", ".345679"],
  ["2026-01-01T00:00:00.1+05:00:03.002", ".098"],
  ["2026-01-01T00:00:00.123456789123456789Z", ".123456789123456789"],
  ["2026-01-01T00:00:00.000000Z", ".000000"],
  ["2026-01-01T00:00:00Z", ""],
  ["", ""],
])("keeps the exact UTC schedule fraction of %s", (iso, fraction) => {
  expect(scheduleFraction(iso)).toBe(fraction);
});

it("accepts equivalent whole-second calendar formats without discarding nonzero fractions", () => {
  expect(calendarInstants("2026-11-01T12:30:07", "Asia/Yekaterinburg")).toEqual(
    ["2026-11-01T07:30:07.000Z"],
  );
  expect(
    calendarInstants("2026-11-01T12:30:07.000", "Asia/Yekaterinburg"),
  ).toEqual(["2026-11-01T07:30:07.000Z"]);
  expect(calendarInstants("2026-11-01T12:30", "Asia/Yekaterinburg")).toEqual([
    "2026-11-01T07:30:00.000Z",
  ]);
  expect(
    calendarInstants("2026-11-01T12:30:07.001", "Asia/Yekaterinburg"),
  ).toEqual([]);
});
