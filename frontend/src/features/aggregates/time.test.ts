import { expect, it } from "vitest";
import {
  calendarInstants,
  scheduleFraction,
  selectedWindow,
  suggestPreviewWindow,
} from "./time";

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

it("suggests an exact completed grid window, marking future-anchor previews outside the process", () => {
  const now = Date.parse("2026-01-01T00:12:00Z");
  expect(suggestPreviewWindow("2026-01-01T00:00:00.123456Z", 300, now)).toBe(
    "2026-01-01T00:00:00.123456Z",
  );
  const suggested = suggestPreviewWindow(
    "2026-01-01T00:20:00.123456Z",
    300,
    now,
  );
  expect(suggested).toBe("2026-01-01T00:05:00.123456Z");
  expect(
    selectedWindow(suggested, "2026-01-01T00:20:00.123456Z", 300, now),
  ).toEqual({
    start: suggested,
    end: "2026-01-01T00:10:00.123456Z",
    index: "-3",
    inProcess: false,
    completed: true,
  });
});
it("checks alignment without rounding copied microseconds or historical fractional offsets", () => {
  const anchor = "1900-01-01T00:00:00.123456+05:21:10";
  expect(selectedWindow(anchor, anchor, 300).end).toBe(
    "1899-12-31T18:43:50.123456Z",
  );
  expect(() =>
    selectedWindow("1900-01-01T00:00:00.123455+05:21:10", anchor, 300),
  ).toThrow("выровнено");
  expect(
    selectedWindow(
      "2026-01-01T00:00:00.123456+00:00:00.000001",
      "2026-01-01T00:00:00.123455Z",
      1,
    ).index,
  ).toBe("0");
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
