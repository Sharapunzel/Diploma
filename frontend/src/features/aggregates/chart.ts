import type { Point } from "../../app/api/aggregates";
import { exact } from "../../app/api/aggregateJson";

/** Honest numeric domain; normalization avoids overflow across opposite extremes. */
export function chartScale(values: number[], top = 30, bottom = 170) {
  if (!values.length) return null;
  if (values.some((v) => !Number.isFinite(v)))
    throw new Error("Non-finite chart input");
  const low = Math.min(...values),
    high = Math.max(...values);
  const constant = low === high;
  const span = high - low;
  const magnitude = Math.max(Math.abs(low), Math.abs(high));
  return {
    low,
    high,
    constant,
    top,
    bottom,
    position(value: number) {
      if (!Number.isFinite(value)) throw new Error("Non-finite chart input");
      if (constant) return (top + bottom) / 2;
      const bounded = Math.min(high, Math.max(low, value));
      const ratio = Number.isFinite(span)
        ? (bounded - low) / span
        : (bounded / magnitude - low / magnitude) /
          (high / magnitude - low / magnitude);
      return bottom - Math.min(1, Math.max(0, ratio)) * (bottom - top);
    },
  };
}

/** Technical window times, not point indices, define the horizontal domain. */
export function chartTimeScale(points: Point[], left = 40, right = 860) {
  if (!points.length) return null;
  const starts = points.map((point) => Date.parse(point.start_utc));
  const ends = points.map((point) => Date.parse(point.end_utc));
  if ([...starts, ...ends].some((value) => !Number.isFinite(value)))
    return null;
  const low = Math.min(...starts);
  const high = Math.max(...ends);
  if (high <= low) return null;
  const span = high - low;
  const position = (value: number) =>
    left + ((value - low) / span) * (right - left);
  return {
    low,
    high,
    position,
    ticks: [low, low + (high - low) / 2, high].map((value) => ({
      value,
      iso: new Date(value).toISOString(),
      x: position(value),
    })),
  };
}

/** Coordinates are approximations only; the original payload is never modified. */
export function chartSeries(points: Point[], feature: string) {
  return ["working", "historical"].map((kind) => {
    const sorted = points
      .filter((p) => p.kind === kind)
      .sort((a, b) => Date.parse(a.start_utc) - Date.parse(b.start_utc));
    let previous: Point | null = null;
    return {
      kind,
      values: sorted.map((point) => {
        const result =
          point.calculation_status === "uncomputed"
            ? undefined
            : point.features?.find((f) => f.key === feature);
        const value =
          result?.value == null ? null : Number(exact(result.value));
        const valid =
          value !== null &&
          Number.isFinite(value) &&
          point.completeness === "complete" &&
          !result?.reason;
        const join =
          !!previous && previous.end_utc === point.start_utc && valid;
        previous = valid ? point : null;
        return {
          point,
          value: value !== null && Number.isFinite(value) ? value : null,
          valid,
          join,
          exact: exact(result?.value),
        };
      }),
    };
  });
}
