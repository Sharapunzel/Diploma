import type { FindingRow } from "../../app/api/findings";

const minimumHitWidth = 14;
export const laneHeight = 34;

export function windowBounds(finding: FindingRow) {
  const start = Date.parse(finding.window_start ?? "");
  const end = Date.parse(finding.window_end ?? "");
  return Number.isFinite(start) && Number.isFinite(end) && start < end
    ? { start, end }
    : null;
}

export function layoutWindows(findings: FindingRow[]) {
  const windows = findings.flatMap((finding) => {
    const bounds = windowBounds(finding);
    return bounds ? [{ finding, ...bounds }] : [];
  });
  if (!windows.length) return null;
  windows.sort((left, right) => left.start - right.start);
  const start = windows[0].start;
  const end = Math.max(...windows.map((window) => window.end));
  const span = end - start;
  const shortest = Math.min(
    ...windows.map((window) => window.end - window.start),
  );
  const minWidth = Math.min(
    4800,
    Math.max(720, Math.ceil((span / shortest) * minimumHitWidth)),
  );
  const minimumHitDuration = (minimumHitWidth / minWidth) * span;
  const occupiedUntil: number[] = [];
  const marks = windows.map(({ finding, start: left, end: right }) => {
    let lane = occupiedUntil.findIndex((until) => until <= left);
    if (lane < 0) lane = occupiedUntil.length;
    occupiedUntil[lane] = Math.max(right, left + minimumHitDuration);
    return {
      finding,
      lane,
      left: ((left - start) / span) * 100,
      width: ((right - left) / span) * 100,
    };
  });
  return { marks, start, end, minWidth, lanes: occupiedUntil.length };
}
