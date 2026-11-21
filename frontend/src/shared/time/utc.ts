const utcPattern =
  /^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,6}))?(?:Z|\+00:00)$/;

export const displayZone =
  Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC";

export function utcSortKey(value: string): string | null {
  const match = utcPattern.exec(value);
  if (!match || match[1].startsWith("0000")) return null;
  const date = new Date(match[1] + "Z");
  if (
    !Number.isFinite(date.getTime()) ||
    date.toISOString().slice(0, 19) !== match[1]
  )
    return null;
  return `${match[1]}.${(match[2] ?? "").padEnd(6, "0")}`;
}

export function localTime(
  value: string | null,
  includeFraction = false,
): string {
  if (!value) return "Нет данных";
  const date = new Date(value);
  if (!Number.isFinite(date.getTime())) return value;
  const local = date.toLocaleString("ru-RU", { hourCycle: "h23" });
  const fraction = includeFraction && utcPattern.exec(value)?.[2];
  return fraction ? `${local}.${fraction}` : local;
}

export function exactLocalTime(value: string | null): string {
  return localTime(value, true);
}
