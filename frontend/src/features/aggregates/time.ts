export function displayTime(iso: string | null, zone: string): string {
  if (!iso) return "Нет диапазона";
  try {
    const date = new Date(iso);
    if (!Number.isFinite(date.getTime())) return iso;
    return `${new Intl.DateTimeFormat("ru-RU", { timeZone: zone, dateStyle: "short", timeStyle: "long" }).format(date)} (${zone}, ${new Intl.DateTimeFormat("en", { timeZone: zone, timeZoneName: "longOffset" }).formatToParts(date).find((p) => p.type === "timeZoneName")?.value})`;
  } catch {
    return iso;
  }
}
export function historicalError(
  start: string,
  anchor: string,
  duration: number,
  created: string,
): string {
  const ms = Date.parse(start);
  const base = Date.parse(anchor);
  if (!Number.isFinite(ms) || !/(Z|[+-]\d\d:\d\d)$/.test(start))
    return "Укажите ISO-время с явным смещением.";
  if ((ms - base) % (duration * 1000))
    return "Начало должно быть выровнено относительно UTC-якоря.";
  if (ms < base)
    return "Историческое окно не может начинаться раньше UTC-якоря.";
  if (ms + duration * 1000 > Date.parse(created))
    return "Историческое окно должно завершиться не позже создания агрегата.";
  return "";
}
