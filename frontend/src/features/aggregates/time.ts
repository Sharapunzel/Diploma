export function displayTime(iso: string | null, zone: string): string {
  if (!iso) return "Нет диапазона";
  try {
    const date = new Date(instantMillis(iso));
    if (!Number.isFinite(date.getTime())) return iso;
    return `${new Intl.DateTimeFormat("ru-RU", { timeZone: zone, dateStyle: "short", timeStyle: "long" }).format(date)} (${zone}, ${new Intl.DateTimeFormat("en", { timeZone: zone, timeZoneName: "longOffset" }).formatToParts(date).find((p) => p.type === "timeZoneName")?.value})${Number.isFinite(Date.parse(iso)) ? "" : `; исходная строка ${iso}`}`;
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

export function browserZone(): string {
  return Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC";
}

/** Exact UTC fraction, including fractional offset seconds, without Date rounding. */
export function scheduleFraction(iso: string): string {
  const match =
    /T\d\d:\d\d(?::\d\d(?:\.(\d+))?)?(?:Z|([+-])\d\d:\d\d(?::\d\d(?:\.(\d+))?)?)$/.exec(
      iso,
    );
  if (!match) return "";
  const local = match[1] ?? "",
    offset = match[3] ?? "";
  const precision = Math.max(local.length, offset.length);
  if (!precision) return "";
  const unit = 10n ** BigInt(precision);
  const localPart = BigInt(local.padEnd(precision, "0"));
  const offsetPart = BigInt(offset.padEnd(precision, "0"));
  const signedOffset = match[2] === "-" ? -offsetPart : offsetPart;
  const fraction = (((localPart - signedOffset) % unit) + unit) % unit;
  return `.${fraction.toString().padStart(precision, "0")}`;
}

const calendarFormatters = new Map<string, Intl.DateTimeFormat>();
function calendarFormatter(zone: string) {
  let formatter = calendarFormatters.get(zone);
  if (!formatter) {
    formatter = new Intl.DateTimeFormat("en-CA", {
      timeZone: zone,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hourCycle: "h23",
    });
    if (calendarFormatters.size >= 16)
      calendarFormatters.delete(calendarFormatters.keys().next().value!);
    calendarFormatters.set(zone, formatter);
  }
  return formatter;
}
export function localCalendar(iso: string, zone: string): string {
  if (!iso || !Number.isFinite(instantMillis(iso))) return "";
  try {
    const parts = calendarFormatter(zone).formatToParts(
      new Date(instantMillis(iso)),
    );
    const get = (type: string) => parts.find((p) => p.type === type)?.value;
    return `${get("year")?.padStart(4, "0")}-${get("month")}-${get("day")}T${get("hour")}:${get("minute")}:${get("second")}`;
  } catch {
    return "";
  }
}
/** Approximate milliseconds for display only; the original ISO remains authoritative. */
function instantMillis(iso: string): number {
  const normal = Date.parse(iso);
  if (Number.isFinite(normal)) return normal;
  const match =
    /^(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?)([+-])(\d\d):(\d\d)(?::(\d\d(?:\.\d+)?))$/.exec(
      iso,
    );
  if (!match) return NaN;
  const offset =
    (Number(match[3]) * 3600 + Number(match[4]) * 60 + Number(match[5])) * 1000;
  return Date.parse(match[1] + "Z") - (match[2] === "+" ? offset : -offset);
}
/** Enumerate actual UTC instants for a wall clock; never silently normalize DST gaps. */
export function calendarInstants(local: string, zone: string): string[] {
  if (!/^\d{4}-\d\d-\d\dT\d\d:\d\d(?::\d\d(?:\.0+)?)?$/.test(local)) return [];
  const seconds = local.replace(/\.0+$/, "");
  const normalized = seconds.length === 16 ? seconds + ":00" : seconds;
  const wall = Date.parse(normalized + "Z");
  if (
    !Number.isFinite(wall) ||
    new Date(wall).toISOString().slice(0, 19) !== normalized
  )
    return [];
  const offsets = new Set<number>();
  // Collect both sides of transitions, including half-hour and historical second offsets.
  for (let h = -48; h <= 48; h += 6) {
    const instant = wall + h * 3600000;
    const rendered = localCalendar(new Date(instant).toISOString(), zone);
    if (!rendered) return [];
    offsets.add(Date.parse(rendered + "Z") - instant);
  }
  return [...offsets]
    .map((offset) => new Date(wall - offset).toISOString())
    .filter((iso) => localCalendar(iso, zone) === normalized)
    .sort();
}
export function calendarBoundary(instant: string, zone: string): string {
  const local = localCalendar(instant, zone);
  const offset = Math.round(
    (Date.parse(local + "Z") - Date.parse(instant)) / 1000,
  );
  const absolute = Math.abs(offset),
    hours = Math.floor(absolute / 3600),
    minutes = Math.floor((absolute % 3600) / 60),
    seconds = absolute % 60;
  const part = (n: number) => String(n).padStart(2, "0");
  return `${local}${offset >= 0 ? "+" : "-"}${part(hours)}:${part(minutes)}${seconds ? `:${part(seconds)}` : ""}`;
}
