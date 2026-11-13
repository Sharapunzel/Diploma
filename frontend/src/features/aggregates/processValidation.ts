type ProcessFields = {
  name: string;
  source: string;
  duration: string;
  timezone: string;
  boundary: string;
};

export function timezoneError(zone: string): string {
  if (zone.trim()) {
    try {
      new Intl.DateTimeFormat("ru-RU", { timeZone: zone });
      return "";
    } catch {
      // Keep the user's text available for correction.
    }
  }
  return "Укажите UTC или существующий часовой пояс, например Asia/Yekaterinburg.";
}

/** Validate the calendar without rewriting a precise ISO string or its offset. */
export function validInstant(value: string): boolean {
  const match =
    /^(\d{4})-(\d\d)-(\d\d)T(\d\d):(\d\d)(?::(\d\d)(?:\.\d+)?)?(Z|[+-](\d\d):(\d\d)(?::(\d\d)(?:\.\d+)?)?)$/.exec(
      value,
    );
  if (!match) return false;
  const [
    ,
    year,
    month,
    day,
    hour,
    minute,
    second,
    ,
    offsetHour,
    offsetMinute,
    offsetSecond,
  ] = match;
  const y = Number(year),
    m = Number(month),
    d = Number(day);
  const leap = y % 4 === 0 && (y % 100 !== 0 || y % 400 === 0);
  const days = [31, leap ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
  return (
    y > 0 &&
    m >= 1 &&
    m <= 12 &&
    d >= 1 &&
    d <= days[m - 1] &&
    Number(hour) < 24 &&
    Number(minute) < 60 &&
    Number(second ?? 0) < 60 &&
    Number(offsetHour ?? 0) < 24 &&
    Number(offsetMinute ?? 0) < 60 &&
    Number(offsetSecond ?? 0) < 60
  );
}

export function processIssues(
  fields: ProcessFields,
): Partial<Record<keyof ProcessFields, string>> {
  const errors: Partial<Record<keyof ProcessFields, string>> = {};
  if (!fields.name.trim() || fields.name.length > 200)
    errors.name =
      "Укажите имя агрегата: от 1 до 200 символов, не только пробелы.";
  if (!fields.source.trim())
    errors.source = "Выберите собственный источник событий.";
  if (
    !/^[0-9]+$/.test(fields.duration) ||
    Number(fields.duration) < 1 ||
    Number(fields.duration) > 604800
  )
    errors.duration = "Длительность: целое число секунд от 1 до 604800.";
  const zone = timezoneError(fields.timezone);
  if (zone) errors.timezone = zone;
  if (!validInstant(fields.boundary))
    errors.boundary =
      "Выберите исходную границу расписания: дату и время в указанном поясе.";
  return errors;
}
