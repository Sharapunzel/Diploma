import {
  parse,
  stringify,
  LosslessNumber,
  isLosslessNumber,
} from "lossless-json";

export type Json =
  | null
  | boolean
  | string
  | number
  | LosslessNumber
  | Json[]
  | { [key: string]: Json };
export type JsonObject = { [key: string]: Json };
export { LosslessNumber, isLosslessNumber };
export function parseAggregate(text: string): Json {
  const value = parse(text) as Json;
  const check = (v: Json): void => {
    if (isLosslessNumber(v) && !Number.isFinite(Number(v.value)))
      throw new Error("Число выходит за конечный числовой диапазон.");
    if (Array.isArray(v)) v.forEach(check);
    else if (object(v)) Object.values(v).forEach(check);
  };
  check(value);
  return value;
}
export function printAggregate(value: unknown, pretty = false): string {
  const result = stringify(value, undefined, pretty ? 2 : undefined);
  if (result === undefined) throw new Error("Не удалось представить JSON");
  return result;
}
export function object(value: unknown): value is JsonObject {
  return (
    !!value &&
    typeof value === "object" &&
    !Array.isArray(value) &&
    !isLosslessNumber(value)
  );
}
export function safeInteger(value: unknown): number {
  const n = isLosslessNumber(value) ? Number(value.value) : value;
  if (typeof n !== "number" || !Number.isSafeInteger(n))
    throw new Error("Ожидалось безопасное целое число");
  return n;
}
export function exact(value: unknown): string {
  return value === null || value === undefined
    ? "Нет значения"
    : isLosslessNumber(value)
      ? value.value
      : typeof value === "string"
        ? value
        : printAggregate(value);
}
