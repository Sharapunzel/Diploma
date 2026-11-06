import {
  object,
  parseAggregate,
  printAggregate,
  type JsonObject,
  type Json,
} from "../../app/api/aggregateJson";
import type { Operator } from "../../app/api/aggregates";

export function initialRule(): JsonObject {
  return { features: [{ key: "events", name: "События", op: "count" }] };
}
export function clone<T>(value: T): T {
  return parseAggregate(printAggregate(value)) as T;
}
export function ruleDocument(text: string, operators: Operator[]): JsonObject {
  const value = parseAggregate(text);
  if (
    !object(value) ||
    !Array.isArray(value.features) ||
    !value.features.length
  )
    throw new Error("Правило должно содержать непустой массив features.");
  const check = (node: Json, kind: string, path: string) => {
    if (!object(node)) throw new Error(`${path}: требуется объект`);
    if (kind === "expression" && (node.ref != null || node.value != null)) {
      if (Object.keys(node).filter((k) => node[k] !== null).length !== 1)
        throw new Error(`${path}: ссылка или константа содержит лишние поля`);
      return;
    }
    const op = operators.find((o) => o.kind === kind && o.name === node.op);
    if (!op)
      throw new Error(`${path}: неизвестная операция ${String(node.op)}`);
    const allowed = new Set([
      "op",
      ...op.parameters,
      ...(kind === "feature" ? ["key", "name", "where"] : []),
    ]);
    if (Object.keys(node).some((k) => !allowed.has(k) && node[k] !== null))
      throw new Error(
        `${path}: нераспознанное поле; исправьте JSON без потери узла`,
      );
    if (node.args !== null && node.args !== undefined) {
      if (!Array.isArray(node.args))
        throw new Error(`${path}.args: требуется список`);
      node.args.forEach((n, i) => check(n, kind, `${path}.args[${i}]`));
    }
    if (node.expr) check(node.expr, "expression", `${path}.expr`);
    if (node.where) check(node.where, "condition", `${path}.where`);
  };
  if (Object.keys(value).some((k) => !["where", "features"].includes(k)))
    throw new Error(
      "Неизвестное поле правила. Редактируется только aggregate_rule.",
    );
  if (value.where) check(value.where, "condition", "where");
  value.features.forEach((node, i) => check(node, "feature", `features[${i}]`));
  return value;
}
export function referenceErrors(rule: JsonObject): string[] {
  const previous = new Set<string>();
  const errors: string[] = [];
  const walk = (v: Json, path: string) => {
    if (Array.isArray(v)) v.forEach((n, i) => walk(n, `${path}[${i}]`));
    else if (object(v)) {
      if (typeof v.ref === "string" && !previous.has(v.ref))
        errors.push(
          `${path}.ref: ссылка ${v.ref} не указывает на предыдущий признак`,
        );
      Object.entries(v).forEach(([k, n]) => walk(n, `${path}.${k}`));
    }
  };
  if (Array.isArray(rule.features))
    rule.features.forEach((v, i) => {
      if (!object(v)) return;
      walk(v.expr ?? null, `features[${i}].expr`);
      if (typeof v.key === "string") {
        if (previous.has(v.key))
          errors.push(`features[${i}].key: повтор ключа`);
        previous.add(v.key);
      }
    });
  return errors;
}
export function aggregatePrompt(operators: Operator[]): string {
  return `Составь только JSON aggregate_rule для агрегирования собственных ECS 9.4.0 событий. Без markdown, версии, имени источника и расписания. Не используй SQL, Python, eval, правила нормализатора. Поля берутся из полного постраничного каталога ECS, а не из первой страницы; каталог не подтверждает наличие данных. Пользователь самостоятельно выбирает передаваемые сведения.\nФорма: {"where":condition,"features":[{"key":"events","name":"События","op":"count"},{"key":"ratio","name":"Отношение","op":"derive","expr":{"op":"ratio","args":[{"ref":"events"},{"value":2}]}}]}. where необязателен. Признаки упорядочены, ключи уникальны [a-z][a-z0-9_]{0,63}, name 1–200. Ссылки только на предыдущие признаки, константы {"value":число}. Условие {"op":операция,"field":поле,"value":типизированное значение}, all/any/not используют args. Индивидуальное where допустимо у базового признака. Списки однородные, числа конечны, long — целая JSON-запись int64, не 1.0; не округляй большие числа. Даты сохраняй точными ISO-строками с явным смещением, включая микросекунды и секунды смещения; UTC диапазон 0001–9999. Статистический null не равен нулю. Нет лимита пять признаков; сервер ограничивает глубину, литералы, память, стоимость и период 1–604800 секунд. Пройдёт обязательные validate и preview до создания.\nПолный каталог операций, совместимые типы, арность, параметры, стоимость:\n${printAggregate(operators, true)}`;
}
