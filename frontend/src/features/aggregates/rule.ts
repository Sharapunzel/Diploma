import {
  object,
  parseAggregate,
  printAggregate,
  type JsonObject,
  type Json,
  isLosslessNumber,
} from "../../app/api/aggregateJson";
import type { Operator } from "../../app/api/aggregates";
export { aggregatePrompt } from "./prompt";

export function initialRule(): JsonObject {
  return { features: [{ key: "events", name: "События", op: "count" }] };
}
export function clone<T>(value: T): T {
  return parseAggregate(printAggregate(value)) as T;
}
export function ruleDocument(text: string, operators: Operator[]): JsonObject {
  const value = parseAggregate(text);
  if (!object(value) || !Array.isArray(value.features))
    throw new Error("Правило должно содержать непустой массив features.");
  const check = (node: Json, kind: string, path: string) => {
    if (!object(node)) throw new Error(`${path}: требуется объект`);
    const known =
      kind === "feature"
        ? ["op", "key", "name", "field", "percentile", "expr", "where"]
        : kind === "aggregate"
          ? ["op", "field", "percentile", "where"]
          : kind === "expression"
            ? ["op", "args", "ref", "value", "aggregate"]
            : ["op", "field", "value", "args"];
    if (Object.keys(node).some((key) => !known.includes(key)))
      throw new Error(
        `${path}: неизвестное поле; исправьте JSON без потери узла`,
      );
    if (kind === "expression" && "aggregate" in node) {
      if (
        !object(node.aggregate) ||
        Object.keys(node).some((k) => k !== "aggregate" && node[k] !== null)
      )
        throw new Error(
          `${path}: aggregate должен содержать одну базовую операцию`,
        );
      check(node.aggregate, "aggregate", `${path}.aggregate`);
      if (["derive", "formula"].includes(String(node.aggregate.op)))
        throw new Error(`${path}: вложенная формула недопустима`);
      return;
    }
    if (kind === "expression" && (node.ref != null || node.value != null)) {
      if (Object.keys(node).filter((k) => node[k] !== null).length !== 1)
        throw new Error(`${path}: ссылка или константа содержит лишние поля`);
      if (node.ref != null && typeof node.ref !== "string")
        throw new Error(`${path}.ref: требуется ключ`);
      if (
        node.value != null &&
        typeof node.value !== "number" &&
        !isLosslessNumber(node.value)
      )
        throw new Error(`${path}.value: требуется число`);
      return;
    }
    const op = operators.find(
      (o) =>
        o.kind === (kind === "aggregate" ? "feature" : kind) &&
        o.name === node.op,
    );
    if (!op)
      throw new Error(`${path}: неизвестная операция ${String(node.op)}`);
    const allowed = new Set([
      "op",
      ...op.parameters,
      ...(kind === "feature"
        ? ["key", "name", "where"]
        : kind === "aggregate"
          ? ["where"]
          : []),
    ]);
    if (Object.keys(node).some((k) => !allowed.has(k) && node[k] !== null))
      throw new Error(
        `${path}: нераспознанное поле; исправьте JSON без потери узла`,
      );
    if (node.args !== null && node.args !== undefined) {
      if (!Array.isArray(node.args))
        throw new Error(`${path}.args: требуется список`);
      node.args.forEach((n, i) => check(n, kind, `${path}.args[${i}]`));
      if (
        op.arity &&
        (node.args.length < op.arity.min ||
          (op.arity.max != null && node.args.length > op.arity.max))
      )
        throw new Error(`${path}.args: неверное число аргументов`);
    }
    for (const param of op.parameters) {
      if (node[param] == null)
        throw new Error(`${path}.${param}: обязательный параметр`);
    }
    if (node.expr) check(node.expr, "expression", `${path}.expr`);
    if (node.where) check(node.where, "condition", `${path}.where`);
  };
  if (
    Object.keys(value).some(
      (k) => !["where", "features", "auxiliary"].includes(k),
    )
  )
    throw new Error(
      "Неизвестное поле правила. Редактируется только aggregate_rule.",
    );
  if (value.where) check(value.where, "condition", "where");
  value.features.forEach((node, i) => check(node, "feature", `features[${i}]`));
  if ("auxiliary" in value) {
    if (!Array.isArray(value.auxiliary) || !value.auxiliary.length)
      throw new Error(
        "auxiliary: требуется непустой список или отсутствие поля",
      );
    value.auxiliary.forEach((node, i) =>
      check(node, "feature", `auxiliary[${i}]`),
    );
  }
  return value;
}
export function definitions(rule: JsonObject): JsonObject[] {
  return [
    ...(Array.isArray(rule.features) ? rule.features : []),
    ...(Array.isArray(rule.auxiliary) ? rule.auxiliary : []),
  ].filter(object);
}
export function refs(value: Json): string[] {
  if (Array.isArray(value)) return value.flatMap(refs);
  if (!object(value)) return [];
  // Only expression references, never a string literal that happens to equal a key.
  if (typeof value.ref === "string") return [value.ref];
  return Object.entries(value)
    .filter(([k]) => k !== "value" && k !== "where")
    .flatMap(([, v]) => refs(v));
}
export function hasAggregate(value: Json): boolean {
  if (Array.isArray(value)) return value.some(hasAggregate);
  return (
    object(value) &&
    (object(value.aggregate) || Object.values(value).some(hasAggregate))
  );
}
function extendedReferences(rule: JsonObject, all = definitions(rule)) {
  return "auxiliary" in rule || all.some((feature) => feature.op === "formula");
}
export function referenceErrors(rule: JsonObject): string[] {
  const all = definitions(rule),
    errors: string[] = [];
  const keys = new Set<string>(),
    previous = new Set<string>();
  const extended = extendedReferences(rule, all);
  const path = (f: JsonObject) => {
    const outputs = Array.isArray(rule.features) ? rule.features : [];
    const i = outputs.indexOf(f);
    return i >= 0
      ? `features[${i}]`
      : `auxiliary[${Array.isArray(rule.auxiliary) ? rule.auxiliary.indexOf(f) : -1}]`;
  };
  for (const f of all) {
    const key = String(f.key ?? "");
    if (!/^[a-z][a-z0-9_]{0,63}$/.test(key))
      errors.push(`${key}: ключ должен соответствовать [a-z][a-z0-9_]{0,63}`);
    if (keys.has(key)) errors.push(`${key}: повтор ключа`);
    keys.add(key);
  }
  for (const f of all) {
    const key = String(f.key);
    for (const ref of refs(f.expr ?? null)) {
      if (ref === key) errors.push(`${key}.expr: ссылка на себя`);
      else if (!(extended ? keys : previous).has(ref))
        errors.push(
          `${path(f)}.expr.ref: неизвестная ссылка или не предыдущий признак ${ref}`,
        );
    }
    if (
      ["derive", "formula"].includes(String(f.op)) &&
      f.where != null &&
      !hasAggregate(f.expr ?? null)
    )
      errors.push(
        `${key}.where: у формулы нет собственных агрегирующих листьев`,
      );
    previous.add(key);
  }
  const visited = new Set<string>(),
    active = new Set<string>();
  const visit = (key: string) => {
    if (active.has(key)) {
      errors.push(`${key}.expr: циклические ссылки`);
      return;
    }
    if (visited.has(key)) return;
    active.add(key);
    const f = all.find((n) => n.key === key);
    if (f)
      refs(f.expr ?? null)
        .filter((r) => keys.has(r))
        .forEach(visit);
    active.delete(key);
    visited.add(key);
  };
  keys.forEach(visit);
  if (!Array.isArray(rule.features) || !rule.features.length)
    errors.push("Выходной вектор не может быть пустым");
  return errors;
}
// Remember disabled coordinates only for the lifetime of a client draft.
// Imported rules start from their wire order; no UI metadata enters JSON.
const definitionOrders = new WeakMap<JsonObject, string[]>();
export function definitionOrder(rule: JsonObject): string[] {
  return (
    definitionOrders.get(rule) ?? definitions(rule).map((f) => String(f.key))
  );
}
export function retainDefinitionOrder(
  from: JsonObject,
  to: JsonObject,
  visual = false,
) {
  if (visual) {
    if (definitionOrders.has(to)) return;
    const keys = definitions(to).map((node) => String(node.key));
    const order = definitionOrder(from).filter((key) => keys.includes(key));
    definitionOrders.set(to, [
      ...order,
      ...keys.filter((key) => !order.includes(key)),
    ]);
    return;
  }
  const remembered = definitionOrders.get(from);
  if (!remembered) return;
  const keys = (rule: JsonObject, group: string) =>
    (Array.isArray(rule[group]) ? rule[group] : []).map((node) =>
      object(node) ? node.key : null,
    );
  if (
    ["features", "auxiliary"].every(
      (group) =>
        JSON.stringify(keys(from, group)) === JSON.stringify(keys(to, group)),
    )
  )
    definitionOrders.set(to, remembered);
}
export function resetDefinitionOrder(rule: JsonObject) {
  definitionOrders.delete(rule);
}
export function assertVisualReferences(from: JsonObject, to: JsonObject) {
  if (!extendedReferences(from) || extendedReferences(to)) return;
  if (
    !referenceErrors(from).length &&
    referenceErrors(to).some((error) =>
      error.includes("неизвестная ссылка или не предыдущий признак"),
    )
  )
    throw new Error(
      "Действие изменит правила ссылок и нарушит зависимость между оставшимися признаками. Сначала исправьте порядок выходов или формулу в JSON: зависимости должны предшествовать использующим их признакам.",
    );
}

export function moveDefinition(
  rule: JsonObject,
  key: string,
  output: boolean,
  order = definitionOrder(rule),
): JsonObject {
  const next = clone(rule),
    all = definitions(next),
    f = all.find((n) => n.key === key);
  if (!f) return next;
  const outputs = (Array.isArray(next.features) ? next.features : []).filter(
    (n) => !object(n) || n.key !== key,
  );
  const aux = (Array.isArray(next.auxiliary) ? next.auxiliary : []).filter(
    (n) => !object(n) || n.key !== key,
  );
  if (output) outputs.push(f);
  else aux.push(f);
  outputs.sort(
    (a, b) =>
      order.indexOf(String(object(a) ? a.key : "")) -
      order.indexOf(String(object(b) ? b.key : "")),
  );
  next.features = outputs;
  if (aux.length) next.auxiliary = aux;
  else delete next.auxiliary;
  if (output && !referenceErrors(rule).length && referenceErrors(next).length)
    throw new Error(
      "Нельзя включить признак в этой позиции: зависимости должны предшествовать использующим их выходам. Сначала задайте корректный порядок выходных признаков в JSON.",
    );
  definitionOrders.set(next, order);
  return next;
}
export function formula(node: Json): string {
  if (!object(node)) return printAggregate(node);
  if (typeof node.ref === "string") return node.ref;
  if (node.value != null && !node.op) return printAggregate(node.value);
  if (object(node.aggregate)) return formula(node.aggregate);
  const args = Array.isArray(node.args) ? node.args.map(formula) : [];
  const symbols: Record<string, string> = {
    add: "+",
    subtract: "−",
    multiply: "×",
    divide: "/",
    ratio: "/",
  };
  const op = String(node.op);
  let result = node.expr
    ? formula(node.expr)
    : symbols[op]
      ? `(${args.join(` ${symbols[op]} `)})`
      : `${op}(${args.length ? args.join(", ") : [node.field, node.percentile != null ? printAggregate(node.percentile) : null].filter(Boolean).join(", ")})`;
  if (node.where != null) result += ` [где ${printAggregate(node.where)}]`;
  return result;
}
