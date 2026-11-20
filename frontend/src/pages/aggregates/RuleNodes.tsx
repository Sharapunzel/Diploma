import { createContext, useContext, useEffect, useId, useState } from "react";
import { Button, Input, Select } from "antd";
import type {
  AggregatesApi,
  SourceField,
  Operator,
} from "../../app/api/aggregates";
import {
  object,
  parseAggregate,
  printAggregate,
  type Json,
  type JsonObject,
} from "../../app/api/aggregateJson";
import { AggregateError } from "./AggregateError";
import styles from "./Aggregates.module.css";
import { FieldLabel, OperationSelect, operationLabel } from "./AggregateHelp";

function nodeLabel(path: string) {
  return path
    .replace(/^feature\.where/, "Условие признака")
    .replace(/^where/, "Общее условие")
    .replace(/^feature/, "Признак")
    .replace(/^expr/, "Формула")
    .replace(
      /\.args\[(\d+)\]/g,
      (_, index: string) => ` · аргумент ${Number(index) + 1}`,
    )
    .replace(/\.aggregate/g, " · вход из событий")
    .replace(/\.where/g, " · условие отбора");
}

function typeLabel(type = "") {
  const labels: Record<string, string> = {
    number: "число",
    boolean: "логическое значение",
    string: "строка",
    scalar: "значение",
    date: "дата",
    ip: "IP-адрес",
    any: "любое значение",
  };
  return type.replace(
    /number|boolean|string|scalar|date|ip|any/g,
    (value) => labels[value],
  );
}

export type Context = {
  operators: Operator[];
  api: AggregatesApi;
  source: string;
  aggregateLeaves?: boolean;
  inputsSeparate?: boolean;
};
export const InvalidContext = createContext<
  (id: string, invalid: boolean) => void
>(() => {});
export function FieldPicker({
  value,
  operation,
  context,
  onChange,
}: {
  value: string;
  operation: string;
  context: Context;
  onChange: (v: string) => void;
}) {
  const [query, setQuery] = useState("");
  const [retry, setRetry] = useState(0);
  const [items, setItems] = useState<SourceField[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<unknown>(null);
  const [empty, setEmpty] = useState("");
  useEffect(() => {
    const controller = new AbortController();
    setItems([]);
    setError(null);
    setLoading(false);
    if (!context.source) return;
    setLoading(true);
    const timer = setTimeout(() => {
      void context.api
        .fieldCatalog(context.source, query, 0, controller.signal)
        .then((page) => {
          if (!controller.signal.aborted) {
            setItems(page.items);
            setEmpty(
              String(page.observation.rows) === "0"
                ? page.normalizer
                  ? "Нет событий; доступны поля нормализатора."
                  : "Нет событий и назначенного нормализатора."
                : "Нет подходящих полей.",
            );
          }
        })
        .catch((e) => {
          if (!controller.signal.aborted) setError(e);
        })
        .finally(() => {
          if (!controller.signal.aborted) setLoading(false);
        });
    }, 200);
    return () => {
      clearTimeout(timer);
      controller.abort();
    };
  }, [context.api, context.source, query, retry]);
  return (
    <div className={styles.field}>
      <div className={styles.field}>
        <FieldLabel help="Найдите ECS-поле по имени. В списке указаны описание, тип, происхождение и совместимость с выбранной операцией. Наличие поля в каталоге не гарантирует значение в каждом событии.">
          Поле ECS
        </FieldLabel>
        <Select
          aria-label="Поле ECS"
          className={styles.fullWidth}
          classNames={{ popup: { root: styles.optionPopup } }}
          placeholder="Начните вводить имя поля ECS"
          optionLabelProp="title"
          virtual={false}
          value={value || undefined}
          showSearch
          filterOption={false}
          onSearch={(q) => {
            setQuery(q);
          }}
          loading={loading}
          disabled={!context.source}
          notFoundContent={
            loading ? "Загрузка…" : error ? "Каталог недоступен" : empty
          }
          onChange={onChange}
          options={[
            ...(value && !items.some((f) => f.name === value)
              ? [
                  {
                    value,
                    title: value,
                    label: `${value} (проверьте совместимость на сервере)`,
                  },
                ]
              : []),
            ...items.map((f) => ({
              value: f.name,
              title: f.name,
              label: `${f.name} · ${f.description} · ${f.provenance.map((p) => (p === "observed" ? "Обнаружено в событиях" : "Предусмотрено нормализатором")).join(" + ")} · ${f.type}${f.is_array ? "[]" : ""}${!f.supported ? ` — ${f.reason || "не поддерживается"}` : !f.operators.includes(operation) ? " — несовместимо" : ""}`,
              disabled: !f.supported || !f.operators.includes(operation),
            })),
          ]}
        />
      </div>
      {error !== null && (
        <AggregateError error={error} onRetry={() => setRetry((n) => n + 1)} />
      )}
    </div>
  );
}
export function Literal({
  value,
  label,
  onChange,
}: {
  value: Json;
  label: string;
  onChange: (v: Json) => void;
}) {
  const serialized = printAggregate(value);
  const [text, setText] = useState(serialized);
  const [error, setError] = useState("");
  const invalid = useContext(InvalidContext);
  const id = useId();
  useEffect(() => {
    invalid(id, !!error);
    return () => invalid(id, false);
  }, [invalid, id, error]);
  useEffect(() => {
    setText(serialized);
    setError("");
  }, [serialized]);
  return (
    <div className={styles.field}>
      <FieldLabel help="Введите точное JSON-значение: число, строку в двойных кавычках, true/false или список одного типа. Значение сравнивается с полем события до агрегации; константа формулы используется как число.">
        {label}
      </FieldLabel>
      <Input
        aria-label={label}
        value={text}
        status={error ? "error" : undefined}
        onChange={(e) => {
          setText(e.target.value);
          try {
            const v = parseAggregate(e.target.value);
            if (
              object(v) ||
              v === null ||
              (Array.isArray(v) &&
                v.some((n) => n === null || object(n) || Array.isArray(n)))
            )
              throw new Error();
            onChange(v);
            setError("");
          } catch {
            setError(
              "Введите точное JSON-значение: число, строку в кавычках, true/false или однородный список. Незавершённый ввод не применён.",
            );
          }
        }}
      />
      {error && <span role="alert">{error}</span>}
    </div>
  );
}
export function defaultNode(op: Operator): JsonObject {
  const node: JsonObject = { op: op.name };
  if (op.parameters.includes("field")) node.field = "";
  if (op.parameters.includes("value"))
    node.value =
      op.parameter_types.value?.includes("[]") || op.name === "between"
        ? []
        : "";
  if (op.parameters.includes("args"))
    node.args = Array.from(
      { length: op.arity?.min ?? 1 },
      (): JsonObject =>
        op.kind === "expression" ? { value: 1 } : { op: "exists", field: "" },
    );
  if (op.parameters.includes("expr")) node.expr = { value: 1 };
  if (op.parameters.includes("percentile"))
    node.percentile = parseAggregate("0.95");
  return node;
}
export function NodeEditor({
  value,
  kind,
  context,
  previous,
  path,
  onChange,
}: {
  value: JsonObject;
  kind: "condition" | "expression";
  context: Context;
  previous: string[];
  path: string;
  onChange: (v: JsonObject) => void;
}) {
  const isExpr = kind === "expression";
  const selected =
    value.aggregate != null
      ? "$aggregate"
      : value.ref != null
        ? "$ref"
        : value.value != null && !value.op
          ? "$value"
          : String(value.op);
  const catalog = context.operators.filter((o) => o.kind === kind);
  const op = catalog.find((o) => o.name === selected);
  const args = Array.isArray(value.args) ? value.args : [];
  const title = nodeLabel(path);
  return (
    <fieldset className={styles.node}>
      <legend>{title}</legend>
      <OperationSelect
        label="Операция"
        name={`${title}: операция`}
        value={selected}
        onChange={(name) =>
          onChange(
            name === "$aggregate"
              ? { aggregate: { op: "count" } }
              : name === "$ref"
                ? { ref: previous[0] || "" }
                : name === "$value"
                  ? { value: 1 }
                  : defaultNode(catalog.find((o) => o.name === name)!),
          )
        }
        options={[
          ...(isExpr
            ? [
                ...(previous.length
                  ? [{ name: "$ref", label: "Именованный признак" }]
                  : []),
                { name: "$value", label: "Числовая константа" },
                ...(context.aggregateLeaves !== false
                  ? [
                      {
                        name: "$aggregate",
                        label: "Итог ECS-операции за окно",
                      },
                    ]
                  : []),
              ]
            : []),
          ...catalog.map((o) => ({
            name: o.name,
            label: `${operationLabel(o.name)}${o.result ? ` (${typeLabel(o.result)})` : ""}`,
          })),
        ]}
      />
      {selected === "$aggregate" && object(value.aggregate) ? (
        <AggregateLeaf
          value={value.aggregate}
          context={context}
          path={`${path}.aggregate`}
          onChange={(aggregate) => onChange({ aggregate })}
        />
      ) : selected === "$ref" ? (
        <div className={styles.field}>
          <FieldLabel help="Ссылка берёт готовый итог указанного определения за то же окно. Его собственные условия отбора сохраняются.">
            Именованный признак
          </FieldLabel>
          <Select
            aria-label={`${title}: ссылка`}
            value={String(value.ref)}
            options={previous.map((k) => ({ value: k, label: k }))}
            onChange={(ref) => onChange({ ref })}
          />
        </div>
      ) : selected === "$value" ? (
        <Literal
          value={value.value}
          label={`${title}: константа`}
          onChange={(v) => onChange({ value: v })}
        />
      ) : (
        <>
          {op?.parameters.includes("field") && (
            <FieldPicker
              value={String(value.field || "")}
              operation={op.name}
              context={context}
              onChange={(field) => onChange({ ...value, field })}
            />
          )}
          {op?.parameters.includes("value") && (
            <Literal
              value={value.value ?? ""}
              label={`${title}: значение (${typeLabel(op.parameter_types.value)})`}
              onChange={(v) => onChange({ ...value, value: v })}
            />
          )}
          {op?.parameters.includes("args") && (
            <div className={styles.children}>
              {args.map(
                (arg, i) =>
                  object(arg) && (
                    <div key={i}>
                      <NodeEditor
                        value={arg}
                        kind={kind}
                        context={context}
                        previous={previous}
                        path={`${path}.args[${i}]`}
                        onChange={(v) =>
                          onChange({
                            ...value,
                            args: args.map((n, j) => (j === i ? v : n)),
                          })
                        }
                      />
                      <Button
                        disabled={args.length <= (op.arity?.min ?? 1)}
                        onClick={() =>
                          onChange({
                            ...value,
                            args: args.filter((_, j) => j !== i),
                          })
                        }
                      >
                        Убрать аргумент {i + 1}
                      </Button>
                    </div>
                  ),
              )}
              <Button
                disabled={
                  op.arity?.max !== null &&
                  op.arity?.max !== undefined &&
                  args.length >= op.arity.max
                }
                onClick={() =>
                  onChange({
                    ...value,
                    args: [
                      ...args,
                      isExpr ? { value: 1 } : { op: "exists", field: "" },
                    ],
                  })
                }
              >
                Добавить аргумент
              </Button>
            </div>
          )}
        </>
      )}
    </fieldset>
  );
}

export function ConditionControl({
  value,
  context,
  path,
  onChange,
}: {
  value: Json;
  context: Context;
  path: string;
  onChange: (v: Json) => void;
}) {
  return (
    <div
      className={styles.node}
      id={path === "where" ? "definition-where" : undefined}
    >
      <h4>
        {path === "where"
          ? "Отбор событий для всего агрегата"
          : path === "feature.where"
            ? "Отбор событий признака"
            : "Отбор событий этого входа"}{" "}
      </h4>
      <p className={styles.notice}>
        {path === "where"
          ? "Сначала окно по ECS @timestamp, затем это условие для всех признаков. Например, event.category=authentication; отдельный счётчик может дополнительно отбирать event.action=logon."
          : "Сначала отбор событий, затем агрегация и формула. Это не порог числового результата."}
      </p>
      {object(value) ? (
        <>
          <NodeEditor
            value={value}
            context={context}
            kind="condition"
            previous={[]}
            path={path}
            onChange={onChange}
          />
          <Button onClick={() => onChange(null)}>Убрать условие</Button>
        </>
      ) : (
        <Button onClick={() => onChange({ op: "exists", field: "" })}>
          Добавить условие
        </Button>
      )}
    </div>
  );
}
export function AggregateLeaf({
  value,
  context,
  path,
  onChange,
  hideOperation = false,
}: {
  value: JsonObject;
  context: Context;
  path: string;
  onChange: (v: JsonObject) => void;
  hideOperation?: boolean;
}) {
  const catalog = context.operators.filter(
    (o) => o.kind === "feature" && !["derive", "formula"].includes(o.name),
  );
  const op = catalog.find((o) => o.name === value.op);
  return (
    <div className={styles.node}>
      {!hideOperation && (
        <OperationSelect
          label="Агрегирующая операция"
          name={`${nodeLabel(path)}: агрегация`}
          value={String(value.op)}
          options={catalog}
          onChange={(name) =>
            onChange({
              ...defaultNode(catalog.find((o) => o.name === name)!),
              ...(value.where != null ? { where: value.where } : {}),
            })
          }
        />
      )}
      {!context.inputsSeparate && op?.parameters.includes("field") && (
        <FieldPicker
          context={context}
          operation={op.name}
          value={String(value.field || "")}
          onChange={(field) => onChange({ ...value, field })}
        />
      )}
      {op?.parameters.includes("percentile") && (
        <Literal
          label={`${nodeLabel(path)}: перцентиль (0…1)`}
          value={value.percentile ?? parseAggregate("0.95")}
          onChange={(percentile) => onChange({ ...value, percentile })}
        />
      )}
      <ConditionControl
        value={value.where ?? null}
        context={context}
        path={`${path}.where`}
        onChange={(where) => onChange({ ...value, where })}
      />
    </div>
  );
}
