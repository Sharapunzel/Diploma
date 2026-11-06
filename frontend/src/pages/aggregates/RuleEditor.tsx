import { createContext, useContext, useEffect, useId, useState } from "react";
import { Button, Input, Select, Alert, Pagination, Spin } from "antd";
import type {
  AggregatesApi,
  AggregateField,
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

type Context = { operators: Operator[]; api: AggregatesApi; source: string };
const InvalidContext = createContext<(id: string, invalid: boolean) => void>(
  () => {},
);
function FieldPicker({
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
  const [offset, setOffset] = useState(0);
  const [items, setItems] = useState<AggregateField[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<unknown>(null);
  useEffect(() => {
    const controller = new AbortController();
    setItems([]);
    if (!context.source) return;
    setLoading(true);
    setError(null);
    const timer = setTimeout(() => {
      void context.api
        .fields(context.source, query, offset, controller.signal)
        .then((page) => {
          if (!controller.signal.aborted) {
            setItems(page.items);
            setTotal(page.total);
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
  }, [context.api, context.source, query, offset]);
  return (
    <div className={styles.field}>
      <label>
        Поле ECS
        <Select
          aria-label="Поле ECS"
          value={value || undefined}
          showSearch
          filterOption={false}
          onSearch={(q) => {
            setQuery(q);
            setOffset(0);
          }}
          loading={loading}
          onChange={onChange}
          options={[
            ...(value && !items.some((f) => f.name === value)
              ? [
                  {
                    value,
                    label: `${value} (проверьте совместимость на сервере)`,
                  },
                ]
              : []),
            ...items.map((f) => ({
              value: f.name,
              label: `${f.name} · ${f.type}${f.is_array ? "[]" : ""}${!f.supported ? ` — ${f.reason || "не поддерживается"}` : !f.operators.includes(operation) ? " — несовместимо" : ""}`,
              disabled: !f.supported || !f.operators.includes(operation),
            })),
          ]}
        />
      </label>
      <Pagination
        simple
        size="small"
        current={offset / 100 + 1}
        total={total}
        pageSize={100}
        showSizeChanger={false}
        onChange={(p) => setOffset((p - 1) * 100)}
      />
      {error !== null && <AggregateError error={error} />}
      <span className={styles.notice}>
        Каталог ECS 9.4.0, не перечень наблюдавшихся данных. Поиск и страницы
        охватывают весь каталог.
      </span>
    </div>
  );
}
function Literal({
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
    <label className={styles.field}>
      {label}
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
      <span className={styles.notice}>
        Числа сохраняются точно: 1 и 1.0 различаются. Даты — строки с явным
        смещением.
      </span>
    </label>
  );
}
function defaultNode(op: Operator): JsonObject {
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
function NodeEditor({
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
    value.ref != null
      ? "$ref"
      : value.value != null && !value.op
        ? "$value"
        : String(value.op);
  const catalog = context.operators.filter((o) => o.kind === kind);
  const op = catalog.find((o) => o.name === selected);
  const args = Array.isArray(value.args) ? value.args : [];
  return (
    <fieldset className={styles.node}>
      <legend>{path}</legend>
      <label className={styles.field}>
        Операция
        <Select
          aria-label={`${path}: операция`}
          value={selected}
          onChange={(name) =>
            onChange(
              name === "$ref"
                ? { ref: previous[0] || "" }
                : name === "$value"
                  ? { value: 1 }
                  : defaultNode(catalog.find((o) => o.name === name)!),
            )
          }
          options={[
            ...(isExpr
              ? [
                  { value: "$ref", label: "Ссылка на предыдущий признак" },
                  { value: "$value", label: "Числовая константа" },
                ]
              : []),
            ...catalog.map((o) => ({
              value: o.name,
              label: `${o.name} (${o.result})`,
            })),
          ]}
        />
      </label>
      {selected === "$ref" ? (
        <label className={styles.field}>
          Предыдущий признак
          <Select
            aria-label={`${path}: ссылка`}
            value={String(value.ref)}
            options={previous.map((k) => ({ value: k, label: k }))}
            onChange={(ref) => onChange({ ref })}
          />
        </label>
      ) : selected === "$value" ? (
        <Literal
          value={value.value}
          label={`${path}: константа`}
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
              label={`${path}: значение (${op.parameter_types.value})`}
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
export function RuleEditor({
  rule,
  operators,
  api,
  source,
  onChange,
  onInvalid,
}: {
  rule: JsonObject;
  operators: Operator[];
  api: AggregatesApi;
  source: string;
  onChange: (rule: JsonObject) => void;
  onInvalid: (id: string, invalid: boolean) => void;
}) {
  const context = { operators, api, source };
  const features = Array.isArray(rule.features) ? rule.features : [];
  const change = (index: number, v: JsonObject) =>
    onChange({
      ...rule,
      features: features.map((f, i) => (i === index ? v : f)),
    });
  if (!operators.length)
    return <Spin aria-label="Загрузка каталога операций" />;
  return (
    <InvalidContext.Provider value={onInvalid}>
      <div>
        <h2>Правило агрегирования</h2>
        {object(rule.where) ? (
          <>
            <NodeEditor
              kind="condition"
              value={rule.where}
              context={context}
              previous={[]}
              path="where"
              onChange={(where) => onChange({ ...rule, where })}
            />
            <Button onClick={() => onChange({ ...rule, where: null })}>
              Убрать общее условие
            </Button>
          </>
        ) : (
          <Button
            onClick={() =>
              onChange({
                ...rule,
                where: { op: "all", args: [{ op: "exists", field: "" }] },
              })
            }
          >
            Добавить общее условие
          </Button>
        )}
        {features.map((feature, index) => {
          if (!object(feature)) return null;
          const op = operators.find(
            (o) => o.kind === "feature" && o.name === feature.op,
          );
          const previous = features
            .slice(0, index)
            .filter(object)
            .map((f) => String(f.key));
          return (
            <fieldset key={index} className={styles.node}>
              <legend>Признак {index + 1}</legend>
              <div className={styles.grid}>
                <label className={styles.field}>
                  Ключ
                  <Input
                    aria-label={`features[${index}].key`}
                    value={String(feature.key)}
                    onChange={(e) =>
                      change(index, { ...feature, key: e.target.value })
                    }
                  />
                </label>
                <label className={styles.field}>
                  Название
                  <Input
                    aria-label={`features[${index}].name`}
                    value={String(feature.name)}
                    onChange={(e) =>
                      change(index, { ...feature, name: e.target.value })
                    }
                  />
                </label>
                <label className={styles.field}>
                  Агрегация
                  <Select
                    aria-label={`features[${index}].op`}
                    value={String(feature.op)}
                    options={operators
                      .filter((o) => o.kind === "feature")
                      .map((o) => ({ value: o.name, label: o.name }))}
                    onChange={(name) =>
                      change(index, {
                        ...defaultNode(
                          operators.find(
                            (o) => o.kind === "feature" && o.name === name,
                          )!,
                        ),
                        key: feature.key,
                        name: feature.name,
                      })
                    }
                  />
                </label>
              </div>
              {op?.parameters.includes("field") && (
                <FieldPicker
                  context={context}
                  operation={op.name}
                  value={String(feature.field || "")}
                  onChange={(field) => change(index, { ...feature, field })}
                />
              )}
              {op?.parameters.includes("percentile") && (
                <Literal
                  label={`features[${index}].percentile (0…1)`}
                  value={feature.percentile ?? parseAggregate("0.95")}
                  onChange={(percentile) =>
                    change(index, { ...feature, percentile })
                  }
                />
              )}
              {feature.op === "derive" && object(feature.expr) ? (
                <NodeEditor
                  kind="expression"
                  value={feature.expr}
                  context={context}
                  previous={previous}
                  path={`features[${index}].expr`}
                  onChange={(expr) => change(index, { ...feature, expr })}
                />
              ) : (
                <>
                  {object(feature.where) ? (
                    <>
                      <NodeEditor
                        kind="condition"
                        value={feature.where}
                        context={context}
                        previous={[]}
                        path={`features[${index}].where`}
                        onChange={(where) =>
                          change(index, { ...feature, where })
                        }
                      />
                      <Button
                        onClick={() =>
                          change(index, { ...feature, where: null })
                        }
                      >
                        Убрать условие признака
                      </Button>
                    </>
                  ) : (
                    <Button
                      onClick={() =>
                        change(index, {
                          ...feature,
                          where: { op: "exists", field: "" },
                        })
                      }
                    >
                      Условие признака
                    </Button>
                  )}
                </>
              )}
              <div className={styles.toolbar}>
                <Button
                  disabled={!index}
                  onClick={() => {
                    const next = [...features];
                    [next[index - 1], next[index]] = [
                      next[index],
                      next[index - 1],
                    ];
                    onChange({ ...rule, features: next });
                  }}
                >
                  Выше
                </Button>
                <Button
                  disabled={index === features.length - 1}
                  onClick={() => {
                    const next = [...features];
                    [next[index], next[index + 1]] = [
                      next[index + 1],
                      next[index],
                    ];
                    onChange({ ...rule, features: next });
                  }}
                >
                  Ниже
                </Button>
                <Button
                  danger
                  disabled={features.length === 1}
                  onClick={() =>
                    onChange({
                      ...rule,
                      features: features.filter((_, i) => i !== index),
                    })
                  }
                >
                  Удалить признак
                </Button>
              </div>
            </fieldset>
          );
        })}
        <Button
          onClick={() =>
            onChange({
              ...rule,
              features: [
                ...features,
                {
                  key: `feature_${features.length + 1}`,
                  name: "Новый признак",
                  op: "count",
                },
              ],
            })
          }
        >
          Добавить признак
        </Button>
        <Alert
          type="info"
          message="Порядок задаёт доступные ссылки. Перестановка и удаление не переписывают ссылки; ошибки требуется исправить до отправки."
        />
      </div>
    </InvalidContext.Provider>
  );
}
