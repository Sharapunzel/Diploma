import { Button, Select } from "antd";
import type { ReactNode } from "react";
import { object, type JsonObject } from "../../app/api/aggregateJson";
import { FieldPicker, type Context } from "./RuleNodes";
import styles from "./Aggregates.module.css";
import { FieldLabel } from "./AggregateHelp";

/** Inputs edit the same leaves rendered by the operation tree. */
export function ExpressionInputs({
  context,
  references,
  value,
  onChange,
}: {
  context: Context;
  references: string[];
  value: JsonObject;
  onChange: (value: JsonObject) => void;
}) {
  const input = (
    node: JsonObject,
    change: (next: JsonObject) => void,
    path: string,
  ): ReactNode => {
    if (object(node.aggregate))
      return input(
        node.aggregate,
        (aggregate) => change({ ...node, aggregate }),
        `${path} · итог`,
      );
    if (node.ref != null)
      return (
        <div className={styles.field} key={path}>
          <FieldLabel help="Готовый итог другого выходного или вспомогательного признака. Его собственные условия не меняются.">
            {path}: именованный вход
          </FieldLabel>
          <Select
            aria-label={`${path}: именованный вход`}
            value={String(node.ref)}
            options={references.map((key) => ({ value: key, label: key }))}
            onChange={(ref) => change({ ...node, ref })}
          />
        </div>
      );
    if (Array.isArray(node.args))
      return node.args.map((child, index) =>
        object(child) ? (
          <div key={`${path}.${index}`}>
            {input(
              child,
              (next) =>
                change({
                  ...node,
                  args: (node.args as JsonObject[]).map((entry, i) =>
                    i === index ? next : entry,
                  ),
                }),
              `${path} · аргумент ${index + 1}`,
            )}
          </div>
        ) : null,
      );
    const operation = context.operators.find(
      (entry) => entry.kind === "feature" && entry.name === node.op,
    );
    return operation?.parameters.includes("field") ? (
      <div key={path}>
        <p>{path}</p>
        <FieldPicker
          context={context}
          operation={operation.name}
          value={String(node.field ?? "")}
          onChange={(field) => change({ ...node, field })}
        />
      </div>
    ) : (
      <p className={styles.notice} key={path}>
        {node.op === "count"
          ? `${path}: количество событий — поле не требуется.`
          : `${path}: числовая константа, без поля источника.`}
      </p>
    );
  };
  return (
    <div className={styles.stack}>
      {object(value.expr)
        ? input(value.expr, (expr) => onChange({ ...value, expr }), "Вход")
        : input(value, onChange, "Вход")}
      <p className={styles.notice}>
        Состав входов следует дереву вычисления ниже. Для нескольких полей
        добавьте агрегирующие листья в формулу; каждое поле относится к своему
        листу.
      </p>
      <Button
        onClick={() => {
          const aggregate = Object.fromEntries(
            Object.entries(value).filter(([key]) =>
              ["op", "field", "percentile"].includes(key),
            ),
          );
          const previous = object(value.expr) ? value.expr : { aggregate };
          onChange({
            key: value.key,
            name: value.name,
            op: "formula",
            expr: {
              op: "add",
              args: [previous, { aggregate: { op: "count" } }],
            },
            ...(value.where != null ? { where: value.where } : {}),
          });
        }}
      >
        Добавить итог к формуле
      </Button>
    </div>
  );
}
