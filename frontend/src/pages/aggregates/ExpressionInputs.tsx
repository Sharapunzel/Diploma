import { useState } from "react";
import { Button, Select } from "antd";
import type { JsonObject } from "../../app/api/aggregateJson";
import { FieldPicker, type Context } from "./RuleNodes";
import styles from "./Aggregates.module.css";
import { FieldLabel, OperationSelect } from "./AggregateHelp";

export function ExpressionInputs({
  context,
  references,
  onUse,
  onAppend,
}: {
  context: Context;
  references: string[];
  onUse: (input: JsonObject) => void;
  onAppend: (input: JsonObject) => void;
}) {
  const [operation, setOperation] = useState("count"),
    [field, setField] = useState(""),
    [ref, setRef] = useState("");
  const base = context.operators.filter(
    (o) => o.kind === "feature" && !["derive", "formula"].includes(o.name),
  );
  const needsField = base
    .find((o) => o.name === operation)
    ?.parameters.includes("field");
  const input: JsonObject = {
    aggregate: {
      op: operation,
      ...(needsField ? { field } : {}),
      ...(operation === "percentile" ? { percentile: 0.95 } : {}),
    },
  };
  return (
    <div className={styles.stack}>
      <OperationSelect
        label="Вход из событий"
        name="Операция входа"
        value={operation}
        options={base}
        onChange={setOperation}
      />
      {needsField && (
        <FieldPicker
          context={context}
          operation={operation}
          value={field}
          onChange={setField}
        />
      )}
      <div className={styles.toolbar}>
        <Button disabled={!!needsField && !field} onClick={() => onUse(input)}>
          Использовать итог окна
        </Button>
        <Button
          disabled={!!needsField && !field}
          onClick={() => onAppend(input)}
        >
          Добавить итог к формуле
        </Button>
      </div>
      {!!references.length && (
        <>
          <div className={styles.field}>
            <FieldLabel help="Используйте готовый итог другого признака, включая внутренние определения. Его отбор не изменится от условий текущего признака.">
              Именованный вход
            </FieldLabel>
            <Select
              aria-label="Именованный вход"
              value={ref || undefined}
              placeholder="Выберите определение"
              options={references.map((key) => ({ value: key, label: key }))}
              onChange={setRef}
            />
          </div>
          <div className={styles.toolbar}>
            <Button disabled={!ref} onClick={() => onUse({ ref })}>
              Использовать ссылку
            </Button>
            <Button disabled={!ref} onClick={() => onAppend({ ref })}>
              Добавить ссылку к формуле
            </Button>
          </div>
        </>
      )}
    </div>
  );
}
