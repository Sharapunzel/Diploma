import { useMemo, useState } from "react";
import { Alert, Button, Switch } from "antd";
import { DeleteOutlined, EditOutlined, EyeOutlined } from "@ant-design/icons";
import { object, type JsonObject } from "../../app/api/aggregateJson";
import {
  definitions,
  definitionOrder,
  assertVisualReferences,
  formula,
  moveDefinition,
  refs,
} from "../../features/aggregates/rule";
import styles from "./Aggregates.module.css";
import { Help } from "./AggregateHelp";

export function FeatureList({
  rule,
  readOnly,
  onChange,
  onOpen,
}: {
  rule: JsonObject;
  readOnly: boolean;
  onChange: (rule: JsonObject) => void;
  onOpen: (
    group: "features" | "auxiliary",
    index: number,
    value: JsonObject,
    original: string,
    derived: boolean,
  ) => void;
}) {
  const rows = useMemo(
    () =>
      (["features", "auxiliary"] as const).flatMap((group) => {
        const list = Array.isArray(rule[group]) ? rule[group] : [];
        return list.map((value, index) => ({
          group,
          index,
          value,
          id:
            object(value) && typeof value.key === "string"
              ? value.key
              : `${group}-${index}`,
        }));
      }),
    [rule],
  );
  const order = definitionOrder(rule);
  const [failure, setFailure] = useState<{
    rule: JsonObject;
    message: string;
  } | null>(null);
  const rank = (id: string) =>
    order.includes(id)
      ? order.indexOf(id)
      : order.length + rows.findIndex((row) => row.id === id);
  const all = definitions(rule);
  return (
    <section className={styles.featureList} aria-label="Признаки">
      {failure?.rule === rule && (
        <Alert
          role="alert"
          type="error"
          style={{ gridColumn: "1 / -1" }}
          message={failure.message}
          closable
          onClose={() => setFailure(null)}
        />
      )}
      {rows.length === 0 && (
        <p className={styles.notice}>
          Признаков пока нет. Добавьте признак для расчёта.
        </p>
      )}
      {[...rows]
        .sort((a, b) => rank(a.id) - rank(b.id))
        .map(({ group, index, value: f, id }) => {
          if (!object(f))
            return (
              <Alert
                key={id}
                type="error"
                message="Нераспознанное определение — исправьте JSON"
              />
            );
          const dependencies = all.filter((n) =>
            refs(n.expr ?? null).includes(String(f.key)),
          );
          return (
            <article
              key={id}
              className={styles.featureRow}
              id={`definition-${group}-${index}`}
            >
              <div className={styles.featureHeading}>
                <strong className={styles.featureTitle}>
                  {String(f.name)}
                </strong>
                <div className={styles.featureActions}>
                  <span className={styles.outputSwitch}>
                    <Switch
                      size="small"
                      disabled={readOnly}
                      checked={group === "features"}
                      aria-label={`${String(f.key)}: включён в выход`}
                      onChange={(enabled) => {
                        try {
                          const next = moveDefinition(
                            rule,
                            String(f.key),
                            enabled,
                            order,
                          );
                          onChange(next);
                          setFailure(null);
                        } catch (error) {
                          setFailure({
                            rule,
                            message:
                              error instanceof Error
                                ? error.message
                                : "Не удалось изменить состав выхода.",
                          });
                        }
                      }}
                    />
                    <Help
                      name={`Выход признака ${String(f.key)}`}
                      text={`Включённый признак добавляет координату в выходной вектор. Выключенный остаётся доступен в формулах других признаков и вычисляется, если нужен им.${readOnly ? " Сохранённый состав неизменяем: для изменения создайте копию агрегата." : " Хотя бы один признак должен входить в выходной вектор."}`}
                    />
                  </span>
                  <Button
                    className={styles.featureEdit}
                    data-definition-open
                    size="small"
                    aria-label={`${String(f.name)} · ${String(f.key)}`}
                    onClick={() =>
                      onOpen(
                        group,
                        index,
                        f,
                        String(f.key),
                        refs(f.expr ?? null).length > 0,
                      )
                    }
                  >
                    <span className={styles.featureEditIcon}>
                      {readOnly ? <EyeOutlined /> : <EditOutlined />}
                    </span>
                    <span className={styles.featureEditLabel}>
                      {readOnly ? "Просмотреть" : "Изменить"}
                    </span>
                  </Button>
                  {!readOnly && (
                    <Button
                      size="small"
                      danger
                      icon={<DeleteOutlined />}
                      disabled={dependencies.length > 0}
                      aria-label={`Удалить признак ${String(f.key)}`}
                      onClick={() => {
                        const list = Array.isArray(rule[group])
                          ? rule[group]
                          : [];
                        const next = {
                          ...rule,
                          [group]: list.filter((_, i) => i !== index),
                        };
                        if (group === "auxiliary" && list.length === 1)
                          delete next.auxiliary;
                        try {
                          assertVisualReferences(rule, next);
                          onChange(next);
                          setFailure(null);
                        } catch (error) {
                          setFailure({
                            rule,
                            message:
                              error instanceof Error
                                ? error.message
                                : "Не удалось удалить признак.",
                          });
                        }
                      }}
                    />
                  )}
                </div>
              </div>
              <p className={styles.featureDetail}>
                {group === "auxiliary" && "Вспомогательный признак · "}
                {refs(f.expr ?? null).length
                  ? "Производный"
                  : "Первичный"} · {String(f.key)} ·{" "}
                <span className={styles.formula}>{formula(f)}</span>
              </p>
              {dependencies.length > 0 && !readOnly && (
                <p className={styles.featureDetail}>
                  Используется:{" "}
                  {dependencies.map((n) => String(n.name)).join(", ")}. Для
                  удаления сначала исправьте ссылки.
                </p>
              )}
            </article>
          );
        })}
    </section>
  );
}
