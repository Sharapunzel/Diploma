import { useCallback, useState } from "react";
import { Alert, Button, Input, Modal, Radio } from "antd";
import type { AggregatesApi, Operator } from "../../app/api/aggregates";
import {
  object,
  printAggregate,
  type JsonObject,
} from "../../app/api/aggregateJson";
import {
  clone,
  definitions,
  formula,
  hasAggregate,
  referenceErrors,
  refs,
  ruleDocument,
  retainDefinitionOrder,
  assertVisualReferences,
} from "../../features/aggregates/rule";
import {
  AggregateLeaf,
  ConditionControl,
  InvalidContext,
  NodeEditor,
  defaultNode,
} from "./RuleNodes";
import styles from "./Aggregates.module.css";
import { FeatureList } from "./FeatureList";
import { ExpressionInputs } from "./ExpressionInputs";
import { FieldLabel, OperationSelect, operationLabel } from "./AggregateHelp";

type Props = {
  rule: JsonObject;
  operators: Operator[];
  api: AggregatesApi;
  source: string;
  onChange: (rule: JsonObject) => void;
  onInvalid: (id: string, invalid: boolean) => void;
  readOnly?: boolean;
};
export function RuleEditor({
  rule,
  operators,
  api,
  source,
  onChange,
  onInvalid,
  readOnly = false,
}: Props) {
  const [edit, setEdit] = useState<{
    group: "features" | "auxiliary";
    index: number;
    value: JsonObject;
    original: string;
    derived: boolean;
    alternate: JsonObject | null;
  } | null>(null);
  const [invalid, setInvalid] = useState(new Set<string>());
  const [error, setError] = useState("");
  const modalInvalid = useCallback(
    (id: string, bad: boolean) =>
      setInvalid((old) => {
        const next = new Set(old);
        if (bad) next.add(id);
        else next.delete(id);
        return next;
      }),
    [],
  );
  const context = { api, source, operators };
  const all = definitions(rule);
  const changeRule = (next: JsonObject) => {
    assertVisualReferences(rule, next);
    retainDefinitionOrder(rule, next, true);
    onChange(next);
  };
  const open = (
    group: "features" | "auxiliary",
    index: number,
    value: JsonObject,
    original: string,
    derived: boolean,
  ) => {
    setInvalid(new Set());
    setError("");
    setEdit({
      group,
      index,
      value: clone(value),
      original,
      derived,
      alternate: null,
    });
  };
  const update = (value: JsonObject) =>
    setEdit((e) => (e ? { ...e, value } : e));
  const add = (derived: boolean) => {
    let n = 1;
    while (all.some((f) => f.key === `feature_${n}`)) n++;
    open(
      "features",
      -1,
      {
        key: `feature_${n}`,
        name: "Новый признак",
        op: derived ? "formula" : "count",
        ...(derived ? { expr: { ref: String(all[0]?.key ?? "") } } : {}),
      },
      "",
      derived,
    );
  };
  const changeType = (derived: boolean) => {
    if (!edit || invalid.size || derived === edit.derived) return;
    const value =
      edit.alternate ??
      (derived
        ? {
            op: "formula",
            expr: {
              ref: String(all.find((f) => f.key !== edit.value.key)?.key ?? ""),
            },
          }
        : { op: "count" });
    const next: JsonObject = clone(value);
    if (edit.value.where != null) next.where = clone(edit.value.where);
    else delete next.where;
    setInvalid(new Set());
    setError("");
    setEdit({
      ...edit,
      derived,
      alternate: edit.value,
      value: { ...next, key: edit.value.key, name: edit.value.name },
    });
  };
  const apply = () => {
    if (!edit || invalid.size) return;
    const next = clone(rule),
      current = next[edit.group],
      list = Array.isArray(current) ? [...current] : [];
    if (edit.index < 0) list.push(edit.value);
    else list[edit.index] = edit.value;
    next[edit.group] = list;
    try {
      ruleDocument(printAggregate(next), operators);
      const errors = referenceErrors(next);
      if (!String(edit.value.name ?? "").trim())
        errors.push("Укажите название");
      if (edit.derived !== refs(edit.value.expr ?? null).length > 0)
        errors.push(
          edit.derived
            ? "Производный признак должен ссылаться на именованный признак"
            : "Первичный признак не содержит ссылок",
        );
      if (errors.length) throw new Error(errors.join("; "));
      changeRule(next);
      setEdit(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Проверьте признак");
    }
  };
  return (
    <div className={styles.stack}>
      <p className={styles.notice}>
        Вектор содержит{" "}
        {Array.isArray(rule.features) ? rule.features.length : 0} координат.
      </p>
      <FeatureList
        rule={rule}
        readOnly={readOnly}
        onChange={changeRule}
        onOpen={open}
      />
      {!readOnly && (
        <>
          <div className={styles.toolbar}>
            <Button onClick={() => add(false)}>Добавить признак</Button>
          </div>
          <InvalidContext.Provider value={onInvalid}>
            <ConditionControl
              value={rule.where ?? null}
              context={context}
              path="where"
              onChange={(where) => changeRule({ ...rule, where })}
            />
          </InvalidContext.Provider>
        </>
      )}
      {readOnly && rule.where != null && (
        <section className={styles.node}>
          <h4>Общий отбор событий</h4>
          <pre className={styles.code}>{printAggregate(rule.where, true)}</pre>
        </section>
      )}
      <Modal
        open={!!edit}
        className={styles.featureModal}
        width="min(1100px, calc(100vw - 24px))"
        style={{
          top: 16,
          margin: "0 auto",
          height: "calc(100% - 32px)",
          maxWidth: "calc(100% - 24px)",
        }}
        title="Признак"
        onCancel={() => setEdit(null)}
        footer={
          readOnly ? null : (
            <>
              <Button onClick={() => setEdit(null)}>Отмена</Button>
              <Button
                type="primary"
                disabled={invalid.size > 0}
                onClick={apply}
              >
                Применить признак
              </Button>
            </>
          )
        }
        destroyOnHidden
      >
        {edit &&
          (readOnly ? (
            <div className={styles.modalBody}>
              <section className={styles.modalTile}>
                <h3>1. Настройки признака</h3>
                <dl>
                  <dt>Название</dt>
                  <dd>{String(edit.value.name)}</dd>
                  <dt>Ключ</dt>
                  <dd>{String(edit.value.key)}</dd>
                  <dt>Тип</dt>
                  <dd>{edit.derived ? "Производный" : "Первичный"}</dd>
                </dl>
              </section>
              <section className={styles.modalTile}>
                <h3>2. Входы</h3>
                <p>
                  {edit.derived
                    ? `Именованные входы: ${refs(edit.value.expr ?? null).join(", ")}`
                    : "События выбранного источника за одно окно."}
                </p>
              </section>
              <section className={styles.modalTile}>
                <h3>3. Операции и условия</h3>
                <pre className={styles.code}>
                  {printAggregate(edit.value, true)}
                </pre>
              </section>
              <section className={styles.modalTile}>
                <h3>4. Итоговая формула</h3>
                <p className={styles.formula} aria-label="Формула признака">
                  {formula(edit.value)}
                </p>
              </section>
            </div>
          ) : (
            <InvalidContext.Provider value={modalInvalid}>
              <div className={styles.modalBody}>
                <section className={styles.modalTile}>
                  <h3>1. Настройки признака</h3>
                  <div className={styles.grid}>
                    <div className={styles.field}>
                      <FieldLabel
                        required
                        help="Понятное название признака. Оно показывается в векторе и результатах расчёта."
                      >
                        Название
                      </FieldLabel>
                      <Input
                        aria-label="Название признака"
                        aria-required
                        value={String(edit.value.name)}
                        onChange={(e) =>
                          update({ ...edit.value, name: e.target.value })
                        }
                      />
                    </div>
                    <div className={styles.field}>
                      <FieldLabel
                        required
                        help="Уникальный идентификатор для ссылок в формулах: латинская строчная буква, затем буквы, цифры или подчёркивание, до 64 символов. Ключ применённого определения защищён от переименования."
                      >
                        Ключ
                      </FieldLabel>
                      <Input
                        aria-label="Ключ признака"
                        aria-required
                        disabled={!!edit.original}
                        value={String(edit.value.key)}
                        onChange={(e) =>
                          update({ ...edit.value, key: e.target.value })
                        }
                      />
                    </div>
                  </div>
                  <div className={styles.field}>
                    <FieldLabel help="Первичный признак использует события. Производный использует другие именованные признаки и может дополняться собственными ECS-входами.">
                      Тип признака
                    </FieldLabel>
                    <Radio.Group
                      disabled={invalid.size > 0}
                      aria-label="Тип признака"
                      value={edit.derived ? "derived" : "primary"}
                      onChange={(e) => changeType(e.target.value === "derived")}
                      options={[
                        { value: "primary", label: "Первичный" },
                        { value: "derived", label: "Производный" },
                      ]}
                    />
                  </div>
                </section>
                <section className={styles.modalTile}>
                  <h3>2. Входы</h3>
                  <p className={styles.notice}>
                    Входы заменяют формулу черновика либо добавляются к ней с
                    операцией «+». Арифметика выполняется над итогами одного
                    окна.
                  </p>
                  <ExpressionInputs
                    key={String(edit.derived)}
                    context={context}
                    references={
                      edit.derived
                        ? all
                            .filter((f) => f.key !== edit.value.key)
                            .map((f) => String(f.key))
                        : []
                    }
                    onUse={(expr) =>
                      update({
                        key: edit.value.key,
                        name: edit.value.name,
                        op: "formula",
                        expr,
                        ...(edit.value.where != null
                          ? { where: edit.value.where }
                          : {}),
                      })
                    }
                    onAppend={(input) => {
                      const { key, name } = edit.value;
                      const aggregate = Object.fromEntries(
                        Object.entries(edit.value).filter(([k]) =>
                          ["op", "field", "percentile"].includes(k),
                        ),
                      );
                      const previous = object(edit.value.expr)
                        ? edit.value.expr
                        : { aggregate };
                      update({
                        key,
                        name,
                        op: "formula",
                        expr: { op: "add", args: [previous, input] },
                        ...(edit.value.where != null
                          ? { where: edit.value.where }
                          : {}),
                      });
                    }}
                  />
                </section>
                <section className={styles.modalTile}>
                  <h3>3. Операции и условия</h3>
                  <p className={styles.notice}>
                    Настройте операции, их параметры и порядок вычислений.
                  </p>
                  <OperationSelect
                    label="Способ вычисления"
                    value={String(edit.value.op)}
                    options={operators
                      .filter(
                        (o) =>
                          o.kind === "feature" &&
                          (!edit.derived ||
                            ["formula", "derive"].includes(o.name)),
                      )
                      .map((o) => ({
                        name: o.name,
                        label: operationLabel(o.name),
                      }))}
                    onChange={(op) =>
                      update({
                        ...defaultNode(
                          operators.find(
                            (o) => o.kind === "feature" && o.name === op,
                          )!,
                        ),
                        key: edit.value.key,
                        name: edit.value.name,
                        ...(edit.value.where != null
                          ? { where: edit.value.where }
                          : {}),
                      })
                    }
                  />
                  {["formula", "derive"].includes(String(edit.value.op)) &&
                  object(edit.value.expr) ? (
                    <>
                      <NodeEditor
                        value={edit.value.expr}
                        kind="expression"
                        context={{
                          ...context,
                          aggregateLeaves: edit.value.op === "formula",
                        }}
                        previous={
                          edit.derived
                            ? all
                                .filter((f) => f.key !== edit.value.key)
                                .map((f) => String(f.key))
                            : []
                        }
                        path="expr"
                        onChange={(expr) => update({ ...edit.value, expr })}
                      />
                      {hasAggregate(edit.value.expr) && (
                        <ConditionControl
                          value={edit.value.where ?? null}
                          context={context}
                          path="feature.where"
                          onChange={(where) => update({ ...edit.value, where })}
                        />
                      )}
                      {!hasAggregate(edit.value.expr) &&
                        edit.value.where != null && (
                          <Alert
                            type="warning"
                            message="У формулы нет собственных агрегирующих листьев: это условие недопустимо и не пересчитывает зависимости."
                            action={
                              <Button
                                onClick={() =>
                                  update({ ...edit.value, where: null })
                                }
                              >
                                Убрать недействующее условие
                              </Button>
                            }
                          />
                        )}
                    </>
                  ) : (
                    <AggregateLeaf
                      value={edit.value}
                      context={context}
                      path="feature"
                      onChange={(v) =>
                        update({
                          ...v,
                          key: edit.value.key,
                          name: edit.value.name,
                        })
                      }
                    />
                  )}
                </section>
                <section className={styles.modalTile}>
                  <h3>4. Итоговая формула</h3>
                  <p className={styles.formula} aria-label="Формула признака">
                    {formula(edit.value)}
                  </p>
                  <p>
                    Символическая формула. Численные значения выходов доступны
                    после предварительного расчёта на сервере.
                  </p>
                </section>
                {error && (
                  <Alert
                    type="error"
                    role="alert"
                    message={error}
                    closable
                    onClose={() => setError("")}
                  />
                )}
              </div>
            </InvalidContext.Provider>
          ))}
      </Modal>
    </div>
  );
}
