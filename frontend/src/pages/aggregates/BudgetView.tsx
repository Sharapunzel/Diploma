import { exact, type JsonObject } from "../../app/api/aggregateJson";
import type {
  ExecutionDiagnostics,
  ExecutionPolicy,
} from "../../app/api/aggregates";
import { executionThresholds } from "../../features/aggregates/performance";
import { reasonText } from "../../features/aggregates/labels";
import styles from "./BudgetView.module.css";

const resourceLabels: Record<string, string> = {
  estimated_rows: "Записей в окне",
  source_rows: "Сохранённых записей источника",
  operator_weight: "Вес операций",
  cost: "Расчётная стоимость",
  vector_width: "Координат вектора",
  estimated_memory_bytes: "Оценка памяти, байт",
  exact_materialized_features: "Точных статистик",
  array_elements_limit: "Лимит элементов массивов",
  statement_columns: "Колонок расчётного плана",
  estimated_row_width_bytes: "Оценка размера записи, байт",
  work_mem_mb_per_operation: "Память на операцию, МиБ",
  rcf_preliminary_bytes: "Предварительная память RCF, байт",
  window_seconds: "Длительность окна, с",
};

export function BudgetView({
  budget,
  title = "Оценка ресурсов",
  duration,
  policy = null,
  execution = null,
}: {
  budget?: JsonObject | null;
  title?: string;
  duration?: string | number;
  policy?: ExecutionPolicy | null;
  execution?: ExecutionDiagnostics | null;
}) {
  const thresholds =
    duration === undefined ? null : executionThresholds(duration);
  const hard =
    duration === undefined
      ? policy?.hard_budget_seconds
      : thresholds?.hardSeconds;
  const resources = Object.entries(budget ?? {}).filter(
    ([key]) => key !== "time_guarantee" && key !== "execution_policy",
  );
  return (
    <section className={styles.budget} aria-label={title}>
      <h4>{title}</h4>
      <section className={styles.section} aria-label="Пороги расчёта">
        <h5>Пороги</h5>
        <p>
          Мягкий ориентир полной задержки online: <strong>5 с</strong>.
          Превышение не отменяет расчёт и не ухудшает качество его данных.
        </p>
        <p>
          Жёсткий бюджет попытки:{" "}
          <strong>
            {hard === undefined
              ? "Укажите корректную длительность окна"
              : `${exact(hard)} с`}
          </strong>
          . T(D) = min(30 с, 0,8 × D); время до запуска задания учитывается
          отдельно.
        </p>
      </section>
      <section className={styles.section} aria-label="Оценка ресурсоёмкости">
        <h5>Оценка ресурсов</h5>
        {resources.length ? (
          <ul className={styles.resources} aria-label="Ресурсы расчёта">
            {resources.map(([key, value]) => (
              <li className={styles.resource} key={key}>
                <span>{resourceLabels[key] ?? `Показатель «${key}»`}:</span>{" "}
                <strong>
                  {typeof value === "boolean"
                    ? value
                      ? "Да"
                      : "Нет"
                    : exact(value)}
                </strong>
              </li>
            ))}
          </ul>
        ) : (
          <p className={styles.notice}>Оценка ресурсов не передана сервером.</p>
        )}
        <p className={styles.notice}>
          Это оценка ресурсоёмкости, а не гарантия своевременного расчёта при
          любой нагрузке.
        </p>
      </section>
      <section
        className={styles.section}
        aria-label="Последний фактический расчёт"
      >
        <h5>Последний фактический расчёт</h5>
        {execution ? (
          <>
            <p>
              <strong>
                {execution.calculation_status === "computed"
                  ? "Рассчитано"
                  : "Не рассчитано"}
              </strong>
              {execution.calculation_reason &&
                `: ${reasonText(execution.calculation_reason)}`}
            </p>
            <p>
              Измеренная часть попытки:{" "}
              <strong>{exact(execution.elapsed_seconds)} с</strong> (
              {
                {
                  before_commit:
                    "до сохранения результата; запись и её подтверждение не включены",
                  preview_complete: "предварительный расчёт завершён",
                  finalization:
                    "включая измеренную часть служебного завершения",
                }[execution.measured_through]
              }
              ).
            </p>
            <p>
              Время до запуска:{" "}
              {execution.queue_seconds === null
                ? "Ещё не измерено"
                : `${exact(execution.queue_seconds)} с`}
              .
            </p>
            {execution.origin === "online" ? (
              <>
                <p>
                  Полная online-задержка до подтверждения публикации:{" "}
                  {execution.online_latency_seconds === null
                    ? "Ещё не измерено"
                    : `${exact(execution.online_latency_seconds)} с`}
                  .
                </p>
                <p>
                  Мягкий ориентир:{" "}
                  {execution.soft_latency_exceeded === null
                    ? "Ещё не измерено"
                    : execution.soft_latency_exceeded
                      ? execution.calculation_status === "computed"
                        ? "Превышен; вычисленные значения сохраняют прежнюю пригодность"
                        : "Превышен; исход окна указан выше"
                      : "Не превышен"}
                  .
                </p>
              </>
            ) : (
              <p className={styles.notice}>
                Для пробы и начальной истории показана длительность попытки.
                Возраст архивного окна не считается задержкой вычисления.
              </p>
            )}
          </>
        ) : (
          <p className={styles.notice}>
            Ещё не измерено. Оценка ресурсов не является прогнозом времени.
          </p>
        )}
      </section>
    </section>
  );
}
