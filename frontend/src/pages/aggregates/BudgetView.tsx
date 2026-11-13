import { exact, type JsonObject } from "../../app/api/aggregateJson";
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
}: {
  budget: JsonObject;
  title?: string;
}) {
  const resources = Object.entries(budget).filter(
    ([key]) => key !== "time_guarantee",
  );
  return (
    <section className={styles.budget} aria-label={title}>
      <h4>{title}</h4>
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
  );
}
