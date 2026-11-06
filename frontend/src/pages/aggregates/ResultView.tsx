import type { FeatureValue, Preview } from "../../app/api/aggregates";
import { exact, printAggregate } from "../../app/api/aggregateJson";
import { displayTime } from "../../features/aggregates/time";
import { reasonText } from "../../features/aggregates/labels";
import styles from "./Aggregates.module.css";

export const quality: Record<string, string> = {
  complete: "Полнота подтверждена",
  unknown: "Полнота неизвестна",
  interrupted: "Обработка прервана",
  unverified: "Полнота не проверена",
};
export function Time({ value, zone }: { value: string | null; zone: string }) {
  return (
    <span>
      {displayTime(value, zone)}
      {value && (
        <span>
          <br />
          <code>{value}</code>
        </span>
      )}
    </span>
  );
}
export function FeatureTable({
  features,
}: {
  features: FeatureValue[] | null;
}) {
  if (!features)
    return <p>У этой старой точки отсутствуют доступные значения признаков.</p>;
  return (
    <div
      className={styles.scroll}
      tabIndex={0}
      role="region"
      aria-label="Значения признаков"
    >
      <table className={styles.table}>
        <thead>
          <tr>
            <th>Признак</th>
            <th>Точное значение</th>
            <th>Причина</th>
            <th>Корректных</th>
            <th>Некорректных</th>
          </tr>
        </thead>
        <tbody>
          {features.map((f) => (
            <tr key={f.key}>
              <th scope="row">{f.key}</th>
              <td>{exact(f.value)}</td>
              <td>{reasonText(f.reason)}</td>
              <td>{exact(f.valid_count)}</td>
              <td>{exact(f.invalid_count)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
export function ResultView({
  result,
  zone,
}: {
  result: Preview;
  zone: string;
}) {
  return (
    <section aria-label="Предварительный результат">
      <h2>Предварительный расчёт</h2>
      <p>
        Техническое окно [<Time value={result.start_utc} zone={zone} />,{" "}
        <Time value={result.end_utc} zone={zone} />
        ). Плановый расчёт: <Time value={result.scheduled_at} zone={zone} />
      </p>
      <p>
        Временное основание: {result.time_basis}.{" "}
        {quality[result.completeness] || result.completeness}:{" "}
        {reasonText(result.completeness_reason)}
      </p>
      <p>
        Наблюдалось записей: {exact(result.observed_rows)}; отобрано:{" "}
        {exact(result.event_count)}. Нулевой count не доказывает работу
        источника.
      </p>
      <p>
        Время возникновения ECS @timestamp:{" "}
        <Time value={result.event_time_min} zone={zone} /> —{" "}
        <Time value={result.event_time_max} zone={zone} />. Этот диапазон
        отличается от технического окна обработки.
      </p>
      <FeatureTable features={result.features} />
      <details>
        <summary>Оценка бюджета</summary>
        <pre className={styles.code}>{printAggregate(result.budget, true)}</pre>
        <p>Оценка не гарантирует своевременность при любой нагрузке.</p>
      </details>
    </section>
  );
}
