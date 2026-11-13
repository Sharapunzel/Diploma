import { Table } from "antd";
import type { ColumnsType } from "antd/es/table";
import type { FeatureValue, Preview } from "../../app/api/aggregates";
import { exact } from "../../app/api/aggregateJson";
import { displayTime } from "../../features/aggregates/time";
import { reasonText, timeBases } from "../../features/aggregates/labels";
import { BudgetView } from "./BudgetView";
import styles from "./ResultView.module.css";

export const quality: Record<string, string> = {
  complete: "Полнота подтверждена",
  unknown: "Полнота неизвестна",
  interrupted: "Обработка прервана",
  unverified: "Полнота не проверена",
};
export function Time({ value, zone }: { value: string | null; zone: string }) {
  return (
    <span className={styles.time}>
      {displayTime(value, zone)}
      {value && <code>{value}</code>}
    </span>
  );
}
export function FeatureTable({
  features,
}: {
  features: FeatureValue[] | null;
}) {
  if (!features)
    return (
      <p className={styles.notice}>
        У этой старой точки отсутствуют доступные значения признаков.
      </p>
    );
  const columns: ColumnsType<FeatureValue> = [
    {
      title: "Признак",
      dataIndex: "key",
      key: "key",
      width: 180,
      rowScope: "row",
    },
    {
      title: "Точное значение",
      key: "value",
      width: 200,
      render: (_, feature) => exact(feature.value),
    },
    {
      title: "Причина",
      key: "reason",
      width: 250,
      render: (_, feature) => reasonText(feature.reason),
    },
    {
      title: "Корректных",
      key: "valid",
      width: 140,
      render: (_, feature) => exact(feature.valid_count),
    },
    {
      title: "Некорректных",
      key: "invalid",
      width: 150,
      render: (_, feature) => exact(feature.invalid_count),
    },
  ];
  return (
    <div
      className={styles.scroll}
      tabIndex={0}
      role="region"
      aria-label="Значения признаков"
    >
      <Table<FeatureValue>
        className={styles.table}
        rowKey="key"
        tableLayout="auto"
        columns={columns}
        dataSource={features}
        pagination={false}
        scroll={{ x: 920 }}
        locale={{ emptyText: "Выходные значения отсутствуют" }}
      />
    </div>
  );
}
export function ResultView({
  result,
  zone,
  showBudget = true,
}: {
  result: Preview;
  zone: string;
  showBudget?: boolean;
}) {
  return (
    <section className={styles.result} aria-label="Предварительный результат">
      <h3>Предварительный расчёт</h3>
      <p className={styles.notice}>
        Пробный расчёт одного завершённого окна расписания. Результат не создаёт
        агрегат или сохранённую точку и не подтверждает полноту рабочего окна.
      </p>
      <div className={styles.overview}>
        <section className={styles.tile}>
          <h4>Расчётное окно</h4>
          <dl className={styles.facts}>
            <div className={styles.fact}>
              <dt>Начало окна, включительно</dt>
              <dd>
                <Time value={result.start_utc} zone={zone} />
              </dd>
            </div>
            <div className={styles.fact}>
              <dt>Конец окна, не включается</dt>
              <dd>
                <Time value={result.end_utc} zone={zone} />
              </dd>
            </div>
            <div className={styles.fact}>
              <dt>Момент закрытия окна по расписанию</dt>
              <dd>
                <Time value={result.scheduled_at} zone={zone} />
              </dd>
            </div>
          </dl>
        </section>
        <section className={styles.tile}>
          <h4>Состояние данных</h4>
          <p className={styles.quality}>
            {quality[result.completeness] || result.completeness}
          </p>
          <p>{reasonText(result.completeness_reason)}</p>
          <dl className={styles.facts}>
            <div className={styles.fact}>
              <dt>Временное основание</dt>
              <dd>{timeBases[result.time_basis] ?? result.time_basis}</dd>
            </div>
            <div className={styles.fact}>
              <dt>Наблюдалось записей</dt>
              <dd>{exact(result.observed_rows)}</dd>
            </div>
            <div className={styles.fact}>
              <dt>Отобрано событий</dt>
              <dd>{exact(result.event_count)}</dd>
            </div>
          </dl>
          <p className={styles.notice}>
            Отсутствие отобранных событий само по себе не доказывает непрерывную
            работу источника.
          </p>
        </section>
        <section className={styles.tile}>
          <h4>Время возникновения событий</h4>
          <p className={styles.notice}>
            Диапазон ECS @timestamp отличается от технического окна обработки.
          </p>
          <dl className={styles.facts}>
            <div className={styles.fact}>
              <dt>Самое раннее событие</dt>
              <dd>
                <Time value={result.event_time_min} zone={zone} />
              </dd>
            </div>
            <div className={styles.fact}>
              <dt>Самое позднее событие</dt>
              <dd>
                <Time value={result.event_time_max} zone={zone} />
              </dd>
            </div>
          </dl>
        </section>
      </div>
      <section className={styles.tile}>
        <h4>Значения признаков</h4>
        <FeatureTable features={result.features} />
      </section>
      {showBudget && (
        <div className={styles.tile}>
          <BudgetView budget={result.budget} />
        </div>
      )}
    </section>
  );
}
