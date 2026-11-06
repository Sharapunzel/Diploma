import { useEffect, useState } from "react";
import { Button, Empty, Pagination, Select, Spin } from "antd";
import { observer } from "mobx-react-lite";
import type { AggregatesApi, Aggregate, Point } from "../../app/api/aggregates";
import { exact } from "../../app/api/aggregateJson";
import { CollectionStore } from "../../features/aggregates/CollectionStore";
import { useAggregateScope } from "../../features/aggregates/useAggregateScope";
import { chartSeries, chartScale } from "../../features/aggregates/chart";
import { reasonText } from "../../features/aggregates/labels";
import { AggregateError } from "./AggregateError";
import { FeatureTable, Time, quality } from "./ResultView";
import styles from "./Aggregates.module.css";

export const PointsView = observer(
  ({
    aggregate,
    zone,
    refresh,
  }: {
    aggregate: Aggregate;
    zone: string;
    refresh: number;
  }) => {
    const { store } = useAggregateScope(
      (api: AggregatesApi) =>
        new CollectionStore((offset, signal) =>
          api.points(aggregate.id, offset, signal),
        ),
      aggregate.id,
    );
    const [feature, setFeature] = useState("");
    const [selected, setSelected] = useState<Point | null>(null);
    useEffect(() => {
      void store.load();
    }, [store, refresh]);
    const keys = [
      ...new Set(
        store.items.flatMap((p) => p.features?.map((f) => f.key) || []),
      ),
    ];
    const key = keys.includes(feature) ? feature : keys[0] || "";
    const series = chartSeries(store.items, key);
    const coords = series.flatMap((s) =>
      s.values.filter((v) => v.value !== null),
    );
    const numbers = coords.map((v) => v.value!);
    const scale = chartScale(numbers);
    const minTime = Math.min(
      ...store.items.map((p) => Date.parse(p.start_utc)),
    );
    const maxTime = Math.max(...store.items.map((p) => Date.parse(p.end_utc)));
    const x = (p: Point) =>
      30 +
      ((Date.parse(p.end_utc) - minTime) / Math.max(1, maxTime - minTime)) *
        640;
    const y = (v: number) => scale!.position(v);
    return (
      <section className={styles.page} aria-label="Сохранённые точки">
        <h2>Сохранённые точки</h2>
        <p>
          График загруженной страницы: {store.items.length} точек, позиции{" "}
          {store.total ? store.offset + 1 : 0}–
          {store.offset + store.items.length} из {store.total}. Весь ряд
          автоматически не загружается.
        </p>
        <Button
          aria-label="Обновить точки"
          loading={store.loading}
          disabled={store.loading}
          onClick={() => void store.load()}
        >
          Обновить точки
        </Button>
        {store.error !== null && (
          <AggregateError
            error={store.error}
            onRetry={() => void store.load()}
          />
        )}
        {!store.loaded && store.loading ? (
          <Spin />
        ) : !store.items.length ? (
          <Empty description="Сохранённых точек пока нет" />
        ) : (
          <>
            <p>
              Охваченный технический диапазон:{" "}
              <Time
                value={store.items.reduce(
                  (a, p) => (p.start_utc < a ? p.start_utc : a),
                  store.items[0].start_utc,
                )}
                zone={zone}
              />{" "}
              —{" "}
              <Time
                value={store.items.reduce(
                  (a, p) => (p.end_utc > a ? p.end_utc : a),
                  store.items[0].end_utc,
                )}
                zone={zone}
              />
            </p>
            <label className={styles.field}>
              Признак графика
              <Select
                aria-label="Признак графика"
                value={key}
                options={keys.map((k) => ({ value: k, label: k }))}
                onChange={setFeature}
              />
            </label>
            <p>
              ● Рабочие, ■ исторические; контур — полнота не подтверждена.
              Пропуски и null разрывают линию. Координаты приближённые: близкие
              большие числа могут сливаться. Точные значения и причины доступны
              в таблице.
            </p>
            {scale ? (
              <figure
                className={styles.chartFigure}
                aria-label="График загруженных точек"
              >
                <figcaption className={styles.chartLabels}>
                  <span>
                    Домен оси (приближённо): [{String(scale.low)},{" "}
                    {String(scale.high)}].
                  </span>
                  {scale.constant && (
                    <span>
                      На приближённой шкале значения совпадают: маркер
                      расположен посередине, нижняя и верхняя границы равны.
                    </span>
                  )}
                  <span>
                    Верхняя граница: {String(scale.high)}; нижняя граница:{" "}
                    {String(scale.low)}.
                  </span>
                </figcaption>
                <svg
                  className={styles.chart}
                  viewBox="0 0 700 200"
                  role="img"
                  aria-label={`График ${key} загруженной страницы`}
                  data-domain-low={scale.low}
                  data-domain-high={scale.high}
                >
                  <title>
                    Загруженная страница. Выбор точки доступен кнопками в
                    таблице.
                  </title>
                  <line
                    x1="30"
                    x2="680"
                    y1="175"
                    y2="175"
                    stroke="currentColor"
                  />
                  {series.map((s) => (
                    <g
                      key={s.kind}
                      stroke={
                        s.kind === "working" ? "var(--color-accent)" : "#9292ee"
                      }
                      fill="none"
                    >
                      {s.values.map(
                        (v, i) =>
                          v.value !== null && (
                            <g key={v.point.id}>
                              {v.join &&
                                i > 0 &&
                                s.values[i - 1].value !== null && (
                                  <line
                                    x1={x(s.values[i - 1].point)}
                                    y1={y(s.values[i - 1].value!)}
                                    x2={x(v.point)}
                                    y2={y(v.value)}
                                  />
                                )}
                              {s.kind === "working" ? (
                                <circle
                                  cx={x(v.point)}
                                  cy={y(v.value)}
                                  r="5"
                                  fill={v.valid ? "currentColor" : "none"}
                                />
                              ) : (
                                <rect
                                  x={x(v.point) - 5}
                                  y={y(v.value) - 5}
                                  width="10"
                                  height="10"
                                  fill={v.valid ? "currentColor" : "none"}
                                />
                              )}
                            </g>
                          ),
                      )}
                    </g>
                  ))}
                </svg>
                <div
                  className={styles.chartLabels}
                  aria-label="Границы временной оси"
                >
                  <div>
                    Начало оси:{" "}
                    <Time value={new Date(minTime).toISOString()} zone={zone} />
                  </div>
                  <div>
                    Конец оси:{" "}
                    <Time value={new Date(maxTime).toISOString()} zone={zone} />
                  </div>
                </div>
              </figure>
            ) : (
              <p role="status">
                Нет числовых значений для графика выбранного признака. Точные
                сведения доступны в таблице.
              </p>
            )}
            <div
              className={styles.scroll}
              tabIndex={0}
              role="region"
              aria-label="Таблица точек"
            >
              <table className={`${styles.table} ${styles.pointsTable}`}>
                <thead>
                  <tr>
                    <th>Точка</th>
                    <th>Техническое окно / плановый расчёт</th>
                    <th>Полнота / основание</th>
                    <th>События</th>
                    <th>Значения</th>
                  </tr>
                </thead>
                <tbody>
                  {store.items.map((p) => (
                    <tr key={p.id}>
                      <td>
                        <Button
                          aria-label={`Выбрать точку ${p.id}`}
                          onClick={() => setSelected(p)}
                        >
                          {p.kind === "working"
                            ? "● Рабочая"
                            : "■ Историческая"}
                        </Button>
                        <code>{p.id}</code>
                      </td>
                      <td>
                        [<Time value={p.start_utc} zone={zone} />,{" "}
                        <Time value={p.end_utc} zone={zone} />)
                        <p>
                          План: <Time value={p.scheduled_at} zone={zone} />
                        </p>
                      </td>
                      <td>
                        {quality[p.completeness] || p.completeness}:{" "}
                        {reasonText(p.completeness_reason)}
                        <p>{p.time_basis}</p>
                      </td>
                      <td>{exact(p.event_count)}</td>
                      <td>
                        <FeatureTable features={p.features} />
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </>
        )}
        <Pagination
          pageSize={50}
          current={store.offset / 50 + 1}
          total={store.total}
          showSizeChanger={false}
          onChange={(p) => {
            setSelected(null);
            void store.load((p - 1) * 50);
          }}
        />
        {selected && (
          <section aria-label="Подробности выбранной точки">
            <h3>Точка {selected.id}</h3>
            <p>
              Вид: {selected.kind}, основание: {selected.time_basis}
            </p>
            <p>
              Начало расчёта:{" "}
              <Time value={selected.actual_started_at} zone={zone} />;
              завершение:{" "}
              <Time value={selected.actual_completed_at} zone={zone} />
            </p>
            <p>
              Диапазон ECS @timestamp:{" "}
              <Time value={selected.event_time_min} zone={zone} /> —{" "}
              <Time value={selected.event_time_max} zone={zone} />
            </p>
            <FeatureTable features={selected.features} />
          </section>
        )}
      </section>
    );
  },
);
