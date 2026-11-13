import { useEffect, useLayoutEffect, useRef, useState } from "react";
import { Button, Empty, Pagination, Select, Spin } from "antd";
import { observer } from "mobx-react-lite";
import type { AggregatesApi, Aggregate, Point } from "../../app/api/aggregates";
import { exact } from "../../app/api/aggregateJson";
import { CollectionStore } from "../../features/aggregates/CollectionStore";
import { useAggregateScope } from "../../features/aggregates/useAggregateScope";
import {
  chartSeries,
  chartScale,
  chartTimeScale,
} from "../../features/aggregates/chart";
import { displayTime, localCalendar } from "../../features/aggregates/time";
import { reasonText, timeBases } from "../../features/aggregates/labels";
import { AggregateError } from "./AggregateError";
import { FieldLabel } from "./AggregateHelp";
import { FeatureTable, Time, quality } from "./ResultView";
import styles from "./PointsView.module.css";

const pageSize = 3;
function featureWord(count: number) {
  const last = count % 10;
  const lastTwo = count % 100;
  return last === 1 && lastTwo !== 11
    ? "признак"
    : last >= 2 && last <= 4 && (lastTwo < 12 || lastTwo > 14)
      ? "признака"
      : "признаков";
}

export const PointsView = observer(
  ({
    aggregate,
    zone,
    refresh,
    historicalPoint = null,
    selectionKey = "",
  }: {
    aggregate: Aggregate;
    zone: string;
    refresh: number;
    historicalPoint?: Point | null;
    selectionKey?: string;
  }) => {
    const { store } = useAggregateScope(
      (api: AggregatesApi) =>
        new CollectionStore((offset, signal) =>
          api.points(aggregate.id, offset, signal, pageSize),
        ),
      aggregate.id,
    );
    const [feature, setFeature] = useState("");
    const plot = useRef<HTMLDivElement>(null);
    const [plotWidth, setPlotWidth] = useState(900);
    const [selectedPoint, setSelectedPoint] = useState<{
      point: Point;
      owner: typeof store;
      scope: string;
      offset: number;
    } | null>(null);
    const [historicalContext, setHistoricalContext] = useState<{
      point: Point;
      owner: typeof store;
      scope: string;
    } | null>(null);
    const previousHistorical = useRef<Point | null>(null);
    const previousSelectionKey = useRef(selectionKey);
    useEffect(() => {
      // Refreshing history does not change the separately calculated window.
      setSelectedPoint(null);
      void store.load();
    }, [store, refresh]);
    useEffect(() => {
      if (previousSelectionKey.current !== selectionKey) {
        previousSelectionKey.current = selectionKey;
        setSelectedPoint(null);
        setHistoricalContext(null);
      }
    }, [selectionKey]);
    useEffect(() => {
      const changed = previousHistorical.current !== historicalPoint;
      previousHistorical.current = historicalPoint;
      if (changed && historicalPoint && selectionKey.trim()) {
        setHistoricalContext({
          point: historicalPoint,
          owner: store,
          scope: selectionKey,
        });
      } else if (!historicalPoint) {
        setHistoricalContext(null);
      }
    }, [historicalPoint, selectionKey, store]);
    const currentHistorical =
      historicalContext?.owner === store &&
      historicalContext.scope === selectionKey &&
      historicalContext.point === historicalPoint &&
      selectionKey.trim()
        ? historicalPoint
        : null;
    const selected =
      selectedPoint?.owner === store &&
      selectedPoint.scope === selectionKey &&
      selectedPoint.offset === store.offset
        ? selectedPoint.point
        : null;
    const selectPoint = (point: Point) =>
      setSelectedPoint({
        point,
        owner: store,
        scope: selectionKey,
        offset: store.offset,
      });
    const chartPoints = currentHistorical ? [currentHistorical] : [];
    const keys = [
      ...new Set(
        chartPoints.flatMap((p) => p.features?.map((f) => f.key) || []),
      ),
    ];
    const key = keys.includes(feature) ? feature : keys[0] || "";
    const series = chartSeries(chartPoints, key);
    const coords = series.flatMap((s) =>
      s.values.filter((v) => v.value !== null),
    );
    const numbers = coords.map((v) => v.value!);
    const scale = chartScale(numbers);
    const timeline = chartTimeScale(chartPoints, 12, plotWidth - 12);
    const hasPlot = !!(scale && timeline);
    useLayoutEffect(() => {
      const element = plot.current;
      if (!element) return;
      const update = (width: number) => {
        if (Number.isFinite(width) && width > 0)
          setPlotWidth(Math.max(32, width));
      };
      update(element.clientWidth);
      if (typeof ResizeObserver === "undefined") return;
      const resize = new ResizeObserver((entries) => {
        const entry = entries.find((item) => item.target === element);
        update(entry?.contentRect.width ?? element.clientWidth);
      });
      resize.observe(element);
      return () => resize.disconnect();
    }, [hasPlot, key, currentHistorical]);
    const x = (p: Point) => timeline!.position(Date.parse(p.end_utc));
    const y = (v: number) => scale!.position(v);
    return (
      <section className={styles.points} aria-label="Сохранённые точки">
        <div className={styles.heading}>
          <h2>Сохранённые точки</h2>
          <Button
            aria-label="Обновить точки"
            loading={store.loading}
            disabled={store.loading}
            onClick={() => {
              setSelectedPoint(null);
              void store.load();
            }}
          >
            Обновить точки
          </Button>
        </div>
        {store.error !== null && (
          <AggregateError
            error={store.error}
            onRetry={() => void store.load()}
          />
        )}
        <section className={styles.tile} aria-label="График сохранённых точек">
          <h3>График выбранного исторического окна</h3>
          {!chartPoints.length ? (
            <p className={styles.notice} role="status">
              Выберите дату исторического окна и рассчитайте его, чтобы увидеть
              график.
            </p>
          ) : (
            <>
              <p className={styles.notice}>
                Показана одна историческая точка рассчитанного окна. Данные
                общей истории не подставляются в график.
              </p>
              <p className={styles.range} aria-label="Границы временной оси">
                Охваченный технический диапазон:{" "}
                <Time
                  value={chartPoints.reduce(
                    (a, p) =>
                      Date.parse(p.start_utc) < Date.parse(a) ? p.start_utc : a,
                    chartPoints[0].start_utc,
                  )}
                  zone={zone}
                />{" "}
                —{" "}
                <Time
                  value={chartPoints.reduce(
                    (a, p) =>
                      Date.parse(p.end_utc) > Date.parse(a) ? p.end_utc : a,
                    chartPoints[0].end_utc,
                  )}
                  zone={zone}
                />
              </p>
              <div className={styles.field}>
                <FieldLabel help="Выберите координату вектора рассчитанного исторического окна. Выбор меняет только отображаемый признак.">
                  Признак графика
                </FieldLabel>
                <Select
                  aria-label="Признак графика"
                  value={key}
                  options={keys.map((k) => ({ value: k, label: k }))}
                  onChange={setFeature}
                />
              </div>
              <p className={styles.notice}>
                ■ Историческая точка; контур — полнота не подтверждена. Для
                одного окна соединяющей линии нет. Координата приближённая;
                точное значение и причина пропуска приведены ниже.
              </p>
              {scale && timeline ? (
                <figure
                  className={styles.chartFigure}
                  aria-label="График выбранного окна"
                >
                  <figcaption className={styles.chartLabels}>
                    <span>
                      Значения: {String(scale.low)} — {String(scale.high)}. Ось
                      времени: конец окна, пояс {zone}.
                    </span>
                    {scale.constant && (
                      <span>
                        На приближённой шкале значения совпадают: маркер
                        расположен посередине, нижняя и верхняя границы равны.
                      </span>
                    )}
                  </figcaption>
                  <div
                    ref={plot}
                    className={styles.chartCanvas}
                    role="region"
                    aria-label="Временная шкала графика"
                  >
                    <svg
                      className={styles.chart}
                      viewBox={`0 0 ${plotWidth} 220`}
                      role="img"
                      aria-label={`График ${key} выбранного окна`}
                      data-domain-low={scale.low}
                      data-domain-high={scale.high}
                      data-time-low={timeline.low}
                      data-time-high={timeline.high}
                    >
                      <title>
                        Показано одно рассчитанное историческое окно. Точные
                        значения доступны в таблице.
                      </title>
                      <line
                        x1="12"
                        x2={plotWidth - 12}
                        y1="180"
                        y2="180"
                        stroke="currentColor"
                      />
                      {timeline.ticks.map((tick) => (
                        <g key={tick.value} className={styles.timeTick}>
                          <title>{`${displayTime(tick.iso, zone)} · ${tick.iso}`}</title>
                          <line
                            x1={tick.x}
                            x2={tick.x}
                            y1="180"
                            y2="186"
                            stroke="currentColor"
                          />
                        </g>
                      ))}
                      <g data-layer="connections" fill="none" strokeWidth="2">
                        {series.map((s) => (
                          <g
                            key={s.kind}
                            stroke={
                              s.kind === "working"
                                ? "var(--color-accent)"
                                : "#9292ee"
                            }
                          >
                            {s.values.map(
                              (v, i) =>
                                v.value !== null &&
                                v.join &&
                                i > 0 &&
                                s.values[i - 1].value !== null && (
                                  <line
                                    key={v.point.id}
                                    x1={x(s.values[i - 1].point)}
                                    y1={y(s.values[i - 1].value!)}
                                    x2={x(v.point)}
                                    y2={y(v.value)}
                                  />
                                ),
                            )}
                          </g>
                        ))}
                      </g>
                      <g data-layer="markers" strokeWidth="2">
                        {series.map((s) => (
                          <g
                            key={s.kind}
                            color={
                              s.kind === "working"
                                ? "var(--color-accent)"
                                : "#9292ee"
                            }
                            stroke="currentColor"
                          >
                            {s.values.map(
                              (v) =>
                                v.value !== null && (
                                  <g key={v.point.id}>
                                    <title>{`${v.point.id}: ${v.exact}; ${displayTime(v.point.end_utc, zone)}`}</title>
                                    {s.kind === "working" ? (
                                      <circle
                                        cx={x(v.point)}
                                        cy={y(v.value)}
                                        r="5"
                                        fill={
                                          v.valid
                                            ? "currentColor"
                                            : "var(--color-surface-solid)"
                                        }
                                      />
                                    ) : (
                                      <rect
                                        x={x(v.point) - 5}
                                        y={y(v.value) - 5}
                                        width="10"
                                        height="10"
                                        fill={
                                          v.valid
                                            ? "currentColor"
                                            : "var(--color-surface-solid)"
                                        }
                                      />
                                    )}
                                  </g>
                                ),
                            )}
                          </g>
                        ))}
                      </g>
                    </svg>
                    <div
                      className={styles.timeAxis}
                      aria-label="Подписи временной оси"
                    >
                      {timeline.ticks.map((tick) => {
                        const local = localCalendar(tick.iso, zone);
                        const [date, time] = local
                          ? local.split("T")
                          : [tick.iso, ""];
                        const tickTime =
                          time && tick.value % 1000
                            ? time + tick.iso.slice(19, 23)
                            : time;
                        return (
                          <time
                            className={styles.axisTick}
                            dateTime={tick.iso}
                            title={displayTime(tick.iso, zone)}
                            key={tick.value}
                          >
                            <span>{tickTime}</span>
                            <span>{date}</span>
                          </time>
                        );
                      })}
                    </div>
                  </div>
                </figure>
              ) : (
                <p role="status">
                  Нет числовых значений для графика выбранного признака. Точные
                  сведения доступны в таблице.
                </p>
              )}
              <FeatureTable features={currentHistorical!.features} />
            </>
          )}
        </section>
        {!store.loaded && store.loading ? (
          <Spin />
        ) : !store.items.length ? (
          <Empty description="Сохранённых точек пока нет" />
        ) : (
          <>
            <section
              className={styles.tile}
              aria-label="Таблица сохранённых точек"
            >
              <h3>История расчётов</h3>
              <p className={styles.notice}>
                Сохранённые рабочие и исторические точки: позиции{" "}
                {store.total ? store.offset + 1 : 0}–
                {store.offset + store.items.length} из {store.total}. Выбор даты
                расчёта не фильтрует этот список. Строка открывает подробности
                своей точки отдельно от графика.
              </p>
              <div
                className={styles.scroll}
                tabIndex={0}
                role="region"
                aria-label="Таблица точек"
              >
                <table className={styles.table}>
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
                          <div className={styles.pointIdentity}>
                            <Button
                              aria-label={`Выбрать точку ${p.id}`}
                              onClick={() => selectPoint(p)}
                            >
                              {p.kind === "working"
                                ? "● Рабочая"
                                : "■ Историческая"}
                            </Button>
                            <code>{p.id}</code>
                          </div>
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
                          <p>{timeBases[p.time_basis] ?? p.time_basis}</p>
                        </td>
                        <td>{exact(p.event_count)}</td>
                        <td>
                          <div className={styles.valuesSummary}>
                            {p.features === null ? (
                              <span className={styles.notice}>
                                Значения недоступны
                              </span>
                            ) : !p.features.length ? (
                              <span className={styles.notice}>
                                Значения отсутствуют
                              </span>
                            ) : (
                              p.features.slice(0, 3).map((feature) => (
                                <div
                                  className={styles.valueLine}
                                  key={feature.key}
                                >
                                  <code>{feature.key}</code>:{" "}
                                  <span>{exact(feature.value)}</span>
                                </div>
                              ))
                            )}
                            {p.features && p.features.length > 3 && (
                              <Button
                                type="link"
                                size="small"
                                className={styles.moreFeatures}
                                aria-label={`Ещё ${p.features.length - 3} ${featureWord(p.features.length - 3)} точки ${p.id}`}
                                onClick={() => selectPoint(p)}
                              >
                                Ещё {p.features.length - 3}{" "}
                                {featureWord(p.features.length - 3)}
                              </Button>
                            )}
                          </div>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </section>
          </>
        )}
        <div className={styles.pagination}>
          <Pagination
            pageSize={pageSize}
            current={store.offset / pageSize + 1}
            total={store.total}
            showSizeChanger={false}
            onChange={(p) => {
              setSelectedPoint(null);
              void store.load((p - 1) * pageSize);
            }}
          />
        </div>
        {selected && (
          <section
            className={styles.tile}
            aria-label="Подробности выбранной точки"
          >
            <h3>Точка {selected.id}</h3>
            <p>
              Техническое окно: [<Time value={selected.start_utc} zone={zone} />
              , <Time value={selected.end_utc} zone={zone} />
              ).
            </p>
            <p>
              Вид: {selected.kind === "working" ? "Рабочая" : "Историческая"},
              основание: {timeBases[selected.time_basis] ?? selected.time_basis}
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
