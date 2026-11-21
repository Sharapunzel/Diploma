import { useCallback, useEffect, useMemo, useState } from "react";
import { Button, Pagination, Spin, Table } from "antd";
import {
  FindingsApi,
  type FindingFilters,
  type FindingLocation,
  type FindingRow,
} from "../../app/api/findings";
import { useStores } from "../../app/providers/StoresProvider";
import { AppRequestError } from "../../app/ui/AppRequestError";
import { timeBasisLabel } from "../../features/findings/labels";
import { useRead } from "../../features/findings/useRead";
import { windowBounds } from "../../features/findings/windows";
import { localTime } from "../../shared/time/utc";
import tableStyles from "../../shared/ui/WorkspaceTable.module.css";
import { ColumnLabel } from "./ColumnLabel";
import { FindingDetails, FindingFlags } from "./FindingDetails";
import { FindingFilterDialog } from "./FindingFilterDialog";
import { Timeline } from "./Timeline";
import styles from "./Findings.module.css";

const PAGE_SIZE = 25;
const TIMELINE_SIZE = 100;
const WEEK_MS = 7 * 24 * 60 * 60 * 1000;
type Jump = { destination: "row" | "window"; id: string };

function defaultFilters(): FindingFilters {
  const now = Date.now();
  return {
    timestamp_from: new Date(now - WEEK_MS).toISOString(),
    timestamp_to: new Date(now).toISOString(),
  };
}

export function FindingsPage() {
  const { api } = useStores();
  const findings = useMemo(() => new FindingsApi(api), [api]);
  const [filters, setFilters] = useState(defaultFilters);
  const [tableLocation, setTableLocation] = useState<FindingLocation>({
    offset: 0,
  });
  const [chartLocation, setChartLocation] = useState<FindingLocation>({
    offset: 0,
  });
  const [pendingJump, setPendingJump] = useState<Jump | null>(null);
  const [highlight, setHighlight] = useState<Jump | null>(null);
  const [active, setActive] = useState<string | null>(null);
  const [filterOpen, setFilterOpen] = useState(false);

  const loadTimeline = useCallback(
    (signal: AbortSignal) =>
      findings.timeline(filters, TIMELINE_SIZE, chartLocation, signal),
    [findings, filters, chartLocation],
  );
  const loadDetectors = useCallback(
    (signal: AbortSignal) => findings.detectors(signal),
    [findings],
  );
  const loadPage = useCallback(
    (signal: AbortSignal) =>
      findings.page(filters, PAGE_SIZE, tableLocation, signal),
    [findings, filters, tableLocation],
  );
  const loadCard = useCallback(
    (signal: AbortSignal) => findings.card(active!, signal),
    [findings, active],
  );

  const timeline = useRead(loadTimeline);
  const detectors = useRead(loadDetectors);
  const rows = useRead(loadPage);
  const card = useRead(active ? loadCard : null);
  const detectorOptions = detectors.data ?? [];

  useEffect(() => {
    if (!pendingJump) return;
    const target = document.getElementById(
      `finding-${pendingJump.destination}-${pendingJump.id}`,
    );
    if (!target) return;
    target.scrollIntoView?.({ block: "center", inline: "center" });
    target.focus({ preventScroll: true });
    setHighlight(pendingJump);
    setPendingJump(null);
  }, [pendingJump, rows.data, timeline.data]);

  useEffect(() => {
    if (!highlight) return;
    const timeout = window.setTimeout(() => setHighlight(null), 2000);
    return () => window.clearTimeout(timeout);
  }, [highlight]);

  function jumpTo(destination: Jump["destination"], id: string) {
    const page = destination === "row" ? rows.data : timeline.data;
    if (!page?.items.some((row) => row.id === id)) {
      const updateLocation =
        destination === "row" ? setTableLocation : setChartLocation;
      updateLocation({ offset: 0, anchorId: id });
    }
    setHighlight(null);
    setPendingJump({ destination, id });
  }

  function changePage(destination: Jump["destination"], page: number) {
    const size = destination === "row" ? PAGE_SIZE : TIMELINE_SIZE;
    const updateLocation =
      destination === "row" ? setTableLocation : setChartLocation;
    updateLocation({ offset: (page - 1) * size });
    setPendingJump(null);
    setHighlight(null);
  }

  function applyFilters(value: FindingFilters) {
    setFilters(value);
    setTableLocation({ offset: 0 });
    setChartLocation({ offset: 0 });
    setPendingJump(null);
    setHighlight(null);
    setFilterOpen(false);
  }

  return (
    <main className={styles.page}>
      <div className={styles.actions}>
        <Button onClick={() => setFilterOpen(true)}>Фильтры</Button>
      </div>
      <section
        aria-label="Таймлайн находок"
        className={styles.panel}
        aria-busy={timeline.loading}
      >
        {timeline.loading && <Spin aria-label="Загрузка таймлайна" />}
        {!!timeline.error && (
          <AppRequestError error={timeline.error} onRetry={timeline.retry} />
        )}
        {timeline.data && (
          <Timeline
            detectors={detectorOptions}
            items={timeline.data.items}
            highlighted={
              highlight?.destination === "window" ? highlight.id : null
            }
            onSelect={(id) => jumpTo("row", id)}
          />
        )}
        {timeline.data && timeline.data.total > TIMELINE_SIZE && (
          <Pagination
            aria-label="Страницы таймлайна"
            className={styles.pagination}
            size="small"
            current={Math.floor(timeline.data.offset / TIMELINE_SIZE) + 1}
            total={timeline.data.total}
            pageSize={TIMELINE_SIZE}
            showSizeChanger={false}
            onChange={(page) => changePage("window", page)}
          />
        )}
      </section>
      <section aria-label="Список находок" className={styles.panel}>
        <div className={styles.period}>
          Период: {localTime(filters.timestamp_from)} —{" "}
          {localTime(filters.timestamp_to)}
        </div>
        {!!rows.error && (
          <AppRequestError error={rows.error} onRetry={rows.retry} />
        )}
        <Table<FindingRow>
          rowKey="id"
          className={tableStyles.table}
          tableLayout="auto"
          pagination={false}
          loading={rows.loading}
          dataSource={rows.data?.items ?? []}
          onRow={(row) => ({
            id: `finding-row-${row.id}`,
            tabIndex: -1,
            "data-highlighted":
              (highlight?.destination === "row" && highlight.id === row.id) ||
              undefined,
          })}
          scroll={{ x: 805 }}
          locale={{
            emptyText: rows.loading
              ? "Загрузка"
              : rows.error
                ? "Не удалось загрузить находки"
                : "Находок по выбранным фильтрам нет",
          }}
          columns={[
            {
              title: (
                <ColumnLabel
                  name="Детектор"
                  help="Имя детектора на момент сохранения находки. Нажмите на имя, чтобы открыть карточку."
                />
              ),
              render: (_, row) => (
                <Button
                  type="link"
                  className={styles.rowLink}
                  onClick={() => setActive(row.id)}
                >
                  {row.detector_name || "Имя не сохранено"}
                </Button>
              ),
            },
            {
              title: (
                <ColumnLabel
                  name="Источник"
                  help="Локальный источник событий, из которых рассчитаны признаки окна."
                />
              ),
              render: (_, row) => row.source_name ?? "Нет данных",
            },
            {
              title: (
                <ColumnLabel
                  name="Окно"
                  help="Границы окна и основание времени, сохранённое в находке. Новые окна идут первыми."
                />
              ),
              onHeaderCell: () => ({ "aria-sort": "descending" }),
              render: (_, row) => (
                <>
                  <div>
                    {localTime(row.window_start)} — {localTime(row.window_end)}
                  </div>
                  <small>{timeBasisLabel[row.time_basis]}</small>
                  {windowBounds(row) && (
                    <Button
                      type="link"
                      size="small"
                      className={styles.chartJump}
                      onClick={() => jumpTo("window", row.id)}
                    >
                      На графике
                    </Button>
                  )}
                </>
              ),
            },
            {
              title: (
                <ColumnLabel
                  name="Степень аномальности"
                  help="Насколько сильно окно отклонилось по шкале RCF от 0 до 1."
                />
              ),
              width: 165,
              render: (_, row) => row.grade ?? "Нет данных",
            },
            {
              title: (
                <ColumnLabel
                  name="Уверенность"
                  help="Достаточность данных для оценки модели по шкале от 0 до 1."
                />
              ),
              width: 135,
              render: (_, row) => row.confidence ?? "Нет данных",
            },
            {
              title: (
                <ColumnLabel
                  name="Отметки"
                  help="Дополнительные сведения о находке. Здесь отмечаются запоздалая оценка и использование ускоренного прогрева модели."
                />
              ),
              render: (_, row) =>
                row.late || row.accelerated_used ? (
                  <FindingFlags finding={row} />
                ) : (
                  <span className={styles.description}>Без отметок</span>
                ),
            },
          ]}
        />
        {rows.data && rows.data.total > PAGE_SIZE && (
          <Pagination
            className={styles.pagination}
            size="small"
            current={Math.floor(rows.data.offset / PAGE_SIZE) + 1}
            total={rows.data.total}
            pageSize={PAGE_SIZE}
            showSizeChanger={false}
            onChange={(page) => changePage("row", page)}
          />
        )}
      </section>
      {filterOpen && (
        <FindingFilterDialog
          initial={filters}
          detectors={detectorOptions}
          loading={detectors.loading}
          error={detectors.error}
          onRetry={detectors.retry}
          onApply={applyFilters}
          onCancel={() => setFilterOpen(false)}
        />
      )}
      <FindingDetails
        open={active !== null}
        finding={card.data}
        loading={card.loading}
        error={card.error}
        onRetry={card.retry}
        onClose={() => setActive(null)}
      />
    </main>
  );
}
