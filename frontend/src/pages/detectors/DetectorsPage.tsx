import { useEffect, useState } from "react";
import { Button, Empty, Input, Modal, Pagination, Select, Table } from "antd";
import type { ColumnsType } from "antd/es/table";
import { observer } from "mobx-react-lite";
import { Link } from "react-router-dom";
import { useStores } from "../../app/providers/StoresProvider";
import { executionStates, type DetectorSummary } from "../../app/api/detectors";
import { exact } from "../../app/api/aggregateJson";
import {
  executions,
  models,
  aggregateStates,
  reasonText,
} from "../../features/detectors/labels";
import { DetectorError, Flame, Time } from "./Parts";
import styles from "./Detectors.module.css";

export const DetectorsPage = observer(() => {
  const { detectors, session } = useStores();
  const s = detectors.list;
  const [filters, setFilters] = useState<{
    aggregate: string;
    execution: string;
  } | null>(null);
  useEffect(() => {
    void s.load();
    return () => {
      s.lifecycle.cancel("read");
      s.lifecycle.cancel("load");
    };
  }, [s]);
  useEffect(() => {
    setFilters(null);
  }, [s, session.version]);
  const loading = s.lifecycle.isPending("load");
  const columns: ColumnsType<DetectorSummary> = [
    {
      title: "Детектор",
      key: "name",
      width: 230,
      render: (_, d) => (
        <div className={styles.inline}>
          <Link to={`/detectors/${d.id}`}>{d.name}</Link>
          <Flame used={d.accelerated_used} />
        </div>
      ),
    },
    {
      title: "Агрегат / источник",
      key: "aggregate",
      width: 240,
      render: (_, d) => (
        <div className={styles.cell}>
          <span>
            {d.aggregate_name} · {aggregateStates[d.aggregate_state]}
          </span>
          <span>{d.source_name}</span>
        </div>
      ),
    },
    {
      title: "Исполнение / модель",
      key: "state",
      width: 210,
      render: (_, d) => (
        <div className={styles.cell}>
          <span>{executions[d.execution_state]}</span>
          <span>{models[d.model_state]}</span>
        </div>
      ),
    },
    {
      title: "Последние окна",
      key: "windows",
      width: 280,
      render: (_, d) => (
        <div className={styles.cell}>
          <span>
            Потреблено:{" "}
            <Time value={d.last_consumed?.end_utc ?? null} zone="UTC" />
          </span>
          <span>
            Оценено:{" "}
            <Time value={d.last_evaluated?.end_utc ?? null} zone="UTC" />
          </span>
        </div>
      ),
    },
    {
      title: "Отставание / причина",
      key: "lag",
      width: 260,
      render: (_, d) => (
        <div className={styles.cell}>
          <span>Окон: {exact(d.backlog)}</span>
          <span>
            Задержка: {exact(d.lag_seconds)}
            {d.lag_seconds !== null && " с"}
          </span>
          {d.reason && <span>{reasonText(d.reason, d.execution_state)}</span>}
        </div>
      ),
    },
  ];
  return (
    <div className={styles.page}>
      <div className={styles.toolbar}>
        {session.has("detectors.write") && (
          <Link to="/detectors/new">
            <Button type="primary">Создать детектор</Button>
          </Link>
        )}
        <Button
          onClick={() =>
            setFilters({ aggregate: s.aggregate, execution: s.execution })
          }
        >
          Фильтры
        </Button>
        <Button loading={loading} onClick={() => void s.load()}>
          Обновить
        </Button>
      </div>
      <section className={styles.panel} aria-label="Список детекторов">
        {s.error !== null && (
          <DetectorError error={s.error} onRetry={() => void s.load()} />
        )}
        <Table<DetectorSummary>
          className={styles.detectorsTable}
          size="small"
          bordered
          rowKey="id"
          columns={columns}
          dataSource={s.items}
          loading={loading}
          pagination={false}
          scroll={{ x: 1220 }}
          locale={{
            emptyText: (
              <Empty
                description={
                  s.error
                    ? "Список недоступен"
                    : s.aggregate || s.execution
                      ? "По фильтрам ничего не найдено"
                      : "Детекторов пока нет"
                }
              />
            ),
          }}
        />
        <div className={styles.tablePagination}>
          <Pagination
            current={s.offset / s.limit + 1}
            total={s.total}
            pageSize={s.limit}
            showSizeChanger={false}
            onChange={(page) => void s.load((page - 1) * s.limit)}
          />
        </div>
      </section>
      <Modal
        title="Фильтры детекторов"
        open={filters !== null}
        onCancel={() => {
          if (!loading) setFilters(null);
        }}
        maskClosable={!loading}
        keyboard={!loading}
        closable={!loading}
        footer={
          <div className={styles.modalActions}>
            <Button disabled={loading} onClick={() => setFilters(null)}>
              Отмена
            </Button>
            <Button
              disabled={loading}
              onClick={() => setFilters({ aggregate: "", execution: "" })}
            >
              Сбросить
            </Button>
            <Button
              type="primary"
              loading={loading}
              disabled={
                !filters ||
                (!!filters.aggregate &&
                  !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(
                    filters.aggregate,
                  ))
              }
              onClick={() => {
                if (filters)
                  void s
                    .filter(filters.aggregate, filters.execution)
                    .then((ok) => {
                      if (ok) setFilters(null);
                    });
              }}
            >
              Применить
            </Button>
          </div>
        }
      >
        <div className={styles.formFields}>
          <label className={styles.field}>
            UUID агрегата
            <Input
              aria-label="UUID агрегата"
              value={filters?.aggregate ?? ""}
              disabled={loading}
              onChange={(e) =>
                setFilters(
                  (old) => old && { ...old, aggregate: e.target.value },
                )
              }
            />
            <span className={styles.hint}>
              Можно указать UUID архивного или удалённого агрегата для чтения
              истории.
            </span>
          </label>
          <label className={styles.field}>
            Исполнение
            <Select
              aria-label="Исполнение"
              value={filters?.execution ?? ""}
              disabled={loading}
              options={[
                { value: "", label: "Все состояния" },
                ...executionStates.map((value) => ({
                  value,
                  label: executions[value],
                })),
              ]}
              onChange={(execution) =>
                setFilters((old) => old && { ...old, execution })
              }
            />
          </label>
          {s.error !== null && <DetectorError error={s.error} />}
        </div>
      </Modal>
    </div>
  );
});
