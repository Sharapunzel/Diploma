import { useEffect, useState } from "react";
import { Button, Empty, Input, Modal, Pagination, Select, Table } from "antd";
import type { ColumnsType } from "antd/es/table";
import { observer } from "mobx-react-lite";
import { Link } from "react-router-dom";
import {
  ListStore,
  type AggregateFilters,
} from "../../features/aggregates/ListStore";
import { useAggregateScope } from "../../features/aggregates/useAggregateScope";
import type { Aggregate } from "../../app/api/aggregates";
import { states } from "../../features/aggregates/labels";
import { AggregateError } from "./AggregateError";
import { FieldLabel } from "./AggregateHelp";
import styles from "./Aggregates.module.css";
export const AggregatesPage = observer(() => {
  const { store, session } = useAggregateScope((api) => new ListStore(api));
  const [filters, setFilters] = useState<AggregateFilters | null>(null);
  useEffect(() => {
    setFilters(null);
    void store.load(0);
  }, [store]);
  const columns: ColumnsType<Aggregate> = [
    {
      title: "Имя",
      key: "name",
      width: 260,
      render: (_, a) => <Link to={`/aggregates/${a.id}`}>{a.name}</Link>,
    },
    {
      title: "Собственный источник",
      dataIndex: "source_name",
      key: "source",
      width: 240,
    },
    {
      title: "Размерность вектора",
      key: "dimension",
      width: 160,
      render: (_, a) =>
        Array.isArray(a.aggregate_rule.features)
          ? a.aggregate_rule.features.length
          : "Нет метаданных",
    },
    {
      title: "Длительность",
      key: "duration",
      width: 150,
      render: (_, a) => `${a.duration_seconds} с`,
    },
    {
      title: "Состояние",
      key: "state",
      width: 160,
      render: (_, a) => states[a.state] || a.state,
    },
    {
      title: "Действия",
      key: "actions",
      width: 150,
      render: (_, a) =>
        session.has("aggregates.write") && (
          <Link to={`/aggregates/new?copy=${encodeURIComponent(a.id)}`}>
            Копировать
          </Link>
        ),
    },
  ];
  return (
    <div className={styles.page}>
      <section className={styles.panel} aria-label="Список агрегатов">
        <div className={styles.sectionBar}>
          <div className={styles.toolbar}>
            {session.has("aggregates.write") && (
              <Link to="/aggregates/new">
                <Button type="primary">Создать агрегат</Button>
              </Link>
            )}
            <Button onClick={() => setFilters({ ...store.filters })}>
              Фильтры
            </Button>
            <Button loading={store.loading} onClick={() => void store.load()}>
              Обновить
            </Button>
          </div>
        </div>
        {store.error !== null && (
          <AggregateError
            error={store.error}
            onRetry={() => void store.load()}
          />
        )}
        <Table<Aggregate>
          className={styles.aggregateTable}
          tableLayout="auto"
          rowKey="id"
          columns={columns}
          dataSource={store.items}
          loading={store.loading}
          pagination={false}
          scroll={{ x: 1120 }}
          locale={{
            emptyText: (
              <Empty
                description={
                  store.error
                    ? "Список недоступен"
                    : store.filters.q || store.filters.state
                      ? "По фильтрам ничего не найдено"
                      : "Агрегатов пока нет"
                }
              />
            ),
          }}
        />
        <div className={styles.tablePagination}>
          <Pagination
            current={store.offset / 50 + 1}
            pageSize={50}
            total={store.total}
            showSizeChanger={false}
            onChange={(p) => void store.load((p - 1) * 50)}
          />
        </div>
      </section>
      <Modal
        style={{ maxWidth: "calc(100% - 24px)" }}
        open={filters !== null}
        title="Фильтры агрегатов"
        onCancel={() => setFilters(null)}
        footer={
          <>
            <Button onClick={() => setFilters(null)}>Отмена</Button>
            <Button
              type="primary"
              loading={store.loading}
              onClick={() => {
                if (filters)
                  void store.load(0, filters).then((ok) => {
                    if (ok) setFilters(null);
                  });
              }}
            >
              Применить фильтры
            </Button>
          </>
        }
      >
        {filters && (
          <div className={styles.field}>
            <div className={styles.field}>
              <FieldLabel help="Поиск по имени во всём серверном списке агрегатов. Изменения применяются кнопкой «Применить фильтры».">
                Имя
              </FieldLabel>
              <Input
                aria-label="Поиск агрегатов по имени"
                maxLength={200}
                value={filters.q}
                onChange={(e) => setFilters({ ...filters, q: e.target.value })}
              />
            </div>
            <div className={styles.field}>
              <FieldLabel help="Активные агрегаты рассчитывают новые окна. Архивные сохраняют историю и могут быть возобновлены.">
                Состояние
              </FieldLabel>
              <Select
                aria-label="Состояние агрегатов"
                value={filters.state}
                options={[
                  { value: "", label: "Все неудалённые" },
                  { value: "active", label: "Активные" },
                  { value: "archived", label: "Архивные" },
                ]}
                onChange={(state) => setFilters({ ...filters, state })}
              />
            </div>
            {store.error !== null && <AggregateError error={store.error} />}
          </div>
        )}
      </Modal>
    </div>
  );
});
