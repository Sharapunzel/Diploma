import { useEffect } from "react";
import { Button, Pagination, Empty } from "antd";
import { LoadingOverlay } from "../../shared/ui/LoadingOverlay";
import { observer } from "mobx-react-lite";
import { Link } from "react-router-dom";
import { CollectionStore } from "../../features/aggregates/CollectionStore";
import { useAggregateScope } from "../../features/aggregates/useAggregateScope";
import { exact } from "../../app/api/aggregateJson";
import { states } from "../../features/aggregates/labels";
import { AggregateError } from "./AggregateError";
import { Time } from "./ResultView";
import styles from "./Aggregates.module.css";

export const AggregatesPage = observer(() => {
  const { store, session } = useAggregateScope(
    (api) => new CollectionStore((offset, signal) => api.list(offset, signal)),
  );
  useEffect(() => {
    void store.load(0);
  }, [store]);
  return (
    <div className={styles.page}>
      <div className={styles.toolbar}>
        {session.has("aggregates.write") && (
          <Link to="/aggregates/new">
            <Button type="primary">Создать агрегат</Button>
          </Link>
        )}
        <Button loading={store.loading} onClick={() => void store.load()}>
          Обновить
        </Button>
      </div>
      {store.error !== null && (
        <AggregateError error={store.error} onRetry={() => void store.load()} />
      )}
      {!store.loaded && store.loading ? (
        <LoadingOverlay />
      ) : !store.items.length ? (
        <Empty
          description={store.error ? "Список недоступен" : "Агрегатов пока нет"}
        />
      ) : (
        <div className={styles.scroll}>
          <table className={styles.table}>
            <thead>
              <tr>
                <th>Имя</th>
                <th>Источник</th>
                <th>Состояние</th>
                <th>Длительность</th>
                <th>Ближайший расчёт (UTC)</th>
                <th>Отставание / ошибка</th>
              </tr>
            </thead>
            <tbody>
              {store.items.map((a) => (
                <tr key={a.id}>
                  <td>
                    <Link to={`/aggregates/${a.id}`}>{a.name}</Link>
                  </td>
                  <td>{a.source_name}</td>
                  <td>{states[a.state] || a.state}</td>
                  <td>{a.duration_seconds} с</td>
                  <td>
                    <Time value={a.next_scheduled_at} zone="UTC" />
                  </td>
                  <td>
                    {exact(a.lag_seconds)} с{" "}
                    {a.last_error &&
                      "Расчёт временно недоступен; откройте карточку."}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      <Pagination
        current={store.offset / 50 + 1}
        pageSize={50}
        total={store.total}
        showSizeChanger={false}
        onChange={(p) => void store.load((p - 1) * 50)}
      />
    </div>
  );
});
