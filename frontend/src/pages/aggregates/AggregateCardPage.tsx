import { useEffect, useState } from "react";
import { Alert, Button, Input } from "antd";
import { LoadingOverlay } from "../../shared/ui/LoadingOverlay";
import { observer } from "mobx-react-lite";
import { Link, useParams, useLocation } from "react-router-dom";
import { exact, printAggregate } from "../../app/api/aggregateJson";
import { CardStore } from "../../features/aggregates/CardStore";
import { useAggregateScope } from "../../features/aggregates/useAggregateScope";
import { historicalError } from "../../features/aggregates/time";
import { states } from "../../features/aggregates/labels";
import { ConfirmationDialog } from "../../shared/ui/ConfirmationDialog";
import { AggregateError } from "./AggregateError";
import { PointsView } from "./PointsView";
import { Time } from "./ResultView";
import styles from "./Aggregates.module.css";

export const AggregateCardPage = observer(() => {
  const location = useLocation();
  const { id = "" } = useParams();
  const { store, session } = useAggregateScope(
    (api) => new CardStore(api, id),
    id,
  );
  const [zone, setZone] = useState("UTC");
  const [confirm, setConfirm] = useState<"archive" | "delete" | null>(null);
  const [start, setStart] = useState("");
  const [historyError, setHistoryError] = useState("");
  const [refresh, setRefresh] = useState(0);
  useEffect(() => {
    void store.load();
  }, [store]);
  const a = store.card;
  const write = session.has("aggregates.write");
  return (
    <div className={styles.page}>
      <Link to="/aggregates">← К агрегатам</Link>
      {location.state?.aggregateCreated === id && (
        <Alert
          type="success"
          role="status"
          message="Агрегат создан. Правило и расписание сохранены."
        />
      )}
      {store.error !== null && (
        <AggregateError error={store.error} onRetry={() => void store.load()} />
      )}
      {store.success && (
        <Alert type="success" role="status" message={store.success} />
      )}
      {store.mutationOutcome === "unknown" && (
        <Alert
          type="warning"
          role="status"
          message="Результат изменения неизвестен. Обновите состояние перед новым действием; запрос не повторяется автоматически."
        />
      )}
      {!a ? (
        store.lifecycle.isPending("load") && <LoadingOverlay />
      ) : (
        <>
          <h2>{a.name}</h2>
          <div className={styles.toolbar}>
            <Button
              aria-label="Обновить состояние"
              disabled={store.lifecycle.isPending("action")}
              loading={store.lifecycle.isPending("load")}
              onClick={() => void store.load()}
            >
              Обновить состояние
            </Button>
            {write && a.state !== "deleted" && (
              <>
                <Link to={`/aggregates/new?copy=${encodeURIComponent(a.id)}`}>
                  <Button>Копировать</Button>
                </Link>
                {a.state === "archived" ? (
                  <Button
                    disabled={!store.canMutate}
                    loading={store.lifecycle.isPending("action")}
                    onClick={() => void store.action("resume")}
                  >
                    Возобновить
                  </Button>
                ) : (
                  <Button
                    disabled={!store.canMutate}
                    onClick={() => setConfirm("archive")}
                  >
                    Архивировать
                  </Button>
                )}
                <Button
                  danger
                  disabled={!store.canMutate}
                  onClick={() => setConfirm("delete")}
                >
                  Удалить агрегат
                </Button>
              </>
            )}
          </div>
          <label className={styles.field}>
            Пояс отображения
            <Input
              aria-label="Пояс отображения"
              value={zone}
              onChange={(e) => setZone(e.target.value)}
            />
          </label>
          <p>
            Пояс отображения меняет только подписи абсолютных моментов, включая
            сезонное смещение. Расписание и правило не пересчитываются.
          </p>
          <dl>
            <dt>UUID</dt>
            <dd>{a.id}</dd>
            <dt>Источник (снимок)</dt>
            <dd>
              {a.source_name} · {a.source_type} · {a.source_id}
            </dd>
            <dt>Создал</dt>
            <dd>
              {a.created_by}; <Time value={a.created_at} zone={zone} />
            </dd>
            <dt>Исходная граница и пояс</dt>
            <dd>
              {a.original_boundary} ({a.timezone})
            </dd>
            <dt>UTC-якорь и длительность</dt>
            <dd>
              <Time value={a.anchor_utc} zone={zone} />; {a.duration_seconds} с
            </dd>
            <dt>Состояние</dt>
            <dd>{states[a.state] || a.state}</dd>
            <dt>Ближайший плановый расчёт</dt>
            <dd>
              <Time value={a.next_scheduled_at} zone={zone} />
            </dd>
            <dt>Отставание</dt>
            <dd>{exact(a.lag_seconds)} с</dd>
          </dl>
          {a.last_error && (
            <Alert
              type="warning"
              message="Расчёт временно недоступен. Обновите состояние; при повторении обратитесь к администратору."
              description={<Time value={a.last_error_at} zone={zone} />}
            />
          )}
          <details>
            <summary>Пропущенные диапазоны</summary>
            <pre className={styles.code}>
              {printAggregate(a.skipped_ranges, true)}
            </pre>
          </details>
          <details open>
            <summary>Неизменяемое правило</summary>
            <pre aria-label="Сохранённое правило" className={styles.code}>
              {printAggregate(a.aggregate_rule, true)}
            </pre>
          </details>
          {write && a.state === "active" && (
            <section>
              <h3>Одно историческое окно</h3>
              <p>
                Завершённое выровненное окно до создания агрегата. Запрос явный,
                повтор не создаёт дубль и не двигает рабочий курсор.
              </p>
              <label className={styles.field}>
                Начало исторического окна
                <Input
                  aria-label="Начало исторического окна"
                  value={start}
                  onChange={(e) => {
                    setStart(e.target.value);
                    setHistoryError("");
                  }}
                />
              </label>
              {historyError && (
                <Alert type="error" role="alert" message={historyError} />
              )}
              <Button
                loading={store.lifecycle.isPending("history")}
                disabled={!store.canMutate}
                onClick={() => {
                  const error = historicalError(
                    start,
                    a.anchor_utc,
                    a.duration_seconds,
                    a.created_at,
                  );
                  setHistoryError(error);
                  if (!error)
                    void store.history(start).then((ok) => {
                      if (ok) setRefresh((n) => n + 1);
                    });
                }}
              >
                Рассчитать историческое окно
              </Button>
            </section>
          )}
          {a.state === "deleted" && (
            <Alert
              type="info"
              message="Агрегат удалён. Новые расчёты окончательно остановлены, сохранённая история доступна."
            />
          )}
          <PointsView aggregate={a} zone={zone} refresh={refresh} />
          <ConfirmationDialog
            open={confirm !== null}
            pending={store.lifecycle.isPending("action")}
            title={
              confirm === "delete"
                ? "Удалить агрегат?"
                : "Архивировать агрегат?"
            }
            message={
              confirm === "delete"
                ? `Новые расчёты «${a.name}» будут окончательно остановлены. Исторические точки и связи сохраняются.`
                : `Новые расчёты «${a.name}» остановятся. История сохранится; после возобновления намеренно пропущенные окна не восполняются.`
            }
            confirmText={confirm === "delete" ? "Удалить" : "Архивировать"}
            danger
            onCancel={() => setConfirm(null)}
            onConfirm={() => {
              if (confirm)
                void store.action(confirm).then((ok) => {
                  if (ok || store.mutationOutcome === "unknown")
                    setConfirm(null);
                });
            }}
          >
            {store.error !== null && <AggregateError error={store.error} />}
          </ConfirmationDialog>
        </>
      )}
    </div>
  );
});
