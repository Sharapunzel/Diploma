import { useEffect, useState } from "react";
import { Button, Input, Spin } from "antd";
import { LoadingOverlay } from "../../shared/ui/LoadingOverlay";
import { observer } from "mobx-react-lite";
import { Link, useParams, useLocation } from "react-router-dom";
import { exact, printAggregate } from "../../app/api/aggregateJson";
import { CardStore } from "../../features/aggregates/CardStore";
import { useAggregateScope } from "../../features/aggregates/useAggregateScope";
import { browserZone } from "../../features/aggregates/time";
import { CalendarTime } from "./CalendarTime";
import { RuleEditor } from "./RuleEditor";
import { RuleExchange } from "./RuleExchange";
import { states } from "../../features/aggregates/labels";
import { ConfirmationDialog } from "../../shared/ui/ConfirmationDialog";
import { AggregateError } from "./AggregateError";
import { PointsView } from "./PointsView";
import { Time } from "./ResultView";
import { FieldLabel } from "./AggregateHelp";
import { AggregateNotice } from "./AggregateNotice";
import styles from "./Aggregates.module.css";

export const AggregateCardPage = observer(() => {
  const location = useLocation();
  const { id = "" } = useParams();
  const { store, session, service } = useAggregateScope(
    (api) => new CardStore(api, id),
    id,
  );
  const [zone, setZone] = useState(browserZone);
  const [confirm, setConfirm] = useState<{
    action: "archive" | "delete";
    owner: typeof store;
  } | null>(null);
  const [lastConfirmAction, setLastConfirmAction] = useState<
    "archive" | "delete"
  >("archive");
  const operators = store.operators;
  const [start, setStart] = useState("");
  const [historyError, setHistoryError] = useState("");
  const [refresh, setRefresh] = useState(0);
  useEffect(() => {
    void store.load();
    setConfirm(null);
    setStart("");
    setHistoryError("");
    void store.loadOperators();
  }, [store, service]);
  const a = store.card;
  const write = session.has("aggregates.write");
  const confirmationOpen = confirm !== null && confirm.owner === store;
  const openConfirmation = (action: "archive" | "delete") => {
    setLastConfirmAction(action);
    setConfirm({ action, owner: store });
  };
  return (
    <div className={styles.page} key={`${session.version}|${id}`}>
      <Link to="/aggregates">← К агрегатам</Link>
      {location.state?.aggregateCreated === id && (
        <AggregateNotice
          type="success"
          role="status"
          message="Агрегат создан. Правило и расписание сохранены."
        />
      )}
      {store.error !== null && (
        <AggregateError error={store.error} onRetry={() => void store.load()} />
      )}
      {store.success && (
        <AggregateNotice type="success" role="status" message={store.success} />
      )}
      {store.mutationOutcome === "unknown" && (
        <AggregateNotice
          type="warning"
          role="status"
          message="Результат изменения неизвестен. Обновите состояние перед новым действием; запрос не повторяется автоматически."
        />
      )}
      {!a ? (
        store.lifecycle.isPending("load") && <LoadingOverlay />
      ) : (
        <>
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
                    key="resume"
                    aria-label="Возобновить"
                    disabled={!store.canMutate}
                    loading={store.lifecycle.isPending("action")}
                    onClick={() => void store.action("resume")}
                  >
                    Возобновить
                  </Button>
                ) : (
                  <Button
                    key="archive"
                    aria-label="Архивировать"
                    disabled={!store.canMutate}
                    onClick={() => openConfirmation("archive")}
                  >
                    Архивировать
                  </Button>
                )}
                <Button
                  danger
                  disabled={!store.canMutate}
                  onClick={() => openConfirmation("delete")}
                >
                  Удалить агрегат
                </Button>
              </>
            )}
          </div>
          <div className={styles.field}>
            <FieldLabel help="UTC или название IANA, например Asia/Yekaterinburg. Меняет только подписи времени; сохранённое расписание остаётся прежним.">
              Пояс отображения
            </FieldLabel>
            <Input
              aria-label="Пояс отображения"
              value={zone}
              onChange={(e) => setZone(e.target.value)}
            />
          </div>

          <section className={styles.block}>
            <h3 aria-label="1. Параметры процесса">1. Параметры процесса </h3>
            <p className={styles.notice}>
              Сохранённые настройки агрегата. Для изменения создайте копию.
            </p>
            <dl>
              <dt>Имя</dt>
              <dd>{a.name}</dd>
              <dt>Идентификатор агрегата</dt>
              <dd>{a.id}</dd>
              <dt>Источник (снимок)</dt>
              <dd>
                {a.source_name} ·{" "}
                {a.source_type === "kafka" ? "Kafka" : a.source_type} ·{" "}
                {a.source_id}
              </dd>
              <dt>Создал</dt>
              <dd>
                {a.created_by}; <Time value={a.created_at} zone={zone} />
              </dd>
              <dt>Исходная граница и пояс</dt>
              <dd>
                {a.original_boundary} ({a.timezone})
              </dd>
              <dt>Начало расписания и длительность окна</dt>
              <dd>
                <Time value={a.anchor_utc} zone={zone} />; {a.duration_seconds}{" "}
                с
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
              <AggregateNotice
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
          </section>
          <section className={styles.block}>
            <h3 aria-label="2. Выходной вектор и неизменяемое правило">
              2. Выходной вектор и неизменяемое правило{" "}
            </h3>
            <p className={styles.notice}>
              Просмотр сохранённых признаков. Тумблер показывает принадлежность
              к выходному вектору.
            </p>
            {(store.lifecycle.isPending("operators") ||
              store.operatorsError !== null) && (
              <div
                className={styles.stack}
                role="region"
                aria-label="Каталог операций"
                aria-busy={store.lifecycle.isPending("operators")}
              >
                {store.lifecycle.isPending("operators") && (
                  <Spin aria-label="Загрузка каталога операций" />
                )}
                {store.operatorsError !== null && (
                  <>
                    <p role="alert">
                      Не удалось загрузить каталог операций. Инструкция для ИИ
                      станет доступна после успешной загрузки. Правило и история
                      доступны.
                    </p>
                    <AggregateError error={store.operatorsError} />
                    <Button
                      disabled={store.lifecycle.isPending("operators")}
                      onClick={() => void store.loadOperators()}
                    >
                      Повторить загрузку операций
                    </Button>
                  </>
                )}
              </div>
            )}
            <RuleExchange
              key={`exchange-${a.id}`}
              api={service}
              source={a.source_id}
              operators={operators}
              text={printAggregate(a.aggregate_rule, true)}
            />
            <RuleEditor
              key={`rule-${a.id}`}
              readOnly
              rule={a.aggregate_rule}
              operators={operators}
              api={service}
              source={a.source_id}
              onChange={() => {}}
              onInvalid={() => {}}
            />
            <details>
              <summary>Сохранённое правило JSON</summary>
              <pre aria-label="Сохранённое правило" className={styles.code}>
                {printAggregate(a.aggregate_rule, true)}
              </pre>
            </details>
          </section>
          <section className={styles.block}>
            <h3 aria-label="3. Проверка, диагностика и история">
              3. Проверка, диагностика и история{" "}
            </h3>
            {write && a.state === "active" && (
              <section className={styles.modalTile}>
                <h3>Расчёт одного прошедшего окна</h3>
                <p>
                  Выберите начало завершённого окна до создания агрегата.
                  Длительность окна — {a.duration_seconds} с, как в расписании.
                  Результат сохранится в истории и будет показан ниже за
                  выбранный период. Повторный расчёт того же окна не создаёт
                  копию и не меняет автоматическое расписание.
                </p>
                <CalendarTime
                  label="Начало окна для дополнительного расчёта"
                  value={start}
                  scheduleAnchor={a.anchor_utc}
                  zone={zone}
                  disabled={store.lifecycle.isPending("history")}
                  onChange={(v) => {
                    setStart(v);
                    setHistoryError("");
                    store.clearHistoricalResult();
                  }}
                />
                {historyError && (
                  <AggregateNotice
                    type="error"
                    role="alert"
                    message={historyError}
                  />
                )}
                {store.historyError !== null && (
                  <AggregateError error={store.historyError} />
                )}
                <Button
                  aria-label="Рассчитать выбранное окно"
                  loading={store.lifecycle.isPending("history")}
                  disabled={
                    !store.canMutate || store.lifecycle.isPending("history")
                  }
                  onClick={() => {
                    if (!start) {
                      setHistoryError(
                        "Выберите начало окна для дополнительного расчёта.",
                      );
                      return;
                    }
                    void store.history(start).then((ok) => {
                      if (ok) setRefresh((n) => n + 1);
                    });
                  }}
                >
                  Рассчитать выбранное окно
                </Button>
              </section>
            )}
            {a.state === "deleted" && (
              <AggregateNotice
                type="info"
                message="Агрегат удалён. Новые расчёты окончательно остановлены, сохранённая история доступна."
              />
            )}
            <PointsView
              aggregate={a}
              zone={zone}
              refresh={refresh}
              historicalPoint={store.historicalResult}
              selectionKey={start}
            />
          </section>
          <ConfirmationDialog
            key={lastConfirmAction}
            open={confirmationOpen}
            pending={confirmationOpen && store.lifecycle.isPending("action")}
            title={
              lastConfirmAction === "delete"
                ? "Удалить агрегат?"
                : "Архивировать агрегат?"
            }
            message={
              lastConfirmAction === "delete"
                ? `Новые расчёты «${a.name}» будут окончательно остановлены. Все связанные детекторы будут остановлены; модели, исторические точки и связи сохраняются.`
                : `Новые расчёты «${a.name}» остановятся. История сохранится; после возобновления намеренно пропущенные окна не восполняются.`
            }
            confirmText={
              lastConfirmAction === "delete" ? "Удалить" : "Архивировать"
            }
            danger
            onCancel={() => setConfirm(null)}
            onConfirm={() => {
              if (confirm && confirm.owner === store)
                void store.action(confirm.action).then((ok) => {
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
