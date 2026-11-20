import { useEffect, useState } from "react";
import { Button, Input, Spin } from "antd";
import { LoadingOverlay } from "../../shared/ui/LoadingOverlay";
import { observer } from "mobx-react-lite";
import { Link, useParams, useLocation } from "react-router-dom";
import { exact, object, printAggregate } from "../../app/api/aggregateJson";
import { CardStore } from "../../features/aggregates/CardStore";
import { useAggregateScope } from "../../features/aggregates/useAggregateScope";
import { browserZone } from "../../features/aggregates/time";
import { CalendarTime } from "./CalendarTime";
import { RuleEditor } from "./RuleEditor";
import { RuleExchange } from "./RuleExchange";
import { states, reasonText } from "../../features/aggregates/labels";
import { ConfirmationDialog } from "../../shared/ui/ConfirmationDialog";
import { AggregateError } from "./AggregateError";
import { PointsView } from "./PointsView";
import { Time } from "./ResultView";
import { BudgetView } from "./BudgetView";
import { FieldLabel } from "./AggregateHelp";
import { AggregateNotice } from "./AggregateNotice";
import { CleanupPanel } from "./CleanupPanel";
import { AutoCleanupPanel } from "./AutoCleanupPanel";
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
    action: "archive" | "delete" | "pause";
    owner: typeof store;
  } | null>(null);
  const [lastConfirmAction, setLastConfirmAction] = useState<
    "archive" | "delete" | "pause"
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
  const openConfirmation = (action: "archive" | "delete" | "pause") => {
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
        store.cardLoading && <LoadingOverlay />
      ) : (
        <>
          <div className={styles.toolbar}>
            <Button
              aria-label="Обновить состояние"
              disabled={
                store.lifecycle.isPending("action") ||
                store.commandBusy ||
                store.lifecycle.isPending("load")
              }
              loading={store.cardLoading}
              onClick={() => void store.load()}
            >
              Обновить состояние
            </Button>
            {write &&
              (a.state !== "deleted" || a.execution_mode === "retired") && (
                <Link to={`/aggregates/new?copy=${encodeURIComponent(a.id)}`}>
                  <Button>Копировать</Button>
                </Link>
              )}
            {write && a.state !== "deleted" && (
              <>
                {a.execution_mode !== "retired" &&
                  (a.state === "archived" ? (
                    <Button
                      key="resume"
                      aria-label="Возобновить"
                      disabled={!store.canMutate || !!a.active_cleanup_id}
                      loading={store.lifecycle.isPending("action")}
                      onClick={() => void store.action("resume")}
                    >
                      Возобновить
                    </Button>
                  ) : (
                    <Button
                      key="archive"
                      aria-label="Архивировать"
                      disabled={!store.canMutate || !!a.active_cleanup_id}
                      onClick={() => openConfirmation("archive")}
                    >
                      Архивировать
                    </Button>
                  ))}
                {a.execution_mode !== "retired" && a.state === "active" && (
                  <Button
                    aria-label={a.paused ? "Продолжить" : "Пауза"}
                    disabled={!store.canCommand || !!a.active_cleanup_id}
                    loading={store.commandBusy}
                    onClick={() =>
                      a.paused
                        ? void store.sendCommand("unpause")
                        : openConfirmation("pause")
                    }
                  >
                    {a.paused ? "Продолжить" : "Пауза"}
                  </Button>
                )}
                <Button
                  danger
                  disabled={!store.canMutate || !!a.active_cleanup_id}
                  onClick={() => openConfirmation("delete")}
                >
                  Удалить агрегат
                </Button>
              </>
            )}
          </div>
          {store.commandOutcome === "unknown" && (
            <div className={styles.stack} role="status">
              <AggregateNotice
                type="warning"
                message="Результат команды неизвестен. Исходные действие, агрегат, тело и ключ сохранены. Противоположная или новая команда не обходит неизвестный результат."
              />
              <Button
                disabled={store.commandBusy}
                loading={store.commandBusy}
                onClick={() => void store.replayCommand()}
              >
                Повторить команду с тем же ключом
              </Button>
            </div>
          )}
          {store.commandOutcome === "confirmed" && (
            <p role="status">
              {store.commandReceipt?.action === "cleanup" ||
              store.commandReceipt?.action === "cleanup_retry"
                ? "Очистка принята. Завершение проверяется отдельно."
                : "Команда подтверждена. Текущее состояние читается отдельно."}
            </p>
          )}
          {store.commandError !== null && (
            <AggregateError error={store.commandError} />
          )}
          {store.commandReadError !== null && (
            <>
              <p>
                Команда принята; чтение карточки не удалось. Повторите только
                чтение.
              </p>
              <AggregateError
                error={store.commandReadError}
                onRetry={() => void store.refreshCommandCard()}
              />
            </>
          )}
          {a.execution_mode === "retired" && (
            <AggregateNotice
              type="info"
              message="Прежний расчёт остановлен. Сохранённые результаты используют прежнее основание времени и доступны для чтения. Для событийного расчёта создайте копию и новый детектор с отдельным прогревом."
            />
          )}
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
              <dt>
                {a.execution_mode === "retired"
                  ? "Прежняя исходная граница и пояс"
                  : "Начало истории, сетка окон и исходный пояс"}
              </dt>
              <dd>
                {a.original_boundary} ({a.timezone})
              </dd>
              <dt>Начало расписания и длительность окна</dt>
              <dd>
                <Time value={a.anchor_utc} zone={zone} />; {a.duration_seconds}{" "}
                с
              </dd>
              <dt>Состояние</dt>
              <dd>
                {states[a.state] || a.state}
                {a.paused && " · Пауза"}
                {a.execution_mode === "retired" &&
                  " · Прежний расчёт остановлен"}
                {a.active_cleanup_id && " · Очистка истории"}
              </dd>
              <dt>Конец следующего окна по расписанию</dt>
              <dd>
                <Time value={a.next_scheduled_at} zone={zone} />
              </dd>
              <dt>Закрытие следующего online-окна не ранее</dt>
              <dd>
                <Time value={a.next_close_not_before_at} zone={zone} />
              </dd>
              <dt>Последняя подтверждённая позиция</dt>
              <dd>Следующий индекс: {exact(a.next_index)}</dd>
              <dt>Запуск процесса / начало online</dt>
              <dd>
                <Time value={a.bootstrap_at} zone={zone} />; индекс{" "}
                {exact(a.online_start_index)}
              </dd>
              <dt>Отставание расчёта после срока</dt>
              <dd>{exact(a.lag_seconds)} с</dd>
            </dl>
            <p>
              Принадлежность:{" "}
              {a.semantic_basis === "event_time"
                ? "исходный ECS @timestamp"
                : "прежнее время доступности"}
              .{" "}
              {a.execution_mode === "current" &&
                "Срок online — конец + min(длительность, 300 секунд). Пауза и перезапуск его не переносят. Начальная завершённая история рассчитывается сразу."}
            </p>
            {a.paused &&
              a.state === "active" &&
              a.execution_mode === "current" && (
                <p role="status">
                  Новые расчёты приостановлены с сохранением позиции. После
                  продолжения доступные накопившиеся окна восполняются; отдельно
                  остановленные детекторы не запускаются.
                </p>
              )}
            {a.state === "archived" && (
              <p role="status">
                Во время архивирования окна намеренно пропускаются и не
                восполняются при архивном возобновлении.
              </p>
            )}
            {a.active_cleanup_id && (
              <p role="status">Ожидание завершения очистки истории.</p>
            )}
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
            <BudgetView
              duration={a.duration_seconds}
              policy={a.execution_policy}
              execution={a.last_execution}
            />
            {write && a.execution_mode !== "retired" && (
              <section className={styles.modalTile}>
                <h3>Просмотр сохранённого окна</h3>
                <p>
                  Выберите начало окна сетки. Длительность —{" "}
                  {a.duration_seconds} с. Это чтение готового результата, без
                  нового расчёта и дополнительной точки. Ещё не готовое окно и
                  очищенное окно дают разные объяснения.
                </p>
                <CalendarTime
                  label="Начало сохранённого окна"
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
                  aria-label="Просмотреть сохранённое окно"
                  loading={store.lifecycle.isPending("history")}
                  disabled={store.lifecycle.isPending("history")}
                  onClick={() => {
                    if (!start) {
                      setHistoryError("Выберите начало сохранённого окна.");
                      return;
                    }
                    void store.history(start).then((ok) => {
                      if (ok) setRefresh((n) => n + 1);
                    });
                  }}
                >
                  Просмотреть сохранённое окно
                </Button>
              </section>
            )}
            {a.state === "deleted" && (
              <AggregateNotice
                type="info"
                message="Агрегат удалён. Новые расчёты окончательно остановлены, сохранённая история доступна."
              />
            )}
            <section
              className={styles.modalTile}
              aria-label="Диагностика событийного времени"
            >
              <h3>Опоздания и исключения</h3>
              <p>
                Опоздавших записей: {exact(a.diagnostics.late_count)}. Они не
                меняют признаки, оценки и прежний снимок.
              </p>
              <p>
                Период событий:{" "}
                <Time
                  value={
                    typeof a.diagnostics.late_start === "string"
                      ? a.diagnostics.late_start
                      : null
                  }
                  zone={zone}
                />{" "}
                —{" "}
                <Time
                  value={
                    typeof a.diagnostics.late_end === "string"
                      ? a.diagnostics.late_end
                      : null
                  }
                  zone={zone}
                />
                .{" "}
                {reasonText(
                  typeof a.diagnostics.late_reason === "string"
                    ? a.diagnostics.late_reason
                    : null,
                )}
              </p>
              {object(a.diagnostics.exclusions) && (
                <ul>
                  {Object.entries(a.diagnostics.exclusions).map(
                    ([reason, count]) => (
                      <li key={reason}>
                        {reasonText(reason)}: {exact(count)}
                      </li>
                    ),
                  )}
                </ul>
              )}
              <p>
                Доступна сводка сохранённой диагностики. Отдельного списка
                поздних событий или участников снимка этот интерфейс не
                получает; исходные события сохраняются.
              </p>
            </section>
            <AutoCleanupPanel store={store} zone={zone} />
            <CleanupPanel store={store} write={write} zone={zone} />
            <PointsView
              aggregate={a}
              zone={zone}
              refresh={refresh}
              clearedVersion={store.pointsVersion}
              clearVersion={store.pointsClearVersion}
              historicalPoint={store.historicalResult}
              selectionKey={start}
            />
          </section>
          <ConfirmationDialog
            key={lastConfirmAction}
            open={confirmationOpen}
            pending={
              confirmationOpen &&
              (store.lifecycle.isPending("action") || store.commandBusy)
            }
            title={
              lastConfirmAction === "pause"
                ? "Поставить агрегат на паузу?"
                : lastConfirmAction === "delete"
                  ? "Удалить агрегат?"
                  : "Архивировать агрегат?"
            }
            message={
              lastConfirmAction === "pause"
                ? `Агрегат «${a.name}» (${a.id}): новые расчёты остановятся с сохранением позиции. После продолжения накопленные окна будут восполнены; правило, история и модели сохранятся.`
                : lastConfirmAction === "delete"
                  ? `Новые расчёты «${a.name}» будут окончательно остановлены. Все связанные детекторы будут остановлены; модели, исторические точки и связи сохраняются.`
                  : `Новые расчёты «${a.name}» остановятся. История сохранится; после возобновления намеренно пропущенные окна не восполняются.`
            }
            confirmText={
              lastConfirmAction === "pause"
                ? "Пауза"
                : lastConfirmAction === "delete"
                  ? "Удалить"
                  : "Архивировать"
            }
            danger
            onCancel={() => setConfirm(null)}
            onConfirm={() => {
              if (confirm && confirm.owner === store) {
                if (confirm.action === "pause") {
                  setConfirm(null);
                  void store.sendCommand("pause");
                  return;
                }
                void store.action(confirm.action).then((ok) => {
                  if (ok || store.mutationOutcome === "unknown")
                    setConfirm(null);
                });
              }
            }}
          >
            {store.error !== null && <AggregateError error={store.error} />}
          </ConfirmationDialog>
        </>
      )}
    </div>
  );
});
