import { useEffect, useState } from "react";
import { Alert, Button, Select } from "antd";
import { observer } from "mobx-react-lite";
import { Link, useParams } from "react-router-dom";
import { useStores } from "../../app/providers/StoresProvider";
import { exact } from "../../app/api/aggregateJson";
import {
  executions,
  models,
  aggregateStates,
  reasonText,
} from "../../features/detectors/labels";
import { ConfirmationDialog } from "../../shared/ui/ConfirmationDialog";
import { LoadingOverlay } from "../../shared/ui/LoadingOverlay";
import { CommandFeedback, Flame, Time, WindowView, FieldLabel } from "./Parts";
import styles from "./Detectors.module.css";
export const DetectorCardPage = observer(() => {
  const { id = "" } = useParams();
  const { detectors, session } = useStores();
  const s = detectors.card(id);
  const c = s.card;
  const [zone, setZone] = useState("UTC");
  const [confirm, setConfirm] = useState<{
    id: string;
    name: string;
    actor: number;
    store: typeof s;
  } | null>(null);
  const actor = session.version;
  const canWrite = session.has("detectors.write");
  useEffect(() => {
    setConfirm(null);
  }, [id, actor, canWrite, s]);
  const [visible, setVisible] = useState(!document.hidden);
  useEffect(() => {
    const visibility = () => {
      s.pause();
      setVisible(!document.hidden);
      if (!document.hidden && session.has("detectors.read")) void s.load();
    };
    if (!document.hidden && session.has("detectors.read")) void s.load();
    document.addEventListener("visibilitychange", visibility);
    return () => {
      s.pause();
      document.removeEventListener("visibilitychange", visibility);
    };
  }, [s, session]);
  useEffect(() => {
    if (
      !visible ||
      !session.has("detectors.read") ||
      s.readError ||
      s.outcome === "unknown" ||
      s.busy
    )
      return;
    const timer = setTimeout(() => void s.load(), 3000);
    return () => clearTimeout(timer);
  }, [s, session, visible, s.readVersion, s.readError, s.outcome, s.busy]);
  const canStart =
    c &&
    c.aggregate_state === "active" &&
    ["created", "stopped", "failed"].includes(c.execution_state);
  const canStop =
    c && ["requested", "starting", "running"].includes(c.execution_state);
  useEffect(() => {
    if (!canStop || s.needsRead || s.outcome === "unknown") setConfirm(null);
  }, [canStop, s.needsRead, s.outcome]);
  return (
    <div className={styles.page}>
      <Link to="/detectors">К списку детекторов</Link>
      <CommandFeedback store={s} />
      <div className={styles.toolbar}>
        <Button
          loading={s.lifecycle.isPending("load")}
          disabled={s.busy}
          onClick={() => void s.load()}
        >
          Обновить состояние
        </Button>
        <label className={styles.zoneField}>
          <FieldLabel
            label="Часовой пояс"
            help="Меняет только отображение абсолютных моментов. UTC-границы, длительность окон, прогрев и серверные запросы не изменяются."
          />
          <Select
            aria-label="Часовой пояс"
            value={zone}
            options={[
              ...new Set([
                "UTC",
                Intl.DateTimeFormat().resolvedOptions().timeZone,
                "Asia/Yekaterinburg",
                "Europe/Moscow",
              ]),
            ].map((value) => ({ value, label: value }))}
            onChange={setZone}
          />
        </label>
      </div>
      {!c ? (
        s.readError === null && <LoadingOverlay />
      ) : (
        <>
          <h2>
            {c.name} <Flame used={c.accelerated_used} />
          </h2>
          <div className={styles.grid}>
            <section className={styles.panel}>
              <h3>Исполнение</h3>
              <p>{executions[c.execution_state]}</p>
              <p>Модель: {models[c.model_state]}</p>
              <p>
                Разрешение исполнения не подтверждает исправность процесса
                worker. Готовая остановленная модель не выполняет оценки.
              </p>
              <p>{reasonText(c.reason, c.execution_state)}</p>
              {session.has("detectors.write") && (
                <div className={styles.toolbar}>
                  {canStart && (
                    <Button
                      type="primary"
                      disabled={s.outcome === "unknown" || s.needsRead}
                      loading={s.busy}
                      onClick={() => void s.send("start")}
                    >
                      Запустить
                    </Button>
                  )}
                  {canStop && (
                    <Button
                      disabled={
                        s.busy || s.outcome === "unknown" || s.needsRead
                      }
                      loading={s.busy}
                      onClick={() =>
                        setConfirm({ id, name: c.name, actor, store: s })
                      }
                    >
                      Остановить
                    </Button>
                  )}
                </div>
              )}
              {c.execution_state === "terminated" && (
                <p>Агрегат удалён: возобновление невозможно.</p>
              )}
              {!canStart && !canStop && c.aggregate_state !== "active" && (
                <p>Для нового запуска нужен активный агрегат.</p>
              )}
            </section>
            <section className={styles.panel}>
              <h3>Агрегат и признаки</h3>
              <Link to={`/aggregates/${c.aggregate_id}`}>
                {c.aggregate_name}
              </Link>
              <p>
                {aggregateStates[c.aggregate_state]} · {c.source_name}
              </p>
              <p>
                Окно: {exact(c.duration_seconds)} с. UTC-основание:{" "}
                <Time value={c.anchor_utc} zone={zone} />.
              </p>
              <ol>
                {c.feature_keys.map((key, i) => (
                  <li key={key}>
                    {c.feature_names[i]} ({key})
                  </li>
                ))}
              </ol>
              <p>Порядок фиксирован. Пояс меняет только отображение.</p>
            </section>
          </div>
          <section className={styles.panel}>
            <h3>Настройки RCF</h3>
            <dl>
              {Object.entries({
                dimensions: "Размерность",
                trees: "Деревья",
                sample_size: "Размер выборки",
                output_after: "Порог оценки",
                seed: "Начальное значение генератора",
              }).map(([key, label]) => (
                <div key={key}>
                  <dt>{label}</dt>
                  <dd>{exact(c.config[key as keyof typeof c.config])}</dd>
                </div>
              ))}
            </dl>
            <p>
              Режим: {c.accelerated ? "Ускоренный" : "Обычный"}; шаг{" "}
              {exact(c.step_seconds)} с.{" "}
              {c.accelerated_used
                ? "Использован ускоренный прогрев."
                : "Ускоренное происхождение не подтверждено."}
            </p>
          </section>
          <section className={styles.panel}>
            <h3>Прогрев</h3>
            <p>
              Историю выбирает система. Обучающие окна не создают находок.
              Перекрывающиеся точки не равны независимым периодам. Техническая
              готовность не гарантирует точность.
            </p>
            {!c.warmup ? (
              <p>Сводки прогрева пока нет.</p>
            ) : (
              <>
                <p>
                  Готовность: {models[c.warmup.readiness]}. Статус:{" "}
                  {
                    {
                      preparing: "Подготовка",
                      prepared: "История подготовлена",
                      waiting: "Ожидание",
                      ready: "Готово",
                    }[c.warmup.status]
                  }
                </p>
                <dl>
                  {Object.entries({
                    required_windows: "Требуется окон",
                    planned_windows: "Запланировано",
                    materialized_windows: "Подготовлено",
                    feature_valid_windows: "Численно пригодно",
                    eligible_windows: "Пригодно для модели",
                    ineligible_windows: "Непригодно",
                    step_seconds: "Шаг, с",
                    materialized_coverage_seconds: "Подготовленный охват, с",
                    eligible_coverage_seconds: "Реальный пригодный охват, с",
                  }).map(([key, label]) => (
                    <div key={key}>
                      <dt>{label}</dt>
                      <dd>{exact(c.warmup![key as keyof typeof c.warmup])}</dd>
                    </div>
                  ))}
                </dl>
                <p>
                  Граница истории: <Time value={c.warmup.cutoff} zone={zone} />
                </p>
                {c.warmup.reason && (
                  <p>{reasonText(c.warmup.reason, "history")}</p>
                )}
              </>
            )}
          </section>
          <div className={styles.grid}>
            <section className={styles.panel}>
              <h3>Последнее потреблённое окно</h3>
              <WindowView value={c.last_consumed} zone={zone} />
            </section>
            <section className={styles.panel}>
              <h3>Последнее оценённое окно</h3>
              <WindowView value={c.last_evaluated} zone={zone} />
            </section>
          </div>
          <section className={styles.panel}>
            <h3>Отставание и восстановление</h3>
            <p>
              Окон в очереди: {exact(c.backlog)}. Задержка:{" "}
              {exact(c.lag_seconds)}
              {c.lag_seconds !== null && " с"}.
            </p>
            <p>
              Следующий срок: <Time value={c.next_scheduled_at} zone={zone} />.
              Ожидание точки:{" "}
              {c.waiting_for_point === null
                ? "Нет значения"
                : c.waiting_for_point
                  ? "Да"
                  : "Нет"}
              .
            </p>
            {c.recovery ? (
              <p>
                {c.recovery.reason === "crash_recovery"
                  ? "Аварийное восстановление"
                  : "Ручное возобновление"}
                : индексы {exact(c.recovery.first_index)} —{" "}
                {exact(c.recovery.last_index)};{" "}
                <Time value={c.recovery.started_at} zone={zone} />.
              </p>
            ) : (
              <p>Сводки восстановления нет.</p>
            )}
          </section>
          {c.last_failure && (
            <Alert
              type="warning"
              message="Предыдущая ошибка"
              description={
                <>
                  {reasonText(c.last_failure.reason, "history")}{" "}
                  <Time value={c.last_failure.at} zone={zone} />
                </>
              }
            />
          )}
          <details className={styles.panel}>
            <summary>Сохранённые моменты и идентификаторы</summary>
            <dl>
              {(
                [
                  "created_at",
                  "updated_at",
                  "start_requested_at",
                  "execution_started_at",
                  "stopped_at",
                ] as const
              ).map((key) => (
                <div key={key}>
                  <dt>
                    {
                      {
                        created_at: "Создан",
                        updated_at: "Обновлён",
                        start_requested_at: "Запуск запрошен",
                        execution_started_at: "Исполнение начато",
                        stopped_at: "Остановлен",
                      }[key]
                    }
                  </dt>
                  <dd>
                    <Time value={c[key]} zone={zone} />
                  </dd>
                </div>
              ))}
              <dt>UUID</dt>
              <dd>{c.id}</dd>
              <dt>Ревизия / поколение</dt>
              <dd>
                {exact(c.revision)} / {exact(c.execution_generation)}
              </dd>
            </dl>
          </details>
        </>
      )}
      <ConfirmationDialog
        open={
          !!confirm &&
          confirm.id === id &&
          confirm.actor === actor &&
          confirm.store === s &&
          canWrite &&
          !!canStop
        }
        title="Остановить детектор?"
        message={`Детектор «${confirm?.name ?? ""}» (${confirm?.id ?? ""}). Новые оценки прекратятся. Модель и курсор сохранятся. Следующий запуск обработает накопленные окна; агрегат и другие детекторы продолжат работу.`}
        confirmText="Остановить"
        pending={s.busy}
        onCancel={() => setConfirm(null)}
        onConfirm={() => {
          const target = confirm;
          setConfirm(null);
          if (
            target?.id === id &&
            target.actor === session.version &&
            target.store === s &&
            session.has("detectors.write") &&
            canStop &&
            !s.busy &&
            s.outcome !== "unknown" &&
            !s.needsRead
          )
            void s.send("stop");
        }}
      />
    </div>
  );
});
