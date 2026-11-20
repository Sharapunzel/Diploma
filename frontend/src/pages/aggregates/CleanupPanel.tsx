import { useState } from "react";
import { Button, Input } from "antd";
import { observer } from "mobx-react-lite";
import type { CardStore } from "../../features/aggregates/CardStore";
import type { CleanupProtection } from "../../app/api/aggregates";
import { exact } from "../../app/api/aggregateJson";
import { reasonText } from "../../features/aggregates/labels";
import { ConfirmationDialog } from "../../shared/ui/ConfirmationDialog";
import { AggregateError } from "./AggregateError";
import { FieldLabel } from "./AggregateHelp";
import { Time } from "./ResultView";
import styles from "./Aggregates.module.css";

const statuses = {
  pending: "Ожидает выполнения",
  running: "Выполняется",
  succeeded: "Завершена",
  failed: "Приостановлена из-за ошибки",
};
export const CleanupPanel = observer(
  ({
    store,
    write,
    zone,
  }: {
    store: CardStore;
    write: boolean;
    zone: string;
  }) => {
    const [confirmation, setConfirmation] = useState<{
      owner: CardStore;
      plan: CleanupProtection;
      keep: string;
    } | null>(null);
    const plan = store.cleanupPlan;
    const operation = store.cleanupOperation;
    const open =
      confirmation?.owner === store &&
      confirmation.plan === plan &&
      confirmation.keep === store.cleanupKeepLast &&
      store.canCleanup;
    return (
      <section className={styles.modalTile} aria-label="Ручная очистка истории">
        <h3>Ручная очистка истории</h3>
        <p>
          Оставить последние N точек, удалить более старые значения. N=0
          означает всё, когда защита разрешает. Исходные события, правило,
          модели и находки сохраняются. Это разовое действие; минимальные
          служебные сведения о закрытых окнах остаются, чтобы не создавать их
          заново. Окна «Не рассчитано» сохраняются как диагностика и не входят в
          счётчик удаляемых данных рассчитанных точек.
        </p>
        <div className={styles.field}>
          <FieldLabel help="Десятичное целое от 0 до 9223372036854775807. Сервер учитывает все связанные модели, включая непрочитанные точки и прогрев. Все возобновляемые детекторы должны быть остановлены явно; необратимо остановленные прежние модели не требуют хвоста.">
            Оставить последние N точек
          </FieldLabel>
          <Input
            aria-label="Оставить последние N точек"
            inputMode="numeric"
            value={store.cleanupKeepLast}
            status={store.cleanupInputError ? "error" : undefined}
            disabled={store.commandBusy || !!store.card?.active_cleanup_id}
            onChange={(event) => store.setCleanupKeepLast(event.target.value)}
          />
          {store.cleanupInputError && (
            <p role="alert">{store.cleanupInputError}</p>
          )}
        </div>
        <div className={styles.toolbar}>
          <Button
            loading={store.cleanupPlanLoading}
            disabled={
              !!store.cleanupInputError ||
              store.cleanupPlanLoading ||
              !!store.card?.active_cleanup_id
            }
            onClick={() => void store.loadCleanupPlan()}
          >
            Проверить защиту истории
          </Button>
          {write && (
            <Button
              danger
              disabled={!store.canCleanup}
              onClick={() =>
                plan &&
                setConfirmation({
                  owner: store,
                  plan,
                  keep: store.cleanupKeepLast,
                })
              }
            >
              Очистить историю
            </Button>
          )}
        </div>
        {store.cleanupPlanError !== null && (
          <AggregateError
            error={store.cleanupPlanError}
            onRetry={() => void store.loadCleanupPlan()}
          />
        )}
        {plan && (
          <div aria-label="План защиты истории">
            <dl>
              <dt>Сохранённых точек</dt>
              <dd>{exact(plan.total_points)}</dd>
              <dt>Минимум сохранить</dt>
              <dd>{exact(plan.minimum_keep)}</dd>
              <dt>Максимум удалить</dt>
              <dd>{exact(plan.maximum_delete)}</dd>
              <dt>Будет удалено</dt>
              <dd>{exact(plan.planned_delete)}</dd>
            </dl>
            {!plan.can_cleanup && (
              <p role="status">
                Очистка пока недоступна. {reasonText(plan.reason)} Обновите план
                после явной остановки всех связанных моделей.
              </p>
            )}
            <ul>
              {plan.blockers.map((blocker) => (
                <li key={blocker.detector_id}>
                  {blocker.name} · {blocker.detector_id}:{" "}
                  {reasonText(blocker.reason)}; минимум{" "}
                  {exact(blocker.minimum_keep)}; защищённые индексы{" "}
                  {exact(blocker.protected_start_index)} —{" "}
                  {exact(blocker.protected_end_index)}.
                </li>
              ))}
            </ul>
            <p>
              План на <Time value={plan.reference_at} zone={zone} />. Сервер
              повторно проверит защиту при принятии команды.
            </p>
          </div>
        )}
        {(operation ||
          store.card?.active_cleanup_id ||
          store.cleanupOperationError !== null) && (
          <section
            className={styles.stack}
            aria-label="Состояние очистки"
            aria-live="polite"
          >
            <h4>Состояние очистки</h4>
            {operation ? (
              <>
                <p>
                  {statuses[operation.status]} · {operation.id}
                </p>
                <p>
                  Удалено точек: {exact(operation.deleted_points)};
                  запланировано: {exact(operation.planned_delete)}. Освобождено
                  байт:{" "}
                  {operation.deleted_bytes_if_measured === null
                    ? "не измерено"
                    : exact(operation.deleted_bytes_if_measured)}
                  .
                </p>
                {operation.status === "succeeded" && (
                  <p role="status">
                    Очистка завершена. Список и количество точек обновлены;
                    очищенные окна не рассчитываются повторно.
                  </p>
                )}
                {operation.status === "failed" && (
                  <>
                    <p role="alert">
                      Операция сохраняет границу и выполненный прогресс.{" "}
                      {reasonText(operation.safe_error)}
                    </p>
                    {write && (
                      <Button
                        disabled={!store.canCommand || store.commandBusy}
                        onClick={() => void store.sendCommand("cleanup_retry")}
                      >
                        Продолжить очистку
                      </Button>
                    )}
                  </>
                )}
              </>
            ) : (
              <p>
                Очистка принята. Загружается фактическое состояние операции.
              </p>
            )}
            {store.cleanupOperationError !== null && (
              <AggregateError error={store.cleanupOperationError} />
            )}
            <Button onClick={() => void store.loadCleanupOperation()}>
              Обновить состояние очистки
            </Button>
          </section>
        )}
        {!!store.card?.active_cleanup_id && (
          <p role="status">
            До завершения очистки новые расчёты и конфликтующие команды этого
            агрегата недоступны. Уход со страницы не отменяет операцию.
          </p>
        )}
        <ConfirmationDialog
          open={!!open}
          title="Очистить историю?"
          confirmText="Очистить"
          danger
          pending={store.commandBusy}
          message={`Агрегат «${store.card?.name}» (${store.id}): оставить ${confirmation?.keep ?? ""}, удалить ${confirmation ? exact(confirmation.plan.planned_delete) : ""} старых точек. Правило, события, модели и находки сохранятся. Значения очищенных окон не восстанавливаются.`}
          onCancel={() => setConfirmation(null)}
          onConfirm={() => {
            if (open) {
              setConfirmation(null);
              void store.sendCommand("cleanup");
            }
          }}
        />
      </section>
    );
  },
);
