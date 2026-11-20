import { Button } from "antd";
import { observer } from "mobx-react-lite";
import type { AutoCleanup } from "../../app/api/aggregates";
import { exact, type Json } from "../../app/api/aggregateJson";
import type { CardStore } from "../../features/aggregates/CardStore";
import { AggregateError } from "./AggregateError";
import { Time } from "./ResultView";
import styles from "./Aggregates.module.css";

const statuses: Record<AutoCleanup["status"], string> = {
  scheduled: "Ожидается проверка хранения",
  idle: "Проверка хранения завершена",
  compacting: "Освобождаются безопасные старые данные",
  protected: "Необходимая история защищена",
  deferred: "Проверка хранения отложена",
  error: "Состояние автоматического хранения не подтверждено",
  inactive: "Новые автоматические удаления приостановлены",
};
const explanations: Record<AutoCleanup["status"], string> = {
  scheduled: "Система ещё не подтвердила текущий план хранения.",
  idle: "Более старые значения удаляются только после проверки потребностей всех моделей.",
  compacting:
    "История освобождается порциями; сведения о закрытых окнах сохраняются.",
  protected:
    "Данные сохраняются для прогрева или продолжения моделей. Защищённая история может превышать обычный резерв.",
  deferred:
    "Текущий проход уступил другой работе. Безопасность удаления будет проверена снова.",
  error:
    "Удаление без подтверждённой защиты недоступно. Неподтверждённые значения не означают ноль удалённых точек.",
  inactive:
    "Пауза, архив, удаление или прежнее исполнение запрещают новые автоматические удаления. Уже принятые порции могут завершиться.",
};
const reasons: Record<string, string> = {
  retention_state_missing: "Служебное состояние хранения отсутствует.",
  aggregate_inactive:
    "Архивный или удалённый агрегат не получает новых автоматических удалений.",
  aggregate_paused:
    "Агрегат на паузе; новые автоматические удаления приостановлены.",
  aggregate_time_semantics_retired:
    "Прежние результаты автоматически не удаляются.",
  cleanup_in_progress: "Выполняется отдельная ручная очистка истории.",
  dependencies_changed:
    "Потребности моделей изменились; защиту нужно проверить снова.",
  protection_unavailable:
    "Проверить защиту всех моделей не удалось, поэтому история сохраняется.",
  retention_budget:
    "Проверка защиты не завершилась в выделенный бюджет; история сохраняется.",
  retention_database_error:
    "Состояние хранения не удалось подтвердить в базе данных.",
  retention_turn_failed:
    "Порция освобождения не завершена. Сохранённый прогресс будет проверен перед продолжением.",
  model_history_reserve:
    "Сохраняется недавняя история для прогрева новых моделей.",
  unconsumed_points: "Сохраняются ещё не потреблённые моделью окна.",
  frozen_warmup: "История закреплена незавершённой подготовкой прогрева.",
  prepared_warmup_continuation:
    "Сохраняется история для продолжения подтверждённой подготовки.",
  planned_warmup: "История нужна для начального прогрева модели.",
  insufficient_warmup_history:
    "Для прогрева пока недостаточно истории; имеющиеся необходимые данные сохраняются.",
};
function reason(code: string) {
  return (
    reasons[code] ?? "Безопасность удаления этой истории пока не подтверждена."
  );
}
function known(value: Json) {
  return value === null ? "неизвестно" : exact(value);
}
export const AutoCleanupPanel = observer(
  ({ store, zone }: { store: CardStore; zone: string }) => {
    const summary = store.card?.auto_cleanup;
    if (!summary) return null;
    return (
      <section
        className={styles.modalTile}
        aria-label="Автоматическое хранение истории"
      >
        <h3>Автоматическое хранение истории</h3>
        <p>
          Система освобождает безопасные старые значения, сохраняя нужную
          моделям историю и резерв прогрева. Исходные события, модели и находки
          сохраняются. Глубину обычного хранения выбирать не требуется; ручная
          очистка — отдельное действие.
        </p>
        <p>
          {statuses[summary.status]}. {explanations[summary.status]}
        </p>
        {summary.reason && summary.reason !== "retention_state_missing" && (
          <p>{reason(summary.reason)}</p>
        )}
        {summary.reason === "retention_state_missing" && (
          <p role="alert">
            Служебное состояние хранения отсутствует. Проверку и удаление нельзя
            считать подтверждёнными.
          </p>
        )}
        <dl>
          <dt>Автоматически очищено точек</dt>
          <dd>{known(summary.auto_cleared_total)}</dd>
          <dt>Последняя автоматическая очистка</dt>
          <dd>
            <Time value={summary.last_cleared_at} zone={zone} />
          </dd>
          <dt>Защищающих моделей</dt>
          <dd>{known(summary.protected_total)}</dd>
        </dl>
        {summary.protected_by.length > 0 && (
          <ul aria-label="Модели, защищающие историю">
            {summary.protected_by.map((model) => (
              <li key={model.detector_id}>
                {model.name} · {model.detector_id}: {reason(model.reason)}
              </li>
            ))}
          </ul>
        )}
        {summary.protected_total !== null &&
          BigInt(exact(summary.protected_total)) >
            BigInt(summary.protected_by.length) && (
            <p>
              Показаны первые {summary.protected_by.length} моделей; защита
              проверяется для всех.
            </p>
          )}
        {store.autoCleanupError !== null && (
          <div>
            <p>
              Не удалось обновить состояние хранения. Показан предыдущий
              подтверждённый снимок.
            </p>
            <AggregateError
              error={store.autoCleanupError}
              onRetry={() => void store.load()}
            />
          </div>
        )}
        <details>
          <summary>Технические сведения хранения</summary>
          <dl>
            <dt>Политика</dt>
            <dd>{summary.policy_version ?? "неизвестно"}</dd>
            <dt>Ревизия истории</dt>
            <dd>{known(summary.history_revision)}</dd>
            <dt>Системный резерв прогрева, окон</dt>
            <dd>{known(summary.reserve_windows)}</dd>
            <dt>Сохранять начиная с индекса</dt>
            <dd>{known(summary.keep_from_index)}</dd>
            <dt>Защищённая история начиная с индекса</dt>
            <dd>{known(summary.protected_from_index)}</dd>
            <dt>Сохранённые индексы</dt>
            <dd>
              {known(summary.retained_from_index)} —{" "}
              {known(summary.retained_to_index)}
            </dd>
            <dt>Проверено</dt>
            <dd>
              <Time value={summary.checked_at} zone={zone} />
            </dd>
            <dt>Следующая проверка</dt>
            <dd>
              <Time value={summary.next_check_at} zone={zone} />
            </dd>
            <dt>Код причины</dt>
            <dd>{summary.reason ?? "—"}</dd>
          </dl>
          {summary.protected_by.length > 0 && (
            <ul>
              {summary.protected_by.map((model) => (
                <li key={model.detector_id}>
                  {model.name}: {model.reason}; защищать с индекса{" "}
                  {known(model.protected_from_index)}, подтверждённый конец
                  диапазона {known(model.protected_to_index)}
                </li>
              ))}
            </ul>
          )}
        </details>
        {summary.status === "error" && (
          <Button loading={store.cardLoading} onClick={() => void store.load()}>
            Обновить состояние хранения
          </Button>
        )}
      </section>
    );
  },
);
