import { Alert, Button, Tooltip } from "antd";
import { InfoCircleOutlined } from "@ant-design/icons";
import styles from "./Detectors.module.css";
import { ApiError } from "../../app/api/client";
import { exact } from "../../app/api/aggregateJson";
import type { DetectorWindow } from "../../app/api/detectors";
import { AppRequestError } from "../../app/ui/AppRequestError";
import { displayTime } from "../../features/aggregates/time";
import { reasonText } from "../../features/detectors/labels";
import type { CommandStore } from "../../features/detectors/Workspace";
import { observer } from "mobx-react-lite";
export function FieldHelp({ label, text }: { label: string; text: string }) {
  return (
    <Tooltip
      title={text}
      trigger={["hover", "focus"]}
      color="var(--color-surface-solid)"
      styles={{
        body: {
          color: "var(--color-text)",
          border: "1px solid var(--color-border)",
        },
      }}
    >
      <button
        type="button"
        className={styles.infoButton}
        aria-label={`Справка: ${label}`}
        onClick={(event) => {
          event.preventDefault();
          event.stopPropagation();
        }}
      >
        <InfoCircleOutlined aria-hidden="true" />
      </button>
    </Tooltip>
  );
}
export function FieldLabel({ label, help }: { label: string; help: string }) {
  return (
    <span className={styles.inline}>
      {label}
      <FieldHelp label={label} text={help} />
    </span>
  );
}
export function Flame({ used }: { used: boolean }) {
  return used ? (
    <Tooltip
      title="Использован ускоренный прогрев"
      color="var(--color-surface-solid)"
      styles={{
        body: {
          color: "var(--color-text)",
          border: "1px solid var(--color-border)",
        },
      }}
    >
      <span role="img" aria-label="Использован ускоренный прогрев" tabIndex={0}>
        🔥
      </span>
    </Tooltip>
  ) : null;
}
export function Time({ value, zone }: { value: string | null; zone: string }) {
  return value ? (
    <time dateTime={value}>{displayTime(value, zone)}</time>
  ) : (
    <>Нет значения</>
  );
}
export function DetectorError({
  error,
  onRetry,
}: {
  error: unknown;
  onRetry?: () => void;
}) {
  const e = error instanceof ApiError ? error : null;
  const message =
    e?.code === "idempotency_conflict"
      ? "Ключ команды уже связан с другим действием. Обновите состояние и обратитесь к администратору."
      : e?.status === 401
        ? "Сессия завершена. Войдите снова."
        : e?.status === 403
          ? "Нет разрешения на действие или проверка сессии не пройдена. Обновите сессию и повторите действие вручную."
          : e?.status === 404
            ? "Детектор или агрегат не найден. Вернитесь к списку."
            : e?.status === 409
              ? "Состояние изменилось. Обновите карточку перед новым действием."
              : e?.status === 422
                ? "Проверьте поля формы и совместимость шага с агрегатом."
                : e?.status === 503
                  ? "Сервис временно недоступен. Обновите состояние перед ручным повтором."
                  : undefined;
  return <AppRequestError error={error} message={message} onRetry={onRetry} />;
}
export const CommandFeedback = observer(
  ({ store }: { store: CommandStore }) => (
    <>
      {store.success && (
        <Alert type="success" role="status" message={store.success} />
      )}
      {store.error !== null && <DetectorError error={store.error} />}
      {store.outcome === "unknown" && (
        <Alert
          type="warning"
          role="alert"
          message="Результат команды неизвестен: сервер мог принять изменение."
          description={
            <>
              <p>
                Исходные данные и ключ сохранены. Автоматического повтора нет.
              </p>
              <Button loading={store.busy} onClick={() => void store.retry()}>
                Повторить исходную команду
              </Button>
              {store.id && (
                <Button onClick={() => void store.load()}>
                  Прочитать текущее состояние
                </Button>
              )}
            </>
          }
        />
      )}
      {store.readError !== null && (
        <DetectorError
          error={store.readError}
          onRetry={() => void store.load()}
        />
      )}
      {store.outcome === "confirmed" &&
        store.receipt &&
        store.readError !== null && (
          <Alert
            type="info"
            message={`Команда подтверждена. Детектор ${store.receipt.detector_id}. Повторите чтение карточки.`}
          />
        )}
    </>
  ),
);
export function WindowView({
  value,
  zone,
}: {
  value: DetectorWindow | null;
  zone: string;
}) {
  return !value ? (
    <p>Нет значения</p>
  ) : (
    <dl>
      <dt>Индекс / результат</dt>
      <dd>
        {exact(value.index)} ·{" "}
        {value.kind === "evaluated" ? "Оценено" : "Пропущено"}
        {value.late && " · Запоздалая обработка"}
      </dd>
      <dt>Технические границы</dt>
      <dd>
        <Time value={value.start_utc} zone={zone} /> —{" "}
        <Time value={value.end_utc} zone={zone} />
      </dd>
      <dt>Плановый момент</dt>
      <dd>
        <Time value={value.scheduled_at} zone={zone} />
      </dd>
      <dt>Расчёт агрегата: начало / конец</dt>
      <dd>
        <Time value={value.aggregation_started_at} zone={zone} /> /{" "}
        <Time value={value.aggregation_completed_at} zone={zone} />
      </dd>
      <dt>Оценка: начало / конец</dt>
      <dd>
        <Time value={value.evaluation_started_at} zone={zone} /> /{" "}
        <Time value={value.evaluation_completed_at} zone={zone} />
      </dd>
      {value.reason && (
        <>
          <dt>Причина</dt>
          <dd>{reasonText(value.reason, "history")}</dd>
        </>
      )}
    </dl>
  );
}
