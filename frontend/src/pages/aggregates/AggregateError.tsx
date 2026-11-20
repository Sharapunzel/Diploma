import { ApiError } from "../../app/api/client";
import { AppRequestError } from "../../app/ui/AppRequestError";
import { object } from "../../app/api/aggregateJson";
import { CatalogConsistencyError } from "../../features/aggregates/CatalogStore";
import { useState } from "react";
import { Button } from "antd";
import { CloseOutlined } from "@ant-design/icons";
import styles from "./Aggregates.module.css";

const messages: Record<string, string> = {
  canonical_point_not_ready:
    "Сохранённый результат окна ещё не готов. Обновите состояние или выберите другое окно.",
  aggregate_point_cleared:
    "Окно было закрыто, но его значения очищены. Они не будут рассчитаны повторно; выберите другое окно.",
  cleanup_in_progress:
    "История очищается. Конфликтующее действие доступно после завершения операции.",
  cleanup_protection_unavailable:
    "Защиту моделей не удалось надёжно проверить. Очистка недоступна; повторите чтение плана.",
  cleanup_protected:
    "План защиты изменился или выбранного хвоста недостаточно. Обновите план; непрочитанные точки не удаляются.",
  cleanup_protected_points:
    "Выбранного хвоста недостаточно для моделей. Обновите план защиты и сохраните не меньше серверного минимума.",
  cleanup_requires_stopped_detectors:
    "Очистка требует подтверждённой остановки всех связанных возобновляемых детекторов. Остановите их явно и обновите план.",
  aggregate_time_semantics_retired:
    "Прежний расчёт остановлен. История доступна для чтения; новый процесс создаётся через копию.",
  field_catalog_budget_exceeded:
    "Каталог источника превышает бюджет. Полный список не получен; уточните объём данных с администратором.",
  field_catalog_timeout:
    "Каталог не получен за отведённое время. Повторите загрузку явно.",
  source_normalizer_invalid:
    "Назначенный нормализатор некорректен. Каталог не подменяется неполными данными; проверьте назначение.",
  window_before_anchor:
    "Окно до начала автоматической истории не входит в сохранённый ряд. Выберите окно процесса; предварительная проба до границы доступна отдельно в черновике.",
  window_not_aligned:
    "Начало окна не совпадает с расписанием. Выберите исходную границу или прибавьте к ней целое число длительностей окна.",
  window_not_completed:
    "Окно ещё не завершено. Выберите завершённое окно сетки; проба до начала истории будет явно отмечена как вне процесса.",
  aggregate_inactive: "Агрегат не принимает новые расчёты. Обновите состояние.",
  resource_budget_exceeded:
    "Расчёт превышает бюджет. Уменьшите окно, список значений или сложность правила.",
  preview_limit_exceeded:
    "Слишком много записей для предварительного расчёта. Уменьшите окно; усечённый результат не используется.",
  historical_time_basis_unknown:
    "Прежнее основание времени не подтверждено. Доступны ранее сохранённые результаты; новый событийный процесс создаётся через копию.",
  barrier_pending:
    "Барьер ещё не обработан. Обновите состояние и повторите запрос позже.",
  capture_unavailable:
    "Захват времени недоступен. Обратитесь к администратору и обновите состояние.",
  availability_capture_unavailable:
    "Захват времени недоступен. Обратитесь к администратору и обновите состояние.",
  source_unavailable:
    "Источник недоступен. История сохраняется; проверьте источник перед новым расчётом или созданием копии.",
  aggregate_timeout:
    "Расчёт не завершился в отведённое время. Уменьшите окно или сложность и повторите вручную.",
  preview_wait_timeout:
    "Время ожидания предварительного расчёта истекло. Это не подтверждает остановку вычисления на сервере. Данные формы сохранены; повторная проба доступна вручную.",
  execution_budget_exceeded:
    "Предварительный расчёт остановлен: исчерпан бюджет времени. Точка не сохранена. Измените правило или окно и повторите пробу вручную.",
  invalid_literal_type:
    "Значение не соответствует типу ECS. Проверьте указанное поле, тип числа или дату с явным смещением.",
  unknown_or_forward_reference:
    "Ссылка должна указывать на предыдущий признак. Исправьте порядок или ссылку.",
  boundary_offset_does_not_match_timezone:
    "Смещение исходной границы не соответствует выбранному IANA-поясу.",
  source_not_found:
    "Исходный источник недоступен. История агрегата сохраняется; для копии выберите доступный собственный источник.",
};
const factors: Record<string, string> = {
  preview_rows: "Число записей для расчёта",
  statement_width: "Число расчётных столбцов",
  operator_cost: "Сложность операций",
  total_memory: "Оценка памяти",
  array_elements: "Число элементов массивов",
};
export function AggregateError({
  error,
  onRetry,
  onDismiss,
}: {
  error: unknown;
  onRetry?: () => void;
  onDismiss?: () => void;
}) {
  const [dismissed, setDismissed] = useState<{ value: unknown } | null>(null);
  const details =
    error instanceof ApiError && object(error.details) ? error.details : null;
  const cleared =
    error instanceof ApiError && error.code === "aggregate_point_cleared";
  const clearedAt =
    cleared &&
    typeof details?.cleared_at === "string" &&
    /^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?(?:Z|\+00:00)$/.test(
      details.cleared_at,
    ) &&
    Number.isFinite(Date.parse(details.cleared_at))
      ? details.cleared_at
      : null;
  const safePath = (value: unknown) =>
    typeof value === "string" &&
    value.length <= 500 &&
    /^[a-zA-Z_][a-zA-Z0-9_.[\]]*$/.test(value)
      ? value
      : "";
  const path = safePath(details?.path);
  const errors = Array.isArray(details?.errors)
    ? details.errors
        .filter(object)
        .flatMap((e) =>
          Array.isArray(e.loc)
            ? [
                e.loc
                  .filter((v) => typeof v === "string" || typeof v === "number")
                  .join("."),
              ].filter((p) => safePath(p))
            : [],
        )
    : [];
  const factor =
    typeof details?.factor === "string" && /^[a-z_]+$/.test(details.factor)
      ? details.factor
      : error instanceof ApiError &&
          error.code === "resource_budget_exceeded" &&
          [
            "preview_rows",
            "statement_width",
            "operator_cost",
            "total_memory",
            "array_elements",
          ].includes(path)
        ? path
        : "";
  const message =
    error instanceof ApiError
      ? messages[error.code]
      : error instanceof CatalogConsistencyError
        ? error.message
        : undefined;
  const locate = (path: string) => {
    const match = /(?:^|\.)(features|auxiliary)(?:\[(\d+)\]|\.(\d+))/.exec(
      path,
    );
    const element = match
      ? document.getElementById(
          `definition-${match[1]}-${match[2] ?? match[3]}`,
        )
      : /(?:^|\.)where\b/.test(path)
        ? document.getElementById("definition-where")
        : null;
    if (element && !match) {
      element.scrollIntoView?.({ block: "center" });
      element.querySelector<HTMLElement>("input,button")?.focus();
      return;
    }
    const button = element?.querySelector<HTMLButtonElement>(
      "[data-definition-open]",
    );
    if (button) {
      element?.scrollIntoView?.({ block: "center" });
      button.focus();
      button.click();
    } else
      document
        .querySelector<HTMLTextAreaElement>(
          'textarea[aria-label="Полное правило JSON"]',
        )
        ?.focus();
  };
  if (dismissed?.value === error)
    return (
      <Button size="small" onClick={() => setDismissed(null)}>
        Показать сообщение об ошибке
      </Button>
    );
  return (
    <div role="alert" className={styles.errorMessage}>
      <Button
        className={styles.dismissError}
        type="text"
        size="small"
        icon={<CloseOutlined />}
        aria-label="Закрыть сообщение об ошибке"
        onClick={() => {
          setDismissed({ value: error });
          onDismiss?.();
        }}
      />
      <AppRequestError error={error} onRetry={onRetry} message={message} />
      {cleared && (
        <p>
          {details?.clear_reason === "automatic"
            ? "Значения очищены автоматически."
            : details?.clear_reason === "manual"
              ? "Значения очищены ручной командой."
              : "Способ очистки прежнего окна неизвестен."}
          {clearedAt ? (
            <>
              {" "}
              Время очистки (UTC): <time dateTime={clearedAt}>{clearedAt}</time>
              .
            </>
          ) : (
            " Время очистки неизвестно."
          )}
        </p>
      )}
      {path && (
        <p>
          Исправьте узел:{" "}
          <button type="button" onClick={() => locate(path)}>
            {path}
          </button>
        </p>
      )}
      {factor && (
        <p>
          Фактор ограничения: {factors[factor] ?? "Ресурсоёмкость расчёта"}.
          Уменьшите период или сложность.
        </p>
      )}
      {errors.map((path, i) => (
        <p key={i}>
          Проверьте поле:{" "}
          <button type="button" onClick={() => locate(path)}>
            {path}
          </button>
        </p>
      ))}
    </div>
  );
}
