import { ApiError } from "../../app/api/client";
import { AppRequestError } from "../../app/ui/AppRequestError";
import { object } from "../../app/api/aggregateJson";
import { CatalogConsistencyError } from "../../features/aggregates/CatalogStore";
import { useState } from "react";
import { Button } from "antd";
import { CloseOutlined } from "@ant-design/icons";
import styles from "./Aggregates.module.css";

const messages: Record<string, string> = {
  field_catalog_budget_exceeded:
    "Каталог источника превышает бюджет. Полный список не получен; уточните объём данных с администратором.",
  field_catalog_timeout:
    "Каталог не получен за отведённое время. Повторите загрузку явно.",
  source_normalizer_invalid:
    "Назначенный нормализатор некорректен. Каталог не подменяется неполными данными; проверьте назначение.",
  window_before_anchor:
    "Окно начинается раньше исходной границы расписания. Выберите завершённое окно не раньше этой границы.",
  window_not_aligned:
    "Начало окна не совпадает с расписанием. Выберите исходную границу или прибавьте к ней целое число длительностей окна.",
  window_not_completed:
    "Окно ещё не завершено. При будущем якоре создание пока недоступно; выберите согласованную границу в прошлом или дождитесь завершения.",
  aggregate_inactive: "Агрегат не принимает новые расчёты. Обновите состояние.",
  resource_budget_exceeded:
    "Расчёт превышает бюджет. Уменьшите окно, список значений или сложность правила.",
  preview_limit_exceeded:
    "Слишком много записей для предварительного расчёта. Уменьшите окно; усечённый результат не используется.",
  historical_time_basis_unknown:
    "Основание времени истории не доказано. Выберите окно внутри подтверждённой истории или используйте ретроспективный пробный расчёт без сохранения точки.",
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
