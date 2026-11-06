import { ApiError } from "../../app/api/client";
import { AppRequestError } from "../../app/ui/AppRequestError";
import { object } from "../../app/api/aggregateJson";

const messages: Record<string, string> = {
  window_before_anchor:
    "Окно начинается раньше UTC-якоря. Выберите завершённое окно не раньше исходной границы.",
  window_not_aligned:
    "Начало окна не совпадает с UTC-расписанием. Используйте UTC-якорь плюс целое число длительностей.",
  window_not_completed:
    "Окно ещё не завершено. При будущем якоре создание пока недоступно; выберите согласованную границу в прошлом или дождитесь завершения.",
  aggregate_inactive: "Агрегат не принимает новые расчёты. Обновите состояние.",
  resource_budget_exceeded:
    "Расчёт превышает бюджет. Уменьшите окно, список значений или сложность правила.",
  preview_limit_exceeded:
    "Слишком много записей для предварительного расчёта. Уменьшите окно; усечённый результат не используется.",
  historical_time_basis_unknown:
    "Основание времени истории не доказано. Выберите окно внутри подтверждённой истории или используйте явно ретроспективный preview без сохранения точки.",
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
export function AggregateError({
  error,
  onRetry,
}: {
  error: unknown;
  onRetry?: () => void;
}) {
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
  const message = error instanceof ApiError ? messages[error.code] : undefined;
  return (
    <div role="alert">
      <AppRequestError error={error} onRetry={onRetry} message={message} />
      {path && (
        <p>
          Исправьте узел: <code>{path}</code>
        </p>
      )}
      {factor && (
        <p>Фактор ограничения: {factor}. Уменьшите период или сложность.</p>
      )}
      {errors.map((path, i) => (
        <p key={i}>
          Проверьте поле: <code>{path}</code>
        </p>
      ))}
    </div>
  );
}
