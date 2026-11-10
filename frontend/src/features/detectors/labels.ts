import type { Reason } from "../../app/api/detectors";
export const executions: Record<string, string> = {
  created: "Создан, не запущен",
  requested: "Запуск запрошен",
  starting: "Запускается",
  running: "Исполнение разрешено",
  stopped: "Остановлен вручную",
  failed: "Запуск не состоялся",
  terminated: "Работа прекращена окончательно",
};
export const models: Record<string, string> = {
  waiting: "Ожидание истории",
  warming: "Прогрев",
  ready: "Модель готова",
  error: "Ошибка модели",
  stopped: "Модель остановлена",
};
export const aggregateStates: Record<string, string> = {
  active: "Активен",
  archived: "Архивирован",
  deleted: "Удалён",
};
const reasons: Record<string, string> = {
  managed_worker_required:
    "Вычисления выполняет worker. Обновите состояние детектора.",
  warmup_required: "Модели нужен прогрев. Запустите детектор явно.",
  working_point_gap:
    "Ожидается отсутствующая точка агрегата; неизвестный интервал не пропускается.",
  working_point_not_completed: "Рабочее окно ещё не завершено.",
  start_failed:
    "Запуск не завершён. Исправьте причину и повторите запуск вручную.",
  execution_failed:
    "Исполнение завершилось с ошибкой. Обновите состояние и повторите запуск вручную; при повторном отказе обратитесь к администратору.",
  ownership_lost:
    "Координатор потерял владение; результат вычисления отменён. Обновите состояние.",
  history_insufficient:
    "Недостаточно истории. Накопите завершённые окна и вручную запустите детектор снова.",
  history_quality:
    "История не подходит для обучения. Проверьте полноту и изменчивость признаков; затем повторите запуск вручную.",
  resource_limit:
    "Превышен ресурсный бюджет. Создайте другой детектор с менее затратными параметрами.",
  checkpoint_invalid:
    "Сохранённая модель не прошла проверку. Обратитесь к администратору.",
  start_interrupted: "Первоначальный запуск прерван. Повторите запуск вручную.",
  working_point_pending: "Ожидается готовая точка агрегата.",
  aggregate_inactive:
    "Агрегат приостановлен. Возобновите его в разделе агрегатов.",
  aggregate_deleted:
    "Агрегат удалён. История сохранена, новый запуск невозможен.",
  manual_stop:
    "Остановлен пользователем. Следующий запуск продолжит с сохранённого места.",
  temporary_unavailable:
    "Обработка временно недоступна. Обновите состояние позже.",
};
export function reasonText(reason: Reason | null, context?: string) {
  const factor: Record<string, string> = {
    model_seconds: "время работы модели",
    preparation_seconds: "время подготовки истории",
    checkpoint_bytes: "размер сохранённой модели",
    operator_cost: "стоимость операций",
    total_memory: "общая память",
    model_memory: "память модели",
    history_windows: "число исторических окон",
    input_bytes: "размер истории",
    dimensions: "размерность",
    warmup_input_bytes: "размер входов прогрева",
    warmup_windows: "число окон прогрева",
    preview_rows: "число записей для расчёта",
    statement_width: "ширина расчёта",
    array_elements: "число элементов массивов",
  };
  let text = reason
    ? (reasons[reason.code] ??
      "Обработка ожидает доступных данных. Обновите состояние; если причина сохраняется, обратитесь к администратору.")
    : "Причина отсутствует";
  if (
    reason?.code === "warmup_required" &&
    ["requested", "starting"].includes(context ?? "")
  )
    text = "Запрос запуска принят. Ожидается подготовка истории и прогрев.";
  if (context === "history")
    text = text.split(". ")[0].replace(/\.$/, "") + ".";
  return reason?.factor && factor[reason.factor]
    ? `${text} Фактор: ${factor[reason.factor]}.`
    : text;
}
