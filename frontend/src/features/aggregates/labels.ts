export const states: Record<string, string> = {
  active: "Активен",
  archived: "В архиве",
  deleted: "Удалён",
  preparing: "Подготовка",
  paused: "Приостановлен",
};
export const timeBases: Record<string, string> = {
  event_time: "По исходному ECS @timestamp",
  availability_at: "По времени доступности сохранённых событий",
  backend_processed_at_historical: "По времени обработки (ретроспективно)",
};
const reasons: Record<string, string> = {
  execution_budget_exceeded:
    "Окно не рассчитано: исчерпан бюджет времени. Автоматического пересчёта нет",
  event_time_missing: "Нет исходного времени события",
  event_time_invalid: "Исходное время события некорректно",
  event_time_fallback: "Вместо исходного времени подставлено время сбора",
  event_time_provenance_unknown:
    "Происхождение времени события не подтверждено",
  event_time_unknown_provenance:
    "Происхождение времени события не подтверждено",
  event_time_exclusions: "Есть записи с непригодным исходным временем",
  event_not_in_closing_snapshot:
    "Запись не входила в сохранённый снимок закрытия",
  detector_not_stopped: "Детектор ещё не остановлен",
  detector_history_protected: "Защищена непрочитанная история детектора",
  cleanup_protection_unavailable: "Защиту истории не удалось проверить",
  cleanup_turn_failed:
    "Порция очистки не завершена. Прогресс сохранён; доступно явное продолжение",
  known_delivery_gap: "Известен разрыв доставки, относящийся к снимку",
  processing_failures: "Есть ошибки обработки записей",
  delivery_backlog: "Есть отставание доставки или обработки",
  cleanup_requires_stopped_detectors:
    "Все связанные возобновляемые детекторы должны быть остановлены",
  cleanup_protected_points: "Выбранный хвост меньше защищённой истории моделей",
  unconsumed_points: "Непрочитанные детектором точки защищены",
  prepared_warmup_continuation:
    "Сохранённая подготовка прогрева требует продолжения",
  planned_warmup: "История необходима для первоначального прогрева",
  insufficient_warmup_history:
    "Недостаточно истории для надёжного плана прогрева",
  event_time_future: "Время события позже момента снимка",
  received_event_snapshot: "Полученный снимок без известных нарушений",
  late_after_snapshot: "Запись поступила после сохранённого снимка окна",
  aggregate_time_semantics_retired:
    "Прежний расчёт остановлен; история сохранена",
  legacy_coverage_unproven:
    "Полнота прежней ретроспективной истории не доказана",
  no_values: "Нет подходящих значений",
  invalid_input_type: "Входные данные имеют неподходящий тип",
  division_by_zero: "Деление на ноль",
  missing_operand: "Отсутствует операнд",
  non_finite_result: "Результат выходит за конечный диапазон",
  non_numeric_operand: "Операнд не является числом",
  non_finite_operand: "Операнд не является конечным числом",
  invalid_clamp_bounds: "Нижняя граница больше верхней",
  preview_snapshot_without_coverage_proof:
    "Снимок предварительного расчёта не доказывает полного покрытия окна",
  known_source_or_processing_stop:
    "Зафиксирована остановка источника или обработки",
  input_interrupted: "Наблюдение исходного входа было прервано",
  input_coverage_unavailable:
    "Нет подтверждения наблюдения всего исходного входа",
  input_coverage_gap: "В подтверждённом покрытии исходного входа есть разрыв",
  invalid_input_attestation: "Свидетельство покрытия не прошло проверку",
  input_attestation_mismatch:
    "Свидетельство не соответствует источнику или настройке",
  input_sequence_or_generation_gap:
    "Нарушена последовательность или поколение свидетельств",
  consumer_coverage_unavailable: "Нет подтверждения непрерывной обработки",
  consumer_coverage_gap: "Обработка входа имеет неподтверждённый разрыв",
  kafka_checkpoint_unavailable: "Нет контрольной позиции Kafka",
  kafka_partition_set_mismatch: "Изменился набор разделов Kafka",
  kafka_backlog_or_retention: "Есть отставание или недоступные данные Kafka",
  kafka_gap_detected: "Обнаружен разрыв Kafka",
  kafka_processing_incomplete: "Обработка Kafka не завершена",
  verified_input_and_kafka_coverage:
    "Вход и обработка Kafka подтверждены на всём интервале",
};
export function reasonText(reason: string | null) {
  return !reason
    ? "—"
    : reasons[reason]
      ? `${reasons[reason]} (${reason})`
      : "Причина недоступна в текущем интерфейсе; состояние не считается подтверждённым.";
}
