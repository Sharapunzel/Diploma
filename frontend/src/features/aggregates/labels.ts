export const states: Record<string, string> = {
  active: "Работает",
  archived: "В архиве",
  deleted: "Удалён",
  preparing: "Подготовка",
  paused: "Приостановлен",
};
const reasons: Record<string, string> = {
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
