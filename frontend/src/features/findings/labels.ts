import type { FindingDetector, TimeBasis } from "../../app/api/findings";

export const timeBasisLabel: Record<TimeBasis, string> = {
  event_time: "Время события (ECS @timestamp)",
  availability_at: "Время доступности данных",
  backend_processed_at_historical: "Время обработки (исторический расчёт)",
};

export const timeBasisHelp: Record<TimeBasis, string> = {
  event_time:
    "Когда произошло событие. Это время берётся из поля ECS @timestamp в записи.",
  availability_at:
    "Когда запись сохранилась и стала доступна для расчёта. Это может произойти позже самого события.",
  backend_processed_at_historical:
    "Когда система обработала запись. Это время использовали в исторических расчётах.",
};

export function detectorLabel(
  detector: FindingDetector,
  options: FindingDetector[],
) {
  const { detector_name: name, detector_id: id } = detector;
  if (!name) return `Детектор ${id}`;
  return options.some(
    (other) => other.detector_id !== id && other.detector_name === name,
  )
    ? `${name} · ${id}`
    : name;
}
