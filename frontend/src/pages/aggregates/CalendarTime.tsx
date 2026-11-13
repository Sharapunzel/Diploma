import { useState } from "react";
import { Alert, Input, Select } from "antd";
import {
  calendarInstants,
  calendarBoundary,
  displayTime,
  localCalendar,
  scheduleFraction,
} from "../../features/aggregates/time";
import styles from "./Aggregates.module.css";
import { FieldLabel } from "./AggregateHelp";
export function CalendarTime({
  label,
  value,
  zone,
  onChange,
  disabled = false,
  zoned = false,
  required = false,
  scheduleAnchor,
}: {
  label: string;
  value: string;
  zone: string;
  onChange: (value: string) => void;
  disabled?: boolean;
  zoned?: boolean;
  required?: boolean;
  scheduleAnchor?: string;
}) {
  const fraction =
    !zoned && scheduleAnchor ? scheduleFraction(scheduleAnchor) : "";
  const [draft, setDraft] = useState<{
    expected: string;
    zone: string;
    fraction: string;
    wall: string;
    options: string[];
    error: string;
  } | null>(null);
  const active =
    draft?.expected === value &&
    draft.zone === zone &&
    draft.fraction === fraction
      ? draft
      : null;
  const wall = active ? active.wall : localCalendar(value, zone);
  return (
    <div className={styles.field}>
      <div className={styles.field}>
        <FieldLabel
          required={required}
          help={`${zoned ? "Начальная граница расписания. Длительность задаёт последовательные окна от этого момента." : "Начало одного завершённого окна, выровненного по расписанию агрегата."} Ввод в поясе ${zone}. ${!zoned && scheduleAnchor ? `Доля секунды расписания: 0${fraction || ".0"}. Она сохраняется при выборе даты; выбранные целые секунды не сдвигаются. ` : ""}${value ? `Отображение: ${displayTime(value, zone)}. Точный момент: ${value}.` : "Момент не выбран."} Смена пояса не меняет сохранённый момент.`}
        >
          {label}
        </FieldLabel>
        <Input
          type="datetime-local"
          step="1"
          aria-label={label}
          aria-required={required}
          disabled={disabled}
          value={wall}
          onChange={(e) => {
            const local = e.target.value;
            const instants = local
              ? calendarInstants(local, zone).map((iso) =>
                  zoned
                    ? calendarBoundary(iso, zone)
                    : fraction
                      ? iso.replace(/\.\d+Z$/, `${fraction}Z`)
                      : iso,
                )
              : [];
            const chosen = instants.length === 1 ? instants[0] : "";
            setDraft({
              expected: chosen,
              zone,
              fraction,
              wall: local,
              options: instants,
              error:
                local && !instants.length
                  ? "Такого местного времени в выбранном поясе нет. Выберите другую дату или время."
                  : "",
            });
            onChange(chosen);
          }}
        />
      </div>

      {active && active.options.length > 1 && (
        <div className={styles.field}>
          <FieldLabel help="При переводе часов это местное время встречается дважды. Выберите нужный абсолютный момент по его смещению.">
            Неоднозначное время: выберите смещение
          </FieldLabel>
          <Select
            aria-label={`${label}: смещение`}
            disabled={disabled}
            value={value || undefined}
            placeholder="Выберите момент"
            options={active.options.map((iso) => ({
              value: iso,
              label: `${displayTime(iso, zone)} · ${iso}`,
            }))}
            onChange={(iso) => {
              setDraft({ ...active, expected: iso });
              onChange(iso);
            }}
          />
        </div>
      )}
      {active?.error && (
        <Alert type="error" role="alert" message={active.error} />
      )}
    </div>
  );
}
