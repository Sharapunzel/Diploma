import { useId, useState, type FormEvent } from "react";
import { Alert, Button, Input, Modal, Select } from "antd";
import type { FindingDetector, FindingFilters } from "../../app/api/findings";
import { AppRequestError } from "../../app/ui/AppRequestError";
import { detectorLabel } from "../../features/findings/labels";
import { displayZone, utcSortKey } from "../../shared/time/utc";
import { FieldHelp } from "../../shared/ui/FieldHelp";
import styles from "./Findings.module.css";

const numeric = [
  {
    low: "grade_min",
    high: "grade_max",
    label: "Степень аномальности",
    description: "Ограничивает результаты по силе отклонения окна.",
  },
  {
    low: "confidence_min",
    high: "confidence_max",
    label: "Уверенность оценки",
    description: "Ограничивает результаты по достаточности данных для модели.",
  },
] as const;
const boundary = [
  {
    name: "timestamp_from",
    label: "Начало периода",
    help: "Находки с началом окна в этот момент и позже попадают в выборку.",
  },
  {
    name: "timestamp_to",
    label: "Конец периода (исключительно)",
    help: "Находки с началом окна в этот момент или позже остаются за пределами выборки.",
  },
] as const;

function localInput(value: string) {
  const date = new Date(value);
  if (!Number.isFinite(date.getTime())) return "";
  return date.toLocaleString("sv-SE").replace(" ", "T").slice(0, 19);
}

export function FindingFilterDialog({
  initial,
  detectors,
  loading,
  error,
  onRetry,
  onCancel,
  onApply,
}: {
  initial: FindingFilters;
  detectors: FindingDetector[];
  loading: boolean;
  error: unknown;
  onRetry: () => void;
  onCancel: () => void;
  onApply: (value: FindingFilters) => void;
}) {
  const prefix = useId();
  const [draft, setDraft] = useState(initial);
  const [invalid, setInvalid] = useState("");
  function update(key: keyof FindingFilters, value: string | undefined) {
    setDraft((old) => {
      const next = { ...old, [key]: value };
      if (value === undefined) delete next[key];
      return next;
    });
    setInvalid("");
  }
  function apply(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const from = utcSortKey(draft.timestamp_from);
    const to = utcSortKey(draft.timestamp_to);
    if (!from || !to || from >= to) {
      setInvalid("Начало периода должно быть раньше конца.");
      return;
    }
    for (const { low, high, label } of numeric) {
      if (
        draft[low] !== undefined &&
        draft[high] !== undefined &&
        +draft[low] > +draft[high]
      ) {
        setInvalid(`${label}. Нижняя граница должна быть не выше верхней.`);
        return;
      }
    }
    onApply(draft);
  }
  return (
    <Modal open title="Фильтры находок" onCancel={onCancel} footer={null}>
      <form className={styles.filterForm} onSubmit={apply}>
        <fieldset className={styles.filterPair}>
          <legend>Период · {displayZone}</legend>
          {boundary.map(({ name, label, help }) => (
            <div className={styles.filterField} key={name}>
              <div className={styles.fieldCaption}>
                <label htmlFor={prefix + name}>{label}</label>
                <FieldHelp name={label} text={help} />
              </div>
              <Input
                id={prefix + name}
                type="datetime-local"
                step="1"
                required
                value={localInput(draft[name])}
                onChange={(event) => {
                  const date = new Date(event.target.value);
                  update(
                    name,
                    Number.isFinite(date.getTime()) ? date.toISOString() : "",
                  );
                }}
              />
            </div>
          ))}
        </fieldset>
        {!!error && <AppRequestError error={error} onRetry={onRetry} />}
        <div className={styles.filterField}>
          <div className={styles.fieldCaption}>
            <label htmlFor={prefix + "detector"}>Детектор</label>
            <FieldHelp
              name="Детектор"
              text="Ограничивает список и таймлайн находками выбранного детектора. Тёзки различаются по UUID."
            />
          </div>
          <Select
            id={prefix + "detector"}
            className={styles.select}
            aria-label="Детектор"
            allowClear
            loading={loading}
            showSearch
            optionFilterProp="label"
            placeholder="Все детекторы"
            value={draft.detector_id}
            options={detectors.map((detector) => ({
              value: detector.detector_id,
              label: detectorLabel(detector, detectors),
            }))}
            onChange={(value: string | undefined) =>
              update("detector_id", value)
            }
          />
        </div>
        {numeric.map(({ low, high, label, description }) => (
          <fieldset key={low} className={styles.filterPair}>
            <legend>{label} · 0–1</legend>
            <p className={styles.filterDescription}>{description}</p>
            {([low, high] as const).map((key, index) => (
              <div className={styles.filterField} key={key}>
                <div className={styles.fieldCaption}>
                  <label htmlFor={prefix + key}>{index ? "До" : "От"}</label>
                  <FieldHelp
                    name={`${label}, ${index ? "до" : "от"}`}
                    text={
                      index
                        ? "Верхняя граница включается в выборку. Пустое поле оставляет её открытой."
                        : "Нижняя граница включается в выборку. Пустое поле оставляет её открытой."
                    }
                  />
                </div>
                <Input
                  id={prefix + key}
                  aria-label={`${label}, ${index ? "до" : "от"}`}
                  type="number"
                  min="0"
                  max="1"
                  step="any"
                  value={draft[key] ?? ""}
                  onChange={(event) =>
                    update(key, event.target.value || undefined)
                  }
                />
              </div>
            ))}
          </fieldset>
        ))}
        {invalid && <Alert type="error" message={invalid} />}
        <div className={styles.actions}>
          <Button onClick={onCancel}>Отмена</Button>
          <Button type="primary" htmlType="submit">
            Применить
          </Button>
        </div>
      </form>
    </Modal>
  );
}
