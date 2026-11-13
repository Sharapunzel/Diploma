import { useEffect, useState } from "react";
import { Select } from "antd";
import type { KafkaSource, ResourcesApi } from "../../app/api/resources";
import { AggregateError } from "./AggregateError";
import { FieldLabel } from "./AggregateHelp";
import styles from "./Aggregates.module.css";
export function SourcePicker({
  api,
  value,
  onChange,
  disabled,
  required = false,
}: {
  api: ResourcesApi;
  value: string;
  onChange: (v: string) => void;
  disabled: boolean;
  required?: boolean;
}) {
  const [q, setQ] = useState(""),
    [retry, setRetry] = useState(0);
  const [items, setItems] = useState<KafkaSource[]>([]),
    [selected, setSelected] = useState<KafkaSource | null>(null);
  const [loading, setLoading] = useState(false),
    [error, setError] = useState<unknown>(null);
  useEffect(() => {
    const c = new AbortController();
    setLoading(true);
    setError(null);
    const timer = setTimeout(() => {
      void api
        .kafkaSources(0, undefined, c.signal, { q, limit: 10 })
        .then((page) => {
          if (!c.signal.aborted) setItems(page.items);
        })
        .catch((e) => {
          if (!c.signal.aborted) setError(e);
        })
        .finally(() => {
          if (!c.signal.aborted) setLoading(false);
        });
    }, 200);
    return () => {
      c.abort();
      clearTimeout(timer);
    };
  }, [api, q, retry]);
  const options = items.map((s) => ({
    value: s.id,
    title: s.name,
    label: `${s.name} · ${s.is_archived ? "архив" : s.is_enabled ? "включён" : "выключен"}`,
    disabled: s.is_archived,
  }));
  if (value && !options.some((o) => o.value === value))
    options.unshift({
      value,
      title: selected?.id === value ? selected.name : value,
      label:
        selected?.id === value
          ? selected.name
          : `${value} · доступность не подтверждена`,
      disabled: false,
    });
  return (
    <div className={styles.field}>
      <div className={styles.field}>
        <FieldLabel
          required={required}
          help="Один собственный источник с ECS-событиями. Начните вводить имя для поиска; архивные источники недоступны."
        >
          Собственный источник
        </FieldLabel>
        <Select
          aria-label="Собственный источник"
          aria-required={required}
          disabled={disabled}
          value={value || undefined}
          showSearch
          filterOption={false}
          onSearch={setQ}
          loading={loading}
          options={options}
          notFoundContent={
            loading
              ? "Загрузка…"
              : error
                ? "Источники недоступны"
                : "Совпадений нет"
          }
          onChange={(id) => {
            setSelected(items.find((s) => s.id === id) ?? null);
            onChange(id);
          }}
        />
      </div>
      {error !== null && (
        <AggregateError error={error} onRetry={() => setRetry((n) => n + 1)} />
      )}
    </div>
  );
}
