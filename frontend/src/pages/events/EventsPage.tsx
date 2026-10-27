import { useEffect, useState } from "react";
import { Alert, Button, Input, Modal, Select, Spin, Table, Tag } from "antd";
import { observer } from "mobx-react-lite";
import { ApiError, errorText } from "../../app/api/client";
import {
  EventsApi,
  type DeleteScope,
  type EventField,
  type EventFilter,
  type EventRow,
} from "../../app/api/events";
import { useStores } from "../../app/providers/StoresProvider";
import { AppRequestError } from "../../app/ui/AppRequestError";
import { EventsStore } from "../../features/events/EventsStore";
import { ConfirmationDialog } from "../../shared/ui/ConfirmationDialog";
import styles from "./EventsPage.module.css";

const operators: Record<string, string> = {
  eq: "Равно",
  neq: "Не равно",
  in: "Одно из",
  contains: "Содержит",
  starts_with: "Начинается с",
  ends_with: "Заканчивается на",
  exists: "Есть",
  not_exists: "Нет",
  gt: "Больше",
  gte: "Не меньше",
  lt: "Меньше",
  lte: "Не больше",
};
const noValue = new Set(["exists", "not_exists"]);
const zone = Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC";
function displayTime(value: string) {
  const date = new Date(value);
  return Number.isNaN(date.getTime())
    ? "—"
    : `${date.toLocaleString("ru-RU")} (${zone})`;
}
function deletionBoundary(value: string): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "—";
  const local = date.toLocaleString("ru-RU", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hourCycle: "h23",
  });
  return `${local}.${String(date.getMilliseconds()).padStart(3, "0")} (${zone}; UTC ${date.toISOString()})`;
}
function localDateTime(value: string): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "";
  return date
    .toLocaleString("sv-SE", {
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      fractionalSecondDigits: 3,
      hourCycle: "h23",
    })
    .replace(" ", "T")
    .replace(",", ".");
}
function utcTime(value: string) {
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? "—" : date.toISOString();
}
function fieldValue(fields: Record<string, unknown>, name: string): unknown {
  if (name in fields) return fields[name];
  return name
    .split(".")
    .reduce<unknown>(
      (value, part) =>
        value && typeof value === "object" && !Array.isArray(value)
          ? (value as Record<string, unknown>)[part]
          : undefined,
      fields,
    );
}
function showValue(value: unknown) {
  if (value === undefined || value === null || value === "") return "—";
  return typeof value === "object" ? JSON.stringify(value) : String(value);
}
function localDiagnostics(card: {
  details?: unknown;
}): { status: unknown; reasons: unknown[] } | null {
  const details = card.details;
  if (!details || typeof details !== "object" || !("metadata" in details))
    return null;
  const metadata = details.metadata;
  if (!metadata || typeof metadata !== "object") return null;
  return {
    status:
      "normalization_status" in metadata ? metadata.normalization_status : null,
    reasons:
      "normalization_diagnostics" in metadata &&
      Array.isArray(metadata.normalization_diagnostics)
        ? metadata.normalization_diagnostics
        : [],
  };
}
function diagnosticText(value: unknown): string {
  if (!value || typeof value !== "object") return "Причина не уточнена";
  const item = value as Record<string, unknown>;
  const reasons: Record<string, string> = {
    optional_mapping_missing: "Отсутствует необязательное поле",
    required_mapping_missing: "Отсутствует обязательное поле",
    conversion_failed: "Не удалось преобразовать значение",
    empty_ecs: "Не удалось получить поля ECS",
    event_time_fallback: "Время события взято из времени сбора",
  };
  const reason = typeof item.code === "string" ? reasons[item.code] : undefined;
  const field = typeof item.field === "string" ? `: ${item.field}` : "";
  return `${reason ?? "Причина не уточнена"}${field}`;
}
export function inputValue(
  value: string,
  field: EventField,
  operator: string,
): unknown {
  if (operator === "in") {
    const parts = value.split(",").map((part) => part.trim());
    if (parts.length > 20 || parts.some((part) => !part))
      throw new Error("invalid list value");
    return parts.map((part) => inputValue(part, field, "eq"));
  }
  if (field.type === "boolean") {
    if (value !== "true" && value !== "false")
      throw new Error("invalid boolean");
    return value === "true";
  }
  if (field.type === "date") {
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) throw new Error("invalid date");
    if (
      field.date_formats?.some((format) =>
        format.includes("date_optional_time"),
      ) ||
      !field.date_formats
    )
      return date.toISOString();
    if (field.date_formats.includes("epoch_millis")) return date.getTime();
    throw new Error("unsupported date format");
  }
  if (
    [
      "integer",
      "long",
      "short",
      "byte",
      "float",
      "double",
      "scaled_float",
      "half_float",
      "unsigned_long",
    ].includes(field.type)
  ) {
    const number = Number(value);
    if (!Number.isFinite(number)) throw new Error("invalid number");
    if (
      ["integer", "long", "short", "byte", "unsigned_long"].includes(
        field.type,
      ) &&
      (!Number.isSafeInteger(number) || !/^-?\d+$/.test(value.trim()))
    )
      throw new Error("invalid integer");
    return number;
  }
  return value;
}
type PendingDelete = {
  title: string;
  message: string;
  sourceId: string;
  scope?: DeleteScope;
  row?: EventRow;
};

function ExpandableText({
  value,
  formatted = false,
}: {
  value: string;
  formatted?: boolean;
}) {
  const [expanded, setExpanded] = useState(false);
  const long = value.length > (formatted ? 240 : 90) || value.includes("\n");
  return (
    <div className={formatted ? styles.textBlock : styles.cell}>
      {formatted ? (
        <pre
          className={`${styles.pre} ${!expanded && long ? styles.collapsed : ""}`}
        >
          {value}
        </pre>
      ) : (
        <span
          className={
            !expanded && long ? styles.collapsedCell : styles.expandedCell
          }
        >
          {value}
        </span>
      )}
      {long && (
        <Button type="link" size="small" onClick={() => setExpanded(!expanded)}>
          {expanded ? "Свернуть" : "Развернуть"}
        </Button>
      )}
    </div>
  );
}

export const EventsPage = observer(() => {
  const { api, session } = useStores();
  const [store] = useState(() => new EventsStore(new EventsApi(api)));
  const eventsApi = new EventsApi(api);
  const [draftFrom, setDraftFrom] = useState("");
  const [draftTo, setDraftTo] = useState("");
  const [draftSort, setDraftSort] = useState<"asc" | "desc">("desc");
  const [draftColumns, setDraftColumns] = useState<string[]>([]);
  const [draftFilters, setDraftFilters] = useState<
    { field: string; operator: string; value: string }[]
  >([]);
  const [filterError, setFilterError] = useState("");
  const [filterErrorIndex, setFilterErrorIndex] = useState<number | null>(null);
  const [deleteFrom, setDeleteFrom] = useState("");
  const [deleteTo, setDeleteTo] = useState("");
  const [deletePeriodError, setDeletePeriodError] = useState("");
  const [pendingDelete, setPendingDelete] = useState<PendingDelete | null>(
    null,
  );
  const [deleting, setDeleting] = useState(false);
  const [deleteError, setDeleteError] = useState<unknown>(null);
  const [notice, setNotice] = useState("");
  const [unknownResult, setUnknownResult] = useState(false);
  const [filtersOpen, setFiltersOpen] = useState(false);
  const [deleteOpen, setDeleteOpen] = useState(false);
  const canDelete =
    session.has("events.delete") && store.source?.source_type === "kafka";
  useEffect(() => {
    void store.loadSources();
    return () => store.dispose();
  }, [store]);

  function selectSource(id: string | null) {
    setFiltersOpen(false);
    setDeleteOpen(false);
    setDraftFilters([]);
    setDraftColumns([]);
    setFilterError("");
    setFilterErrorIndex(null);
    setDeleteError(null);
    setPendingDelete(null);
    setUnknownResult(false);
    setNotice("");
    void store.select(id);
    setDraftFrom(store.from);
    setDraftTo(store.to);
    setDraftSort("desc");
  }

  async function refresh() {
    const operation = store.refresh();
    setDraftFrom(store.from);
    setDraftTo(store.to);
    await operation;
    if (!store.error) setUnknownResult(false);
  }

  async function apply() {
    if (store.loading) return;
    const sourceId = store.source?.id;
    setFilterError("");
    setFilterErrorIndex(null);
    const lower = new Date(draftFrom);
    const upper = new Date(draftTo);
    if (
      Number.isNaN(lower.getTime()) ||
      Number.isNaN(upper.getTime()) ||
      lower >= upper
    ) {
      setFilterError("Укажите корректный период: начало раньше конца.");
      return;
    }
    const filters: EventFilter[] = [];
    if (draftFilters.length > 20) {
      setFilterError("Допускается не более 20 условий.");
      return;
    }
    for (const [index, item] of draftFilters.entries()) {
      const field = store.fields.find(
        (candidate) => candidate.name === item.field,
      );
      if (!field || !field.operators.includes(item.operator)) {
        setFilterErrorIndex(index);
        setFilterError("Выберите доступное поле и оператор.");
        return;
      }
      if (!noValue.has(item.operator) && !item.value.trim()) {
        setFilterErrorIndex(index);
        setFilterError(`Укажите значение для ${field.name}.`);
        return;
      }
      try {
        const value = noValue.has(item.operator)
          ? undefined
          : inputValue(item.value, field, item.operator);
        const values = Array.isArray(value) ? value : [value];
        if (
          values.some(
            (entry) => typeof entry === "number" && !Number.isFinite(entry),
          )
        )
          throw new Error("invalid number");
        filters.push(
          noValue.has(item.operator)
            ? { field: item.field, operator: item.operator }
            : { field: item.field, operator: item.operator, value },
        );
      } catch {
        setFilterErrorIndex(index);
        setFilterError(`Проверьте значение для ${field.name}.`);
        return;
      }
    }
    const applied = await store.applySearch(
      lower.toISOString(),
      upper.toISOString(),
      draftSort,
      filters,
      draftColumns,
    );
    if (store.source?.id !== sourceId) return;
    if (applied) {
      setFiltersOpen(false);
      return;
    }
    const error = store.error;
    if (
      error instanceof ApiError &&
      error.code === "event_filter_invalid" &&
      error.details &&
      typeof error.details === "object" &&
      "index" in error.details &&
      typeof error.details.index === "number" &&
      Number.isInteger(error.details.index) &&
      error.details.index >= 0 &&
      error.details.index < draftFilters.length
    ) {
      setFilterErrorIndex(error.details.index);
      setFilterError(
        "Источник отклонил это условие. Проверьте поле, оператор и значение.",
      );
      return;
    }
    setFilterErrorIndex(null);
    setFilterError(
      error instanceof ApiError && error.status === 0
        ? "Не удалось связаться с сервером. Поиск не применён; проверьте соединение и повторите попытку."
        : `Не удалось применить поиск. ${errorText(error)}`,
    );
  }

  async function confirmDelete() {
    if (
      !pendingDelete ||
      deleting ||
      store.source?.id !== pendingDelete.sourceId
    )
      return;
    setDeleting(true);
    setDeleteError(null);
    setUnknownResult(false);
    try {
      if (pendingDelete.row) {
        await eventsApi.deleteOne(pendingDelete.sourceId, pendingDelete.row.id);
        setNotice("Событие удалено.");
      } else if (pendingDelete.scope) {
        const result = await eventsApi.deleteMany(pendingDelete.scope);
        setNotice(`Удалено событий: ${result.deleted_count}.`);
      }
      setPendingDelete(null);
      store.closeCard();
      await store.refresh();
    } catch (error) {
      setDeleteError(error);
      if (
        error instanceof ApiError &&
        (error.status === 0 || error.code === "invalid_response")
      ) {
        setUnknownResult(true);
        setPendingDelete(null);
      }
    } finally {
      setDeleting(false);
    }
  }

  function deletePeriod(allSources: boolean) {
    const from = new Date(deleteFrom);
    const to = new Date(deleteTo);
    if (
      Number.isNaN(from.getTime()) ||
      Number.isNaN(to.getTime()) ||
      from >= to
    ) {
      setDeletePeriodError(
        "Укажите корректный период удаления: начало раньше конца.",
      );
      return;
    }
    setDeletePeriodError("");
    setDeleteOpen(false);
    const scope: DeleteScope = {
      timestamp_from: from.toISOString(),
      timestamp_to: to.toISOString(),
    };
    if (!allSources && store.source) scope.source_id = store.source.id;
    setPendingDelete({
      title: "Удалить события за период",
      sourceId: store.source!.id,
      scope,
      message: `Необратимо удалить события: ${allSources ? "все локальные источники" : store.source?.name}. Время события от ${deletionBoundary(scope.timestamp_from!)} включительно до ${deletionBoundary(scope.timestamp_to!)} исключительно. Другие фильтры таблицы не учитываются.`,
    });
  }

  return (
    <main className={styles.page}>
      {notice && (
        <Alert
          type="success"
          message={notice}
          closable
          onClose={() => setNotice("")}
        />
      )}
      {!!store.sourceError && (
        <AppRequestError
          error={store.sourceError}
          onRetry={() => void store.loadSources()}
        />
      )}
      <div className={styles.toolbar}>
        <label className={`${styles.field} ${styles.source}`}>
          Источник
          <Select
            aria-label="Источник"
            disabled={deleting || unknownResult}
            showSearch={false}
            optionFilterProp="label"
            onPopupScroll={(event) => {
              const target = event.currentTarget;
              if (
                target.scrollTop + target.clientHeight >=
                target.scrollHeight - 32
              )
                void store.loadMoreSources();
            }}
            loading={store.loadingSources}
            value={store.source?.id ?? undefined}
            placeholder="Выберите источник"
            onChange={selectSource}
            options={store.sources.map((source) => ({
              value: source.id,
              label: `${source.name} · ${source.source_type === "kafka" ? "Kafka" : "OpenSearch"}${source.is_enabled ? "" : " · выключен"}`,
            }))}
          />
          <Button
            disabled={
              store.loadingSources ||
              store.sourceOffsets.kafka >= store.sourceTotals.kafka
            }
            onClick={() => void store.loadMoreSources("kafka")}
          >
            Ещё источники Kafka
          </Button>
          <Button
            disabled={
              store.loadingSources ||
              store.sourceOffsets.external >= store.sourceTotals.external
            }
            onClick={() => void store.loadMoreSources("external")}
          >
            Ещё внешние источники
          </Button>
        </label>
      </div>
      {!store.source && (
        <Alert type="info" message="Выберите источник для просмотра событий." />
      )}
      {store.source && (
        <>
          {!!store.catalogError && (
            <AppRequestError
              error={store.catalogError}
              onRetry={() => void store.loadCatalog()}
            />
          )}
          <div className={styles.searchBar}>
            <div className={styles.applied}>
              <span>
                Период: {displayTime(store.from)} — {displayTime(store.to)}
              </span>
              <span>
                Порядок:{" "}
                {store.sort === "desc" ? "Новые сначала" : "Старые сначала"}
              </span>
              {store.filters.length === 0 ? (
                <span>Условий: 0</span>
              ) : (
                <span
                  className={styles.conditions}
                  title={store.filters
                    .map(
                      (filter) =>
                        `${filter.field} ${operators[filter.operator] ?? filter.operator}${"value" in filter ? ` ${showValue(filter.value)}` : ""}`,
                    )
                    .join("; ")}
                >
                  Условия:{" "}
                  {store.filters
                    .slice(0, 2)
                    .map(
                      (filter) =>
                        `${filter.field} ${operators[filter.operator] ?? filter.operator}${"value" in filter ? ` ${showValue(filter.value)}` : ""}`,
                    )
                    .join("; ")}
                  {store.filters.length > 2
                    ? `; ещё ${store.filters.length - 2}`
                    : ""}
                </span>
              )}
            </div>
            <div className={styles.actions}>
              <Button
                onClick={() => {
                  setDraftColumns(store.columns);
                  setFiltersOpen(true);
                }}
              >
                Фильтры и поля
              </Button>
              <Button
                onClick={() => void refresh()}
                disabled={!store.catalogReady}
              >
                Обновить
              </Button>
              {canDelete && (
                <Button danger onClick={() => setDeleteOpen(true)}>
                  Удаление событий
                </Button>
              )}
            </div>
          </div>
          <Modal
            title="Фильтры и поля"
            open={filtersOpen}
            onCancel={() => {
              if (!store.loading) setFiltersOpen(false);
            }}
            footer={null}
            width={760}
            destroyOnHidden
          >
            <div className={styles.panelContent}>
              <div className={styles.toolbar}>
                <label className={styles.field}>
                  От (время события, {zone})
                  <Input
                    type="datetime-local"
                    value={
                      draftFrom && !Number.isNaN(new Date(draftFrom).getTime())
                        ? new Date(draftFrom)
                            .toLocaleString("sv-SE")
                            .replace(" ", "T")
                            .slice(0, 16)
                        : ""
                    }
                    onChange={(event) =>
                      setDraftFrom(
                        event.target.value
                          ? new Date(event.target.value).toISOString()
                          : "",
                      )
                    }
                  />
                </label>
                <label className={styles.field}>
                  До (исключительно, {zone})
                  <Input
                    type="datetime-local"
                    value={
                      draftTo && !Number.isNaN(new Date(draftTo).getTime())
                        ? new Date(draftTo)
                            .toLocaleString("sv-SE")
                            .replace(" ", "T")
                            .slice(0, 16)
                        : ""
                    }
                    onChange={(event) =>
                      setDraftTo(
                        event.target.value
                          ? new Date(event.target.value).toISOString()
                          : "",
                      )
                    }
                  />
                </label>
                <label className={styles.field}>
                  Порядок
                  <Select
                    value={draftSort}
                    onChange={setDraftSort}
                    options={[
                      { value: "desc", label: "Новые сначала" },
                      { value: "asc", label: "Старые сначала" },
                    ]}
                  />
                </label>
              </div>
              <label className={styles.field}>
                Колонки
                <Select
                  mode="multiple"
                  disabled={!store.catalogReady}
                  showSearch
                  value={draftColumns}
                  onChange={setDraftColumns}
                  options={store.fields.map((field) => ({
                    value: field.name,
                    label: field.name,
                  }))}
                  placeholder="Выберите поля"
                />
              </label>
              <section className={styles.filters} aria-label="Фильтры по полям">
                {draftFilters.map((filter, index) => {
                  const field = store.fields.find(
                    (item) => item.name === filter.field,
                  );
                  return (
                    <div className={styles.filter} key={index}>
                      <Select
                        className={styles.filterField}
                        aria-label="Поле условия"
                        showSearch
                        optionFilterProp="label"
                        placeholder="Поле"
                        value={filter.field || undefined}
                        onChange={(value) =>
                          setDraftFilters(
                            draftFilters.map((item, at) =>
                              at === index
                                ? { field: value, operator: "", value: "" }
                                : item,
                            ),
                          )
                        }
                        options={store.fields.map((item) => ({
                          value: item.name,
                          label: item.name,
                        }))}
                      />
                      <Select
                        aria-label="Оператор условия"
                        placeholder="Оператор"
                        value={filter.operator || undefined}
                        onChange={(value) =>
                          setDraftFilters(
                            draftFilters.map((item, at) =>
                              at === index
                                ? { ...item, operator: value }
                                : item,
                            ),
                          )
                        }
                        options={(field?.operators ?? []).map((operator) => ({
                          value: operator,
                          label: operators[operator] || operator,
                        }))}
                      />
                      {!noValue.has(filter.operator) &&
                        (field?.type === "boolean" &&
                        filter.operator !== "in" ? (
                          <Select
                            className={styles.filterValue}
                            value={filter.value || undefined}
                            onChange={(value) =>
                              setDraftFilters(
                                draftFilters.map((item, at) =>
                                  at === index ? { ...item, value } : item,
                                ),
                              )
                            }
                            options={[
                              { value: "true", label: "Истина" },
                              { value: "false", label: "Ложь" },
                            ]}
                          />
                        ) : (
                          <Input
                            className={styles.filterValue}
                            type={
                              field?.type === "date" && filter.operator !== "in"
                                ? "datetime-local"
                                : filter.operator !== "in" &&
                                    [
                                      "integer",
                                      "long",
                                      "float",
                                      "double",
                                    ].includes(field?.type ?? "")
                                  ? "number"
                                  : "text"
                            }
                            value={filter.value}
                            onChange={(event) =>
                              setDraftFilters(
                                draftFilters.map((item, at) =>
                                  at === index
                                    ? { ...item, value: event.target.value }
                                    : item,
                                ),
                              )
                            }
                            placeholder={
                              filter.operator === "in"
                                ? "Значения через запятую"
                                : "Значение"
                            }
                          />
                        ))}
                      <Button
                        onClick={() =>
                          setDraftFilters(
                            draftFilters.filter((_, at) => at !== index),
                          )
                        }
                      >
                        Убрать
                      </Button>
                      {filterErrorIndex === index && (
                        <Alert type="error" message={filterError} />
                      )}
                    </div>
                  );
                })}
                <div className={styles.actions}>
                  <Button
                    disabled={!store.catalogReady || draftFilters.length >= 20}
                    onClick={() =>
                      setDraftFilters([
                        ...draftFilters,
                        { field: "", operator: "", value: "" },
                      ])
                    }
                  >
                    Добавить условие
                  </Button>
                  <Button
                    type="primary"
                    onClick={() => void apply()}
                    loading={store.loading}
                    disabled={!store.catalogReady || store.loading}
                  >
                    Искать
                  </Button>
                </div>
                {filterError && filterErrorIndex === null && (
                  <Alert type="error" message={filterError} />
                )}
              </section>
            </div>
          </Modal>
          {canDelete && (
            <Modal
              title="Удаление событий"
              open={deleteOpen}
              onCancel={() => setDeleteOpen(false)}
              footer={null}
              width={720}
              destroyOnHidden
            >
              <section
                className={styles.panelContent}
                aria-label="Область удаления"
              >
                <h2>Область удаления</h2>
                <p>
                  Удаление использует время события. Другие фильтры таблицы не
                  учитываются.
                </p>
                <div className={styles.toolbar}>
                  <label className={styles.field}>
                    От ({zone}, включительно)
                    <Input
                      aria-label="Начало периода удаления"
                      type="datetime-local"
                      step="0.001"
                      value={deleteFrom}
                      onChange={(event) => setDeleteFrom(event.target.value)}
                    />
                  </label>
                  <label className={styles.field}>
                    До ({zone}, исключительно)
                    <Input
                      aria-label="Конец периода удаления"
                      type="datetime-local"
                      step="0.001"
                      value={deleteTo}
                      onChange={(event) => setDeleteTo(event.target.value)}
                    />
                  </label>
                  <Button
                    onClick={() => {
                      setDeleteFrom(localDateTime(store.from));
                      setDeleteTo(localDateTime(store.to));
                    }}
                  >
                    Взять период применённого поиска
                  </Button>
                </div>
                {deletePeriodError && (
                  <Alert type="error" message={deletePeriodError} />
                )}
                <div className={styles.actions}>
                  <Button
                    danger
                    disabled={unknownResult}
                    onClick={() => {
                      setDeleteOpen(false);
                      setPendingDelete({
                        title: "Удалить источник целиком",
                        sourceId: store.source!.id,
                        scope: { source_id: store.source!.id },
                        message: `Необратимо удалить все локальные события источника «${store.source!.name}» независимо от времени и фильтров таблицы.`,
                      });
                    }}
                  >
                    Удалить события источника
                  </Button>
                  <Button
                    danger
                    disabled={unknownResult}
                    onClick={() => deletePeriod(false)}
                  >
                    Удалить источник за период
                  </Button>
                  <Button
                    danger
                    disabled={unknownResult}
                    onClick={() => deletePeriod(true)}
                  >
                    Удалить все локальные за период
                  </Button>
                </div>
              </section>
            </Modal>
          )}
          {!!store.error && !filtersOpen && (
            <AppRequestError
              error={store.error}
              onRetry={() => void store.search()}
            />
          )}
          <div className={styles.table}>
            <Table<EventRow>
              rowKey="id"
              loading={store.loading}
              dataSource={store.results?.items ?? []}
              pagination={false}
              size="small"
              locale={{
                emptyText: store.loading
                  ? "Загрузка"
                  : store.error
                    ? "Не удалось загрузить события"
                    : "Событий нет",
              }}
              columns={[
                {
                  title: `Время (${zone})`,
                  dataIndex: "event_timestamp",
                  render: (value: string) => displayTime(value),
                },
                ...store.columns.map((name) => ({
                  title: name,
                  key: name,
                  render: (_: unknown, row: EventRow) => (
                    <ExpandableText
                      value={showValue(fieldValue(row.fields, name))}
                    />
                  ),
                })),
                ...(store.source.source_type === "kafka"
                  ? [
                      {
                        title: "Нормализация",
                        dataIndex: "normalization_status",
                        render: (status: string) =>
                          status ? <Tag>{status}</Tag> : "—",
                      },
                    ]
                  : []),
                {
                  title: "Действия",
                  key: "actions",
                  render: (_: unknown, row: EventRow) => (
                    <div className={styles.actions}>
                      <Button onClick={() => void store.openCard(row.id)}>
                        Открыть
                      </Button>
                      {canDelete && (
                        <Button
                          danger
                          disabled={unknownResult}
                          onClick={() =>
                            setPendingDelete({
                              title: "Удалить событие",
                              sourceId: store.source!.id,
                              row,
                              message: `Необратимо удалить одно локальное событие источника «${store.source!.name}» от ${displayTime(row.event_timestamp)}.`,
                            })
                          }
                        >
                          Удалить
                        </Button>
                      )}
                    </div>
                  ),
                },
              ]}
            />
          </div>
          <div className={styles.actions}>
            <Button
              disabled={store.page === 0 || store.loading}
              onClick={() => void store.search(store.page - 1)}
            >
              Назад
            </Button>
            <span>Страница {store.page + 1}</span>
            <Button
              disabled={
                !store.results?.has_more ||
                !store.results.next_cursor ||
                store.loading
              }
              onClick={() => void store.search(store.page + 1)}
            >
              Вперёд
            </Button>
          </div>
        </>
      )}
      <Modal
        title="Событие"
        open={!!store.card || store.cardLoading || !!store.cardError}
        onCancel={() => store.closeCard()}
        footer={null}
        width={720}
        destroyOnHidden
        className={styles.cardModal}
      >
        {store.cardLoading && <Spin aria-label="Загрузка события" />}
        {!!store.cardError && <AppRequestError error={store.cardError} />}
        {store.card && (
          <div className={styles.card}>
            <div>Время: {displayTime(store.card.event_timestamp)}</div>
            <div>
              Исходное время / UTC: {store.card.event_timestamp} /{" "}
              {utcTime(store.card.event_timestamp)}
            </div>
            {store.card.source_type === "kafka" &&
              store.card.details &&
              "raw" in store.card.details && (
                <>
                  <h3>Исходная запись</h3>
                  <ExpandableText
                    formatted
                    value={String(store.card.details.raw)}
                  />
                  {localDiagnostics(store.card)?.status === "partial" && (
                    <Alert
                      type="warning"
                      message="Нормализация выполнена частично"
                      description={
                        <div>
                          Причины обработки:{" "}
                          {localDiagnostics(store.card)?.reasons.length
                            ? localDiagnostics(store.card)
                                ?.reasons.map(diagnosticText)
                                .join("; ")
                            : "подробные причины отсутствуют"}
                        </div>
                      }
                    />
                  )}
                  <h3>Метаданные</h3>
                  <ExpandableText
                    formatted
                    value={
                      JSON.stringify(store.card.details.metadata, null, 2) ??
                      "—"
                    }
                  />
                </>
              )}
            <h3>Поля события</h3>
            <ExpandableText
              formatted
              value={JSON.stringify(store.card.fields, null, 2) ?? "—"}
            />
          </div>
        )}
      </Modal>
      <ConfirmationDialog
        open={!!pendingDelete}
        pending={deleting}
        confirmDisabled={unknownResult}
        title={pendingDelete?.title ?? ""}
        message={pendingDelete?.message ?? ""}
        confirmText="Удалить"
        danger
        onCancel={() => {
          if (!deleting) {
            setPendingDelete(null);
            setDeleteError(null);
          }
        }}
        onConfirm={() => void confirmDelete()}
      >
        {!!deleteError && <AppRequestError error={deleteError} />}
      </ConfirmationDialog>
      {unknownResult && (
        <Alert
          type="warning"
          message="Результат удаления неизвестен"
          description={
            <div>
              Ответ потерян или некорректен. События могли быть удалены.
              Обновите список и заново решите, требуется ли удаление.
              <Button onClick={() => void refresh()}>Обновить события</Button>
            </div>
          }
        />
      )}
    </main>
  );
});
