import { useEffect, useMemo, useRef, useState } from "react";
import {
  Alert,
  Button,
  Input,
  InputNumber,
  Modal,
  Select,
  Switch,
  Tag,
} from "antd";
import { SearchOutlined } from "@ant-design/icons";
import {
  Link,
  useNavigate,
  useParams,
  useSearchParams,
} from "react-router-dom";
import { ApiError } from "../../app/api/client";
import {
  NormalizersApi,
  type Block,
  type BlockType,
  type Candidate,
  type EcsField,
  type ExtractBlock,
  type MapBlock,
  type PreviewResult,
  type RuleV1,
  type ValueSource,
} from "../../app/api/normalizers";
import type { Normalizer, Page } from "../../app/api/resources";
import { useStores } from "../../app/providers/StoresProvider";
import { AppRequestError } from "../../app/ui/AppRequestError";
import { ConfirmationDialog } from "../../shared/ui/ConfirmationDialog";
import { LoadingOverlay } from "../../shared/ui/LoadingOverlay";
import {
  aiPrompt,
  defaultBlock,
  emptyRule,
  parseRule,
  referenceWarnings,
  templates,
} from "./rules";
import { buildRuleTree, type TreeNode } from "./ruleTree";
import { ContextHelp, helpTopicForLabel } from "./NormalizerHelp";
import {
  edgeSpaces,
  previewReasons,
  ruleIssues,
  type RuleIssue,
} from "./normalizerFeedback";
import styles from "./Normalizers.module.css";
import { FeedbackLayer, NormalizerFeedback } from "./NotificationOverlay";
import { aiFeedback, draftIssues, draftRisks } from "./draftDiagnostics";
import { SourceDebugPanel } from "./SourceDebugPanel";

const clone = <T,>(value: T): T => structuredClone(value);
const label = (text: string, element: React.ReactNode) => {
  const topic = helpTopicForLabel(text);
  return (
    <div className={styles.fieldWithHelp}>
      <label className={styles.field}>
        <span>{text}</span>
        {element}
      </label>
      {topic && <ContextHelp topic={topic} name={text} />}
    </div>
  );
};
const diagnosticText: Record<string, string> = {
  rule_invalid: "Правило не прошло проверку структуры или ссылок.",
  invalid_envelope: "Проверьте строку log или служебное время проверки.",
  log_too_long: "Строка log превышает допустимый размер.",
  no_variant_matched: "Ни один вариант не подошёл.",
  required_mapping_missing: "Обязательное поле не получено.",
  conversion_failed: "Значение не удалось преобразовать в тип ECS.",
  optional_mapping_missing: "Необязательное поле не получено.",
  event_time_fallback: "Время события взято из служебного времени проверки.",
  empty_ecs: "Не удалось сопоставить ни одного ECS-поля.",
  extraction_missing: "Извлечение не нашло значение.",
};
const previewStatus: Record<PreviewResult["status"], string> = {
  complete: "Полностью обработано",
  partial: "Частично обработано",
  failed: "Не обработано",
};
const traceResult: Record<string, string> = {
  matched: "Условие выполнено",
  missing: "Значение не найдено",
  mapped: "Поле сопоставлено",
  conversion_failed: "Ошибка преобразования",
};
function IssueSummary({
  issues,
  onSelect,
}: {
  issues: RuleIssue[];
  onSelect: (issue: RuleIssue) => void;
}) {
  if (!issues.length) return null;
  return (
    <Alert
      type="error"
      showIcon
      message={`Ошибки правила: ${issues.length}`}
      description={
        <div className={styles.issueList}>
          {issues.map((issue, index) => (
            <Button
              type="link"
              key={`${issue.path}-${index}`}
              onClick={() => onSelect(issue)}
            >
              {issue.path}: {issue.message} Перейти к полю
            </Button>
          ))}
        </div>
      }
    />
  );
}

function FieldPicker({
  value,
  onChange,
  service,
}: {
  value: string;
  onChange: (value: string) => void;
  service: NormalizersApi;
}) {
  const [query, setQuery] = useState("");
  const [search, setSearch] = useState("");
  const [reload, setReload] = useState(0);
  const [fields, setFields] = useState<EcsField[]>([]);
  const [selectedField, setSelectedField] = useState<EcsField | null>(null);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<unknown>(null);
  const currentQuery = useRef(query);
  currentQuery.current = query;
  useEffect(() => {
    const timer = window.setTimeout(() => setSearch(query.trim()), 250);
    return () => window.clearTimeout(timer);
  }, [query]);
  useEffect(() => {
    if (!search || search.length > 200) {
      setFields([]);
      setTotal(0);
      setLoading(false);
      return;
    }
    const controller = new AbortController();
    setFields([]);
    setLoading(true);
    service
      .fields(search, 0, controller.signal)
      .then((page) => {
        setFields(page.items);
        setTotal(page.total);
        setError(null);
      })
      .catch((reason) => {
        if (!controller.signal.aborted) setError(reason);
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoading(false);
      });
    return () => controller.abort();
  }, [service, search, reload]);
  useEffect(() => {
    if (!value) {
      setSelectedField(null);
      return;
    }
    const controller = new AbortController();
    service
      .field(value, controller.signal)
      .then(setSelectedField)
      .catch(() => {
        if (!controller.signal.aborted) setSelectedField(null);
      });
    return () => controller.abort();
  }, [service, value]);
  async function more() {
    if (loading || fields.length >= total || !search) return;
    const requestedQuery = currentQuery.current;
    setLoading(true);
    try {
      const page = await service.fields(search, fields.length);
      if (currentQuery.current !== requestedQuery) return;
      setFields((previous) => [...previous, ...page.items]);
    } catch (reason) {
      setError(reason);
    } finally {
      setLoading(false);
    }
  }
  return (
    <div className={`${styles.stack} ${styles.fieldPicker}`}>
      {label(
        "Поле ECS",
        <Select
          aria-label="Поле ECS"
          showSearch
          filterOption={false}
          suffixIcon={<SearchOutlined />}
          popupMatchSelectWidth
          popupClassName={styles.ecsDropdown}
          searchValue={query}
          onSearch={(text) => {
            setQuery(text);
            setFields([]);
          }}
          onPopupScroll={(event) => {
            const target = event.currentTarget;
            if (
              target.scrollTop + target.clientHeight >=
              target.scrollHeight - 24
            )
              void more();
          }}
          placeholder="Поиск поля ECS"
          notFoundContent={
            query.trim()
              ? loading
                ? "Поиск…"
                : "Поля не найдены"
              : "Начните вводить название или описание поля"
          }
          value={value || undefined}
          onChange={(target) => {
            onChange(target);
            setQuery("");
          }}
          loading={loading}
          options={(query.trim() ? fields : []).map((field) => ({
            value: field.name,
            label: field.name,
            description: field.description,
            type: field.type,
          }))}
          optionRender={(option) => (
            <div className={styles.ecsOption}>
              <strong>{String(option.data.label)}</strong>
              <span>
                {String(option.data.type)} · {String(option.data.description)}
              </span>
            </div>
          )}
        />,
      )}
      {value && selectedField?.name === value && (
        <p className={styles.muted}>{selectedField.description}</p>
      )}
      {!!error && (
        <AppRequestError
          error={error}
          onRetry={() => setReload((value) => value + 1)}
        />
      )}
    </div>
  );
}

function CandidateEditor({
  candidate,
  kind,
  onChange,
  onRemove,
  onInvalid,
  readOnly = false,
}: {
  candidate: Candidate;
  kind: ExtractBlock["kind"];
  onChange: (value: Candidate) => void;
  onRemove: () => void;
  onInvalid: (field: "set" | "extract", invalid: boolean) => void;
  readOnly?: boolean;
}) {
  const update = (key: string, value: unknown) =>
    onChange({ ...candidate, [key]: value });
  const [setText, setSetText] = useState(
    JSON.stringify(candidate.set || {}, null, 2),
  );
  const [extractText, setExtractText] = useState(
    JSON.stringify(candidate.extract || {}, null, 2),
  );
  const [errors, setErrors] = useState<
    Partial<Record<"set" | "extract", string>>
  >({});
  useEffect(() => {
    setSetText(JSON.stringify(candidate.set || {}, null, 2));
  }, [candidate.set]);
  useEffect(() => {
    setExtractText(JSON.stringify(candidate.extract || {}, null, 2));
  }, [candidate.extract]);
  function parseObject(text: string, key: "set" | "extract") {
    try {
      const parsed: unknown = JSON.parse(text);
      if (!parsed || typeof parsed !== "object" || Array.isArray(parsed))
        throw Error();
      update(key, parsed);
      setErrors((current) => ({ ...current, [key]: "" }));
      onInvalid(key, false);
    } catch {
      setErrors((current) => ({ ...current, [key]: "Требуется JSON-объект." }));
      onInvalid(key, true);
    }
  }
  return (
    <div className={styles.candidate}>
      <div className={styles.toolbar}>
        <div className={styles.headingWithHelp}>
          <strong>Кандидат</strong>
          <ContextHelp topic="candidate" name="Кандидат" />
        </div>
        {!readOnly && (
          <Button danger size="small" onClick={onRemove}>
            Удалить кандидат
          </Button>
        )}
      </div>
      {kind === "regex" &&
        label(
          "Шаблон RE2",
          <Input
            disabled={readOnly}
            value={String(candidate.pattern || "")}
            onChange={(event) => update("pattern", event.target.value)}
          />,
        )}
      {kind === "template" &&
        label(
          "Шаблон %{field}",
          <Input
            disabled={readOnly}
            value={String(candidate.template || "")}
            onChange={(event) => update("template", event.target.value)}
          />,
        )}
      {kind === "columns" && (
        <>
          {label(
            "Разделитель",
            <Select
              disabled={readOnly}
              value={String(candidate.delimiter || " ")}
              onChange={(value) => update("delimiter", value)}
              options={[" ", ",", ";", "|", "\t"].map((value) => ({
                value,
                label:
                  value === " "
                    ? "Пробел"
                    : value === "\t"
                      ? "Табуляция"
                      : value,
              }))}
            />,
          )}
          {label(
            "Колонки по порядку (через запятую)",
            <Input
              disabled={readOnly}
              value={
                Array.isArray(candidate.columns)
                  ? candidate.columns.join(", ")
                  : ""
              }
              onChange={(event) =>
                update(
                  "columns",
                  event.target.value.split(",").map((part) => part.trim()),
                )
              }
            />,
          )}
        </>
      )}
      {kind === "json" && (
        <>
          {label(
            "Обязательные JSON-пути (через запятую)",
            <Input
              disabled={readOnly}
              value={
                Array.isArray(candidate.requires)
                  ? candidate.requires.join(", ")
                  : ""
              }
              onChange={(event) =>
                update(
                  "requires",
                  event.target.value
                    ? event.target.value.split(",").map((part) => part.trim())
                    : [],
                )
              }
            />,
          )}
          {label(
            "Имена извлечений и JSON-пути",
            <Input.TextArea
              readOnly={readOnly}
              rows={3}
              value={extractText}
              onChange={(event) => {
                setExtractText(event.target.value);
                onInvalid("extract", true);
              }}
              onBlur={() => parseObject(extractText, "extract")}
            />,
          )}
          {errors.extract && (
            <span role="alert" className={styles.error}>
              Поле extract: {errors.extract}
            </span>
          )}
        </>
      )}
      {label(
        "Константы set (JSON-объект)",
        <Input.TextArea
          readOnly={readOnly}
          rows={2}
          value={setText}
          onChange={(event) => {
            setSetText(event.target.value);
            onInvalid("set", true);
          }}
          onBlur={() => parseObject(setText, "set")}
        />,
      )}
      {errors.set && (
        <span role="alert" className={styles.error}>
          Поле set: {errors.set}
        </span>
      )}
    </div>
  );
}

function MapEditor({
  block,
  onChange,
  service,
  canFields,
  onInvalid,
  readOnly = false,
}: {
  block: MapBlock;
  onChange: (block: MapBlock) => void;
  service: NormalizersApi;
  canFields: boolean;
  onInvalid: (invalid: boolean) => void;
  readOnly?: boolean;
}) {
  const source = block.source;
  const mode =
    "ref" in source
      ? "ref"
      : "literal" in source
        ? "literal"
        : "join" in source
          ? "join"
          : "array";
  const sourceText =
    "ref" in source
      ? source.ref
      : "literal" in source
        ? JSON.stringify(source.literal)
        : source.refs.join(", ");
  const [literalText, setLiteralText] = useState(
    mode === "literal" ? sourceText : '"value"',
  );
  const [literalError, setLiteralError] = useState("");
  const dateHasZone = block.transform?.date_format.includes("%z") || false;
  const dateNeedsPolicy =
    !!block.transform && !dateHasZone && block.transform.timezone !== "UTC";
  useEffect(() => {
    if (mode === "literal") setLiteralText(sourceText);
  }, [mode, sourceText]);
  const setSource = (value: ValueSource) =>
    onChange({ ...block, source: value });
  return (
    <div className={styles.stack}>
      {canFields && !readOnly ? (
        <FieldPicker
          value={block.target}
          service={service}
          onChange={(target) => onChange({ ...block, target })}
        />
      ) : (
        <>
          <div>Поле ECS: {block.target}</div>
          {!canFields && (
            <Alert
              type="info"
              message="Для выбора поля ECS требуется право events.read."
            />
          )}
        </>
      )}
      <div className={styles.requiredField}>
        <strong>Обязательное поле</strong>
        <Switch
          aria-label="Обязательное поле"
          disabled={readOnly}
          checked={block.required}
          onChange={(required) => onChange({ ...block, required })}
        />
        <ContextHelp topic="required" name="Обязательное поле" />
      </div>
      {label(
        "Источник значения",
        <Select
          disabled={readOnly}
          aria-label="Источник значения"
          value={mode}
          onChange={(value) => {
            onInvalid(false);
            setLiteralError("");
            setSource(
              value === "ref"
                ? { ref: "log" }
                : value === "literal"
                  ? { literal: "value" }
                  : value === "join"
                    ? { refs: ["block1.value"], join: " " }
                    : { refs: ["block1.value"], as_array: true },
            );
          }}
          options={[
            { value: "ref", label: "Ссылка ref" },
            { value: "literal", label: "Константа literal" },
            { value: "join", label: "Несколько refs + join" },
            { value: "array", label: "Несколько refs + as_array" },
          ]}
        />,
      )}
      {mode === "ref" &&
        label(
          "Ссылка (log или block.field)",
          <Input
            disabled={readOnly}
            value={(source as { ref: string }).ref}
            onChange={(event) => setSource({ ref: event.target.value })}
          />,
        )}
      {mode === "literal" &&
        label(
          "Константа (JSON-значение)",
          <Input
            disabled={readOnly}
            value={literalText}
            onChange={(event) => setLiteralText(event.target.value)}
            onBlur={() => {
              try {
                setSource({ literal: JSON.parse(literalText) as string });
                setLiteralError("");
                onInvalid(false);
              } catch {
                setLiteralError("Введите корректное JSON-значение.");
                onInvalid(true);
              }
            }}
          />,
        )}
      {literalError && (
        <span role="alert" className={styles.error}>
          {literalError}
        </span>
      )}
      {(mode === "join" || mode === "array") &&
        label(
          "Ссылки через запятую",
          <Input
            disabled={readOnly}
            value={(source as { refs: string[] }).refs.join(", ")}
            onChange={(event) =>
              setSource(
                mode === "join"
                  ? {
                      refs: event.target.value
                        .split(",")
                        .map((part) => part.trim()),
                      join: (source as { join: string }).join,
                    }
                  : {
                      refs: event.target.value
                        .split(",")
                        .map((part) => part.trim()),
                      as_array: true,
                    },
              )
            }
          />,
        )}
      {mode === "join" &&
        label(
          "Разделитель",
          <Input
            disabled={readOnly}
            value={(source as { join: string }).join}
            onChange={(event) =>
              setSource({
                refs: (source as { refs: string[] }).refs,
                join: event.target.value,
              })
            }
          />,
        )}
      {label(
        "Формат даты (необязательно)",
        <Input
          disabled={readOnly}
          placeholder="%Y-%m-%dT%H:%M:%S%z"
          value={block.transform?.date_format || ""}
          onChange={(event) =>
            onChange({
              ...block,
              transform: event.target.value
                ? {
                    ...block.transform,
                    date_format: event.target.value,
                  }
                : null,
            })
          }
        />,
      )}
      {block.transform && (
        <>
          {label(
            "Зона исходного времени",
            <Select
              aria-label="Зона исходного времени"
              disabled={readOnly}
              placeholder="Выберите политику времени"
              value={
                block.transform.timezone === "UTC"
                  ? "utc"
                  : dateHasZone
                    ? "log"
                    : undefined
              }
              options={[
                {
                  value: "log",
                  label: "Зона указана в логе (%z)",
                  disabled: !dateHasZone,
                },
                { value: "utc", label: "Считать время без зоны UTC" },
              ]}
              onChange={(value) => {
                const transform = { ...block.transform! };
                if (value === "utc") transform.timezone = "UTC";
                else delete transform.timezone;
                onChange({ ...block, transform });
              }}
            />,
          )}
          {dateNeedsPolicy && (
            <Alert
              type="error"
              showIcon
              message="В исходном времени нет зоны (%z). Выберите UTC явно, иначе сервер не сможет определить момент события."
            />
          )}
        </>
      )}
      {block.transform &&
        label(
          "Явный год (если отсутствует в формате)",
          <InputNumber
            disabled={readOnly}
            min={1}
            max={9999}
            value={block.transform.year}
            onChange={(year) =>
              onChange({
                ...block,
                transform: { ...block.transform!, year: year || undefined },
              })
            }
          />,
        )}
    </div>
  );
}

type Variant = RuleV1["variants"][number];

function SettingsDialog({
  original,
  blockIndex,
  canWrite,
  canFields,
  service,
  issues,
  focusIssue,
  onClose,
  onApply,
  onDelete,
}: {
  original: Variant;
  blockIndex: number | null;
  canWrite: boolean;
  canFields: boolean;
  service: NormalizersApi;
  issues: RuleIssue[];
  focusIssue: RuleIssue | null;
  onClose: () => void;
  onApply: (value: Variant | Block) => void;
  onDelete: () => void;
}) {
  const [draft, setDraft] = useState<Variant | Block>(() =>
    clone(blockIndex === null ? original : original.blocks[blockIndex]),
  );
  const [invalid, setInvalid] = useState<Record<string, boolean>>({});
  const [issue, setIssue] = useState("");
  const [spaceChoice, setSpaceChoice] = useState<"keep" | "trim" | null>(null);
  const isVariant = blockIndex === null;
  const block = isVariant ? null : (draft as Block);
  const variant = isVariant ? (draft as Variant) : null;
  const changeBlock = (value: Block) => setDraft(value);
  function apply() {
    if (Object.values(invalid).some(Boolean)) {
      setIssue("Исправьте ошибки параметров перед применением.");
      return;
    }
    if (
      block?.kind === "map_ecs" &&
      block.transform &&
      !block.transform.date_format.includes("%z") &&
      block.transform.timezone !== "UTC"
    ) {
      setIssue("Для формата даты без %z явно выберите политику UTC.");
      return;
    }
    const next = clone(original);
    if (isVariant && !(draft as Variant).when.value.trim()) {
      setIssue("Задайте непустое условие. Пробелы без текста не подходят.");
      return;
    }
    if (
      isVariant &&
      (draft as Variant).when.kind !== original.when.kind &&
      edgeSpaces((draft as Variant).when.value) &&
      !spaceChoice
    ) {
      setIssue(
        "Выберите, убрать ли пробелы по краям условия или сознательно оставить их.",
      );
      return;
    }
    if (blockIndex === null) Object.assign(next, draft);
    else next.blocks[blockIndex] = draft as Block;
    try {
      parseRule(JSON.stringify({ format_version: 1, variants: [next] }));
      setIssue("");
      onApply(draft);
    } catch (reason) {
      setIssue(
        reason instanceof Error ? reason.message : "Некорректные параметры.",
      );
    }
  }
  function focusProblem() {
    if (!focusIssue) return;
    const dialog = [
      ...document.querySelectorAll<HTMLElement>('[role="dialog"]'),
    ].find((element) => element.textContent?.includes("Настройки"));
    if (!dialog) return;
    const fieldNames: Record<string, string> = {
      columns: "Колонки по порядку",
      pattern: "Шаблон RE2",
      template: "Шаблон %{field}",
      value: "Значение условия",
      priority: "Приоритет",
      target: "Поле ECS",
      source: "Источник значения",
      input: "Вход:",
      set: "Константы set",
      extract: "Имена извлечений",
      date_format: "Формат даты",
      required: "Обязательное поле",
      key: blockIndex === null ? "Ключ" : "Ключ блока",
    };
    const name = fieldNames[focusIssue.field || ""] || focusIssue.field || "";
    if (focusIssue.field === "target" || focusIssue.field === "source") {
      dialog.querySelector<HTMLElement>(`[aria-label="${name}"]`)?.focus();
      return;
    }
    const container =
      focusIssue.candidate === null
        ? dialog
        : dialog.querySelectorAll<HTMLElement>(`[class*="candidate"]`)[
            focusIssue.candidate
          ] || dialog;
    const field = [
      ...container.querySelectorAll<HTMLLabelElement>("label"),
    ].find((element) => element.textContent?.startsWith(name));
    (
      field?.querySelector<HTMLElement>("input,textarea,[role=combobox]") ||
      dialog.querySelector<HTMLElement>("input,textarea")
    )?.focus();
  }
  return (
    <Modal
      open
      afterOpenChange={(open) => {
        if (open) focusProblem();
      }}
      title={
        isVariant
          ? `Настройки варианта ${original.key}`
          : `Настройки блока ${block?.kind} ${block?.key}`
      }
      onCancel={onClose}
      footer={
        canWrite
          ? [
              <Button key="delete" danger onClick={onDelete}>
                Удалить
              </Button>,
              <Button key="cancel" onClick={onClose}>
                Отмена
              </Button>,
              <Button key="apply" type="primary" onClick={apply}>
                Применить
              </Button>,
            ]
          : [
              <Button key="close" onClick={onClose}>
                Закрыть
              </Button>,
            ]
      }
      width={720}
      destroyOnHidden
    >
      <div className={`${styles.stack} ${styles.settingsBody}`}>
        {issues.map((item, index) => (
          <Alert
            key={`${item.path}-${index}`}
            type="error"
            showIcon
            message={`${item.path}: ${item.message}`}
          />
        ))}
        {variant ? (
          <>
            {label(
              "Ключ",
              <Input
                value={variant.key}
                disabled={!canWrite}
                onChange={(event) =>
                  setDraft({ ...variant, key: event.target.value })
                }
              />,
            )}
            {label(
              "Приоритет",
              <InputNumber
                min={0}
                max={1000000}
                value={variant.priority}
                disabled={!canWrite}
                onChange={(value) =>
                  setDraft({ ...variant, priority: value || 0 })
                }
              />,
            )}
            {label(
              "Условие",
              <Select
                value={variant.when.kind}
                disabled={!canWrite}
                onChange={(value) => (
                  setSpaceChoice(null),
                  setDraft({
                    ...variant,
                    when: { ...variant.when, kind: value },
                  })
                )}
                options={["prefix", "contains", "regex"].map((value) => ({
                  value,
                  label: value,
                }))}
              />,
            )}
            {label(
              "Значение условия",
              <Input
                value={variant.when.value}
                disabled={!canWrite}
                onChange={(event) => (
                  setSpaceChoice(null),
                  setDraft({
                    ...variant,
                    when: { ...variant.when, value: event.target.value },
                  })
                )}
              />,
            )}
            {!variant.when.value.trim() && (
              <Alert
                type="error"
                showIcon
                message="Условие пустое. Укажите текст или шаблон RE2."
              />
            )}
            {variant.when.kind !== original.when.kind &&
              edgeSpaces(variant.when.value) && (
                <Alert
                  type="warning"
                  showIcon
                  message={
                    <>
                      В условии есть пробелы по краям:{" "}
                      <code>{JSON.stringify(variant.when.value)}</code>. Они
                      изменят совпадение.
                    </>
                  }
                  action={
                    <div className={styles.row}>
                      <Button
                        size="small"
                        onClick={() => {
                          setDraft({
                            ...variant,
                            when: {
                              ...variant.when,
                              value: variant.when.value.trim(),
                            },
                          });
                          setSpaceChoice("trim");
                        }}
                      >
                        Убрать пробелы по краям
                      </Button>
                      <Button
                        size="small"
                        onClick={() => setSpaceChoice("keep")}
                      >
                        Оставить пробелы
                      </Button>
                    </div>
                  }
                />
              )}
          </>
        ) : block ? (
          <>
            {label(
              "Ключ блока",
              <Input
                value={block.key}
                disabled={!canWrite}
                onChange={(event) =>
                  changeBlock({ ...block, key: event.target.value } as Block)
                }
              />,
            )}
            {block.kind === "map_ecs" ? (
              <MapEditor
                block={block}
                service={service}
                canFields={canFields}
                readOnly={!canWrite}
                onChange={changeBlock}
                onInvalid={(value) =>
                  setInvalid((current) => ({ ...current, map: value }))
                }
              />
            ) : (
              <>
                {label(
                  "Вход: log или результат предшествующего блока",
                  <Input
                    value={block.input || "log"}
                    disabled={!canWrite}
                    onChange={(event) =>
                      changeBlock({ ...block, input: event.target.value })
                    }
                  />,
                )}
                {block.candidates.map((candidate, index) => (
                  <CandidateEditor
                    key={index}
                    candidate={candidate}
                    kind={block.kind}
                    readOnly={!canWrite}
                    onChange={(value) => {
                      const next = clone(block);
                      next.candidates[index] = value;
                      changeBlock(next);
                    }}
                    onRemove={() => {
                      const next = clone(block);
                      next.candidates.splice(index, 1);
                      setInvalid((current) => {
                        const shifted: Record<string, boolean> = {};
                        for (const [key, value] of Object.entries(current)) {
                          const match = /^(\d+):(set|extract)$/.exec(key);
                          if (!match) shifted[key] = value;
                          else if (Number(match[1]) !== index)
                            shifted[
                              `${Number(match[1]) > index ? Number(match[1]) - 1 : Number(match[1])}:${match[2]}`
                            ] = value;
                        }
                        return shifted;
                      });
                      changeBlock(next);
                    }}
                    onInvalid={(field, value) =>
                      setInvalid((current) => ({
                        ...current,
                        [`${index}:${field}`]: value,
                      }))
                    }
                  />
                ))}
                {canWrite && (
                  <Button
                    disabled={block.candidates.length >= 8}
                    onClick={() => {
                      const next = clone(block);
                      next.candidates.push(
                        (defaultBlock(block.kind, 1) as ExtractBlock)
                          .candidates[0],
                      );
                      changeBlock(next);
                    }}
                  >
                    Добавить кандидат
                  </Button>
                )}
              </>
            )}
          </>
        ) : null}
        {issue && <Alert type="error" showIcon message={issue} />}
      </div>
    </Modal>
  );
}

export function NormalizerEditor() {
  const { id } = useParams();
  const [params] = useSearchParams();
  const copyId = params.get("copy");
  const editing = !!id && id !== "new";
  const readOnly = editing && !location.pathname.endsWith("/edit");
  const { api, session } = useStores();
  const service = useMemo(() => new NormalizersApi(api), [api]);
  const navigate = useNavigate();
  const canWrite = session.has("normalizers.write") && !readOnly;
  const [record, setRecord] = useState<Normalizer | null>(null);
  const [sourceStatus, setSourceStatus] = useState<
    "unknown" | "loading" | "active" | "none" | "error"
  >("unknown");
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [rule, setRule] = useState<RuleV1>(emptyRule);
  const [text, setText] = useState(() => JSON.stringify(emptyRule(), null, 2));
  const [mode, setMode] = useState<"blocks" | "json">("blocks");
  const [newKinds, setNewKinds] = useState<Record<number, Block["kind"]>>({});
  const [settings, setSettings] = useState<{
    variant: number;
    block: number | null;
  } | null>(null);
  const [focusedIssue, setFocusedIssue] = useState<RuleIssue | null>(null);
  const settingsTrigger = useRef<HTMLElement | null>(null);
  const [collapsed, setCollapsed] = useState<Record<number, boolean>>({});
  const [blockTypes, setBlockTypes] = useState<BlockType[]>([]);
  const [blockTypesError, setBlockTypesError] = useState<unknown>(null);
  const [error, setError] = useState<unknown>(null);
  const [validation, setValidation] = useState("");
  const [spaceWarning, setSpaceWarning] = useState("");
  const [notice, setNotice] = useState("");
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [previewing, setPreviewing] = useState(false);
  const [copyingExternal, setCopyingExternal] = useState(false);
  const [preview, setPreview] = useState<PreviewResult | null>(null);
  const [previewError, setPreviewError] = useState<unknown>(null);
  const [stale, setStale] = useState(false);
  const [previewTime, setPreviewTime] = useState("");
  const [log, setLog] = useState("");
  const [debugMode, setDebugMode] = useState<"manual" | "source">("manual");
  const [debugSession, setDebugSession] = useState(false);
  const [basis, setBasis] = useState("empty");
  const [basisQuery, setBasisQuery] = useState("");
  const [basisPickerKey, setBasisPickerKey] = useState(0);
  const [basisPage, setBasisPage] = useState<Page<Normalizer> | null>(null);
  const basisQueryRef = useRef(basisQuery);
  basisQueryRef.current = basisQuery;
  const [basisError, setBasisError] = useState<unknown>(null);
  const [basisLoading, setBasisLoading] = useState(false);
  const [confirm, setConfirm] = useState<{
    title: string;
    message: string;
    action: () => void;
  } | null>(null);
  const [dirty, setDirty] = useState(false);
  const [conflict, setConflict] = useState(false);
  const [unknownResult, setUnknownResult] = useState(false);
  const previewSeq = useRef(0);
  useEffect(() => {
    if (settings || !settingsTrigger.current) return;
    const trigger = settingsTrigger.current;
    settingsTrigger.current = null;
    const frame = window.requestAnimationFrame(() => {
      if (trigger.isConnected) trigger.focus();
    });
    return () => window.cancelAnimationFrame(frame);
  }, [settings]);
  const saveRef = useRef(false);
  const bypassNavigation = useRef(false);
  const historyGuard = useRef(false);
  const fileRef = useRef<HTMLInputElement>(null);
  const warnings = useMemo(() => referenceWarnings(rule), [rule]);
  const saveIssues = ruleIssues(error, rule);
  const previewIssues = ruleIssues(previewError, rule);
  const nodeIssues = [...saveIssues, ...previewIssues];
  const variantWarnings = useMemo(() => {
    const keys = new Set<string>();
    const priorities = new Set<number>();
    return rule.variants.map((variant) => {
      const issues: string[] = [];
      if (keys.has(variant.key))
        issues.push(`Ключ варианта ${variant.key} повторяется.`);
      if (priorities.has(variant.priority))
        issues.push(`Приоритет ${variant.priority} повторяется.`);
      keys.add(variant.key);
      priorities.add(variant.priority);
      return issues;
    });
  }, [rule]);
  function updateRule(next: RuleV1) {
    setRule(next);
    setText(JSON.stringify(next, null, 2));
    setDirty(true);
    setStale(true);
    setValidation("");
    setError(null);
    setPreviewError(null);
    previewSeq.current++;
  }
  function replaceRule(next: RuleV1, newName = "", sample?: { log: string }) {
    setRule(clone(next));
    setText(JSON.stringify(next, null, 2));
    setName(newName);
    setDescription("");
    if (sample) setLog(sample.log);
    setMode("blocks");
    setDirty(true);
    setStale(true);
    setValidation("");
    previewSeq.current++;
  }
  function currentRule(): RuleV1 | null {
    try {
      const problems = draftIssues(text);
      if (problems.length) {
        setValidation(
          "Проверьте замечания к JSON ниже. Известные проблемы перечислены; проверка может быть неполной.",
        );
        return null;
      }
      const parsed = parseRule(text);
      const empty = parsed.variants.findIndex(
        (variant) => !variant.when.value.trim(),
      );
      if (empty >= 0) {
        setValidation(
          `Вариант «${parsed.variants[empty].key}»: задайте непустое значение условия.`,
        );
        return null;
      }
      const spaced = parsed.variants.filter((variant) =>
        edgeSpaces(variant.when.value),
      );
      setSpaceWarning(
        spaced.length
          ? `Пробелы по краям условия: ${spaced.map((variant) => `вариант «${variant.key}» (${JSON.stringify(variant.when.value)})`).join("; ")}. Проверьте, нужны ли они.`
          : "",
      );
      setValidation("");
      return parsed;
    } catch (reason) {
      setValidation(
        reason instanceof SyntaxError
          ? `Ошибка JSON: ${reason.message}`
          : reason instanceof Error
            ? reason.message
            : "Некорректное правило.",
      );
      return null;
    }
  }
  useEffect(() => {
    if (!canWrite) return;
    const controller = new AbortController();
    service
      .blockTypes(controller.signal)
      .then((items) => {
        if (Array.isArray(items)) setBlockTypes(items);
        else setBlockTypesError(new Error("invalid block types"));
      })
      .catch((reason) => {
        if (!controller.signal.aborted) setBlockTypesError(reason);
      });
    return () => controller.abort();
  }, [service, session.version, canWrite]);
  useEffect(() => {
    if (!editing && !copyId) return;
    const controller = new AbortController();
    setLoading(true);
    service
      .detail(editing ? id! : copyId!, controller.signal)
      .then((item) => {
        setRecord(editing ? item : null);
        setName(editing ? item.name : `${item.name} (копия)`);
        setDescription(item.description || "");
        setRule(item.rule as RuleV1);
        setText(JSON.stringify(item.rule, null, 2));
        setMode(item.rule_status !== "ready" ? "json" : "blocks");
        setDirty(!editing);
      })
      .catch((reason) => {
        if (!controller.signal.aborted) setError(reason);
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoading(false);
      });
    return () => controller.abort();
  }, [service, id, editing, copyId, session.version, readOnly]);
  useEffect(() => {
    if (!editing || !canWrite || !record?.id) return;
    if (!session.has("sources.read")) {
      setSourceStatus("unknown");
      return;
    }
    const controller = new AbortController();
    setSourceStatus("loading");
    const check = async () => {
      try {
        let offset = 0;
        while (!controller.signal.aborted) {
          const page = await service.sources(
            record.id,
            offset,
            controller.signal,
          );
          if (controller.signal.aborted) return;
          if (page.items.some((source) => source.is_enabled)) {
            setSourceStatus("active");
            return;
          }
          offset += page.items.length;
          if (!page.items.length || offset >= page.total) {
            setSourceStatus("none");
            return;
          }
        }
      } catch {
        if (!controller.signal.aborted) setSourceStatus("error");
      }
    };
    void check();
    return () => controller.abort();
  }, [editing, canWrite, record?.id, service, session.version, session]);
  useEffect(() => {
    if (basis !== "existing" || !session.has("normalizers.read")) return;
    const controller = new AbortController();
    setBasisLoading(true);
    service
      .list(0, basisQuery, controller.signal)
      .then(setBasisPage)
      .catch((reason) => {
        if (!controller.signal.aborted) setBasisError(reason);
      })
      .finally(() => {
        if (!controller.signal.aborted) setBasisLoading(false);
      });
    return () => controller.abort();
  }, [basis, basisQuery, service, session]);
  useEffect(() => {
    const handler = (event: BeforeUnloadEvent) => {
      if (dirty || debugSession) event.preventDefault();
    };
    window.addEventListener("beforeunload", handler);
    return () => window.removeEventListener("beforeunload", handler);
  }, [dirty, debugSession]);
  useEffect(() => {
    if (!dirty && !debugSession) return;
    if (!historyGuard.current) {
      window.history.pushState(
        { ...window.history.state, normalizerDraftGuard: true },
        "",
        window.location.href,
      );
      historyGuard.current = true;
    }
    const onClick = (event: MouseEvent) => {
      if (
        bypassNavigation.current ||
        event.defaultPrevented ||
        event.ctrlKey ||
        event.metaKey ||
        event.shiftKey ||
        event.altKey
      )
        return;
      const target = event.target;
      const link = target instanceof Element ? target.closest("a[href]") : null;
      if (
        !(link instanceof HTMLAnchorElement) ||
        link.target === "_blank" ||
        link.origin !== window.location.origin ||
        link.pathname === window.location.pathname
      )
        return;
      event.preventDefault();
      event.stopPropagation();
      setConfirm({
        title: "Покинуть редактор?",
        message: `${dirty ? "Несохранённые изменения будут потеряны. " : ""}${debugSession ? "Результаты отладки будут потеряны." : ""}`,
        action: () => {
          bypassNavigation.current = true;
          navigate(`${link.pathname}${link.search}${link.hash}`);
        },
      });
    };
    const onPopState = () => {
      if (bypassNavigation.current) return;
      window.history.pushState(
        { ...window.history.state, normalizerDraftGuard: true },
        "",
        window.location.href,
      );
      setConfirm({
        title: "Покинуть редактор?",
        message: `${dirty ? "Несохранённые изменения будут потеряны. " : ""}${debugSession ? "Результаты отладки будут потеряны." : ""}`,
        action: () => {
          bypassNavigation.current = true;
          window.history.go(-2);
        },
      });
    };
    document.addEventListener("click", onClick, true);
    window.addEventListener("popstate", onPopState);
    return () => {
      document.removeEventListener("click", onClick, true);
      window.removeEventListener("popstate", onPopState);
    };
  }, [dirty, debugSession, navigate]);
  function askReplace(action: () => void, force = false, name = "") {
    if (dirty || force)
      setConfirm({
        title: "Заменить черновик?",
        message: `${force ? `Нормализатор «${name}» будет скопирован в черновик. ` : ""}${dirty ? "Текущие несохранённые изменения будут потеряны. " : ""}Исходный шаблон или нормализатор не изменится.`,
        action,
      });
    else action();
  }
  function leave() {
    if (dirty || debugSession)
      setConfirm({
        title: "Покинуть редактор?",
        message: `${dirty ? "Несохранённые изменения будут потеряны. " : ""}${debugSession ? "Результаты отладки будут потеряны." : ""}`,
        action: () => navigate("/normalizers"),
      });
    else navigate("/normalizers");
  }
  async function applyBasis(value: string, confirmSelection = false) {
    if (value === "empty") {
      askReplace(() => replaceRule(emptyRule()));
      return;
    }
    const template = templates.find((item) => item.id === value);
    if (template) {
      askReplace(() =>
        replaceRule(template.rule as RuleV1, template.name, template.sample),
      );
      return;
    }
    if (value.startsWith("normalizer:")) {
      try {
        const item = await service.detail(value.slice(11));
        askReplace(
          () => {
            replaceRule(item.rule as RuleV1, `${item.name} (копия)`);
            setDescription(item.description || "");
          },
          confirmSelection,
          item.name,
        );
      } catch (reason) {
        setBasisError(reason);
      }
    }
  }
  function editVariant(
    index: number,
    change: (variant: RuleV1["variants"][number]) => void,
  ) {
    const next = clone(rule);
    change(next.variants[index]);
    updateRule(next);
  }
  async function save() {
    if (saveRef.current || !canWrite) return;
    const parsed = currentRule();
    if (!parsed) return;
    if (!name.trim()) {
      setValidation("Введите имя нормализатора.");
      return;
    }
    saveRef.current = true;
    setSaving(true);
    setError(null);
    try {
      const payload = {
        name: name.trim(),
        description: description.trim() || null,
        rule: parsed,
      };
      const saved =
        editing && record
          ? await service.update(record.id, {
              ...payload,
              version: record.version,
            })
          : await service.create(payload);
      setRecord(saved);
      setName(saved.name);
      setDescription(saved.description || "");
      setRule(saved.rule as RuleV1);
      setText(JSON.stringify(saved.rule, null, 2));
      setDirty(false);
      setConflict(false);
      setUnknownResult(false);
      setNotice(`Нормализатор сохранён, версия ${saved.version}.`);
      if (!editing)
        navigate(`/normalizers/${saved.id}/edit`, { replace: true });
    } catch (reason) {
      setError(reason);
      if (reason instanceof ApiError && reason.code === "version_conflict")
        setConflict(true);
      if (
        reason instanceof ApiError &&
        (reason.status === 0 || reason.code === "invalid_response")
      )
        setUnknownResult(true);
    } finally {
      setSaving(false);
      saveRef.current = false;
    }
  }
  async function runPreview() {
    if (previewing || !canWrite) return;
    const parsed = currentRule();
    if (!parsed) return;
    const seq = ++previewSeq.current;
    setPreviewing(true);
    setPreviewError(null);
    const timestamp = new Date().toISOString();
    try {
      const result = await service.preview(parsed, { timestamp, log });
      if (seq === previewSeq.current) {
        setPreview(result);
        setPreviewTime(timestamp);
        setStale(false);
      }
    } catch (reason) {
      if (seq === previewSeq.current) setPreviewError(reason);
    } finally {
      setPreviewing(false);
    }
  }
  async function importFile(file: File) {
    if (
      !file.name.toLowerCase().endsWith(".json") ||
      (file.type !== "" &&
        file.type !== "application/json" &&
        file.type !== "text/json") ||
      file.size > 65536 ||
      !file.size
    ) {
      setValidation("Выберите JSON-файл размером до 64 КБ.");
      return;
    }
    try {
      const content = await file.text();
      const problems = draftIssues(content);
      if (problems.length) {
        askReplace(() => {
          setText(content);
          setMode("json");
          setDirty(true);
          setStale(true);
          setValidation(
            "Импортированный JSON требует исправления. Исходный текст сохранён.",
          );
        });
      } else {
        const parsed = parseRule(content);
        askReplace(() => updateRule(parsed));
      }
    } catch (reason) {
      setValidation(
        reason instanceof Error ? reason.message : "Не удалось прочитать файл.",
      );
    }
  }
  function exportRule() {
    const parsed = currentRule();
    if (!parsed) return;
    const url = URL.createObjectURL(
      new Blob([JSON.stringify(parsed, null, 2)], { type: "application/json" }),
    );
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = "normalizer-rule.json";
    anchor.click();
    URL.revokeObjectURL(url);
    setNotice("Актуальный JSON скачан.");
  }
  async function copyRule() {
    const parsed = currentRule();
    if (!parsed) return;
    try {
      await navigator.clipboard.writeText(JSON.stringify(parsed, null, 2));
      setNotice("Актуальный JSON скопирован.");
    } catch {
      setValidation(
        "Не удалось скопировать JSON. Проверьте разрешение браузера.",
      );
    }
  }
  function switchMode(next: "blocks" | "json") {
    if (next === mode) return;
    if (next === "blocks") {
      const parsed = currentRule();
      if (!parsed) return;
      setRule(parsed);
    } else setText(JSON.stringify(rule, null, 2));
    setMode(next);
  }
  function jumpToIssue(issue: RuleIssue) {
    setFocusedIssue(issue);
    if (issue.variant !== null && mode !== "json") {
      setCollapsed((current) => ({ ...current, [issue.variant!]: false }));
      setSettings({ variant: issue.variant, block: issue.block });
      return;
    }
    if (issue.path.startsWith("rule")) setMode("json");
    window.requestAnimationFrame(() => {
      const target = issue.path.startsWith("rule")
        ? document.querySelector<HTMLTextAreaElement>("textarea[class*='json']")
        : [...document.querySelectorAll<HTMLLabelElement>("label")]
            .find((element) =>
              element.textContent
                ?.toLowerCase()
                .includes(issue.field === "description" ? "описание" : "имя"),
            )
            ?.querySelector<HTMLInputElement>("input,textarea");
      target?.focus();
    });
  }
  function renderTreeNode(
    node: TreeNode,
    variant: Variant,
    variantIndex: number,
  ): React.ReactNode {
    const block = node.block;
    const messages = warnings[`${variant.key}/${block.key}`] || [];
    const serverIssues = nodeIssues.filter(
      (item) => item.variant === variantIndex && item.block === node.index,
    );
    return (
      <li className={styles.treeBranch} key={node.index}>
        <div className={styles.treeCard}>
          <div className={styles.treeHeading}>
            <strong>
              {block.kind} · {block.key}
            </strong>
            <span className={styles.muted}>шаг {node.index + 1}</span>
            <Button
              size="small"
              aria-label={`Настройки блока ${block.kind} ${block.key}`}
              onClick={(event) => {
                settingsTrigger.current = event.currentTarget;
                setSettings({ variant: variantIndex, block: node.index });
              }}
            >
              ⋯
            </Button>
          </div>
          {block.kind === "map_ecs" && (
            <div className={styles.treeDetail}>
              ECS {block.target}{" "}
              {block.required && <Tag color="orange">обязательно</Tag>}
            </div>
          )}
          <div className={styles.treeDetail}>
            {node.inputs.length
              ? `Вход${node.inputs.length > 1 ? "ы" : ""}: ${node.inputs.join(", ")}`
              : "Источник: константа"}
          </div>
          {block.kind !== "map_ecs" && (
            <>
              <div className={styles.treeDetail}>
                Возможные выходы:{" "}
                {node.outputs.length
                  ? node.outputs
                      .map((name) => `${block.key}.${name}`)
                      .join(", ")
                  : "выход определяется правилом"}
              </div>
              <span className={styles.muted}>
                Кандидатов: {block.candidates.length}
              </span>
            </>
          )}
          {messages.map((message) => (
            <Alert key={message} type="warning" showIcon message={message} />
          ))}
          {serverIssues.map((item, index) => (
            <Alert
              key={`${item.path}-${index}`}
              type="error"
              showIcon
              message={`${item.path}: ${item.message}`}
            />
          ))}
        </div>
        {node.children.length > 0 && (
          <ul className={styles.treeChildren}>
            {node.children.map((child) =>
              renderTreeNode(child, variant, variantIndex),
            )}
          </ul>
        )}
      </li>
    );
  }
  if (loading) return <LoadingOverlay />;
  return (
    <main className={`${styles.page} ${styles.editorPage}`}>
      <FeedbackLayer>
        {notice && (
          <NormalizerFeedback
            type="success"
            message={notice}
            onClose={() => setNotice("")}
          />
        )}
        {!!error && (
          <div className={styles.feedbackError}>
            <AppRequestError error={error} />
            <Button
              aria-label="Закрыть сообщение об ошибке"
              onClick={() => setError(null)}
            >
              ×
            </Button>
          </div>
        )}
        {conflict && (
          <NormalizerFeedback
            type="error"
            message="Версия изменилась на сервере. Черновик сохранён локально. Откройте свежую версию в отдельной вкладке и разрешите конфликт вручную."
            onClose={() => setConflict(false)}
            action={
              record && (
                <Link target="_blank" to={`/normalizers/${record.id}`}>
                  Свежая версия
                </Link>
              )
            }
          />
        )}
        {unknownResult && (
          <NormalizerFeedback
            type="error"
            message="Ответ сервера потерян: запись могла сохраниться. Сверьте состояние перед ручным повтором."
            onClose={() => setUnknownResult(false)}
            action={
              <Link
                target="_blank"
                to={record ? `/normalizers/${record.id}` : "/normalizers"}
              >
                Сверить
              </Link>
            }
          />
        )}
      </FeedbackLayer>
      <div className={styles.columns}>
        <section
          className={`${styles.panel} ${styles.stack} ${styles.editorColumn}`}
          aria-label="Правило"
        >
          {canWrite && (
            <>
              {!!blockTypesError && (
                <AppRequestError
                  error={blockTypesError}
                  onRetry={() => {
                    setBlockTypesError(null);
                    void service
                      .blockTypes()
                      .then(setBlockTypes)
                      .catch(setBlockTypesError);
                  }}
                />
              )}
              <div className={styles.row}>
                <ContextHelp topic="basis" name="Основа и шаблоны" />
                <Select
                  aria-label="Основа правила"
                  value={basis}
                  onChange={setBasis}
                  options={[
                    { value: "empty", label: "Пустое правило" },
                    ...templates.map((item) => ({
                      value: item.id,
                      label: `Шаблон: ${item.name}`,
                    })),
                    { value: "existing", label: "Существующий нормализатор" },
                  ]}
                />
                <Button onClick={() => void applyBasis(basis)}>
                  Применить основу
                </Button>
              </div>
              {basis !== "empty" && basis !== "existing" && (
                <p className={styles.muted}>
                  {templates.find((item) => item.id === basis)?.description}
                </p>
              )}
              {basis === "existing" && (
                <div className={styles.stack}>
                  <Select
                    key={basisPickerKey}
                    aria-label="Выбрать существующий нормализатор"
                    showSearch
                    filterOption={false}
                    suffixIcon={<SearchOutlined />}
                    searchValue={basisQuery}
                    onSearch={setBasisQuery}
                    placeholder="Поиск по имени"
                    notFoundContent={
                      basisLoading ? "Поиск…" : "Нормализаторы не найдены"
                    }
                    onPopupScroll={async (event) => {
                      const target = event.currentTarget;
                      if (
                        target.scrollTop + target.clientHeight <
                          target.scrollHeight - 24 ||
                        basisLoading ||
                        !basisPage ||
                        basisPage.items.length >= basisPage.total
                      )
                        return;
                      const requestedQuery = basisQueryRef.current;
                      setBasisLoading(true);
                      try {
                        const next = await service.list(
                          basisPage.items.length,
                          requestedQuery,
                        );
                        if (basisQueryRef.current !== requestedQuery) return;
                        setBasisPage((current) =>
                          current
                            ? {
                                ...next,
                                items: [...current.items, ...next.items],
                              }
                            : next,
                        );
                      } catch (reason) {
                        setBasisError(reason);
                      } finally {
                        setBasisLoading(false);
                      }
                    }}
                    loading={basisLoading}
                    options={(basisPage?.items || []).map((item) => ({
                      value: `normalizer:${item.id}`,
                      label: `${item.name} · v${item.version}`,
                    }))}
                    onChange={(value) => {
                      setBasisPickerKey((current) => current + 1);
                      setBasisQuery("");
                      void applyBasis(value, true);
                    }}
                  />
                  {!!basisError && <AppRequestError error={basisError} />}
                </div>
              )}
            </>
          )}
          {label(
            "Имя",
            <Input
              maxLength={200}
              value={name}
              disabled={!canWrite}
              onChange={(event) => {
                setName(event.target.value);
                setDirty(true);
              }}
            />,
          )}
          {label(
            "Описание",
            <Input.TextArea
              rows={2}
              value={description}
              disabled={!canWrite}
              onChange={(event) => {
                setDescription(event.target.value);
                setDirty(true);
              }}
            />,
          )}
          {!readOnly && (
            <div className={styles.row}>
              <ContextHelp topic="modes" name="Режимы Блоки и JSON" />
              <Button
                type={mode === "blocks" ? "primary" : "default"}
                onClick={() => switchMode("blocks")}
              >
                Блоки
              </Button>
              <Button
                type={mode === "json" ? "primary" : "default"}
                onClick={() => switchMode("json")}
              >
                JSON
              </Button>
            </div>
          )}
          {!readOnly && (
            <div className={styles.row}>
              <ContextHelp
                topic="exchange"
                name="Копирование, импорт, экспорт и промпт"
              />
              <Button onClick={() => void copyRule()}>Копировать JSON</Button>
              <Button onClick={exportRule}>Скачать .json</Button>
              {canWrite && (
                <>
                  <input
                    ref={fileRef}
                    type="file"
                    accept=".json,application/json"
                    hidden
                    onChange={(event) => {
                      const file = event.target.files?.[0];
                      if (file) void importFile(file);
                      event.target.value = "";
                    }}
                  />
                  <Button onClick={() => fileRef.current?.click()}>
                    Импорт .json
                  </Button>
                </>
              )}
              <Button
                loading={copyingExternal}
                disabled={copyingExternal}
                onClick={async () => {
                  setCopyingExternal(true);
                  setNotice("");
                  try {
                    await navigator.clipboard.writeText(aiPrompt);
                    setValidation("");
                    setNotice(
                      "Промпт скопирован. Ответ внешней нейросети проверьте через preview перед сохранением.",
                    );
                  } catch {
                    setValidation(
                      "Не удалось скопировать промпт. Проверьте разрешение браузера.",
                    );
                  } finally {
                    setCopyingExternal(false);
                  }
                }}
              >
                Промпт для ИИ
              </Button>
            </div>
          )}
          {mode === "json" ? (
            <>
              {draftRisks(text).map((warning, index) => (
                <Alert key={index} type="warning" showIcon message={warning} />
              ))}
              {(draftIssues(text).length > 0 ||
                saveIssues.length > 0 ||
                validation) && (
                <Alert
                  type="error"
                  showIcon
                  message={
                    draftIssues(text).length > 0
                      ? `Замечания к JSON: ${draftIssues(text).length}`
                      : saveIssues.length > 0
                        ? `Ошибки правила: ${saveIssues.length}`
                        : validation
                  }
                  description={
                    <>
                      {draftIssues(text).length > 0 && (
                        <>
                          <p>
                            Известные проблемы черновика. Список может быть
                            неполным; окончательную компиляцию проверяет сервер.
                          </p>
                          <ol>
                            {draftIssues(text).map((issue, index) => (
                              <li key={`${issue.path}-${index}`}>
                                <button
                                  type="button"
                                  onClick={() =>
                                    document
                                      .querySelector<HTMLTextAreaElement>(
                                        "textarea[class*='json']",
                                      )
                                      ?.focus()
                                  }
                                >
                                  {issue.path}
                                </button>
                                : {issue.message}
                              </li>
                            ))}
                          </ol>
                        </>
                      )}
                      {saveIssues.length > 0 && (
                        <div className={styles.issueList}>
                          {saveIssues.map((issue, index) => (
                            <Button
                              type="link"
                              key={`${issue.path}-${index}`}
                              onClick={() => jumpToIssue(issue)}
                            >
                              {issue.path}: {issue.message} Перейти к полю
                            </Button>
                          ))}
                        </div>
                      )}
                      {validation &&
                        draftIssues(text).length > 0 &&
                        !validation.startsWith(
                          "Проверьте замечания к JSON",
                        ) && <p>{validation}</p>}
                      {draftIssues(text).length > 0 && (
                        <Button
                          loading={copyingExternal}
                          disabled={copyingExternal}
                          onClick={async () => {
                            setCopyingExternal(true);
                            setNotice("");
                            try {
                              await navigator.clipboard.writeText(
                                aiFeedback(draftIssues(text)),
                              );
                              setValidation("");
                              setNotice(
                                "Замечания для ИИ скопированы без JSON и лога.",
                              );
                            } catch {
                              setValidation(
                                "Не удалось скопировать замечания. Проверьте разрешение браузера.",
                              );
                            } finally {
                              setCopyingExternal(false);
                            }
                          }}
                        >
                          Скопировать замечания для ИИ
                        </Button>
                      )}
                    </>
                  }
                />
              )}
              {spaceWarning && (
                <Alert type="warning" showIcon message={spaceWarning} />
              )}
              {label(
                "Полный JSON RuleV1",
                <Input.TextArea
                  className={styles.json}
                  rows={24}
                  value={text}
                  readOnly={!canWrite}
                  onChange={(event) => {
                    setText(event.target.value);
                    setDirty(true);
                    setStale(true);
                    previewSeq.current++;
                  }}
                  onBlur={() => {
                    if (canWrite) currentRule();
                  }}
                />,
              )}
            </>
          ) : (
            <>
              <p className={styles.muted}>
                Варианты проверяются по приоритету; применяется первый
                подходящий. Подписи связей показывают поля правила, а не
                значения события.
              </p>
              {rule.variants.map((variant, vi) => (
                <div className={styles.treeVariant} key={vi}>
                  <div className={styles.treeCard}>
                    <div className={styles.treeHeading}>
                      <Button
                        size="small"
                        aria-label={
                          collapsed[vi]
                            ? `Развернуть вариант ${variant.key}`
                            : `Свернуть вариант ${variant.key}`
                        }
                        aria-expanded={!collapsed[vi]}
                        onClick={() =>
                          setCollapsed((current) => ({
                            ...current,
                            [vi]: !current[vi],
                          }))
                        }
                      >
                        {collapsed[vi] ? "▸" : "▾"}
                      </Button>
                      <strong>
                        Вариант {vi + 1} · {variant.key}
                      </strong>
                      <span className={styles.muted}>
                        Приоритет {variant.priority}
                      </span>
                      <Button
                        size="small"
                        aria-label={`Настройки варианта ${variant.key}`}
                        onClick={(event) => {
                          settingsTrigger.current = event.currentTarget;
                          setSettings({ variant: vi, block: null });
                        }}
                      >
                        ⋯
                      </Button>
                    </div>
                    <div className={styles.treeDetail}>
                      Условие {variant.when.kind}: {variant.when.value}
                    </div>
                    {variantWarnings[vi].map((issue) => (
                      <Alert
                        key={issue}
                        type="warning"
                        showIcon
                        message={issue}
                      />
                    ))}
                    {nodeIssues
                      .filter(
                        (item) => item.variant === vi && item.block === null,
                      )
                      .map((item, index) => (
                        <Alert
                          key={`${item.path}-${index}`}
                          type="error"
                          showIcon
                          message={`${item.path}: ${item.message}`}
                        />
                      ))}
                    {!variant.when.value.trim() && (
                      <Alert
                        type="warning"
                        showIcon
                        message="Условие ещё не задано. Откройте настройки варианта."
                      />
                    )}
                  </div>
                  {!collapsed[vi] && (
                    <>
                      <ul className={styles.treeChildren}>
                        {buildRuleTree(variant).map((node) =>
                          renderTreeNode(node, variant, vi),
                        )}
                      </ul>
                      {canWrite && (
                        <div className={styles.treeAdd}>
                          <ContextHelp topic="blocks" name="Типы блоков" />
                          <Select
                            aria-label={`Тип нового блока варианта ${variant.key}`}
                            value={newKinds[vi] || "regex"}
                            options={(blockTypes.length
                              ? blockTypes
                              : [
                                  "regex",
                                  "template",
                                  "columns",
                                  "json",
                                  "map_ecs",
                                ].map((kind) => ({ kind, description: "" }))
                            )
                              .filter((item) =>
                                [
                                  "regex",
                                  "template",
                                  "columns",
                                  "json",
                                  "map_ecs",
                                ].includes(item.kind),
                              )
                              .map((item) => ({
                                value: item.kind,
                                label: `${item.kind}${item.description ? ` · ${item.description}` : ""}`,
                              }))}
                            onChange={(value) =>
                              setNewKinds((current) => ({
                                ...current,
                                [vi]: value,
                              }))
                            }
                          />
                          <Button
                            onClick={() => {
                              const kind = newKinds[vi] || "regex";
                              editVariant(vi, (item) => {
                                item.blocks.push(
                                  defaultBlock(kind, item.blocks.length + 1),
                                );
                              });
                            }}
                          >
                            Добавить блок
                          </Button>
                        </div>
                      )}
                    </>
                  )}
                </div>
              ))}
              {canWrite && (
                <Button
                  onClick={() => {
                    const next = clone(rule);
                    next.variants.push({
                      key: `variant${next.variants.length + 1}`,
                      priority:
                        Math.max(
                          ...next.variants.map((item) => item.priority),
                          0,
                        ) + 10,
                      when: { kind: "contains", value: "" },
                      blocks: [defaultBlock("map_ecs", 1)],
                    });
                    updateRule(next);
                  }}
                >
                  Добавить вариант
                </Button>
              )}
            </>
          )}
          {mode !== "json" && validation && (
            <Alert type="error" showIcon message={validation} />
          )}
          {mode !== "json" && spaceWarning && (
            <Alert type="warning" showIcon message={spaceWarning} />
          )}
          {mode !== "json" && (
            <IssueSummary issues={saveIssues} onSelect={jumpToIssue} />
          )}
          {editing && canWrite && sourceStatus === "active" && (
            <Alert
              type="warning"
              showIcon
              message="Этот нормализатор назначен работающему Kafka-источнику. После сохранения новую версию получат только будущие сообщения; сохранённые события не пересчитаются."
            />
          )}
          {editing && canWrite && sourceStatus === "loading" && (
            <Alert
              type="info"
              showIcon
              message="Проверяем связанные источники перед сохранением…"
            />
          )}
          {editing && canWrite && sourceStatus === "unknown" && (
            <Alert
              type="info"
              showIcon
              message="Нет права просмотра связанных источников. Нельзя определить, используется ли нормализатор работающим источником."
            />
          )}
          {editing && canWrite && sourceStatus === "error" && (
            <Alert
              type="warning"
              showIcon
              message="Не удалось проверить связанные источники. Нельзя утверждать, что работающих источников нет."
            />
          )}
          <div className={styles.row}>
            <Button onClick={leave}>Отмена</Button>
            {canWrite && (
              <Button
                type="primary"
                loading={saving}
                disabled={saving || unknownResult || conflict}
                onClick={() => void save()}
              >
                {editing ? "Сохранить" : "Создать"}
              </Button>
            )}
          </div>
        </section>
        <section
          className={`${styles.panel} ${styles.stack} ${styles.editorColumn}`}
          aria-label="Отладка"
        >
          <div className={styles.row}>
            <Button
              type={debugMode === "manual" ? "primary" : "default"}
              onClick={() => {
                const action = () => {
                  setDebugMode("manual");
                  setDebugSession(false);
                };
                if (debugSession)
                  setConfirm({
                    title: "Сбросить результаты отладки?",
                    message: "Текущий прогон и результаты будут потеряны.",
                    action,
                  });
                else action();
              }}
            >
              Свой лог
            </Button>
            {canWrite && (
              <Button
                type={debugMode === "source" ? "primary" : "default"}
                onClick={() => setDebugMode("source")}
              >
                Лог из источника
              </Button>
            )}
          </div>
          {debugMode === "source" ? (
            <SourceDebugPanel
              service={service}
              currentRule={currentRule}
              onSessionChange={setDebugSession}
            />
          ) : (
            <>
              <h2>Введённый пример</h2>
              <p className={styles.muted}>
                Preview нормализует введённую строку log. Служебное время
                проверки устанавливается автоматически. Изменение правила не
                пересчитывает сохранённые события.
              </p>
              {label(
                "log — одна сырая строка",
                <Input.TextArea
                  rows={7}
                  value={log}
                  disabled={!canWrite}
                  onChange={(event) => {
                    setLog(event.target.value);
                    setStale(true);
                    previewSeq.current++;
                  }}
                />,
              )}
              {canWrite ? (
                <div className={styles.row}>
                  <ContextHelp
                    topic="preview"
                    name="Проверка черновика и статусы"
                  />
                  <Button
                    loading={previewing}
                    disabled={previewing}
                    onClick={() => void runPreview()}
                  >
                    Проверить черновик
                  </Button>
                </div>
              ) : (
                <Alert
                  type="info"
                  message="Серверная проверка доступна только с правом normalizers.write."
                />
              )}
              {!!previewError && <AppRequestError error={previewError} />}
              <IssueSummary issues={previewIssues} onSelect={jumpToIssue} />
              {preview && (
                <div className={styles.stack}>
                  <div className={styles.row}>
                    <Tag
                      color={
                        preview.status === "complete"
                          ? "green"
                          : preview.status === "partial"
                            ? "orange"
                            : "red"
                      }
                    >
                      {previewStatus[preview.status]} ({preview.status})
                    </Tag>
                    {stale && <Tag color="orange">Результат устарел</Tag>}
                  </div>
                  <Alert
                    type={
                      preview.status === "failed"
                        ? "error"
                        : preview.status === "partial"
                          ? "warning"
                          : "success"
                    }
                    showIcon
                    message={
                      stale
                        ? "Причины относятся к предыдущему черновику."
                        : "Причина результата"
                    }
                    description={
                      <ul className={styles.reasonList}>
                        {previewReasons(preview).map((reason, index) => (
                          <li key={index}>{reason}</li>
                        ))}
                      </ul>
                    }
                  />
                  <div>Вариант: {preview.variant_key || "не выбран"}</div>
                  <div className={styles.muted}>
                    Служебное время ручной проверки: {previewTime}
                  </div>
                  <h3>ECS</h3>
                  <pre className={styles.preview}>
                    {JSON.stringify(preview.ecs_data, null, 2)}
                  </pre>
                  <h3>Диагностика</h3>
                  {preview.diagnostics.length ? (
                    preview.diagnostics.map((item, index) => (
                      <Alert
                        key={index}
                        type="warning"
                        message={`${item.variant || "Правило"}${item.block ? ` / ${item.block}` : ""}${item.field ? ` / ${item.field}` : ""}: ${diagnosticText[item.code] || "Проверьте данные и правило."} (${item.code})`}
                      />
                    ))
                  ) : (
                    <span>Диагностик нет.</span>
                  )}
                  <h3>Трассировка</h3>
                  {preview.trace.map((step, index) => (
                    <div key={index}>
                      {step.variant} / {step.block}:{" "}
                      {traceResult[step.result] || "Результат"} ({step.result})
                      {step.candidate !== null
                        ? `, кандидат ${step.candidate + 1}`
                        : ""}
                    </div>
                  ))}
                </div>
              )}
            </>
          )}
        </section>
      </div>
      {settings && (
        <SettingsDialog
          key={`${settings.variant}/${settings.block ?? "variant"}`}
          original={rule.variants[settings.variant]}
          blockIndex={settings.block}
          canWrite={canWrite}
          canFields={session.has("events.read")}
          service={service}
          issues={nodeIssues.filter(
            (item) =>
              item.variant === settings.variant &&
              item.block === settings.block,
          )}
          focusIssue={focusedIssue}
          onClose={() => setSettings(null)}
          onApply={(value) => {
            const next = clone(rule);
            if (settings.block === null) {
              const changed = value as Variant;
              next.variants[settings.variant] = {
                ...next.variants[settings.variant],
                key: changed.key,
                priority: changed.priority,
                when: changed.when,
              };
            } else
              next.variants[settings.variant].blocks[settings.block] =
                value as Block;
            updateRule(next);
            setSettings(null);
          }}
          onDelete={() => {
            setConfirm({
              title:
                settings.block === null
                  ? "Удалить вариант из черновика?"
                  : "Удалить блок из черновика?",
              message:
                settings.block === null
                  ? `Вариант «${rule.variants[settings.variant].key}» и все его блоки будут убраны из черновика. Изменение попадёт на сервер только после сохранения нормализатора.`
                  : `Блок «${rule.variants[settings.variant].blocks[settings.block].key}» будет убран из черновика. Ссылки на его поля могут стать недействительными. Изменение попадёт на сервер только после сохранения нормализатора.`,
              action: () => {
                const next = clone(rule);
                if (settings.block === null)
                  next.variants.splice(settings.variant, 1);
                else
                  next.variants[settings.variant].blocks.splice(
                    settings.block,
                    1,
                  );
                updateRule(next);
                setSettings(null);
              },
            });
          }}
        />
      )}
      <ConfirmationDialog
        open={!!confirm}
        zIndex={1200}
        title={confirm?.title || ""}
        message={confirm?.message || ""}
        confirmText={
          confirm?.title.startsWith("Удалить") ? "Удалить" : "Продолжить"
        }
        danger={!!confirm?.title.startsWith("Удалить")}
        onCancel={() => setConfirm(null)}
        onConfirm={() => {
          const action = confirm?.action;
          setConfirm(null);
          action?.();
        }}
      />
    </main>
  );
}
