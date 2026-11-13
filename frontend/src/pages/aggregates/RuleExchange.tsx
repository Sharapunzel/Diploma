import { useEffect, useMemo, useRef, useState } from "react";
import { Alert, Button, Input } from "antd";
import { observer } from "mobx-react-lite";
import type { AggregatesApi, Operator } from "../../app/api/aggregates";
import {
  object,
  parseAggregate,
  printAggregate,
} from "../../app/api/aggregateJson";
import { aggregatePrompt } from "../../features/aggregates/rule";
import { CatalogStore } from "../../features/aggregates/CatalogStore";
import { ConfirmationDialog } from "../../shared/ui/ConfirmationDialog";
import { AggregateError } from "./AggregateError";
import { Help } from "./AggregateHelp";
import styles from "./RuleExchange.module.css";

function ruleText(text: string) {
  const value = parseAggregate(text);
  if (
    !object(value) ||
    !Array.isArray(value.features) ||
    Object.keys(value).some(
      (key) => !["features", "auxiliary", "where"].includes(key),
    )
  )
    throw new Error("invalid rule document");
  return printAggregate(value, true);
}
export const RuleExchange = observer(function RuleExchange({
  api,
  source,
  operators,
  text,
  onImport,
  disabled = false,
}: {
  api: AggregatesApi;
  source: string;
  operators: Operator[];
  text: string;
  onImport?: (text: string) => void;
  disabled?: boolean;
}) {
  const catalog = useMemo(() => new CatalogStore(api, source), [api, source]);
  const scope = useRef(catalog);
  scope.current = catalog;
  const enabled = useRef(!disabled);
  enabled.current = !disabled;
  const alive = useRef(true),
    reading = useRef<CatalogStore | null>(null),
    copying = useRef<CatalogStore | null>(null);
  const file = useRef<HTMLInputElement>(null);
  const [busy, setBusy] = useState(false),
    [error, setError] = useState("");
  const [success, setSuccess] = useState(""),
    [manual, setManual] = useState("");
  const [pending, setPending] = useState<string | null>(null);
  const [copyKind, setCopyKind] = useState<
    "rule" | "prompt" | "fields" | "manual" | null
  >(null);
  const [dismissedCatalogError, setDismissedCatalogError] =
    useState<unknown>(null);
  useEffect(() => {
    if (!success) return;
    const timer = setTimeout(() => setSuccess(""), 5000);
    return () => clearTimeout(timer);
  }, [success]);
  useEffect(() => {
    alive.current = true;
    setManual("");
    setSuccess("");
    setError("");
    setPending(null);
    setBusy(false);
    setCopyKind(null);
    setDismissedCatalogError(null);
    return () => {
      alive.current = false;
      catalog.dispose();
    };
  }, [catalog]);
  const current = (owner: CatalogStore) =>
    alive.current && scope.current === owner;
  const copy = async (kind: "rule" | "prompt" | "fields") => {
    if (copying.current === catalog || !enabled.current) return;
    copying.current = catalog;
    setBusy(true);
    setCopyKind(kind);
    setDismissedCatalogError(null);
    setError("");
    setSuccess("");
    setManual("");
    const owner = catalog;
    try {
      const output =
        kind === "fields"
          ? await owner.fullText()
          : kind === "prompt"
            ? aggregatePrompt(operators)
            : ruleText(text);
      if (!output || !current(owner)) return;
      try {
        if (!navigator.clipboard) throw new Error();
        await navigator.clipboard.writeText(output);
        if (current(owner))
          setSuccess(
            kind === "fields"
              ? "Полный каталог полей скопирован."
              : kind === "prompt"
                ? "Инструкция скопирована."
                : "JSON правила скопирован.",
          );
      } catch {
        if (current(owner)) {
          setManual(output);
          setError(
            "Буфер обмена недоступен или истёк срок разрешения. Нажмите «Копировать подготовленный текст» либо выделите текст и нажмите Ctrl+C.",
          );
        }
      }
    } catch {
      if (current(owner))
        setError(
          kind === "rule"
            ? "Текущий текст нельзя скопировать как JSON правила. Исправьте его; ввод сохранён."
            : "Не удалось подготовить текст для копирования. Повторите действие.",
        );
    } finally {
      if (copying.current === owner) copying.current = null;
      if (current(owner)) {
        setBusy(false);
        setCopyKind(null);
      }
    }
  };
  return (
    <div className={styles.exchange}>
      <div className={styles.row} role="group" aria-label="Обмен правилом">
        <Button
          loading={copyKind === "rule"}
          disabled={busy || disabled}
          onClick={() => void copy("rule")}
        >
          Копировать JSON
        </Button>
        <Button
          disabled={disabled || busy}
          onClick={() => {
            try {
              const url = URL.createObjectURL(
                new Blob([ruleText(text)], {
                  type: "application/json;charset=utf-8",
                }),
              );
              try {
                const anchor = document.createElement("a");
                anchor.href = url;
                anchor.download = "aggregate-rule.json";
                anchor.click();
                setSuccess("Правило подготовлено для скачивания.");
                setError("");
              } finally {
                URL.revokeObjectURL(url);
              }
            } catch {
              setError(
                "Текущий текст нельзя экспортировать как правило JSON. Исправьте его; ввод сохранён.",
              );
            }
          }}
        >
          Скачать .json
        </Button>
        {onImport && (
          <>
            <Button
              loading={busy && reading.current === catalog}
              disabled={disabled || busy}
              onClick={() => file.current?.click()}
            >
              Импорт .json
            </Button>
            <input
              ref={file}
              type="file"
              accept=".json,application/json"
              hidden
              aria-label="Файл правила JSON"
              onChange={(e) => {
                const selected = e.target.files?.[0];
                e.target.value = "";
                if (
                  !selected ||
                  reading.current === catalog ||
                  !enabled.current
                )
                  return;
                setError("");
                setSuccess("");
                setManual("");
                if (!selected.size || selected.size > 1048576) {
                  setError(
                    "Выберите непустой UTF-8 JSON до 1 MiB. Это транспортный предел; компактное правило отдельно проверяет сервер.",
                  );
                  return;
                }
                reading.current = catalog;
                setBusy(true);
                const owner = catalog;
                void selected
                  .arrayBuffer()
                  .then((buffer) => {
                    const content = new TextDecoder("utf-8", {
                      fatal: true,
                    }).decode(buffer);
                    if (!content.trim()) throw new Error();
                    if (current(owner) && enabled.current) setPending(content);
                  })
                  .catch(() => {
                    if (current(owner))
                      setError(
                        "Не удалось прочитать UTF-8 файл. Прежний черновик сохранён.",
                      );
                  })
                  .finally(() => {
                    if (reading.current === owner) reading.current = null;
                    if (current(owner)) setBusy(false);
                  });
              }}
            />
          </>
        )}
        <Button
          loading={copyKind === "prompt"}
          disabled={busy || disabled || !operators.length}
          onClick={() => void copy("prompt")}
        >
          Инструкция для ИИ
        </Button>
        <Button
          disabled={!source || disabled || busy}
          loading={copyKind === "fields"}
          onClick={() => void copy("fields")}
        >
          Скопировать поля источника
        </Button>
        <span className={styles.helpSlot}>
          <Help
            name="Копирование, импорт, экспорт и инструкция"
            text="Копирование и скачивание берут текущее JSON-правило с точными числами. Импорт заменяет только правило черновика и не сохраняет его на сервере. Инструкция для ИИ копируется отдельно от правила и полей; приложение не обращается к ИИ. Для отдельного копирования полного каталога полей сначала выберите собственный источник."
          />
        </span>
      </div>
      {catalog.error !== null && catalog.error !== dismissedCatalogError && (
        <AggregateError
          error={catalog.error}
          onDismiss={() => setDismissedCatalogError(catalog.error)}
        />
      )}
      {error && (
        <Alert
          type="warning"
          role="alert"
          message={error}
          closable
          onClose={() => setError("")}
        />
      )}
      {success && (
        <Alert
          type="success"
          role="status"
          message={success}
          closable
          onClose={() => setSuccess("")}
        />
      )}
      {manual && (
        <>
          <Button
            loading={copyKind === "manual"}
            disabled={busy || disabled}
            onClick={() => {
              if (copying.current === catalog || !enabled.current) return;
              copying.current = catalog;
              const owner = catalog;
              setBusy(true);
              setCopyKind("manual");
              void (
                navigator.clipboard?.writeText(manual) ??
                Promise.reject(new Error("clipboard unavailable"))
              )
                .then(() => {
                  if (current(owner)) setSuccess("Текст скопирован.");
                })
                .catch(() => {
                  if (current(owner))
                    setError("Используйте выделение текста и Ctrl+C.");
                })
                .finally(() => {
                  if (copying.current === owner) copying.current = null;
                  if (current(owner)) {
                    setBusy(false);
                    setCopyKind(null);
                  }
                });
            }}
          >
            Копировать подготовленный текст
          </Button>
          <Input.TextArea
            aria-label="Текст для ручного копирования"
            rows={8}
            readOnly
            value={manual}
            onFocus={(e) => e.target.select()}
          />
        </>
      )}
      <ConfirmationDialog
        open={pending !== null && !disabled}
        title="Заменить правило черновика?"
        message="Будет заменено только правило. Имя, источник и расписание сохранятся. Ошибочный JSON останется доступен для исправления; сохранение на сервер не выполняется."
        confirmText="Заменить правило"
        onCancel={() => setPending(null)}
        onConfirm={() => {
          if (pending !== null && enabled.current) {
            onImport?.(pending);
            setSuccess(
              "Правило импортировано в черновик. Выполните проверку и предварительный расчёт.",
            );
            setPending(null);
          }
        }}
      />
    </div>
  );
});
