import { useEffect, useMemo, useState } from "react";
import {
  Alert,
  Button,
  Input,
  Modal,
  Select,
  Pagination,
  Radio,
  Spin,
} from "antd";
import { observer } from "mobx-react-lite";
import { Link, useNavigate, useSearchParams } from "react-router-dom";
import { runInAction } from "mobx";
import { DraftStore } from "../../features/aggregates/DraftStore";
import { useAggregateScope } from "../../features/aggregates/useAggregateScope";
import { PromptStore } from "../../features/aggregates/PromptStore";
import { useDraftGuard } from "../../features/aggregates/useDraftGuard";
import { CollectionStore } from "../../features/aggregates/CollectionStore";
import { ResourcesApi } from "../../app/api/resources";
import { useStores } from "../../app/providers/StoresProvider";
import { ConfirmationDialog } from "../../shared/ui/ConfirmationDialog";
import { AggregateError } from "./AggregateError";
import { RuleEditor } from "./RuleEditor";
import { ResultView, Time } from "./ResultView";
import styles from "./Aggregates.module.css";

export const AggregateCreatePage = observer(() => {
  const [params] = useSearchParams();
  const from = params.get("copy") || "";
  const navigate = useNavigate();
  const { api } = useStores();
  const { store, service, session } = useAggregateScope(
    (service) => new DraftStore(service, from),
    from,
  );
  // The source list has the same identity lifetime as its draft.
  const sources = useMemo(() => {
    void store;
    const resources = new ResourcesApi(api);
    return new CollectionStore((offset, signal) =>
      resources.kafkaSources(offset, undefined, signal),
    );
  }, [api, store]);
  const [promptOpen, setPromptOpen] = useState(false);
  const [clipboardError, setClipboardError] = useState(false);
  const [copied, setCopied] = useState(false);
  const { leaving, cancelLeave, confirmLeave } = useDraftGuard(store);
  const { store: promptStore } = useAggregateScope(
    (api) => new PromptStore(api, store.source),
    store.source,
  );
  const [zone, setZone] = useState("UTC");
  const [catalogError, setCatalogError] = useState<unknown>(null);
  useEffect(() => {
    const c = new AbortController();
    void sources.load(0);
    void service
      .operators(c.signal)
      .then((operators) => {
        if (!c.signal.aborted)
          runInAction(() => {
            store.setOperators(operators);
          });
      })
      .catch((e) => {
        if (!c.signal.aborted) setCatalogError(e);
      });
    if (from) void store.loadCopy();
    return () => {
      c.abort();
      sources.dispose();
    };
  }, [store, sources, service, from]);
  const available = sources.items.filter((s) => !s.is_archived);
  const prompt = promptStore.text;
  return (
    <div className={styles.page}>
      <Link to="/aggregates">← К агрегатам</Link>
      <h2>{from ? "Копия — новый неизменяемый агрегат" : "Новый агрегат"}</h2>
      <Alert
        type="info"
        message="Сохранённые правило, источник и расписание неизменяемы. Граница в прошлом задаёт фазу UTC-окон; для создания требуется завершённое выровненное окно. Будущая граница не переносится назад автоматически."
      />
      {catalogError !== null && <AggregateError error={catalogError} />}
      {!store.operators.length && catalogError === null && (
        <Spin aria-label="Загрузка каталога операций" />
      )}
      {!store.editable && (
        <section
          aria-label="Загрузка копии"
          aria-busy={store.copyState === "loading"}
        >
          {store.copyState === "loading" ? (
            <Spin aria-label="Загрузка исходного агрегата" />
          ) : (
            <AggregateError
              error={store.copyError}
              onRetry={() => void store.loadCopy()}
            />
          )}
          <p>
            Редактор откроется после загрузки копии. Ввод не будет заменён
            запоздалым ответом.
          </p>
        </section>
      )}
      {store.editable && (
        <>
          <fieldset
            disabled={store.lifecycle.isPending("create")}
            className={styles.node}
          >
            <legend>Параметры агрегата</legend>
            <div className={styles.grid}>
              <label className={styles.field}>
                Имя
                <Input
                  aria-label="Имя агрегата"
                  maxLength={200}
                  value={store.name}
                  onChange={(e) => store.setField("name", e.target.value)}
                />
              </label>
              <label className={styles.field}>
                Собственный источник
                <Select
                  aria-label="Собственный источник"
                  value={store.source || undefined}
                  loading={sources.loading}
                  options={[
                    ...(store.source &&
                    !available.some((s) => s.id === store.source)
                      ? [
                          {
                            value: store.source,
                            label: `${store.source} — доступность не подтверждена`,
                          },
                        ]
                      : []),
                    ...available.map((s) => ({ value: s.id, label: s.name })),
                  ]}
                  onChange={(v) => store.setField("source", v)}
                />
              </label>
              <Pagination
                simple
                pageSize={50}
                current={sources.offset / 50 + 1}
                total={sources.total}
                showSizeChanger={false}
                onChange={(p) => void sources.load((p - 1) * 50)}
              />
              {sources.error !== null && (
                <AggregateError
                  error={sources.error}
                  onRetry={() => void sources.load()}
                />
              )}
              <label className={styles.field}>
                Длительность, секунды (1–604800)
                <Input
                  aria-label="Длительность, секунды"
                  inputMode="numeric"
                  value={store.duration}
                  onChange={(e) => store.setField("duration", e.target.value)}
                />
              </label>
              <label className={styles.field}>
                Исходная граница с явным смещением
                <Input
                  aria-label="Исходная граница"
                  placeholder="2026-11-01T00:00:00+05:00"
                  value={store.boundary}
                  onChange={(e) => store.setField("boundary", e.target.value)}
                />
              </label>
              <label className={styles.field}>
                Исходный IANA-пояс
                <Input
                  aria-label="Исходный IANA-пояс"
                  placeholder="Asia/Yekaterinburg"
                  value={store.timezone}
                  onChange={(e) => store.setField("timezone", e.target.value)}
                />
              </label>
            </div>
            <p>
              Сервер проверит согласованность смещения с поясом, включая
              неоднозначное и несуществующее местное время.
            </p>
            <div className={styles.toolbar}>
              <Radio.Group
                aria-label="Режим правила"
                value={store.mode}
                onChange={(e) => store.switchMode(e.target.value)}
                options={[
                  { value: "visual", label: "Визуальный" },
                  { value: "json", label: "JSON" },
                ]}
              />
              <Button
                disabled={!store.operators.length}
                onClick={() => {
                  setClipboardError(false);
                  setCopied(false);
                  setPromptOpen(true);
                  void promptStore.load(store.operators);
                }}
              >
                Промпт для ИИ
              </Button>
            </div>
            {store.mode === "json" ? (
              <label className={styles.field}>
                aggregate_rule
                <Input.TextArea
                  aria-label="aggregate_rule"
                  rows={18}
                  value={store.text}
                  onChange={(e) => store.setText(e.target.value)}
                />
              </label>
            ) : (
              <RuleEditor
                rule={store.rule}
                operators={store.operators}
                api={service}
                source={store.source}
                onChange={store.setRule}
                onInvalid={store.setInvalid}
              />
            )}
            <div className={styles.grid}>
              <label className={styles.field}>
                Начало preview-окна (ISO)
                <Input
                  aria-label="Начало preview-окна"
                  value={store.window}
                  onChange={(e) => store.setField("window", e.target.value)}
                />
              </label>
              <label className={styles.field}>
                Основание preview
                <Select
                  aria-label="Основание preview"
                  value={store.timeBasis}
                  onChange={(v) => store.setField("timeBasis", v)}
                  options={[
                    {
                      value: "availability_at",
                      label: "Доступность после commit (по умолчанию)",
                    },
                    {
                      value: "backend_processed_at_historical",
                      label: "Ретроспективное время обработки (непроверенное)",
                    },
                  ]}
                />
              </label>
            </div>
            {store.timeBasis !== "availability_at" && (
              <Alert
                type="warning"
                message="Ретроспективный preview не доказывает полноту и не разрешает сохранение исторической точки."
              />
            )}
          </fieldset>
          {store.localError && (
            <Alert type="error" role="alert" message={store.localError} />
          )}
          {store.errors.map((e) => (
            <Alert type="error" role="alert" key={e} message={e} />
          ))}
          {store.error !== null && <AggregateError error={store.error} />}
          {store.success && (
            <Alert type="success" role="status" message={store.success} />
          )}
          <div className={styles.toolbar}>
            <Button
              aria-label="Проверить правило"
              aria-busy={store.lifecycle.isPending("validate")}
              loading={store.lifecycle.isPending("validate")}
              disabled={
                store.busy ||
                !store.operators.length ||
                !!store.localError ||
                !!store.invalidNodes.size ||
                !!store.errors.length
              }
              onClick={() => void store.validate()}
            >
              Проверить правило
            </Button>
            <Button
              aria-label="Предварительный расчёт"
              aria-busy={store.lifecycle.isPending("preview")}
              loading={store.lifecycle.isPending("preview")}
              disabled={!store.validation || store.busy}
              onClick={() => void store.runPreview()}
            >
              Предварительный расчёт
            </Button>
            <Button
              type="primary"
              aria-label={
                store.unknownCreate
                  ? "Повторить создание с тем же ключом"
                  : "Создать агрегат"
              }
              loading={store.lifecycle.isPending("create")}
              disabled={!store.canCreate || !session.has("aggregates.write")}
              onClick={() =>
                void store.create().then((a) => {
                  if (a)
                    navigate(`/aggregates/${a.id}`, {
                      state: { aggregateCreated: a.id },
                    });
                })
              }
            >
              {store.unknownCreate
                ? "Повторить создание с тем же ключом"
                : "Создать агрегат"}
            </Button>
          </div>
          {store.unknownCreate && (
            <Alert
              type="warning"
              message="Результат создания неизвестен. Явный повтор неизменённого запроса использует тот же ключ и возвращает тот же UUID. Изменение тела создаст новый ключ."
            />
          )}
          <label className={styles.field}>
            Пояс отображения
            <Input
              aria-label="Пояс отображения"
              value={zone}
              onChange={(e) => setZone(e.target.value)}
            />
          </label>
          {store.validation && (
            <p>
              UTC-якорь:{" "}
              <Time value={store.validation.anchor_utc} zone={zone} />;
              длительность {store.validation.duration_seconds} с. Плановый
              расчёт совпадает с концом окна.
            </p>
          )}
          {store.preview && <ResultView result={store.preview} zone={zone} />}
        </>
      )}
      <Modal
        open={promptOpen}
        title="Промпт для внешнего ИИ"
        onCancel={() => setPromptOpen(false)}
        footer={
          <Button
            disabled={!prompt || promptStore.lifecycle.isPending("load")}
            onClick={() => {
              void (
                navigator.clipboard?.writeText(prompt) ??
                Promise.reject(new Error("clipboard unavailable"))
              )
                .then(() => setCopied(true))
                .catch(() => setClipboardError(true));
            }}
          >
            Копировать инструкцию
          </Button>
        }
      >
        <p>
          Приложение не обращается к ИИ и не передаёт данные. Прочитайте
          инструкцию и самостоятельно выберите сведения для передачи. Ответ
          вставьте в JSON-режим и проверьте.
        </p>
        {promptStore.lifecycle.isPending("load") && (
          <Spin aria-label="Загрузка полного каталога" />
        )}
        {promptStore.error !== null && (
          <AggregateError
            error={promptStore.error}
            onRetry={() => void promptStore.load(store.operators)}
          />
        )}
        <Input.TextArea
          aria-label="Инструкция для ИИ"
          readOnly
          rows={14}
          value={prompt}
          onFocus={(e) => e.target.select()}
        />
        {copied && <p role="status">Инструкция скопирована.</p>}
        {clipboardError && (
          <Alert
            type="warning"
            message="Буфер обмена недоступен. Выделите инструкцию и скопируйте вручную Ctrl+C."
          />
        )}
      </Modal>
      <ConfirmationDialog
        open={!!leaving}
        title="Покинуть черновик?"
        message="Несохранённое правило и параметры будут потеряны."
        confirmText="Покинуть"
        onCancel={cancelLeave}
        onConfirm={confirmLeave}
      />
    </div>
  );
});
