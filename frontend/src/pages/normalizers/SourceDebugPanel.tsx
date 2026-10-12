import { useEffect, useMemo, useRef, useState } from "react";
import { Alert, Button, Pagination, Select, Tag } from "antd";
import { ApiError } from "../../app/api/client";
import {
  NormalizersApi,
  type RuleV1,
  type SnapshotRecord,
  type SourceSnapshot,
} from "../../app/api/normalizers";
import type { KafkaSource } from "../../app/api/resources";
import { AppRequestError } from "../../app/ui/AppRequestError";
import { ConfirmationDialog } from "../../shared/ui/ConfirmationDialog";
import { previewReasons } from "./normalizerFeedback";
import { safeDebugReport, type DebugOutcome } from "./debugReport";
import styles from "./SourceDebugPanel.module.css";

const issueText: Record<NonNullable<SnapshotRecord["issue"]>, string> = {
  invalid_envelope: "неверный формат",
  message_too_large: "слишком большой размер",
  response_limit: "лимит ответа",
};

type Props = {
  service: NormalizersApi;
  currentRule: () => RuleV1 | null;
  onSessionChange: (value: boolean) => void;
};

export function SourceDebugPanel({
  service,
  currentRule,
  onSessionChange,
}: Props) {
  const [sources, setSources] = useState<KafkaSource[]>([]);
  const [sourceError, setSourceError] = useState<unknown>(null);
  const [sourceId, setSourceId] = useState("");
  const [snapshot, setSnapshot] = useState<SourceSnapshot | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<unknown>(null);
  const [selected, setSelected] = useState(0);
  const [outcomes, setOutcomes] = useState<DebugOutcome[]>([]);
  const [running, setRunning] = useState(false);
  const [paused, setPaused] = useState(false);
  const [current, setCurrent] = useState<number | null>(null);
  const [inspected, setInspected] = useState<number | null>(null);
  const [pageOk, setPageOk] = useState(1);
  const [pageFailed, setPageFailed] = useState(1);
  const [confirmAction, setConfirmAction] = useState<(() => void) | null>(null);
  const generation = useRef(0);
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const frozenRule = useRef<RuleV1 | null>(null);
  const outcomesRef = useRef<DebugOutcome[]>([]);
  const pausedRef = useRef(false);
  const nextRef = useRef(0);
  const activeRef = useRef(false);
  const requestSeq = useRef(0);
  const records = snapshot?.records ?? [];
  const usable = records.filter((item) => !item.issue);
  const hasSession = outcomes.length > 0 || running;
  useEffect(() => onSessionChange(hasSession), [hasSession, onSessionChange]);
  useEffect(() => {
    const controller = new AbortController();
    const load = async () => {
      try {
        let offset = 0;
        const items: KafkaSource[] = [];
        do {
          const page = await service.kafkaSources(offset, controller.signal);
          items.push(
            ...page.items.filter(
              (item) => item.source_type === "kafka" && !item.is_archived,
            ),
          );
          offset += page.items.length;
          if (!page.items.length || offset >= page.total) break;
        } while (!controller.signal.aborted);
        if (!controller.signal.aborted) setSources(items);
      } catch (reason) {
        if (!controller.signal.aborted) setSourceError(reason);
      }
    };
    void load();
    return () => controller.abort();
  }, [service]);
  useEffect(
    () => () => {
      generation.current++;
      if (timer.current) clearTimeout(timer.current);
    },
    [],
  );
  function stop() {
    generation.current++;
    requestSeq.current++;
    if (timer.current) clearTimeout(timer.current);
    timer.current = null;
    activeRef.current = false;
    setRunning(false);
    setPaused(false);
    pausedRef.current = false;
  }
  function reset() {
    stop();
    setError(null);
    outcomesRef.current = [];
    setOutcomes([]);
    setCurrent(null);
    setInspected(null);
    setPageOk(1);
    setPageFailed(1);
  }
  function withLossConfirmation(action: () => void) {
    if (hasSession) setConfirmAction(() => action);
    else action();
  }
  async function load(id: string) {
    reset();
    setSourceId(id);
    setSnapshot(null);
    setSelected(0);
    setError(null);
    if (!id) return;
    const token = ++generation.current;
    setLoading(true);
    try {
      const next = await service.sourceSnapshot(id);
      if (token === generation.current) setSnapshot(next);
    } catch (reason) {
      if (token === generation.current) setError(reason);
    } finally {
      if (token === generation.current) setLoading(false);
    }
  }
  function append(outcome: DebugOutcome) {
    outcomesRef.current = [...outcomesRef.current, outcome];
    setOutcomes(outcomesRef.current);
  }
  async function test(position: number, rule: RuleV1, token: number) {
    const item = records.find((record) => record.position === position);
    if (!item || item.issue || item.timestamp === null || item.log === null)
      return;
    setCurrent(position);
    const request = ++requestSeq.current;
    try {
      const result = await service.preview(rule, {
        timestamp: item.timestamp,
        log: item.log,
      });
      if (token === generation.current && request === requestSeq.current)
        append({ position, result });
    } catch (reason) {
      if (token !== generation.current || request !== requestSeq.current)
        return;
      const session =
        reason instanceof ApiError && [401, 403].includes(reason.status);
      const ruleError = reason instanceof ApiError && reason.status === 422;
      append({
        position,
        error: session ? "session" : ruleError ? "rule" : "transport",
      });
      setError(reason);
      stop();
    }
  }
  async function fixedTest() {
    const rule = currentRule();
    if (!rule) return;
    reset();
    frozenRule.current = structuredClone(rule);
    const token = generation.current;
    setRunning(true);
    activeRef.current = true;
    setInspected(selected);
    await test(selected, frozenRule.current, token);
    if (token === generation.current) {
      activeRef.current = false;
      setRunning(false);
    }
  }
  async function step(token: number) {
    if (token !== generation.current || !activeRef.current || pausedRef.current)
      return;
    const item = usable[nextRef.current++];
    if (!item || !frozenRule.current) {
      activeRef.current = false;
      setRunning(false);
      return;
    }
    const started = Date.now();
    await test(item.position, frozenRule.current, token);
    if (token !== generation.current || !activeRef.current || pausedRef.current)
      return;
    if (nextRef.current >= usable.length) {
      activeRef.current = false;
      setRunning(false);
      return;
    }
    timer.current = setTimeout(
      () => void step(token),
      Math.max(0, 5000 - (Date.now() - started)),
    );
  }
  function autoStart() {
    const rule = currentRule();
    if (!rule) return;
    reset();
    frozenRule.current = structuredClone(rule);
    nextRef.current = Math.max(
      0,
      usable.findIndex((item) => item.position === selected),
    );
    activeRef.current = true;
    setRunning(true);
    void step(generation.current);
  }
  function pauseResume() {
    if (pausedRef.current) {
      pausedRef.current = false;
      setPaused(false);
      timer.current = setTimeout(() => void step(generation.current), 5000);
    } else {
      pausedRef.current = true;
      setPaused(true);
      if (timer.current) clearTimeout(timer.current);
    }
  }
  const successes = useMemo(
    () =>
      outcomes.filter(
        (item) => item.result?.status !== "failed" && item.result,
      ),
    [outcomes],
  );
  const failures = useMemo(
    () => outcomes.filter((item) => item.result?.status === "failed"),
    [outcomes],
  );
  const infrastructure = useMemo(
    () => outcomes.filter((item) => item.error && item.error !== "rule"),
    [outcomes],
  );
  const ruleErrors = useMemo(
    () => outcomes.filter((item) => item.error === "rule"),
    [outcomes],
  );
  const currentRecord = records.find((item) => item.position === current);
  const selectedRecord = records.find((item) => item.position === selected);
  const displayedRecord = running ? currentRecord : selectedRecord;
  const inspectedOutcome = outcomes.find((item) => item.position === inspected);
  function download() {
    if (!outcomes.length || !frozenRule.current) return;
    const blob = new Blob(
      [JSON.stringify(safeDebugReport(outcomes, frozenRule.current), null, 2)],
      { type: "application/json" },
    );
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = "normalizer-debug-report.json";
    link.click();
    URL.revokeObjectURL(url);
  }
  const resultRow = (item: DebugOutcome) => (
    <div key={item.position} className={styles.resultRow}>
      <Button type="link" onClick={() => setInspected(item.position)}>
        Запись {item.position + 1}
      </Button>
      {item.result?.status ?? "Ошибка доступа или сети"}
      {item.result && (
        <ul>
          {previewReasons(item.result).map((reason, index) => (
            <li key={index}>{reason}</li>
          ))}
        </ul>
      )}
    </div>
  );
  const paged = (
    items: DebugOutcome[],
    page: number,
    setPage: (page: number) => void,
  ) => (
    <>
      <div className={styles.list}>
        {items.slice((page - 1) * 5, page * 5).map(resultRow)}
      </div>
      {items.length > 5 && (
        <Pagination
          simple
          current={page}
          total={items.length}
          pageSize={5}
          onChange={setPage}
        />
      )}
    </>
  );
  return (
    <div className={styles.root}>
      <p>
        Снимок содержит до 50 последних доступных записей. Порядок между
        партициями условный. Новые записи появляются только после обновления.
      </p>
      <Select
        aria-label="Kafka-источник"
        className={styles.select}
        value={sourceId || undefined}
        placeholder="Выберите Kafka-источник"
        options={sources.map((item) => ({ value: item.id, label: item.name }))}
        onChange={(id) => withLossConfirmation(() => void load(id))}
      />
      <Button
        disabled={!sourceId || loading}
        loading={loading}
        onClick={() => withLossConfirmation(() => void load(sourceId))}
      >
        Обновить снимок
      </Button>
      {!!sourceError && <AppRequestError error={sourceError} />}
      {!!error && <AppRequestError error={error} />}
      {snapshot && (
        <>
          <div role="status">
            Доступно записей: {usable.length}. Пропущено: {snapshot.skipped}.
          </div>
          {records.length === 0 ? (
            <Alert type="info" message="В снимке нет записей." />
          ) : (
            <>
              <Select
                aria-label="Запись снимка"
                className={styles.select}
                value={selected}
                options={records.map((item) => ({
                  value: item.position,
                  label: `Запись ${item.position + 1}: ${item.issue ? issueText[item.issue] : (item.log ?? "").replace(/\s+/g, " ").slice(0, 48)}${(item.log?.length ?? 0) > 48 ? "…" : ""}`,
                  disabled: !!item.issue,
                }))}
                onChange={(position) => {
                  setSelected(position);
                  if (!running) setCurrent(position);
                }}
              />
              <div className={styles.actions}>
                <Button
                  disabled={
                    running ||
                    !usable.some((item) => item.position === selected)
                  }
                  onClick={() => withLossConfirmation(() => void fixedTest())}
                >
                  Тест
                </Button>
                <Button
                  disabled={
                    running ||
                    !usable.some((item) => item.position === selected)
                  }
                  onClick={() => withLossConfirmation(autoStart)}
                >
                  Авто
                </Button>
                {running && (
                  <>
                    <Button onClick={pauseResume}>
                      {paused ? "Продолжить" : "Пауза"}
                    </Button>
                    <Button onClick={stop}>Остановить</Button>
                  </>
                )}
              </div>
              <h3>Текущая запись</h3>
              <pre className={styles.raw}>
                {displayedRecord?.log ?? "Проверка ещё не запускалась."}
              </pre>
            </>
          )}
        </>
      )}
      {snapshot && (
        <>
          <div className={styles.actions}>
            <span role="status">Проверено: {outcomes.length}</span>
            <Button disabled={!outcomes.length} onClick={download}>
              Скачать безопасный отчёт
            </Button>
          </div>
          <h3>Результаты ECS: {successes.length}</h3>
          {paged(successes, pageOk, setPageOk)}
          <h3>Ошибки нормализации: {failures.length}</h3>
          {paged(failures, pageFailed, setPageFailed)}
          {ruleErrors.length > 0 && (
            <Alert
              type="error"
              showIcon
              message="Черновик правила отклонён сервером"
              description="Проверьте поля правила и запустите новый прогон после исправления."
            />
          )}
          {infrastructure.length > 0 && (
            <Alert
              type="error"
              showIcon
              message={`Ошибки сети или доступа: ${infrastructure.length}`}
              description="Проверьте соединение и сессию перед повторным запуском."
            />
          )}
          {inspectedOutcome?.result && (
            <div className={styles.detail}>
              <Tag>{inspectedOutcome.result.status}</Tag>
              <h3>ECS записи {inspected! + 1}</h3>
              <pre className={styles.raw}>
                {JSON.stringify(inspectedOutcome.result.ecs_data, null, 2)}
              </pre>
              <h3>Диагностика</h3>
              {inspectedOutcome.result.diagnostics.map((item, index) => (
                <div key={index}>{item.code}</div>
              ))}
              <h3>Трассировка</h3>
              {inspectedOutcome.result.trace.map((step, index) => (
                <div key={index}>
                  {step.variant} / {step.block}: {step.result}
                </div>
              ))}
            </div>
          )}
        </>
      )}
      <ConfirmationDialog
        open={!!confirmAction}
        title="Сбросить результаты отладки?"
        message="Текущий прогон и накопленные результаты будут потеряны."
        confirmText="Продолжить"
        onCancel={() => setConfirmAction(null)}
        onConfirm={() => {
          const action = confirmAction;
          setConfirmAction(null);
          action?.();
        }}
      />
    </div>
  );
}
