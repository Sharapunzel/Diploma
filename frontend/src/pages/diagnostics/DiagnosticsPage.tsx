import { useEffect, useMemo, useRef, useState } from "react";
import {
  Alert,
  Button,
  Card,
  DatePicker,
  Descriptions,
  Input,
  List,
  Modal,
  Pagination,
  Select,
  Space,
  Spin,
  Tag,
  Typography,
} from "antd";
import { observer } from "mobx-react-lite";
import { Link, useSearchParams } from "react-router-dom";
import {
  DiagnosticsApi,
  type KafkaEvent,
  type ProcessingError,
} from "../../app/api/diagnostics";
import { useStores } from "../../app/providers/StoresProvider";
import { ApiError } from "../../app/api/client";
import { AppRequestError } from "../../app/ui/AppRequestError";
import { DiagnosticsStore } from "../../features/diagnostics/DiagnosticsStore";
import styles from "./DiagnosticsPage.module.css";

const workerText: Record<string, string> = {
  running: "Работает",
  retrying: "Повторяет попытку",
  stopped: "Остановлен",
  not_running: "Не запущен",
  unknown: "Неизвестно",
};
const assignmentText: Record<string, string> = {
  pending: "создаётся",
  ready: "оформлено",
  error: "ошибка",
  removal_pending: "снимается",
  revocation_pending: "отзывается",
};
const agentRecordText: Record<string, string> = {
  provisioning: "оформляется",
  ready: "доступ оформлен",
  error: "ошибка оформления",
  revocation_pending: "доступ отзывается",
  revoked: "доступ отозван",
};
const stageText: Record<string, string> = {
  decode: "Декодирование",
  envelope: "Конверт",
  normalization: "Нормализация",
};
const kindText: Record<string, string> = {
  retention_gap: "Потеря диапазона Kafka",
  topic_recreated: "Топик пересоздан",
};
const knownReasons: Record<string, string> = {
  invalid_utf8: "Невозможно прочитать кодировку сообщения.",
  invalid_json: "Сообщение не является корректным JSON.",
  null_payload: "Сообщение не содержит данных.",
  invalid_envelope: "Некорректный конверт события.",
  log_too_long: "Событие превышает допустимую длину.",
  no_variant_matched: "Ни один вариант правила не подошёл.",
  conversion_failed: "Не удалось преобразовать поле.",
  rule_invalid: "Правило нормализации недействительно.",
  empty_ecs: "Правило не сформировало ECS-событие.",
  normalization_failed: "Нормализация не завершилась.",
  partition_stopped: "Остановлена как минимум одна партиция обработчика.",
  processing_not_durable: "Результат обработки не удалось надёжно сохранить.",
  storage_unavailable:
    "Хранилище обработки или диагностики временно недоступно.",
  kafka_commit_unavailable: "Не удалось подтвердить позицию чтения в Kafka.",
  retention_gap_detected: "Часть позиций Kafka стала недоступна.",
  topic_recreated: "Топик получил новое поколение.",
  topic_not_found: "Топик не найден в Kafka.",
  supervisor_unavailable:
    "Супервизор Kafka недоступен в этом процессе приложения.",
  worker_not_started: "Для включённого источника обработчик ещё не запущен.",
  worker_starting: "Обработчик источника запускается.",
  worker_state_unavailable: "Состояние обработчика временно недоступно.",
  worker_stopped: "Обработчик источника остановлен.",
  all_partitions_stopped: "Остановлены все партиции этого источника.",
  cluster_identity_conflict:
    "Идентификатор кластера Kafka не совпадает с сохранённой конфигурацией.",
  cluster_identity_unavailable: "Kafka не предоставила идентификатор кластера.",
  gap_recovery_unavailable:
    "Не удалось безопасно восстановить пропуск позиций Kafka.",
  invalid_position: "Kafka вернула некорректную позицию чтения.",
  kafka_consumer_unavailable: "Не удалось создать потребителя Kafka.",
  kafka_metadata_unavailable: "Не удалось получить метаданные Kafka.",
  kafka_pause_unavailable:
    "Не удалось приостановить затронутую партицию Kafka.",
  kafka_poll_unavailable: "Не удалось получить следующее сообщение из Kafka.",
  kafka_position_unavailable: "Не удалось определить доступную позицию Kafka.",
  kafka_revoke_unavailable: "Не удалось завершить назначение партиций Kafka.",
  offset_missing_within_watermarks:
    "Ожидаемая позиция отсутствует в доступном диапазоне Kafka.",
  position_outside_available_range:
    "Сохранённая позиция находится за пределами доступного диапазона Kafka.",
  position_unknown_after_history:
    "Невозможно безопасно восстановить позицию чтения по сохранённой истории.",
  processing_configuration_invalid:
    "Конфигурация обработки источника некорректна.",
  retention_gap_storage_unavailable:
    "Не удалось сохранить сведения о пропуске Kafka.",
  retention_history_unavailable:
    "Не удалось проверить историю пропусков Kafka.",
  retention_watermark_regressed:
    "Границы доступности Kafka противоречат сохранённой истории.",
  source_configuration_changed:
    "Конфигурация источника изменилась во время обработки.",
  source_disabled: "Источник выключен.",
  topic_identity_history_mismatch:
    "Идентификатор топика не совпадает с сохранённой историей.",
  topic_identity_unavailable: "Kafka не предоставила идентификатор топика.",
  topic_identity_unavailable_after_history:
    "Идентификатор топика недоступен, хотя история уже существует.",
  stopped_by_lifecycle:
    "Обработчик остановился при выключении или перенастройке источника.",
  consumer_runtime_failure:
    "Обработчик остановился из-за внутренней ошибки выполнения.",
};
const kafkaReasonParts: Record<string, string> = {
  metadata: "получить метаданные Kafka",
  consumer: "создать потребителя Kafka",
  position: "прочитать позиции Kafka",
  poll: "получить сообщение Kafka",
  commit: "подтвердить позицию Kafka",
  pause: "приостановить партицию Kafka",
  revoke: "освободить назначение Kafka",
  unavailable: "Kafka недоступна",
  timeout: "время ожидания запроса Kafka истекло",
  tls_failed: "не пройдена проверка TLS-сертификата или имени узла Kafka",
  authentication_failed: "Kafka отклонила учётные данные службы",
  acl_denied: "ACL Kafka запретил операцию",
  commit_failed: "Kafka не подтвердила запрошенную позицию",
  security_configuration_invalid:
    "адрес не разрешён профилем безопасности Kafka",
};
export const explainDiagnosticReason = (code?: string | null) => {
  if (!code) return "Причина неизвестна.";
  if (knownReasons[code]) return knownReasons[code];
  const kafka = /^kafka_([a-z]+)_([a-z_]+)$/.exec(code);
  if (kafka && kafkaReasonParts[kafka[1]] && kafkaReasonParts[kafka[2]]) {
    return `Не удалось ${kafkaReasonParts[kafka[1]]}: ${kafkaReasonParts[kafka[2]]}.`;
  }
  return "Неизвестная причина; подробное объяснение недоступно.";
};
const explain = explainDiagnosticReason;
const when = (value: string) => new Date(value).toLocaleString("ru-RU");
function ReadLink({
  permission,
  to,
  children,
}: {
  permission: boolean;
  to: string;
  children: React.ReactNode;
}) {
  return permission ? (
    <Link to={to}>{children}</Link>
  ) : (
    <Typography.Text type="secondary">
      Раздел недоступен по правам
    </Typography.Text>
  );
}

export const DiagnosticsPage = observer(() => {
  const { api, session } = useStores();
  const diagnosticsApi = useMemo(() => new DiagnosticsApi(api), [api]);
  const store = useMemo(
    () => new DiagnosticsStore(diagnosticsApi),
    [diagnosticsApi],
  );
  const [params, setParams] = useSearchParams();
  const selectedId = params.get("source") ?? "";
  const [search, setSearch] = useState("");
  const [sourceOffset, setSourceOffset] = useState(0);
  const [node, setNode] = useState("");
  const [errorStage, setErrorStage] = useState<string>();
  const [eventKind, setEventKind] = useState<string>();
  const [start, setStart] = useState("");
  const [end, setEnd] = useState("");
  const [errorOffset, setErrorOffset] = useState(0);
  const [eventOffset, setEventOffset] = useState(0);
  const [record, setRecord] = useState<ProcessingError | KafkaEvent | null>(
    null,
  );
  const [raw, setRaw] = useState("");
  const [rawError, setRawError] = useState<unknown>(null);
  const [rawLoading, setRawLoading] = useState(false);
  const [rawTruncated, setRawTruncated] = useState(false);
  const [normalizerCheck, setNormalizerCheck] = useState<{
    id: string;
    status: "checking" | "available" | "missing" | "unknown";
  } | null>(null);
  const [normalizerRetry, setNormalizerRetry] = useState(0);
  const [freshnessNow, setFreshnessNow] = useState(() => Date.now());
  const rawRequest = useRef(0);
  useEffect(() => () => store.dispose(), [store]);
  useEffect(() => {
    void store.loadSources(sourceOffset, search);
  }, [store, sourceOffset, search]);
  useEffect(() => {
    store.select(selectedId);
    setNode("");
    setRecord(null);
    setRaw("");
    setRawLoading(false);
    setRawError(null);
    setRawTruncated(false);
    rawRequest.current++;
  }, [store, selectedId]);
  useEffect(() => {
    void store.loadErrors(
      selectedId || undefined,
      errorStage,
      start,
      end,
      errorOffset,
    );
  }, [store, selectedId, errorStage, start, end, errorOffset]);
  useEffect(() => {
    void store.loadEvents(
      selectedId || undefined,
      eventKind,
      start,
      end,
      eventOffset,
    );
  }, [store, selectedId, eventKind, start, end, eventOffset]);
  useEffect(() => {
    if (!store.source) return;
    const staleAt = new Date(store.source.captured_at).getTime() + 120_001;
    const timer = window.setTimeout(
      () => setFreshnessNow(Date.now()),
      Math.max(0, staleAt - Date.now()),
    );
    return () => window.clearTimeout(timer);
  }, [store.source]);
  useEffect(() => {
    if (
      !record ||
      !("stage" in record) ||
      record.stage !== "normalization" ||
      !record.normalizer_id ||
      !session.has("normalizers.read")
    )
      return;
    const controller = new AbortController();
    const normalizerId = record.normalizer_id;
    setNormalizerCheck({ id: normalizerId, status: "checking" });
    void api
      .request(`/normalizers/${normalizerId}`, {
        signal: controller.signal,
      })
      .then(() => setNormalizerCheck({ id: normalizerId, status: "available" }))
      .catch((error: unknown) => {
        if (!controller.signal.aborted)
          setNormalizerCheck({
            id: normalizerId,
            status:
              error instanceof ApiError && error.status === 404
                ? "missing"
                : "unknown",
          });
      });
    return () => controller.abort();
  }, [api, record, session, normalizerRetry]);
  const choose = (id: string) => {
    setParams(id ? { source: id } : {});
    setErrorOffset(0);
    setEventOffset(0);
  };
  const source = store.source;
  const topology = store.topology;
  const agentAssigned = Boolean(topology?.agent_id);
  const normalizerAssigned = Boolean(topology?.normalizer_id);
  const agentMissing = topology !== null && !agentAssigned;
  const normalizerMissing = topology !== null && !normalizerAssigned;
  const normalizerExists =
    record && "normalizer_id" in record && record.normalizer_id
      ? normalizerCheck?.id === record.normalizer_id
        ? normalizerCheck.status
        : null
      : null;
  const staleSnapshot = source
    ? freshnessNow - new Date(source.captured_at).getTime() > 120_000
    : false;
  const displayedWorker = source
    ? source.is_archived || !source.is_enabled
      ? "not_running"
      : staleSnapshot
        ? "unknown"
        : source.worker_status
    : "unknown";
  const has = (permission: string) => session.has(permission);
  const nodes = source
    ? [
        {
          key: "agent",
          title: "1. Агент и сбор",
          name: topology
            ? topology.agent_id
              ? (topology.agent_name ?? "Назначенная запись недоступна")
              : "Не назначен"
            : "Связь загружается или недоступна",
          detail: `Собирает события и отправляет их в Kafka. Назначение: ${topology?.assignment_state ? (assignmentText[topology.assignment_state] ?? "неизвестно") : "неизвестно"}. Запись агента: ${topology?.agent_state ? (agentRecordText[topology.agent_state] ?? "неизвестно") : "неизвестно"}. Фактическое состояние агента неизвестно: телеметрии нет.`,
        },
        {
          key: "kafka",
          title: "2. Kafka",
          name: source.topic,
          detail: `Топик для передачи событий. Подключение: ${source.connection_name ?? "неизвестно"}.`,
        },
        {
          key: "source",
          title: "3. Источник и обработчик",
          name: source.source_name,
          detail: `Читает сообщения из Kafka. ${workerText[displayedWorker]}. ${source.reason_code ? `${explain(source.reason_code)} Код: ${source.reason_code}.` : ""}`,
        },
        {
          key: "normalizer",
          title: "4. Нормализатор",
          name: topology
            ? topology.normalizer_id
              ? (topology.normalizer_name ?? "Назначенная запись недоступна")
              : "Не назначен"
            : "Связь загружается или недоступна",
          detail: "Приводит события к полям ECS.",
        },
        {
          key: "storage",
          title: "5. PostgreSQL",
          name: "Хранение",
          detail:
            "Сохраняет результат обработки. Отдельной проверки доступности здесь нет.",
        },
      ]
    : [];
  async function showRaw(id: string) {
    const request = ++rawRequest.current;
    setRawLoading(true);
    setRawError(null);
    setRaw("");
    try {
      const result = await diagnosticsApi.raw(id);
      if (rawRequest.current !== request) return;
      const bytes = Uint8Array.from(atob(result.data), (char) =>
        char.charCodeAt(0),
      );
      setRaw(new TextDecoder("utf-8", { fatal: false }).decode(bytes));
      setRawTruncated(result.truncated);
    } catch (error) {
      if (rawRequest.current === request) setRawError(error);
    } finally {
      if (rawRequest.current === request) setRawLoading(false);
    }
  }
  return (
    <main className={styles.page}>
      <header>
        <Typography.Title level={2}>Диагностика конвейера</Typography.Title>
        <Typography.Paragraph>
          Здесь показан собственный путь событий: сбор агентом, передача через
          Kafka, обработка источником, нормализация и сохранение. Текущий снимок
          процесса и сохранённая история ниже относятся к разным моментам
          времени. Внешние ECS-источники проходят другим путём.
        </Typography.Paragraph>
      </header>
      <Card
        title="Выбрать Kafka-источник"
        loading={store.loading.sources && !store.sources}
      >
        <div className={styles.selector}>
          <Input.Search
            aria-label="Поиск источника"
            placeholder="Поиск по названию"
            value={search}
            onChange={(event) => {
              setSearch(event.target.value);
              setSourceOffset(0);
            }}
            style={{ maxWidth: 300 }}
            allowClear
          />
          <Select
            aria-label="Источник"
            value={selectedId || undefined}
            placeholder="Выберите источник"
            style={{ minWidth: 240, maxWidth: "100%" }}
            options={[
              ...(source &&
              !store.sources?.items.some(
                (item) => item.source_id === source.source_id,
              )
                ? [{ value: source.source_id, label: source.source_name }]
                : []),
              ...(store.sources?.items.map((item) => ({
                value: item.source_id,
                label: `${item.source_name}${item.is_archived ? " (архив)" : !item.is_enabled ? " (выключен)" : ""}`,
              })) ?? []),
            ]}
            onChange={choose}
            showSearch={false}
            allowClear
          />
          <Button
            onClick={() => {
              void store.loadSources(sourceOffset, search);
              if (selectedId) store.refreshSource();
            }}
          >
            Обновить
          </Button>
        </div>
        {!!store.failures.sources && (
          <AppRequestError
            error={store.failures.sources}
            onRetry={() => void store.loadSources(sourceOffset, search)}
          />
        )}
        {store.sources && (
          <Pagination
            current={sourceOffset / 20 + 1}
            pageSize={20}
            total={store.sources.total}
            onChange={(page) => setSourceOffset((page - 1) * 20)}
            showSizeChanger={false}
          />
        )}
      </Card>
      {selectedId && (
        <Card
          title="Текущий путь выбранного источника"
          extra={
            <Button onClick={() => store.refreshSource()}>
              Обновить снимок
            </Button>
          }
        >
          {store.loading.source && !source && (
            <Spin aria-label="Загрузка состояния источника" />
          )}
          {!!store.failures.source && (
            <AppRequestError
              error={store.failures.source}
              onRetry={() => store.refreshSource()}
            />
          )}
          {!!store.failures.topology && (
            <AppRequestError
              error={store.failures.topology}
              onRetry={() => store.refreshSource()}
            />
          )}
          {source && (
            <>
              <Alert
                type={
                  source.is_archived ||
                  !source.is_enabled ||
                  displayedWorker !== "running"
                    ? "warning"
                    : "info"
                }
                showIcon
                message={`${source.is_archived ? "Архивный источник. " : !source.is_enabled ? "Источник выключен. " : ""}Обработчик: ${workerText[displayedWorker]}`}
                description={
                  <span>
                    Снимок процесса приложения: {when(source.captured_at)}.{" "}
                    {staleSnapshot && "Снимок устарел; обновите его вручную. "}{" "}
                    {source.reason_code &&
                      `${explain(source.reason_code)} Код: ${source.reason_code}.`}{" "}
                    Отсутствие ошибок не подтверждает доставку событий.
                  </span>
                }
              />
              <ol
                className={styles.flow}
                aria-label={`Путь событий${agentMissing ? ". Связь с агентом отсутствует" : topology ? "" : ". Топология неизвестна"}${normalizerMissing ? ". Связь с нормализатором и хранением отсутствует" : topology ? "" : ". Топология неизвестна"}`}
              >
                {nodes.map((item, index) => {
                  const broken =
                    (index === 0 && agentMissing) ||
                    ((index === 2 || index === 3) && normalizerMissing);
                  return (
                    <li
                      key={item.key}
                      data-edge={
                        index < 4
                          ? broken
                            ? "broken"
                            : topology
                              ? "connected"
                              : "unknown"
                          : undefined
                      }
                    >
                      <Button
                        className={styles.node}
                        type={node === item.key ? "primary" : "default"}
                        aria-pressed={node === item.key}
                        onClick={() => setNode(item.key)}
                      >
                        <span className={styles.nodeTitle}>{item.title}</span>
                        <span>{item.name}</span>
                        <span className={styles.nodeDetail}>{item.detail}</span>
                      </Button>
                      {index < 4 && (
                        <span className={styles.edge}>
                          {broken ? "Разрыв связи" : "→"}
                        </span>
                      )}
                    </li>
                  );
                })}
              </ol>
              {node && (
                <Card
                  size="small"
                  title={`Подробности: ${nodes.find((item) => item.key === node)?.title}`}
                >
                  <p>{nodes.find((item) => item.key === node)?.detail}</p>
                  {node === "agent" &&
                    (topology?.agent_id ? (
                      <ReadLink
                        permission={has("agents.read")}
                        to={`/agents/${topology.agent_id}`}
                      >
                        Перейти к агенту
                      </ReadLink>
                    ) : (
                      <p>Связь с агентом не назначена или неизвестна.</p>
                    ))}
                  {node === "kafka" && source.connection_id && (
                    <ReadLink
                      permission={has("connections.read")}
                      to={`/connections?id=${source.connection_id}`}
                    >
                      Перейти к Kafka-подключению
                    </ReadLink>
                  )}
                  {node === "source" && (
                    <ReadLink
                      permission={has("sources.read")}
                      to={`/sources?kind=kafka&id=${source.source_id}`}
                    >
                      Перейти к источнику
                    </ReadLink>
                  )}
                  {node === "normalizer" &&
                    (topology?.normalizer_id && topology.normalizer_name ? (
                      <ReadLink
                        permission={has("normalizers.read")}
                        to={`/normalizers/${topology.normalizer_id}`}
                      >
                        Перейти к нормализатору
                      </ReadLink>
                    ) : (
                      <p>Нормализатор не назначен или удалён.</p>
                    ))}
                </Card>
              )}
              <Typography.Title level={4}>
                Партиции обработчика
              </Typography.Title>
              {source.partitions.length ? (
                <List
                  dataSource={source.partitions}
                  renderItem={(part) => (
                    <List.Item>
                      Партиция {part.partition}:{" "}
                      {workerText[part.status] ?? "Неизвестно"}.{" "}
                      {part.reason_code && (
                        <span>
                          {explain(part.reason_code)} Код: {part.reason_code}
                        </span>
                      )}
                    </List.Item>
                  )}
                />
              ) : (
                <Typography.Text type="secondary">
                  Данных о партициях нет.
                </Typography.Text>
              )}
            </>
          )}
        </Card>
      )}
      <Card title="Период обеих историй">
        <div className={styles.filters}>
          <DatePicker
            aria-label="Начало периода истории"
            showTime
            onChange={(value) => {
              setStart(value?.toISOString() ?? "");
              setErrorOffset(0);
              setEventOffset(0);
            }}
          />
          <DatePicker
            aria-label="Конец периода истории"
            showTime
            onChange={(value) => {
              setEnd(value?.toISOString() ?? "");
              setErrorOffset(0);
              setEventOffset(0);
            }}
          />
        </div>
      </Card>
      <div className={styles.histories}>
        <Card
          title="История ошибок обработки"
          extra={
            <Button
              onClick={() =>
                void store.loadErrors(
                  selectedId || undefined,
                  errorStage,
                  start,
                  end,
                  errorOffset,
                )
              }
            >
              Обновить
            </Button>
          }
        >
          <div className={styles.filters}>
            <Select
              aria-label="Этап ошибки"
              placeholder="Все этапы"
              allowClear
              value={errorStage}
              onChange={(value) => {
                setErrorStage(value);
                setErrorOffset(0);
              }}
              options={Object.entries(stageText).map(([value, label]) => ({
                value,
                label,
              }))}
              style={{ minWidth: 160 }}
            />
          </div>
          {!!store.failures.errors && (
            <AppRequestError
              error={store.failures.errors}
              onRetry={() =>
                void store.loadErrors(
                  selectedId || undefined,
                  errorStage,
                  start,
                  end,
                  errorOffset,
                )
              }
            />
          )}
          {store.loading.errors && <Spin aria-label="Загрузка ошибок" />}
          {store.errors && (
            <>
              <List
                dataSource={store.errors.items}
                locale={{ emptyText: "Ошибок по фильтрам нет" }}
                renderItem={(item) => (
                  <List.Item>
                    <Button
                      type="link"
                      className={styles.record}
                      onClick={() => {
                        setRecord(item);
                        setRaw("");
                        setRawLoading(false);
                        setRawError(null);
                        setRawTruncated(false);
                        rawRequest.current++;
                      }}
                    >
                      {when(item.backend_processed_at)} ·{" "}
                      {stageText[item.stage]} · {item.source_name} ·{" "}
                      {item.kafka_topic}/{item.kafka_partition}/
                      {item.kafka_offset}
                    </Button>
                  </List.Item>
                )}
              />
              <Pagination
                current={errorOffset / 20 + 1}
                pageSize={20}
                total={store.errors.total}
                onChange={(page) => setErrorOffset((page - 1) * 20)}
                showSizeChanger={false}
              />
            </>
          )}
        </Card>
        <Card
          title="История событий Kafka"
          extra={
            <Button
              onClick={() =>
                void store.loadEvents(
                  selectedId || undefined,
                  eventKind,
                  start,
                  end,
                  eventOffset,
                )
              }
            >
              Обновить
            </Button>
          }
        >
          <div className={styles.filters}>
            <Select
              aria-label="Тип события Kafka"
              placeholder="Все типы"
              allowClear
              value={eventKind}
              onChange={(value) => {
                setEventKind(value);
                setEventOffset(0);
              }}
              options={Object.entries(kindText).map(([value, label]) => ({
                value,
                label,
              }))}
              style={{ minWidth: 200 }}
            />
          </div>
          {!!store.failures.events && (
            <AppRequestError
              error={store.failures.events}
              onRetry={() =>
                void store.loadEvents(
                  selectedId || undefined,
                  eventKind,
                  start,
                  end,
                  eventOffset,
                )
              }
            />
          )}
          {store.loading.events && <Spin aria-label="Загрузка событий Kafka" />}
          {store.events && (
            <>
              <List
                dataSource={store.events.items}
                locale={{ emptyText: "Событий по фильтрам нет" }}
                renderItem={(item) => (
                  <List.Item>
                    <Button
                      type="link"
                      className={styles.record}
                      onClick={() => {
                        rawRequest.current++;
                        setRaw("");
                        setRawLoading(false);
                        setRawError(null);
                        setRawTruncated(false);
                        setRecord(item);
                      }}
                    >
                      {when(item.detected_at)} · {kindText[item.kind]} ·{" "}
                      {item.source_name} · {item.topic}
                    </Button>
                  </List.Item>
                )}
              />
              <Pagination
                current={eventOffset / 20 + 1}
                pageSize={20}
                total={store.events.total}
                onChange={(page) => setEventOffset((page - 1) * 20)}
                showSizeChanger={false}
              />
            </>
          )}
        </Card>
      </div>
      <Modal
        title="Сохранённая диагностическая запись"
        open={!!record}
        onCancel={() => {
          setRecord(null);
          setRaw("");
          setRawLoading(false);
          setRawError(null);
          setRawTruncated(false);
          rawRequest.current++;
        }}
        footer={null}
        width={720}
        destroyOnHidden
      >
        {record &&
          ("stage" in record ? (
            <>
              <Descriptions
                column={1}
                size="small"
                bordered
                items={[
                  {
                    key: "time",
                    label: "Обработано",
                    children: when(record.backend_processed_at),
                  },
                  {
                    key: "stage",
                    label: "Этап",
                    children: stageText[record.stage],
                  },
                  {
                    key: "source",
                    label: "Источник",
                    children: record.source_name,
                  },
                  {
                    key: "kafka",
                    label: "Kafka",
                    children: `${record.kafka_topic} / партиция ${record.kafka_partition} / offset ${record.kafka_offset}`,
                  },
                  {
                    key: "normalizer",
                    label: "Правило на момент обработки",
                    children: `${record.normalizer_name} · версия ${record.normalizer_version}`,
                  },
                ]}
              />
              {record.diagnostics.map((diagnostic, index) => (
                <p key={index}>
                  <Tag>{diagnostic.code ?? "без кода"}</Tag>
                  {explain(diagnostic.code)}{" "}
                  {diagnostic.variant && `Вариант: ${diagnostic.variant}. `}
                  {diagnostic.block && `Блок: ${diagnostic.block}. `}
                  {diagnostic.field && `Поле: ${diagnostic.field}.`}
                </p>
              ))}
              <Space wrap>
                {record.source_id && (
                  <ReadLink
                    permission={has("sources.read")}
                    to={`/sources?kind=kafka&id=${record.source_id}`}
                  >
                    Перейти к источнику
                  </ReadLink>
                )}
                {record.connection_id && (
                  <ReadLink
                    permission={has("connections.read")}
                    to={`/connections?id=${record.connection_id}`}
                  >
                    Перейти к подключению
                  </ReadLink>
                )}
                {record.normalizer_id &&
                record.stage === "normalization" &&
                record.normalizer_name ? (
                  normalizerExists === "available" ? (
                    <ReadLink
                      permission={has("normalizers.read")}
                      to={`/normalizers/${record.normalizer_id}`}
                    >
                      Перейти к нормализатору
                    </ReadLink>
                  ) : normalizerExists === "missing" ? (
                    <Typography.Text type="secondary">
                      Сохранённый нормализатор удалён или недоступен.
                    </Typography.Text>
                  ) : normalizerExists === "unknown" ? (
                    <Space wrap>
                      <Typography.Text type="secondary">
                        Не удалось проверить нормализатор; ссылка не
                        подтверждена.
                      </Typography.Text>
                      <Button
                        size="small"
                        onClick={() =>
                          setNormalizerRetry((attempt) => attempt + 1)
                        }
                      >
                        Повторить проверку
                      </Button>
                    </Space>
                  ) : has("normalizers.read") ? (
                    <Typography.Text type="secondary">
                      Проверяем доступность нормализатора…
                    </Typography.Text>
                  ) : (
                    <ReadLink permission={false} to="">
                      Перейти к нормализатору
                    </ReadLink>
                  )
                ) : record.stage === "normalization" && record.normalizer_id ? (
                  <Typography.Text type="secondary">
                    Сохранённый нормализатор недоступен; перейти к нему нельзя.
                  </Typography.Text>
                ) : null}
                {record.stage === "normalization" &&
                  record.normalizer_id &&
                  record.normalizer_name &&
                  !record.diagnostics.some((item) => item.block) && (
                    <Typography.Text type="secondary">
                      Конкретный блок в диагностике не указан.
                    </Typography.Text>
                  )}
              </Space>
              {has("processing_errors.raw.read") && (
                <Card
                  size="small"
                  title="Сырой payload"
                  style={{ marginTop: 12 }}
                >
                  <Alert
                    type="warning"
                    showIcon
                    message="Данные могут быть чувствительными; ответ может быть усечён."
                  />
                  <Button
                    loading={rawLoading}
                    onClick={() => void showRaw(record.id)}
                  >
                    Показать payload
                  </Button>
                  {!!rawError && (
                    <AppRequestError
                      error={rawError}
                      onRetry={() => void showRaw(record.id)}
                    />
                  )}
                  {raw && <pre className={styles.raw}>{raw}</pre>}
                  {rawTruncated && <p>Ответ усечён.</p>}
                </Card>
              )}
            </>
          ) : (
            <>
              <Descriptions
                column={1}
                size="small"
                bordered
                items={[
                  {
                    key: "time",
                    label: "Обнаружено",
                    children: when(record.detected_at),
                  },
                  {
                    key: "kind",
                    label: "Тип",
                    children: kindText[record.kind],
                  },
                  {
                    key: "source",
                    label: "Источник",
                    children: record.source_name,
                  },
                  { key: "topic", label: "Топик", children: record.topic },
                  {
                    key: "partition",
                    label: "Партиция",
                    children: record.partition ?? "Не указана",
                  },
                  {
                    key: "offset",
                    label: "Диапазон позиций",
                    children:
                      record.offset_start == null
                        ? "Не указан"
                        : `${record.offset_start}–${record.offset_end ?? "?"}`,
                  },
                ]}
              />
              <p>
                {explain(record.reason_code)} Диапазон offset не равен точному
                числу потерянных сообщений.
              </p>
              {record.source_id && (
                <ReadLink
                  permission={has("sources.read")}
                  to={`/sources?kind=kafka&id=${record.source_id}`}
                >
                  Перейти к источнику
                </ReadLink>
              )}
            </>
          ))}
      </Modal>
    </main>
  );
});
