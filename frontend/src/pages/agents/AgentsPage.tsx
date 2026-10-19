import { useEffect, useMemo, useRef, useState } from "react";
import {
  Alert,
  Button,
  Form,
  Input,
  Modal,
  Pagination,
  Radio,
  Select,
  Space,
  Spin,
  Table,
  Tag,
  Typography,
} from "antd";
import { observer } from "mobx-react-lite";
import { Link, useNavigate, useParams } from "react-router-dom";
import {
  AgentsApi,
  type AgentCredentials,
  type AgentTopicInput,
  type AgentTransport,
  type Agent as AgentRecord,
} from "../../app/api/agents";
import { ApiError } from "../../app/api/client";
import {
  ResourcesApi,
  type KafkaConnection,
  type Topic,
} from "../../app/api/resources";
import { useStores } from "../../app/providers/StoresProvider";
import { AppRequestError } from "../../app/ui/AppRequestError";
import { AgentStore } from "../../features/agents/AgentStore";
import {
  fluentBitConfig,
  agentHostPlatform,
  type AgentInputKind,
  type AgentInputSelection,
  type AgentHostPlatform,
} from "../../features/agents/fluentBitConfig";
import { setAgentSecretVisible } from "../../features/agents/activeSecret";
import { ConfirmationDialog } from "../../shared/ui/ConfirmationDialog";
import styles from "./AgentsPage.module.css";

type TopicDraft = AgentTopicInput;
type FormData = { name: string; description?: string; topics: TopicDraft[] };
type CreatePayload = {
  name: string;
  description: string | null;
  topics: TopicDraft[];
};
type PendingCreate = { key: string; payload: CreatePayload };
const PENDING_CREATE_KEY = "diploma.agent.pending-create";

function readPendingCreate(): PendingCreate | null {
  try {
    const value = JSON.parse(
      sessionStorage.getItem(PENDING_CREATE_KEY) ?? "null",
    );
    if (
      value &&
      typeof value.key === "string" &&
      /^[0-9a-f-]{36}$/i.test(value.key) &&
      typeof value.payload?.name === "string" &&
      Array.isArray(value.payload?.topics)
    )
      return value as PendingCreate;
  } catch {
    /* Storage may be unavailable; the in-memory key remains usable. */
  }
  return null;
}
type Confirm = {
  title: string;
  message: string;
  label: string;
  execute: () => Promise<unknown>;
};

const stateLabel: Record<string, string> = {
  provisioning: "Настройка Kafka",
  ready: "Доступ готов",
  error: "Ошибка настройки",
  revocation_pending: "Отзыв доступа",
  revoked: "Доступ отозван",
  pending: "Ожидает",
  running: "Выполняется",
  retry: "Повторяется",
  succeeded: "Завершена",
  failed: "Ошибка",
};

export const AgentsPage = observer(() => {
  const { id } = useParams();
  const navigate = useNavigate();
  const { api, session } = useStores();
  const agentsApi = useMemo(() => new AgentsApi(api), [api]);
  const store = useMemo(() => new AgentStore(agentsApi), [agentsApi]);
  const resources = useMemo(() => new ResourcesApi(api), [api]);
  const canWrite = session.has("agents.write");
  const [offset, setOffset] = useState(0);
  const [form] = Form.useForm<FormData>();
  const [editForm] = Form.useForm();
  const [createOpen, setCreateOpen] = useState(false);
  const [addOpen, setAddOpen] = useState(false);
  const [confirm, setConfirm] = useState<Confirm | null>(null);
  const [confirmPending, setConfirmPending] = useState(false);
  const [connections, setConnections] = useState<KafkaConnection[]>([]);
  const [topics, setTopics] = useState<Record<string, Topic[]>>({});
  const [ownedTopics, setOwnedTopics] = useState<Set<string>>(new Set());
  const [optionError, setOptionError] = useState<unknown>(null);
  const [config, setConfig] = useState<string | null>(null);
  const [configCredentials, setConfigCredentials] =
    useState<AgentCredentials | null>(null);
  const [configAgent, setConfigAgent] = useState<AgentRecord | null>(null);
  const [configTransport, setConfigTransport] = useState<AgentTransport | null>(
    null,
  );
  const [inputSelections, setInputSelections] = useState<AgentInputSelection>(
    {},
  );
  const [inputError, setInputError] = useState("");
  const [hostPlatform, setHostPlatform] = useState<AgentHostPlatform>("linux");
  const [templateConfig, setTemplateConfig] = useState<string | null>(null);
  const [templateOpen, setTemplateOpen] = useState(false);
  const [templatePending, setTemplatePending] = useState(false);
  const [templateError, setTemplateError] = useState<unknown>(null);
  const [templateWarning, setTemplateWarning] = useState(false);
  const [ca, setCa] = useState<string | null>(null);
  const [claimPending, setClaimPending] = useState(false);
  const [claimError, setClaimError] = useState<unknown>(null);
  const [claimWarning, setClaimWarning] = useState("");
  const [leaveTarget, setLeaveTarget] = useState<string | null>(null);
  const [pendingCreate, setPendingCreate] = useState(readPendingCreate);
  const claimContext = useRef({ id, sessionVersion: session.version });
  claimContext.current = { id, sessionVersion: session.version };
  const agent = id ? store.agent : null;

  useEffect(() => {
    if (id) void store.get(id);
    else void store.list(offset);
  }, [id, offset, store]);
  useEffect(() => {
    if (
      !id ||
      !store.agent?.operation_id ||
      !store.operation ||
      ["succeeded", "failed"].includes(store.operation.state)
    )
      return;
    const timer = window.setInterval(() => {
      if (!store.pending && !store.loading) void store.get(id);
    }, 2000);
    return () => window.clearInterval(timer);
  }, [
    id,
    store,
    store.agent?.operation_id,
    store.operation?.state,
    store.pending,
    store.loading,
  ]);
  useEffect(() => {
    setConfig(null);
    setConfigCredentials(null);
    setConfigAgent(null);
    setConfigTransport(null);
    setCa(null);
    setClaimError(null);
    setClaimWarning("");
  }, [id, session.version]);
  useEffect(() => {
    setAgentSecretVisible(!!config);
    return () => setAgentSecretVisible(false);
  }, [config]);
  useEffect(() => {
    if (!configCredentials || !configAgent || !configTransport) return;
    try {
      setConfig(
        fluentBitConfig(
          configAgent,
          configCredentials,
          configTransport,
          inputSelections,
          hostPlatform,
        ),
      );
    } catch (error) {
      setInputError(
        error instanceof Error
          ? error.message
          : "Не удалось собрать конфигурацию.",
      );
    }
  }, [
    configCredentials,
    configAgent,
    configTransport,
    inputSelections,
    hostPlatform,
  ]);
  function selectInput(topicId: string, value: AgentInputKind) {
    const next = { ...inputSelections, [topicId]: value };
    const kinds =
      (configAgent ?? agent)?.topics.map((topic) => next[topic.id] ?? "tail") ??
      [];
    try {
      const nextPlatform =
        value === "winevtlog"
          ? "windows"
          : value === "systemd"
            ? "linux"
            : hostPlatform;
      agentHostPlatform(kinds, nextPlatform);
      setInputError("");
      setInputSelections(next);
      setHostPlatform(nextPlatform);
    } catch (error) {
      setInputError(
        error instanceof Error ? error.message : "Несовместимые входы.",
      );
    }
  }
  function selectHostPlatform(value: AgentHostPlatform) {
    try {
      agentHostPlatform(
        ((configAgent ?? agent)?.topics ?? []).map(
          (topic) => inputSelections[topic.id] ?? "tail",
        ),
        value,
      );
      setHostPlatform(value);
      setInputError("");
    } catch (error) {
      setInputError(
        error instanceof Error ? error.message : "Несовместимая ОС агента.",
      );
    }
  }
  const selectedPlatform = hostPlatform;
  useEffect(() => {
    if (agent && agent.id === id)
      editForm.setFieldsValue({
        name: agent.name,
        description: agent.description,
      });
  }, [agent, id, editForm]);
  useEffect(() => {
    if (!config) return;
    window.history.pushState(
      { agentSecretGuard: true },
      "",
      window.location.href,
    );
    const beforeUnload = (event: BeforeUnloadEvent) => {
      event.preventDefault();
      event.returnValue = "";
    };
    const click = (event: MouseEvent) => {
      const link = (event.target as Element | null)?.closest("a[href]");
      if (
        !link ||
        !(link instanceof HTMLAnchorElement) ||
        link.origin !== location.origin ||
        link.pathname === location.pathname
      )
        return;
      event.preventDefault();
      event.stopPropagation();
      setLeaveTarget(link.pathname + link.search);
    };
    window.addEventListener("beforeunload", beforeUnload);
    const back = () => setLeaveTarget("__back__");
    window.addEventListener("popstate", back);
    document.addEventListener("click", click, true);
    return () => {
      window.removeEventListener("beforeunload", beforeUnload);
      window.removeEventListener("popstate", back);
      document.removeEventListener("click", click, true);
    };
  }, [config]);

  async function loadConnections() {
    setOptionError(null);
    try {
      const page = await resources.kafkaConnections();
      setConnections(page.items);
    } catch (error) {
      setOptionError(error);
    }
  }
  async function loadOwnership() {
    try {
      const owned = new Set<string>();
      let offset = 0;
      while (true) {
        const page = await agentsApi.list(offset);
        for (const existing of page.items)
          for (const assignment of existing.topics)
            owned.add(`${assignment.connection_id}:${assignment.topic_name}`);
        offset += page.items.length;
        if (!page.items.length || offset >= page.total) break;
      }
      setOwnedTopics(owned);
    } catch (error) {
      setOptionError(error);
    }
  }
  async function loadTopics(connectionId: string) {
    if (!connectionId) return;
    try {
      const page = await resources.topics(connectionId);
      setTopics((previous) => ({ ...previous, [connectionId]: page.items }));
    } catch (error) {
      setOptionError(error);
    }
  }
  function chooseConnection(connectionId: string) {
    void loadTopics(connectionId);
  }
  function openCreate() {
    form.resetFields();
    form.setFieldsValue(
      pendingCreate
        ? {
            ...pendingCreate.payload,
            description: pendingCreate.payload.description ?? undefined,
          }
        : ({ topics: [{ action: "create" }] } as FormData),
    );
    setCreateOpen(true);
    void loadConnections();
    void loadOwnership();
  }
  function openAdd() {
    form.resetFields();
    form.setFieldsValue({ topics: [{ action: "create" }] } as FormData);
    setAddOpen(true);
    void loadConnections();
    void loadOwnership();
  }
  function topicFields(index: number, current: TopicDraft | undefined) {
    const discovered = topics[current?.connection_id ?? ""] ?? [];
    return (
      <Space wrap key={index} align="start">
        <Form.Item
          name={[index, "connection_id"]}
          label="Подключение Kafka"
          rules={[{ required: true, message: "Выберите подключение" }]}
        >
          <Select
            className={styles.connection}
            options={connections.map((connection) => ({
              value: connection.id,
              label: connection.name,
            }))}
            onChange={chooseConnection}
          />
        </Form.Item>
        <Form.Item
          name={[index, "action"]}
          label="Действие"
          rules={[{ required: true }]}
        >
          <Radio.Group
            options={[
              { label: "Создать", value: "create" },
              { label: "Закрепить", value: "attach" },
            ]}
          />
        </Form.Item>
        {current?.action === "attach" ? (
          <Form.Item
            name={[index, "topic_name"]}
            label="Свободный топик"
            rules={[{ required: true, message: "Выберите топик" }]}
          >
            <Select
              className={styles.connection}
              options={discovered.map((topic) => ({
                value: topic.name,
                label: `${topic.name}${ownedTopics.has(`${current?.connection_id}:${topic.name}`) ? " — занят агентом" : topic.is_registered ? " — источник уже зарегистрирован, топик свободен" : " — свободен"}`,
                disabled: ownedTopics.has(
                  `${current?.connection_id}:${topic.name}`,
                ),
              }))}
            />
          </Form.Item>
        ) : (
          <Form.Item
            name={[index, "topic_name"]}
            label="Имя нового топика"
            rules={[
              {
                required: true,
                pattern: /^[A-Za-z0-9._-]{1,249}$/,
                message: "Используйте 1–249 символов: буквы, цифры, ., _ или -",
              },
            ]}
          >
            <Input className={styles.connection} />
          </Form.Item>
        )}
      </Space>
    );
  }
  async function submitForm(values: FormData) {
    if (!addOpen && pendingCreate) return;
    const keys = values.topics.map(
      (topic) => `${topic.connection_id}:${topic.topic_name}`,
    );
    if (new Set(keys).size !== keys.length) {
      form.setFields([
        { name: ["topics"], errors: ["Топики в форме не должны повторяться"] },
      ]);
      return;
    }
    if (addOpen && id) {
      const topic = values.topics[0];
      const result = await store.run(
        (key) => agentsApi.addTopic(id, topic, key),
        id,
      );
      if (
        result?.status === "succeeded" ||
        result?.status === "pending" ||
        result?.status === "observation_error"
      ) {
        setAddOpen(false);
        if (result.status !== "observation_error") void store.get(id);
      }
    } else {
      const payload = {
        name: values.name.trim(),
        description: values.description?.trim() || null,
        topics: values.topics,
      };
      const pending = { key: crypto.randomUUID(), payload };
      try {
        sessionStorage.setItem(PENDING_CREATE_KEY, JSON.stringify(pending));
      } catch {
        setOptionError(
          new Error(
            "Не удалось сохранить ключ операции в этой вкладке. Проверьте настройки браузера и повторите действие.",
          ),
        );
        return;
      }
      setPendingCreate(pending);
      const result = await store.run(
        (key) => agentsApi.create(payload, key),
        undefined,
        true,
        false,
        pending.key,
      );
      if (result && result.status !== "unknown") clearPendingCreate();
      if (
        result?.status === "succeeded" ||
        result?.status === "pending" ||
        result?.status === "observation_error"
      ) {
        setCreateOpen(false);
        navigate(`/agents/${result.agentId}`);
      }
    }
  }
  function clearPendingCreate() {
    try {
      sessionStorage.removeItem(PENDING_CREATE_KEY);
    } catch {
      /* No stored key remains. */
    }
    setPendingCreate(null);
  }
  async function replayPendingCreate() {
    if (!pendingCreate) return;
    const result = await store.run(
      (key) => agentsApi.create(pendingCreate.payload, key),
      undefined,
      true,
      true,
      pendingCreate.key,
    );
    if (result && result.status !== "unknown") clearPendingCreate();
    if (
      result?.status === "succeeded" ||
      result?.status === "pending" ||
      result?.status === "observation_error"
    ) {
      setCreateOpen(false);
      navigate(`/agents/${result.agentId}`);
    }
  }
  async function updateDetails(values: { name: string; description?: string }) {
    if (!id) return;
    const result = await store.run(
      () =>
        agentsApi.patch(id, {
          name: values.name.trim(),
          description: values.description?.trim() || null,
        }),
      id,
      false,
    );
    if (result?.status === "succeeded")
      editForm.setFieldsValue({
        name: store.agent?.name,
        description: store.agent?.description,
      });
  }
  async function claim() {
    if (!id || store.agent?.state !== "ready" || claimPending || config) return;
    const expected = { id, sessionVersion: session.version };
    setClaimPending(true);
    setClaimError(null);
    setClaimWarning("");
    let consumed = false;
    try {
      const snapshot = await agentsApi.get(id);
      const transport = await agentsApi.transport();
      if (!snapshot.credentials_claimable || snapshot.state !== "ready")
        throw new Error(
          "Однократная выдача больше недоступна. Обновите карточку агента.",
        );
      // Validate all non-secret inputs and the renderer before consuming the password.
      fluentBitConfig(
        snapshot,
        {
          agent_id: id,
          revision: snapshot.access_revision,
          username: "preflight-user",
          password: "preflight-password",
        },
        transport,
        inputSelections,
        hostPlatform,
      );
      if (
        claimContext.current.id !== expected.id ||
        claimContext.current.sessionVersion !== expected.sessionVersion
      )
        return;
      const credentials = await agentsApi.claim(id);
      consumed = true;
      if (
        claimContext.current.id !== expected.id ||
        claimContext.current.sessionVersion !== expected.sessionVersion
      ) {
        if (claimContext.current.sessionVersion === expected.sessionVersion)
          setClaimWarning(
            "Пароль выдан, но карточка агента сменилась до получения ответа. Повторная выдача невозможна; вернитесь к агенту и перевыпустите доступ.",
          );
        return;
      }
      const rendered = fluentBitConfig(
        snapshot,
        credentials,
        transport,
        inputSelections,
        hostPlatform,
      );
      setConfigCredentials(credentials);
      setConfigAgent(snapshot);
      setConfigTransport(transport);
      setConfig(rendered);
      setCa(transport.ca_pem);
      if (
        credentials.agent_id !== snapshot.id ||
        credentials.revision !== snapshot.access_revision
      )
        setClaimWarning(
          "После начала выдачи состояние агента изменилось. Сохраните полученный конфиг, затем сверьте назначения и при необходимости перевыпустите доступ.",
        );
      void store.get(id);
    } catch (error) {
      setClaimError(error);
      if (consumed)
        setClaimWarning(
          "Пароль уже выдан сервером, но конфигурацию не удалось подготовить. Повторная выдача невозможна; после сверки состояния перевыпустите доступ.",
        );
      else if (!(error instanceof ApiError))
        setClaimWarning(
          "Настройки агента не позволяют подготовить конфигурацию. Пароль не запрашивался; обновите карточку и проверьте состояние топиков.",
        );
    } finally {
      setClaimPending(false);
    }
  }
  async function previewTemplate() {
    if (!id || !canWrite || templatePending) return;
    setTemplatePending(true);
    setTemplateError(null);
    try {
      const [snapshot, transport] = await Promise.all([
        agentsApi.get(id),
        agentsApi.transport(),
      ]);
      const changedSinceCard =
        snapshot.access_revision !== agent?.access_revision ||
        snapshot.topics
          .map((topic) => `${topic.id}:${topic.topic_name}`)
          .join("|") !==
          (agent?.topics
            .map((topic) => `${topic.id}:${topic.topic_name}`)
            .join("|") ?? "");
      setTemplateWarning(changedSinceCard);
      setTemplateConfig(
        fluentBitConfig(
          snapshot,
          null,
          transport,
          inputSelections,
          hostPlatform,
        ),
      );
      setTemplateOpen(true);
    } catch (error) {
      setTemplateError(error);
    } finally {
      setTemplatePending(false);
    }
  }
  function download(name: string, content: string) {
    const url = URL.createObjectURL(
      new Blob([content], { type: "text/plain;charset=utf-8" }),
    );
    const link = document.createElement("a");
    link.href = url;
    link.download = name;
    link.click();
    setTimeout(() => URL.revokeObjectURL(url), 0);
  }
  return (
    <main className={styles.page}>
      <div className={styles.heading}>
        <div>
          <Typography.Title level={2}>
            {id ? "Карточка агента" : "Агенты"}
          </Typography.Title>
          <Typography.Text type="secondary">
            Готовность доступа не означает, что Fluent Bit сейчас работает или
            отправляет записи.
          </Typography.Text>
        </div>
        {canWrite && !id && (
          <Button type="primary" onClick={openCreate}>
            Зарегистрировать агента
          </Button>
        )}
      </div>
      {id && <Link to="/agents">← Все агенты</Link>}
      {store.error != null && !store.observationFailed && (
        <AppRequestError
          error={store.error}
          onRetry={() => void store.reconcile(id)}
        />
      )}
      {store.notice && <Alert type="info" showIcon message={store.notice} />}
      {store.loading && <Spin aria-label="Загрузка агентов" />}
      {pendingCreate && !id && (
        <Alert
          type="warning"
          showIcon
          message="Результат регистрации ещё не подтверждён"
          description="Новая регистрация заблокирована. Сверьте список и при необходимости повторите тот же запрос с прежним ключом: сервер не создаст дубль."
          action={
            <Space wrap>
              <Button
                disabled={store.pending}
                onClick={() => void store.list(offset)}
              >
                Сверить список
              </Button>
              <Button
                disabled={store.pending}
                onClick={() => void replayPendingCreate()}
              >
                Повторить с прежним ключом
              </Button>
            </Space>
          }
        />
      )}
      {!id && (
        <>
          <Table
            rowKey="id"
            dataSource={store.page}
            loading={store.loading}
            pagination={false}
            columns={[
              {
                title: "Название",
                dataIndex: "name",
                render: (value: string, row) => (
                  <Link to={`/agents/${row.id}`}>{value}</Link>
                ),
              },
              { title: "Описание", dataIndex: "description", ellipsis: true },
              {
                title: "Состояние",
                dataIndex: "state",
                render: (value: string) => (
                  <Tag>{stateLabel[value] ?? value}</Tag>
                ),
              },
              {
                title: "Топики",
                dataIndex: "topics",
                render: (value: AgentTopicInput[]) =>
                  `${value.length}: ${value.map((topic) => topic.topic_name).join(", ")}`,
              },
              {
                title: "Последняя операция",
                dataIndex: "operation_id",
                render: (value: string | null, row) =>
                  value ? (
                    <Link to={`/agents/${row.id}`} title={`Операция ${value}`}>
                      {value.slice(0, 8)}…
                    </Link>
                  ) : (
                    "—"
                  ),
              },
              { title: "Ошибка", dataIndex: "safe_error" },
            ]}
          />
          <Pagination
            current={offset / 20 + 1}
            total={store.total}
            pageSize={20}
            onChange={(page) => setOffset((page - 1) * 20)}
          />
        </>
      )}
      {id && agent?.id === id && (
        <section className={styles.card}>
          <Space wrap>
            <Tag>{stateLabel[agent.state] ?? agent.state}</Tag>
            <Typography.Text>
              Ревизия доступа: {agent.access_revision}
            </Typography.Text>
            <Button onClick={() => void store.get(id)}>
              Обновить состояние
            </Button>
          </Space>
          {agent.safe_error && (
            <Alert type="error" showIcon message={agent.safe_error} />
          )}
          {store.operation && (
            <Alert
              type={store.operation.state === "failed" ? "error" : "info"}
              showIcon
              message={`Операция: ${stateLabel[store.operation.state] ?? store.operation.state}; попыток: ${store.operation.attempts}`}
              description={store.operation.safe_error}
            />
          )}
          <Form
            form={editForm}
            layout="vertical"
            initialValues={{ name: agent.name, description: agent.description }}
            onFinish={(values) => void updateDetails(values)}
          >
            <Form.Item
              name="name"
              label="Название"
              rules={[{ required: true, max: 200 }]}
            >
              <Input
                disabled={
                  !canWrite || store.pending || agent.state === "revoked"
                }
              />
            </Form.Item>
            <Form.Item
              name="description"
              label="Описание"
              rules={[{ max: 2000 }]}
            >
              <Input.TextArea
                disabled={
                  !canWrite || store.pending || agent.state === "revoked"
                }
              />
            </Form.Item>
            {canWrite && agent.state !== "revoked" && (
              <Button htmlType="submit" loading={store.pending}>
                Сохранить
              </Button>
            )}
          </Form>
          <Typography.Title level={4}>Назначения</Typography.Title>
          <Alert
            type="info"
            message="Новый источник выключен и не имеет нормализатора. Для запуска обработки назначьте нормализатор и включите источник в разделе «Источники»."
          />
          <Table
            rowKey="id"
            pagination={false}
            dataSource={agent.topics}
            columns={[
              { title: "Топик", dataIndex: "topic_name" },
              {
                title: "Вход Fluent Bit",
                dataIndex: "id",
                render: (topicId: string, row) => (
                  <Select<AgentInputKind>
                    aria-label={`Вход Fluent Bit для топика ${row.topic_name}`}
                    value={inputSelections[topicId] ?? "tail"}
                    disabled={!canWrite}
                    className={styles.connection}
                    options={[
                      { value: "tail", label: "Файл (tail)" },
                      {
                        value: "winevtlog",
                        label: "Журнал Windows (winevtlog)",
                      },
                      {
                        value: "systemd",
                        label: "Linux journald (systemd)",
                      },
                      { value: "other", label: "Другой вход" },
                    ]}
                    onChange={(value) => selectInput(topicId, value)}
                  />
                ),
              },
              {
                title: "Состояние",
                dataIndex: "state",
                render: (state: string) => stateLabel[state] ?? state,
              },
              {
                title: "Источник",
                dataIndex: "source_id",
                render: (sourceId: string) => (
                  <Link to={`/sources?kind=kafka&id=${sourceId}`}>
                    Открыть источник
                  </Link>
                ),
              },
              {
                title: "",
                dataIndex: "id",
                render: (topicId: string, row) =>
                  canWrite && agent.state !== "revoked" ? (
                    <Button
                      danger
                      disabled={store.pending}
                      onClick={() =>
                        setConfirm({
                          title: "Снять назначение",
                          message: `Снять топик ${row.topic_name}? Его источник и накопленные события сохранятся.`,
                          label: "Снять",
                          execute: () =>
                            store.run(
                              (key) => agentsApi.removeTopic(id, topicId, key),
                              id,
                            ),
                        })
                      }
                    >
                      Снять
                    </Button>
                  ) : null,
              },
            ]}
          />
          <Alert
            type="info"
            showIcon
            message="Настройте каждый вход отдельно"
            description={
              <ul>
                <li>
                  Файл: укажите путь службы. Read_from_Head On читает и старые
                  строки; DB хранит позицию между перезапусками.
                </li>
                <li>
                  Windows: укажите каналы и проверьте права чтения; DB хранит
                  позицию событий.
                </li>
                <li>
                  journald: выберите unit и сохраните DB cursor в постоянном
                  каталоге. Исходное время события остаётся в строке log, а
                  timestamp получает время обработки агентом.
                </li>
                <li>
                  Другой вход: замените секцию INPUT и адаптируйте пример FILTER
                  так, чтобы на выходе оставалось строковое поле log.
                </li>
              </ul>
            }
          />
          {canWrite && agent.state !== "revoked" && (
            <Space wrap>
              <Button disabled={store.pending || !!config} onClick={openAdd}>
                Добавить топик
              </Button>
              <Button
                disabled={store.pending || !!config || agent.state !== "ready"}
                onClick={() =>
                  setConfirm({
                    title: "Перевыпустить доступ",
                    message:
                      "После операции прежний доступ немедленно отзывается. Отправка остановится до установки нового конфига. Старую конфигурацию нельзя будет использовать.",
                    label: "Перевыпустить",
                    execute: () =>
                      store.run((key) => agentsApi.rotate(id, key), id),
                  })
                }
              >
                Перевыпустить доступ
              </Button>
              <Button
                danger
                disabled={store.pending || !!config}
                onClick={() =>
                  setConfirm({
                    title: "Отозвать агента",
                    message:
                      "Запись агента прекратится. Топики, источники и события останутся; включённый источник сможет дочитать накопленное. Историческая запись агента сохранится, Kafka-пользователь будет удалён.",
                    label: "Отозвать",
                    execute: () =>
                      store.run((key) => agentsApi.revoke(id, key), id),
                  })
                }
              >
                Отозвать доступ
              </Button>
            </Space>
          )}
          {canWrite && agent.credentials_claimable && !config && (
            <Button
              type="primary"
              loading={claimPending}
              disabled={store.pending}
              onClick={() => void claim()}
            >
              Получить конфигурацию
            </Button>
          )}
          {canWrite && agent.state === "ready" && !config && (
            <Space wrap>
              <Button
                loading={templatePending}
                onClick={() => void previewTemplate()}
              >
                Просмотреть шаблон
              </Button>
            </Space>
          )}
          {templateError != null && (
            <AppRequestError
              error={templateError}
              onRetry={() => void previewTemplate()}
            />
          )}
          <Select<AgentHostPlatform>
            aria-label="ОС хоста агента"
            value={hostPlatform}
            disabled={!canWrite}
            options={[
              { value: "linux", label: "Linux" },
              { value: "windows", label: "Windows" },
            ]}
            onChange={selectHostPlatform}
          />
          <Alert
            type="info"
            showIcon
            message={`Один конфиг работает на одном хосте: ${selectedPlatform === "windows" ? "Windows" : "Linux"}. Каталоги состояния и CA должны существовать и быть доступны учётной записи Fluent Bit.`}
          />
          {inputError && <Alert type="error" showIcon message={inputError} />}
          {claimError != null && (
            <AppRequestError
              error={claimError}
              onRetry={() => {
                setClaimError(null);
                void store.get(id);
              }}
            />
          )}
          {claimWarning && (
            <Alert type="warning" showIcon message={claimWarning} />
          )}
          {claimError instanceof ApiError &&
            (claimError.status === 0 ||
              claimError.code === "invalid_response") && (
              <Alert
                type="warning"
                message="Ответ при выдаче пароля потерян: результат неизвестен. Обновите состояние; если пароль уже выдан, потребуется явный перевыпуск."
              />
            )}
        </section>
      )}
      <Modal
        open={createOpen || addOpen}
        title={addOpen ? "Добавить топик" : "Регистрация агента"}
        onCancel={() => {
          setCreateOpen(false);
          setAddOpen(false);
        }}
        footer={null}
        destroyOnHidden
      >
        {optionError != null && (
          <AppRequestError
            error={optionError}
            onRetry={() => void loadConnections()}
          />
        )}
        <Form
          form={form}
          layout="vertical"
          onFinish={(values) => void submitForm(values)}
        >
          {!addOpen && (
            <>
              <Form.Item
                name="name"
                label="Название"
                rules={[{ required: true, max: 200 }]}
              >
                <Input />
              </Form.Item>
              <Form.Item
                name="description"
                label="Описание"
                rules={[{ max: 2000 }]}
              >
                <Input.TextArea />
              </Form.Item>
            </>
          )}
          <Form.List
            name="topics"
            rules={[
              {
                validator: async (_, value) => {
                  if (!value?.length || value.length > 50)
                    throw new Error("Нужно от 1 до 50 топиков");
                },
              },
            ]}
          >
            {(fields, { add, remove }, { errors }) => (
              <>
                <Form.ErrorList errors={errors} />
                {fields.map((field) => (
                  <Form.Item key={field.key}>
                    <Form.Item noStyle shouldUpdate>
                      {({ getFieldValue }) =>
                        topicFields(
                          field.name,
                          getFieldValue(["topics", field.name]),
                        )
                      }
                    </Form.Item>
                    {!addOpen && fields.length > 1 && (
                      <Button onClick={() => remove(field.name)}>Убрать</Button>
                    )}
                  </Form.Item>
                ))}
                {!addOpen && fields.length < 50 && (
                  <Button onClick={() => add({ action: "create" })}>
                    Ещё топик
                  </Button>
                )}
              </>
            )}
          </Form.List>
          <Typography.Paragraph type="secondary">
            Доступность топика проверяется окончательно на сервере при
            выполнении операции.
          </Typography.Paragraph>
          {store.error != null && (
            <AppRequestError
              error={store.error}
              onRetry={() => {
                if (id) void store.reconcile(id);
                else void store.list();
              }}
            />
          )}
          {store.operation?.state === "failed" &&
            store.operation.safe_error && (
              <Alert
                type="error"
                showIcon
                message={store.operation.safe_error}
              />
            )}
          <Button
            type="primary"
            htmlType="submit"
            loading={store.pending}
            disabled={store.pending || (!!pendingCreate && !addOpen)}
          >
            Отправить
          </Button>
        </Form>
      </Modal>
      <Modal
        open={templateOpen}
        title="Шаблон конфигурации Fluent Bit"
        width={850}
        onCancel={() => setTemplateOpen(false)}
        footer={null}
        destroyOnHidden
      >
        <Alert
          type="warning"
          showIcon
          message="Это шаблон без пароля. Он не авторизует агента. Если меняете вход, подставьте пароль из ранее безопасно сохранённого конфига. При утрате пароля выполните явный перевыпуск доступа."
          description="Шаблон построен заново по текущим назначениям и выбранным входам. Он не считывает файл, установленный на хосте агента, и может отличаться от него."
        />
        {templateWarning && (
          <Alert
            type="warning"
            showIcon
            message="С момента загрузки карточки изменились ревизия доступа или назначения. Ранее установленный конфиг может отличаться."
          />
        )}
        <pre className={styles.config}>{templateConfig}</pre>
        <Button
          onClick={() =>
            templateConfig &&
            download("fluent-bit-template.conf", templateConfig)
          }
        >
          Скачать шаблон без пароля
        </Button>
      </Modal>
      <Modal
        open={!!config}
        title="Однократная конфигурация Fluent Bit"
        width={850}
        onCancel={() =>
          setConfirm({
            title: "Закрыть конфигурацию",
            message:
              "Пароль и конфигурация исчезнут из памяти страницы. Повторно получить пароль нельзя; при потере потребуется перевыпуск.",
            label: "Закрыть",
            execute: async () => {
              setConfig(null);
              setConfigCredentials(null);
              setConfigAgent(null);
              setConfigTransport(null);
              setCa(null);
            },
          })
        }
        footer={null}
      >
        <Alert
          type="warning"
          showIcon
          message="Содержит пароль. Сохраните файл с правами только для владельца; после ухода он не появится снова."
        />
        {configAgent && (
          <Space direction="vertical" style={{ width: "100%", marginTop: 16 }}>
            {configAgent.topics.map((topic) => (
              <Select<AgentInputKind>
                key={topic.id}
                aria-label={`Вход Fluent Bit для топика ${topic.topic_name}`}
                value={inputSelections[topic.id] ?? "tail"}
                disabled={!canWrite}
                style={{ width: "100%" }}
                options={[
                  { value: "tail", label: "Файл (tail)" },
                  { value: "winevtlog", label: "Журнал Windows (winevtlog)" },
                  { value: "systemd", label: "Linux journald (systemd)" },
                  { value: "other", label: "Другой вход" },
                ]}
                onChange={(value) => selectInput(topic.id, value)}
              />
            ))}
          </Space>
        )}
        <Alert
          type="info"
          showIcon
          message={`ОС агента: ${selectedPlatform === "windows" ? "Windows" : "Linux"}. Каталоги состояния и CA должны быть доступны Fluent Bit.`}
        />
        {inputError && <Alert type="error" showIcon message={inputError} />}
        <pre className={styles.config}>{config}</pre>
        <Space wrap>
          <Button onClick={() => config && download("fluent-bit.conf", config)}>
            Скачать конфиг
          </Button>
          {ca && (
            <Button onClick={() => download("diploma-kafka-ca.pem", ca)}>
              Скачать CA
            </Button>
          )}
        </Space>
      </Modal>
      <ConfirmationDialog
        open={!!confirm}
        title={confirm?.title ?? ""}
        message={confirm?.message ?? ""}
        confirmText={confirm?.label ?? ""}
        danger
        pending={confirmPending}
        onCancel={() => setConfirm(null)}
        onConfirm={() => {
          if (!confirm) return;
          setConfirmPending(true);
          void confirm.execute().finally(() => {
            setConfirmPending(false);
            setConfirm(null);
          });
        }}
      />
      <ConfirmationDialog
        open={!!leaveTarget}
        title="Покинуть страницу"
        message="Однократно выданный пароль исчезнет. Сохраните конфигурацию до ухода."
        confirmText="Покинуть"
        onCancel={() => {
          if (leaveTarget === "__back__")
            window.history.pushState(
              { agentSecretGuard: true },
              "",
              window.location.href,
            );
          setLeaveTarget(null);
        }}
        onConfirm={() => {
          const target = leaveTarget;
          setConfig(null);
          setCa(null);
          setLeaveTarget(null);
          if (target === "__back__") navigate(-1);
          else if (target) navigate(target);
        }}
      />
      {store.observationFailed && store.observationAgentId === id && (
        <Alert
          type="warning"
          showIcon
          message="Запрос принят сервером, но состояние операции пока не удалось прочитать. Повторите чтение; новая операция отправлена не будет."
          action={
            <Button
              loading={store.loading}
              disabled={store.loading}
              onClick={() => void store.retryObservation(id)}
            >
              Повторить чтение состояния
            </Button>
          }
        />
      )}
      {store.canRetryMutation &&
        store.error instanceof ApiError &&
        (store.error.status === 0 ||
          store.error.code === "invalid_response") && (
          <Button onClick={() => void store.retryUnknown(id)}>
            Повторить запрос с прежним ключом
          </Button>
        )}
    </main>
  );
});
