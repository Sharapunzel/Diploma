import { useEffect, useMemo, useState } from "react";
import { InfoCircleOutlined } from "@ant-design/icons";
import {
  Alert,
  Button,
  Form,
  Input,
  Modal,
  Pagination,
  Spin,
  Switch,
  Table,
  Tag,
  Tooltip,
  Typography,
} from "antd";
import { observer } from "mobx-react-lite";
import { Link } from "react-router-dom";
import {
  type ExternalConnection,
  type ExternalConnectionInput,
  type ExternalConnectionPatch,
  type KafkaConnection,
  type KafkaConnectionInput,
  type KafkaConnectionPatch,
  matchPattern,
  targetName,
  validationFields,
} from "../../app/api/resources";
import { ApiError } from "../../app/api/client";
import { AppRequestError } from "../../app/ui/AppRequestError";
import { ConfirmationDialog } from "../../shared/ui/ConfirmationDialog";
import { useResources } from "../../features/resources/useResources";
import styles from "./Resources.module.css";

type Kind = "kafka" | "external";
type RecordType = KafkaConnection | ExternalConnection;
type Confirm = {
  title: string;
  message: string;
  label: string;
  execute: () => Promise<unknown>;
};
export const ConnectionsPage = observer(() => {
  const { lists, lifecycle, connections, session } = useResources();
  const canWrite = session.has("connections.write");
  const canReadSources = session.has("sources.read");
  const [editing, setEditing] = useState<{
    kind: Kind;
    item?: RecordType;
  } | null>(null);
  const [form] = Form.useForm();
  const [replacePassword, setReplacePassword] = useState(false);
  const [saving, setSaving] = useState(false);
  const [formError, setFormError] = useState<unknown>(null);
  const [verificationLoading, setVerificationLoading] = useState(false);
  const [verificationError, setVerificationError] = useState<unknown>(null);
  const [notice, setNotice] = useState("");
  const [activeKind, setActiveKind] = useState<Kind>("kafka");
  const [helpOpen, setHelpOpen] = useState(false);
  const [operationError, setOperationError] = useState<unknown>(null);
  const [verificationName, setVerificationName] = useState("");
  const [confirm, setConfirm] = useState<Confirm | null>(null);
  const [confirmPending, setConfirmPending] = useState(false);
  const [caFile, setCaFile] = useState<File | null>(null);
  const [caFor, setCaFor] = useState<ExternalConnection | null>(null);
  const [caError, setCaError] = useState<unknown>(null);
  const [caPending, setCaPending] = useState(false);
  const {
    detail,
    discovery,
    discoveryKind,
    includeInternal,
    topicStatus,
    links,
    linksLoading,
    linksError,
    linksComplete,
  } = connections;
  const permissionStamp = session.data?.permissions.join("|") || "";
  useEffect(() => {
    setEditing(null);
    connections.closeDetail();
    setCaFor(null);
    setCaFile(null);
    setConfirm(null);
    setReplacePassword(false);
    setFormError(null);
    setVerificationLoading(false);
    setVerificationError(null);
    setOperationError(null);
    setNotice("");
    setSaving(false);
    setCaPending(false);
    setConfirmPending(false);
    form.resetFields();
  }, [session.version, permissionStamp, form, connections]);

  useEffect(() => {
    void lists.kafkaConnections.load(0);
    void lists.externalConnections.load(0);
  }, [lists]);
  useEffect(() => {
    if (!notice) return;
    const timeout = window.setTimeout(() => setNotice(""), 5000);
    return () => window.clearTimeout(timeout);
  }, [notice]);
  const closeEdit = () => {
    if (saving) return;
    lifecycle.cancel("connection-save");
    setEditing(null);
    form.resetFields();
    setReplacePassword(false);
    setFormError(null);
  };
  const verifyFormState = async () => {
    if (!editing) return;
    setVerificationLoading(true);
    setVerificationError(null);
    const store =
      editing.kind === "kafka"
        ? lists.kafkaConnections
        : lists.externalConnections;
    await store.load();
    setVerificationLoading(false);
    if (store.error) setVerificationError(store.error);
    else setFormError(null);
  };
  const openEdit = (kind: Kind, item?: RecordType) => {
    lifecycle.cancel("connection-save");
    setFormError(null);
    setReplacePassword(false);
    setEditing({ kind, item });
    form.setFieldsValue(
      item
        ? kind === "kafka"
          ? {
              name: item.name,
              bootstrap: (item as KafkaConnection).bootstrap_servers.join("\n"),
            }
          : {
              name: item.name,
              base_url: (item as ExternalConnection).base_url,
              username: (item as ExternalConnection).username,
            }
        : { name: "", bootstrap: "", base_url: "", username: "", password: "" },
    );
  };
  const save = async (values: Record<string, string>) => {
    if (!editing || saving) return;
    const identityVersion = session.version;
    const permissions = session.data?.permissions.join("|") || "";
    const { kind, item } = editing;
    const selectedItem = item?.id;
    let changed = false;
    let submit: () => Promise<
      import("../../features/resources/ConnectionsStore").OperationResult<unknown>
    >;
    if (kind === "kafka") {
      const servers = values.bootstrap
        .split(/[\n,]/)
        .map((s) => s.trim())
        .filter(Boolean);
      if (
        servers.length < 1 ||
        servers.length > 16 ||
        new Set(servers).size !== servers.length ||
        servers.some(
          (s) =>
            !/^(?:\[[\da-fA-F:]+\]|[A-Za-z0-9][A-Za-z0-9.-]*):(?:[1-9]\d{0,4})$/.test(
              s,
            ) || +s.slice(s.lastIndexOf(":") + 1) > 65535,
        )
      ) {
        form.setFields([
          {
            name: "bootstrap",
            errors: [
              "Укажите 1–16 уникальных адресов host:port или [IPv6]:port.",
            ],
          },
        ]);
        return;
      }
      if (item) {
        const patch: KafkaConnectionPatch = {};
        if (values.name.trim() !== item.name) patch.name = values.name.trim();
        if (
          JSON.stringify(servers) !==
          JSON.stringify((item as KafkaConnection).bootstrap_servers)
        )
          patch.bootstrap_servers = servers;
        changed = Object.keys(patch).length > 0;
        submit = () => connections.saveKafka(item.id, patch);
      } else {
        const input: KafkaConnectionInput = {
          name: values.name.trim(),
          bootstrap_servers: servers,
          security_protocol: "PLAINTEXT",
        };
        changed = true;
        submit = () => connections.saveKafka(undefined, input);
      }
    } else {
      try {
        const url = new URL(values.base_url);
        if (
          url.protocol !== "https:" ||
          url.username ||
          url.password ||
          url.search ||
          url.hash
        )
          throw new Error();
      } catch {
        form.setFields([
          {
            name: "base_url",
            errors: [
              "Нужен HTTPS URL без учётных данных, запроса и фрагмента.",
            ],
          },
        ]);
        return;
      }
      const previous = item as ExternalConnection | undefined;
      if (previous) {
        const patch: ExternalConnectionPatch = {};
        if (values.name.trim() !== previous.name)
          patch.name = values.name.trim();
        if (values.base_url !== previous.base_url)
          patch.base_url = values.base_url;
        if (values.username.trim() !== previous.username)
          patch.username = values.username.trim();
        if (replacePassword) patch.password = values.password;
        changed = Object.keys(patch).length > 0;
        submit = () => connections.saveExternal(previous.id, patch);
      } else {
        const input: ExternalConnectionInput = {
          name: values.name.trim(),
          base_url: values.base_url,
          username: values.username.trim(),
          password: values.password,
        };
        changed = true;
        submit = () => connections.saveExternal(undefined, input);
      }
    }
    if (!changed) {
      closeEdit();
      return;
    }
    const execute = async () => {
      setSaving(true);
      setFormError(null);
      try {
        const result = await submit();
        if (result.stale) return;
        if ("error" in result) {
          setFormError(result.error);
          form.setFields(
            validationFields(result.error, {
              name: "name",
              bootstrap_servers: "bootstrap",
              base_url: "base_url",
              username: "username",
              password: "password",
            }),
          );
          throw result.error;
        } else {
          await Promise.all([
            lists.kafkaConnections.load(),
            lists.externalConnections.load(),
          ]);
          setNotice(item ? "Подключение сохранено." : "Подключение создано.");
          setEditing(null);
          form.resetFields();
          setReplacePassword(false);
        }
      } finally {
        setSaving(false);
      }
    };
    if (item && kind === "external") {
      setSaving(true);
      const precheck = await connections.activeExternalCount(
        item.id,
        canReadSources,
      );
      setSaving(false);
      if (
        precheck.stale ||
        !connections.isCurrentPrecheck(precheck.token) ||
        session.version !== identityVersion ||
        (session.data?.permissions.join("|") || "") !== permissions ||
        editing?.item?.id !== selectedItem
      )
        return;
      const activeCount = precheck.count;
      if (activeCount !== 0) {
        setConfirm({
          title: "Изменить внешнее подключение?",
          message:
            activeCount === null
              ? "Не удалось проверить связанные источники. Изменение настроек может выключить их; проверьте состояние после сохранения."
              : `Будут выключены ${activeCount} активных связанных источников. Включить их потребуется вручную.`,
          label: "Сохранить",
          execute,
        });
        return;
      }
    }
    await execute();
  };
  const remove = (kind: Kind, item: RecordType) =>
    setConfirm({
      title: "Удалить подключение?",
      message: `«${item.name}» и все связанные источники будут удалены. Сохранённые локальные события и документы индексера останутся.`,
      label: "Удалить",
      execute: async () => {
        const result = await connections.deleteConnection(kind, item.id);
        if (!result.stale && !("error" in result)) {
          await Promise.all([
            lists.kafkaConnections.load(),
            lists.externalConnections.load(),
          ]);
          setNotice("Подключение удалено.");
        } else if ("error" in result) throw result.error;
      },
    });
  const runConfirm = async () => {
    if (!confirm || confirmPending) return;
    setConfirmPending(true);
    setOperationError(null);
    setVerificationName("");
    try {
      await confirm.execute();
      setConfirm(null);
    } catch (error) {
      setOperationError(error);
      setConfirm(null);
    } finally {
      setConfirmPending(false);
    }
  };
  const test = (kind: Kind, item: RecordType) =>
    void (async () => {
      setOperationError(null);
      setVerificationName(item.name);
      const result = await connections.testConnection(kind, item.id);
      if (result.stale) return;
      if ("error" in result) {
        setOperationError(result.error);
      } else {
        setNotice(
          kind === "kafka"
            ? `Подключение «${item.name}»: проверка успешна — ${result.value.broker_count} брокеров, ${result.value.topic_count} топиков, ${result.value.latency_ms} мс.`
            : `Подключение «${item.name}»: проверка успешна.`,
        );
      }
    })();
  const uploadCa = async () => {
    if (!caFor || !caFile || caPending) return;
    const selectedId = caFor.id;
    const selectedFile = caFile;
    setCaPending(true);
    setCaError(null);
    try {
      if (selectedFile.size < 1 || selectedFile.size > 65536)
        throw new Error("Размер CA должен быть 1–65536 байт.");
      const pem = await selectedFile.text();
      if (caFor?.id !== selectedId) return;
      if (/-----BEGIN [^-]*PRIVATE KEY-----/.test(pem))
        throw new Error("Приватный ключ загружать нельзя.");
      const result = await connections.uploadCa(selectedId, pem);
      if (result.stale) return;
      if ("error" in result) throw result.error;
      await lists.externalConnections.load();
      setNotice("CA сохранён. Связанные источники выключены.");
      setCaFor(null);
      setCaFile(null);
    } catch (error) {
      setCaError(
        error instanceof Error
          ? error
          : new Error("Не удалось прочитать выбранный файл."),
      );
    } finally {
      setCaPending(false);
    }
  };
  const requestCaUpload = async () => {
    if (!caFor || !caFile || caPending) return;
    const identityVersion = session.version;
    const permissions = session.data?.permissions.join("|") || "";
    const selectedId = caFor.id;
    setCaPending(true);
    try {
      const precheck = await connections.activeExternalCount(
        selectedId,
        canReadSources,
      );
      if (
        precheck.stale ||
        !connections.isCurrentPrecheck(precheck.token) ||
        session.version !== identityVersion ||
        (session.data?.permissions.join("|") || "") !== permissions ||
        caFor?.id !== selectedId
      )
        return;
      setCaPending(false);
      const activeCount = precheck.count;
      if (activeCount === 0) {
        await uploadCa();
        return;
      }
      setConfirm({
        title: "Загрузить CA?",
        message:
          activeCount === null
            ? "Не удалось проверить связанные источники. Изменение CA может выключить их; проверьте состояние после сохранения."
            : `Изменение CA выключит ${activeCount} активных связанных источников. Включить их потребуется вручную.`,
        label: "Загрузить",
        execute: uploadCa,
      });
    } finally {
      setCaPending(false);
    }
  };
  const relation = (
    name: string,
    kind: "indices" | "data-streams" | "topics",
  ) => {
    if (!canReadSources)
      return <span className={styles.muted}>Связи недоступны</span>;
    if (linksError)
      return (
        <span className={styles.muted}>
          Связи не загружены. Обновите сведения в сообщении ниже.
        </span>
      );
    if (linksLoading || !linksComplete)
      return <span className={styles.muted}>Связи загружаются…</span>;
    const found = links.filter((s) =>
      s.source_type === "kafka"
        ? kind === "topics" && s.topic_name === name
        : kind !== "topics" &&
          ((s.target_type === (kind === "indices" ? "index" : "data_stream") &&
            targetName(s) === name) ||
            (s.target_type ===
              (kind === "indices" ? "index_pattern" : "data_stream_pattern") &&
              matchPattern(targetName(s), name))),
    );
    return found.length ? (
      found.map((s) => (
        <div key={s.id}>
          <Link
            to={`/sources?kind=${s.source_type}&id=${encodeURIComponent(s.id)}`}
          >
            {s.name}
          </Link>{" "}
          <Tag>{s.is_enabled ? "Включён" : "Выключен"}</Tag>
          {s.source_type === "kafka" && s.is_archived && <Tag>Архивный</Tag>}
        </div>
      ))
    ) : (
      <span className={styles.muted}>Связей нет</span>
    );
  };
  const columns = (kind: Kind) => [
    { title: "Имя", dataIndex: "name", key: "name" },
    {
      title: "Адрес",
      key: "address",
      render: (_: unknown, item: RecordType) =>
        kind === "kafka"
          ? (item as KafkaConnection).bootstrap_servers.join(", ")
          : (item as ExternalConnection).base_url,
    },
    {
      title: "Сведения",
      key: "info",
      render: (_: unknown, item: RecordType) =>
        kind === "kafka"
          ? "Kafka · PLAINTEXT"
          : `Индексер · ${(item as ExternalConnection).username} · пароль ${(item as ExternalConnection).has_password ? "задан" : "не задан"} · CA ${(item as ExternalConnection).has_ca ? "загружен" : "не загружен"}`,
    },
    {
      title: "Создано",
      dataIndex: "created_at",
      key: "created",
      render: (value: string) => new Date(value).toLocaleString("ru-RU"),
    },
    {
      title: "Обновлено",
      dataIndex: "updated_at",
      key: "updated",
      render: (value: string) => new Date(value).toLocaleString("ru-RU"),
    },
    {
      title: "Действия",
      key: "actions",
      render: (_: unknown, item: RecordType) => (
        <div className={styles.actions}>
          <Button
            onClick={() => {
              connections.openDetail(kind, item, canWrite, canReadSources);
            }}
          >
            Наборы
          </Button>
          {(kind === "kafka" || canWrite) && (
            <Button
              loading={lifecycle.isPending(`connection:${item.id}`)}
              onClick={() => void test(kind, item)}
            >
              Проверить
            </Button>
          )}
          {canWrite && (
            <>
              <Button onClick={() => openEdit(kind, item)}>Изменить</Button>
              {kind === "external" && (
                <Button
                  onClick={() => {
                    setCaFor(item as ExternalConnection);
                    setCaError(null);
                  }}
                >
                  CA
                </Button>
              )}
              <Button danger onClick={() => remove(kind, item)}>
                Удалить
              </Button>
            </>
          )}
        </div>
      ),
    },
  ];
  const sections = useMemo(
    () => [
      {
        kind: "kafka" as const,
        label: "Собственные подключения",
        description:
          "Собственные подключения задают доступ приложения к Kafka-брокерам. Указывайте адреса брокеров внутри сети приложения; список топиков можно проверить в наборах данных.",
        store: lists.kafkaConnections,
      },
      {
        kind: "external" as const,
        label: "Внешние подключения",
        description:
          "Внешние подключения задают доступ backend к OpenSearch или Wazuh Indexer. Укажите HTTPS-адрес и учётную запись; сертификат центра сертификации добавляется отдельно, если он нужен для доверия.",
        store: lists.externalConnections,
      },
    ],
    [lists],
  );
  return (
    <main className={styles.page}>
      <div className={styles.pageToolbar}>
        <nav
          aria-label="Категория подключений"
          className={styles.categoryTabs}
          role="tablist"
        >
          {sections.map(({ kind, label }, index) => (
            <span className={styles.categoryTabWrap} key={kind}>
              {index > 0 && (
                <span aria-hidden="true" className={styles.categorySeparator}>
                  /
                </span>
              )}
              <button
                aria-selected={activeKind === kind}
                aria-controls="connections-panel"
                className={styles.categoryTab}
                id={`connections-tab-${kind}`}
                onClick={() => setActiveKind(kind)}
                role="tab"
                type="button"
              >
                {label}
              </button>
            </span>
          ))}
        </nav>
        <Button
          aria-label={`Описание: ${sections.find(({ kind }) => kind === activeKind)?.label}`}
          className={styles.helpButton}
          icon={<InfoCircleOutlined />}
          onClick={() => setHelpOpen(true)}
          type="text"
        />
      </div>
      {notice && (
        <div className={styles.feedback}>
          <Alert
            type="success"
            showIcon
            closable
            message={notice}
            onClose={() => setNotice("")}
          />
        </div>
      )}
      {!!operationError && (
        <div className={styles.feedback}>
          {verificationName && (
            <strong className={styles.feedbackTitle}>
              Проверка подключения «{verificationName}»
            </strong>
          )}
          <AppRequestError
            error={operationError}
            onRetry={() => {
              setOperationError(null);
              void Promise.all([
                lists.kafkaConnections.load(),
                lists.externalConnections.load(),
              ]);
            }}
          />
        </div>
      )}
      {sections
        .filter(({ kind }) => kind === activeKind)
        .map(({ kind, store }) => (
          <section
            aria-labelledby={`connections-tab-${kind}`}
            className={styles.section}
            id="connections-panel"
            key={kind}
            role="tabpanel"
            tabIndex={0}
          >
            <div className={styles.bar}>
              {canWrite && (
                <Button type="primary" onClick={() => openEdit(kind)}>
                  Добавить подключение
                </Button>
              )}
            </div>
            {!!store.error && (
              <AppRequestError
                error={store.error}
                onRetry={() => void store.load()}
              />
            )}
            <div className={styles.scroll}>
              <Table
                rowKey="id"
                loading={store.loading}
                columns={columns(kind)}
                dataSource={store.items}
                className={styles.connectionsTable}
                tableLayout="auto"
                pagination={false}
                locale={{ emptyText: "Подключений нет" }}
                scroll={{ x: 850 }}
              />
            </div>
            <div className={styles.tablePagination}>
              <Pagination
                current={Math.floor(store.offset / 50) + 1}
                pageSize={50}
                total={store.total}
                onChange={(page) => void store.load((page - 1) * 50)}
                showSizeChanger={false}
              />
            </div>
          </section>
        ))}
      <Modal
        open={helpOpen}
        title={sections.find(({ kind }) => kind === activeKind)?.label}
        onCancel={() => setHelpOpen(false)}
        footer={null}
      >
        <p className={styles.helpText}>
          {sections.find(({ kind }) => kind === activeKind)?.description}
        </p>
      </Modal>
      <Modal
        open={!!editing && !confirm}
        title={editing?.item ? "Изменить подключение" : "Новое подключение"}
        onCancel={closeEdit}
        footer={null}
      >
        <Form form={form} layout="vertical" onFinish={(v) => void save(v)}>
          <Form.Item
            name="name"
            label="Имя"
            rules={[{ required: true, whitespace: true, max: 200 }]}
          >
            <Input maxLength={200} />
          </Form.Item>
          {editing?.kind === "kafka" ? (
            <>
              <Form.Item
                name="bootstrap"
                label="Адреса брокеров, по одному на строку"
                rules={[{ required: true }]}
              >
                <Input.TextArea rows={3} />
              </Form.Item>
              <p className={styles.formHelp}>
                Адрес внутри Compose обычно kafka:9092. Браузер к брокеру не
                подключается.
              </p>
            </>
          ) : (
            <>
              {editing?.item && (
                <Alert
                  type="warning"
                  message="Изменение настроек внешнего подключения выключит связанные источники. После сохранения проверьте их состояние."
                />
              )}
              <Form.Item
                name="base_url"
                label="HTTPS URL индексера"
                rules={[{ required: true, max: 2048 }]}
              >
                <Input />
              </Form.Item>
              <Form.Item
                name="username"
                label="Пользователь"
                rules={[{ required: true, whitespace: true, max: 200 }]}
              >
                <Input />
              </Form.Item>
              {editing?.item && (
                <Switch
                  checked={replacePassword}
                  onChange={setReplacePassword}
                  aria-label="Заменить пароль"
                />
              )}
              <span> {editing?.item ? "Заменить пароль" : "Пароль"}</span>
              {(!editing?.item || replacePassword) && (
                <Form.Item
                  name="password"
                  label="Пароль"
                  rules={[{ required: true }]}
                >
                  <Input.Password autoComplete="new-password" />
                </Form.Item>
              )}
            </>
          )}
          {!!formError && (
            <AppRequestError
              error={formError}
              onRetry={() => void verifyFormState()}
            />
          )}
          {verificationLoading && <span>Сверка актуального состояния…</span>}
          {!!verificationError && (
            <AppRequestError
              error={verificationError}
              onRetry={() => void verifyFormState()}
            />
          )}
          <div className={styles.actions}>
            <Button onClick={closeEdit}>Отмена</Button>
            <Button type="primary" htmlType="submit" loading={saving}>
              Сохранить
            </Button>
          </div>
        </Form>
      </Modal>
      <Modal
        open={!!detail}
        title={`Наборы данных для подключения «${detail?.item.name || ""}»`}
        onCancel={() => connections.closeDetail()}
        footer={null}
        width={720}
      >
        <div className={styles.detail}>
          {detail?.kind === "external" && !canWrite ? (
            <Alert
              type="info"
              message="Для просмотра наборов требуются права изменения подключений."
            />
          ) : (
            <>
              <div className={styles.datasetControls}>
                {detail?.kind === "external" ? (
                  <>
                    <Button
                      type={discoveryKind === "indices" ? "primary" : "default"}
                      onClick={() =>
                        connections.setDiscoveryKind("indices", canWrite)
                      }
                    >
                      Индексы
                    </Button>
                    <Button
                      type={
                        discoveryKind === "data-streams" ? "primary" : "default"
                      }
                      onClick={() =>
                        connections.setDiscoveryKind("data-streams", canWrite)
                      }
                    >
                      Потоки данных
                    </Button>
                  </>
                ) : (
                  <Switch
                    checked={includeInternal}
                    onChange={(value) =>
                      connections.setIncludeInternal(value, canWrite)
                    }
                    checkedChildren="Внутренние"
                    unCheckedChildren="Обычные"
                  />
                )}
                {detail?.kind === "kafka" && (
                  <Tooltip title="Внутренние наборы — служебные топики Kafka, которые не относятся к пользовательским источникам.">
                    <InfoCircleOutlined
                      aria-label="Зачем скрывать служебные топики"
                      tabIndex={0}
                    />
                  </Tooltip>
                )}
                <Button
                  className={styles.datasetRefresh}
                  onClick={() => void connections.refresh(canWrite)}
                  disabled={discovery.loading}
                >
                  Обновить наборы
                </Button>
              </div>
              <div className={styles.datasetContent}>
                {discovery.loading && (
                  <div
                    aria-live="polite"
                    className={styles.datasetLoading}
                    role="status"
                  >
                    <Spin aria-label="Загрузка наборов" size="small" />
                    <span>Загрузка наборов…</span>
                  </div>
                )}
                {!!linksError && (
                  <AppRequestError
                    error={linksError}
                    onRetry={() => void connections.refresh(canWrite)}
                  />
                )}
                {!!discovery.error && (
                  <AppRequestError
                    error={discovery.error}
                    onRetry={() => void connections.loadLinks()}
                  />
                )}
                {!discovery.loading && !discovery.error && (
                  <>
                    <Typography.Text>
                      Найдено: {discovery.total}
                    </Typography.Text>
                    <div className={styles.list}>
                      {discovery.names.map((name) => (
                        <div className={styles.listItem} key={name}>
                          <strong>{name}</strong>
                          {detail?.kind === "kafka" && topicStatus[name] && (
                            <span>
                              {" "}
                              · {topicStatus[name].partitions} разделов ·{" "}
                              {topicStatus[name].registered
                                ? "Зарегистрирован в текущем поколении"
                                : "Не зарегистрирован в текущем поколении"}
                            </span>
                          )}
                          <div>
                            {relation(
                              name,
                              detail?.kind === "kafka"
                                ? "topics"
                                : discoveryKind === "data-streams"
                                  ? "data-streams"
                                  : "indices",
                            )}
                          </div>
                        </div>
                      ))}
                      {discovery.total === 0 && "Наборов нет"}
                    </div>
                    {detail?.kind === "external" &&
                      discovery.names.length < discovery.total && (
                        <Button
                          loading={lifecycle.isPending(
                            "connection-discovery-more",
                          )}
                          onClick={() => void connections.loadMoreDiscovery()}
                        >
                          Загрузить ещё
                        </Button>
                      )}
                  </>
                )}
              </div>
            </>
          )}
        </div>
      </Modal>
      <Modal
        open={!!caFor && !confirm}
        title={`CA для ${caFor?.name || ""}`}
        onCancel={() => {
          if (!caPending) {
            lifecycle.cancel("ca-context");
            lifecycle.cancel("ca-upload");
            setCaFor(null);
            setCaFile(null);
            setCaError(null);
          }
        }}
        footer={null}
      >
        <p>
          Публичный общедоверенный CA обычно не нужно загружать. Корпоративный
          CA применяется только к этому подключению. Изменение CA выключит
          связанные источники.
        </p>
        <Input
          type="file"
          accept=".pem,.crt"
          onChange={(event) => setCaFile(event.target.files?.[0] || null)}
        />
        {!!caError &&
          (caError instanceof ApiError ? (
            <AppRequestError error={caError} />
          ) : (
            <Alert
              type="error"
              message={
                caError instanceof Error
                  ? caError.message
                  : "Не удалось загрузить CA."
              }
            />
          ))}
        <div className={styles.actions}>
          <Button
            loading={caPending}
            disabled={!caFile}
            onClick={() => void requestCaUpload()}
          >
            Загрузить CA
          </Button>
          {caFor?.has_ca && (
            <Button
              danger
              onClick={() => {
                const item = caFor;
                setConfirm({
                  title: "Удалить CA?",
                  message: "CA будет удалён. Связанные источники выключатся.",
                  label: "Удалить CA",
                  execute: async () => {
                    const result = await connections.deleteCa(item.id);
                    if (!result.stale && !("error" in result)) {
                      await lists.externalConnections.load();
                      setCaFor(null);
                      setNotice("CA удалён. Связанные источники выключены.");
                    } else if ("error" in result) throw result.error;
                  },
                });
              }}
            >
              Удалить CA
            </Button>
          )}
        </div>
      </Modal>
      <ConfirmationDialog
        open={!!confirm}
        pending={confirmPending}
        title={confirm?.title || ""}
        message={confirm?.message || ""}
        confirmText={confirm?.label || "Продолжить"}
        danger={confirm?.label.startsWith("Удалить")}
        onCancel={() => setConfirm(null)}
        onConfirm={() => void runConfirm()}
      >
        {!!operationError && <AppRequestError error={operationError} />}
      </ConfirmationDialog>
    </main>
  );
});
