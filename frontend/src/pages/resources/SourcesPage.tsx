import { useEffect, useMemo, useRef, useState } from "react";
import { InfoCircleOutlined } from "@ant-design/icons";
import {
  Alert,
  Button,
  Form,
  Input,
  Modal,
  Pagination,
  Select,
  Table,
  Tag,
} from "antd";
import { observer } from "mobx-react-lite";
import { useSearchParams } from "react-router-dom";
import {
  type ExternalSource,
  type ExternalSourceInput,
  type ExternalSourcePatch,
  type KafkaSource,
  type KafkaSourceInput,
  type KafkaSourcePatch,
  type TargetType,
  externalTargetPayload,
  targetLabel,
  targetName,
  validationFields,
} from "../../app/api/resources";
import { ApiError } from "../../app/api/client";
import { AppRequestError } from "../../app/ui/AppRequestError";
import { ConfirmationDialog } from "../../shared/ui/ConfirmationDialog";
import { useResources } from "../../features/resources/useResources";
import styles from "./Resources.module.css";

type Kind = "kafka" | "external";
type Source = KafkaSource | ExternalSource;
type Edit = { kind: Kind; source?: Source };
type Confirm = {
  title: string;
  message: string;
  label: string;
  execute: () => Promise<void>;
  verify?: () => Promise<boolean>;
};
export const SourcesPage = observer(() => {
  const { lists, lifecycle, options, sources, session } = useResources();
  const [params] = useSearchParams();
  const requestedId = params.get("id");
  const selectedId =
    requestedId &&
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(
      requestedId,
    )
      ? requestedId
      : null;
  const selectedKind =
    params.get("kind") === "kafka"
      ? "kafka"
      : params.get("kind") === "external"
        ? "external"
        : null;
  const canWrite = session.has("sources.write");
  const [form] = Form.useForm();
  const permissionStamp = session.data?.permissions.join("|") || "";
  const [editing, setEditing] = useState<Edit | null>(null);
  const [formError, setFormError] = useState<unknown>(null);
  const [verificationLoading, setVerificationLoading] = useState(false);
  const [verificationError, setVerificationError] = useState<unknown>(null);
  const [operationError, setOperationError] = useState<unknown>(null);
  const [notice, setNotice] = useState("");
  const [activeKind, setActiveKind] = useState<Kind>("kafka");
  const [helpOpen, setHelpOpen] = useState(false);
  const [confirm, setConfirm] = useState<Confirm | null>(null);
  const confirmRef = useRef(confirm);
  confirmRef.current = confirm;
  const pendingConfirmRef = useRef<symbol | null>(null);
  const [confirmPending, setConfirmPending] = useState(false);
  const [confirmNeedsRefresh, setConfirmNeedsRefresh] = useState(false);
  const connectionOptions = options.connections.items;
  const normalizers = options.normalizers.items;
  const [normalizerFor, setNormalizerFor] = useState<KafkaSource | null>(null);
  const [normalizerChoice, setNormalizerChoice] = useState<string | null>(null);
  const [normalizerError, setNormalizerError] = useState<unknown>(null);
  const {
    focused: focusedSource,
    focusedError,
    knownConnections,
    knownNormalizers,
    topics,
    targets,
    targetTotal,
    targetError,
    targetLoading,
    selectedNormalizerError,
  } = sources;
  useEffect(() => {
    setEditing(null);
    setNormalizerFor(null);
    setConfirm(null);
    sources.clear();
    options.connections.cancel();
    options.normalizers.cancel();
    setNotice("");
    setOperationError(null);
    setFormError(null);
    setVerificationLoading(false);
    setVerificationError(null);
    setNormalizerError(null);
    setConfirmPending(false);
    pendingConfirmRef.current = null;
    setConfirmNeedsRefresh(false);
    form.resetFields();
  }, [session.version, permissionStamp, form, options, sources]);
  const connectionId = Form.useWatch("connection_id", form) as
    | string
    | undefined;
  const targetType = Form.useWatch("target_type", form) as
    | TargetType
    | undefined;
  useEffect(() => {
    void lists.kafkaSources.load(0);
    void lists.externalSources.load(0);
    if (session.has("connections.read")) {
      void lists.kafkaConnections.load(0);
      void lists.externalConnections.load(0);
    }
  }, [lists, session]);
  useEffect(() => {
    if (!notice) return;
    const timeout = window.setTimeout(() => setNotice(""), 5000);
    return () => window.clearTimeout(timeout);
  }, [notice]);
  useEffect(() => {
    void sources.loadFocused(selectedId, selectedKind);
  }, [selectedId, selectedKind, session.version, sources]);
  const visibleRefs = `${lists.kafkaSources.items.map((s) => `k:${s.connection_id}:${s.normalizer_id || ""}`).join("|")}|${lists.externalSources.items.map((s) => `e:${s.external_connection_id}`).join("|")}`;
  useEffect(() => {
    void sources.loadKnown(
      lists.kafkaSources.items,
      lists.externalSources.items,
      session.has("connections.read"),
      session.has("normalizers.read"),
    );
  }, [visibleRefs, lists, session, sources]);
  useEffect(() => {
    if (!editing || !session.has("connections.read")) {
      options.connections.cancel();
      return;
    }
    options.connections.configure(
      `source-options:${editing.kind}`,
      editing.kind === "kafka"
        ? (offset, signal) => sources.connectionPage("kafka", offset, signal)
        : (offset, signal) =>
            sources.connectionPage("external", offset, signal),
    );
    void options.connections.load();
    return () => options.connections.cancel();
  }, [editing, session, options, sources]);
  useEffect(() => {
    if (!editing) {
      sources.clearTargets();
      return;
    }
    void sources.loadTargets(
      editing.kind,
      connectionId,
      targetType,
      session.has("connections.write"),
    );
  }, [editing, connectionId, targetType, session, sources]);
  const connectionName = (id: string, kind: Kind) =>
    (kind === "kafka"
      ? lists.kafkaConnections.items
      : lists.externalConnections.items
    ).find((c) => c.id === id)?.name ||
    knownConnections[id]?.name ||
    "Подключение недоступно";
  const cancelFormRequests = () => {
    for (const context of [
      "source-options",
      "source-targets",
      "source-save",
      "normalizer-options",
      "selected-normalizer",
      "normalizer-mutation",
    ])
      lifecycle.cancel(context);
    options.connections.cancel();
    options.normalizers.cancel();
    sources.clearTargets();
  };
  const openEdit = (kind: Kind, source?: Source) => {
    cancelFormRequests();
    setEditing({ kind, source });
    setFormError(null);
    form.setFieldsValue(
      source
        ? source.source_type === "kafka"
          ? {
              name: source.name,
              connection_id: source.connection_id,
              topic_name: source.topic_name,
            }
          : {
              name: source.name,
              connection_id: source.external_connection_id,
              target_type: source.target_type,
              target: targetName(source),
            }
        : {
            name: "",
            connection_id: undefined,
            topic_name: undefined,
            target_type: "index",
            target: "",
          },
    );
  };
  const closeEdit = () => {
    if (
      lifecycle.isPending("source-create") ||
      (!!editing?.source && lifecycle.isPending(`source:${editing.source.id}`))
    )
      return;
    cancelFormRequests();
    setEditing(null);
    form.resetFields();
    setFormError(null);
  };
  const verifyFormState = async () => {
    if (!editing) return;
    setVerificationLoading(true);
    setVerificationError(null);
    const store =
      editing.kind === "kafka" ? lists.kafkaSources : lists.externalSources;
    await store.load();
    setVerificationLoading(false);
    if (store.error) setVerificationError(store.error);
    else setFormError(null);
  };
  const save = async (values: {
    name: string;
    connection_id: string;
    topic_name?: string;
    target_type?: TargetType;
    target?: string;
  }) => {
    if (
      !editing ||
      lifecycle.isPending("source-create") ||
      (!!editing.source && lifecycle.isPending(`source:${editing.source.id}`))
    )
      return;
    const source = editing.source;
    const kind = editing.kind;
    let externalConfigurationChanged = false;
    let submit: () => Promise<
      import("../../features/resources/ConnectionsStore").OperationResult<unknown>
    >;
    if (kind === "kafka") {
      const current = source as KafkaSource | undefined;
      if (current) {
        const patch: KafkaSourcePatch = {};
        if (values.name.trim() !== current.name)
          patch.name = values.name.trim();
        if (
          !current.is_enabled &&
          current.connection_id !== values.connection_id
        )
          patch.connection_id = values.connection_id;
        if (!current.is_enabled && current.topic_name !== values.topic_name)
          patch.topic_name = values.topic_name;
        if (!Object.keys(patch).length) {
          closeEdit();
          return;
        }
        submit = () => sources.saveKafka(current.id, patch);
      } else {
        const input: KafkaSourceInput = {
          name: values.name.trim(),
          connection_id: values.connection_id,
          topic_name: values.topic_name || "",
        };
        submit = () => sources.saveKafka(undefined, input);
      }
    } else {
      const current = source as ExternalSource | undefined;
      const targetType = values.target_type;
      const target = values.target?.trim() || "";
      if (!targetType) return;
      if (current) {
        const patch: ExternalSourcePatch = {};
        if (values.name.trim() !== current.name)
          patch.name = values.name.trim();
        if (current.external_connection_id !== values.connection_id) {
          patch.external_connection_id = values.connection_id;
          externalConfigurationChanged = true;
        }
        const targetChanged =
          current.target_type !== targetType || targetName(current) !== target;
        if (targetChanged) {
          externalConfigurationChanged = true;
          Object.assign(patch, externalTargetPayload(targetType, target));
        }
        if (!Object.keys(patch).length) {
          closeEdit();
          return;
        }
        submit = () => sources.saveExternal(current.id, patch);
      } else {
        const input: ExternalSourceInput = {
          name: values.name.trim(),
          external_connection_id: values.connection_id,
          ...externalTargetPayload(targetType, target),
        };
        externalConfigurationChanged = true;
        submit = () => sources.saveExternal(undefined, input);
      }
    }
    const execute = async () => {
      setFormError(null);
      const result = await submit();
      if (result.stale) return;
      if ("error" in result) {
        setFormError(result.error);
        form.setFields(
          validationFields(result.error, {
            name: "name",
            connection_id: "connection_id",
            external_connection_id: "connection_id",
            topic_name: "topic_name",
            target_type: "target_type",
            index_name: "target",
            index_pattern: "target",
            data_stream_name: "target",
            data_stream_pattern: "target",
          }),
        );
        throw result.error;
      }
      await (
        kind === "kafka" ? lists.kafkaSources : lists.externalSources
      ).load();
      setNotice(
        source
          ? "Источник сохранён."
          : "Источник создан выключенным. Его можно включить после настройки.",
      );
      setEditing(null);
      form.resetFields();
    };
    if (
      source?.is_enabled &&
      kind === "external" &&
      externalConfigurationChanged
    ) {
      setConfirmNeedsRefresh(false);
      setConfirm({
        title: "Изменить активный источник?",
        message:
          "Смена подключения или цели выключит источник. Включить его потребуется вручную.",
        label: "Сохранить",
        execute,
        verify: () => lists.externalSources.load(0),
      });
      return;
    }
    await execute();
  };
  const mutate = async (
    kind: Kind,
    source: Source,
    action: "enable" | "disable" | "delete",
  ): Promise<void> => {
    setOperationError(null);
    const result = await sources.action(kind, source.id, action);
    if (result.stale) return;
    if ("error" in result) {
      setOperationError(result.error);
      throw result.error;
    }
    const refreshed = await (
      kind === "kafka" ? lists.kafkaSources : lists.externalSources
    ).load();
    if (refreshed === false) {
      setOperationError(
        new Error("Действие выполнено, но список не удалось обновить."),
      );
      setConfirm(null);
      return;
    }
    setNotice(
      action === "enable"
        ? "Источник включён. Это не подтверждает здоровье обработчика."
        : action === "disable"
          ? "Источник выключен."
          : "Источник удалён. Сохранённая история не удалена.",
    );
  };
  const requestAction = (
    kind: Kind,
    source: Source,
    action: "enable" | "disable" | "delete",
  ) => {
    if (action === "enable") {
      void mutate(kind, source, action);
      return;
    }
    setConfirm({
      title: action === "disable" ? "Выключить источник?" : "Удалить источник?",
      message:
        action === "disable"
          ? `Получение данных «${source.name}» прекратится. Сохранённые события останутся.`
          : `Источник «${source.name}» будет удалён. Сохранённые локальные события и документы индексера останутся.`,
      label: action === "disable" ? "Выключить" : "Удалить",
      execute: () => mutate(kind, source, action),
      verify: async () =>
        (kind === "kafka" ? lists.kafkaSources : lists.externalSources).load(0),
    });
    setConfirmNeedsRefresh(false);
  };
  const runConfirm = async () => {
    if (!confirm || confirmPending) return;
    const activeConfirm = confirm;
    const token = Symbol("source-confirm");
    pendingConfirmRef.current = token;
    setConfirmPending(true);
    setOperationError(null);
    try {
      await activeConfirm.execute();
      if (confirmRef.current === activeConfirm) {
        setConfirm(null);
        setConfirmNeedsRefresh(false);
      }
    } catch (error) {
      if (confirmRef.current === activeConfirm) {
        setOperationError(error);
        setConfirmNeedsRefresh(
          !(error instanceof ApiError) ||
            error.status === 0 ||
            error.status >= 500,
        );
      }
    } finally {
      if (pendingConfirmRef.current === token) {
        pendingConfirmRef.current = null;
        setConfirmPending(false);
      }
    }
  };
  const verifyConfirmOutcome = async () => {
    if (!confirm?.verify || !confirmNeedsRefresh) return;
    const loaded = await confirm.verify();
    if (loaded) {
      setConfirmNeedsRefresh(false);
      setOperationError(null);
    } else {
      setOperationError(new Error("Не удалось обновить состояние источника."));
    }
  };
  const loadNormalizers = (sourceId: string) => {
    options.normalizers.configure(
      `normalizer-options:${sourceId}`,
      (offset, signal) => sources.normalizerPage(offset, signal),
    );
    return options.normalizers.load();
  };
  const setNormalizer = async (choice: string | null) => {
    if (!normalizerFor) return;
    const source = normalizerFor;
    setNormalizerError(null);
    const result = await sources.assignNormalizer(source.id, choice);
    if (result.stale) return;
    if ("error" in result) {
      setNormalizerError(result.error);
    } else {
      await lists.kafkaSources.load();
      setNotice(
        choice ? "Нормализатор назначен." : "Назначение нормализатора снято.",
      );
      setNormalizerFor(null);
    }
  };
  const columns = (kind: Kind) => [
    { title: "Имя", dataIndex: "name", key: "name" },
    {
      title: "Подключение",
      key: "connection",
      render: (_: unknown, item: Source) =>
        connectionName(
          kind === "kafka"
            ? (item as KafkaSource).connection_id
            : (item as ExternalSource).external_connection_id,
          kind,
        ),
    },
    {
      title: "Цель",
      key: "target",
      render: (_: unknown, item: Source) =>
        kind === "kafka"
          ? (item as KafkaSource).topic_name
          : `${targetLabel[(item as ExternalSource).target_type]}: ${targetName(item as ExternalSource)}`,
    },
    {
      title: "Состояние",
      key: "state",
      render: (_: unknown, item: Source) => (
        <>
          <Tag color={item.is_enabled ? "green" : "default"}>
            {item.is_enabled ? "Включён" : "Выключен"}
          </Tag>
          {item.source_type === "kafka" && (
            <>
              {item.is_archived && <Tag>Архивный</Tag>}
              <div>
                Нормализатор:{" "}
                {item.normalizer_id
                  ? knownNormalizers[item.normalizer_id]?.name ||
                    (Object.prototype.hasOwnProperty.call(
                      knownNormalizers,
                      item.normalizer_id,
                    )
                      ? "Сведения получить не удалось"
                      : session.has("normalizers.read")
                        ? "Загрузка сведений…"
                        : "Сведения недоступны")
                  : "не назначен"}
              </div>
            </>
          )}
        </>
      ),
    },
    {
      title: "Действия",
      key: "actions",
      render: (_: unknown, item: Source) =>
        canWrite && (
          <div className={styles.actions}>
            {!(item.source_type === "kafka" && item.is_archived) && (
              <>
                <Button onClick={() => openEdit(kind, item)}>Изменить</Button>
                {kind === "kafka" && (
                  <Button
                    disabled={
                      item.is_enabled || !session.has("normalizers.read")
                    }
                    title={
                      !session.has("normalizers.read")
                        ? "Нужны права просмотра нормализаторов"
                        : undefined
                    }
                    onClick={() => {
                      const source = item as KafkaSource;
                      lifecycle.cancel("normalizer-options");
                      lifecycle.cancel("normalizer-mutation");
                      options.normalizers.cancel();
                      setNormalizerError(null);
                      setNormalizerFor(source);
                      setNormalizerChoice(source.normalizer_id);
                      void loadNormalizers(source.id);
                      if (
                        source.normalizer_id &&
                        !knownNormalizers[source.normalizer_id]
                      )
                        void sources.readSelectedNormalizer(
                          source.normalizer_id,
                        );
                    }}
                  >
                    Нормализатор
                  </Button>
                )}
                {item.is_enabled ? (
                  <Button
                    loading={lifecycle.isPending(`source:${item.id}`)}
                    onClick={() => requestAction(kind, item, "disable")}
                  >
                    Выключить
                  </Button>
                ) : (
                  <Button
                    loading={lifecycle.isPending(`source:${item.id}`)}
                    disabled={
                      item.source_type === "kafka" &&
                      (!item.normalizer_id ||
                        knownNormalizers[item.normalizer_id]?.rule_status ===
                          "legacy_incompatible")
                    }
                    title={
                      item.source_type === "kafka" &&
                      (!item.normalizer_id ||
                        knownNormalizers[item.normalizer_id]?.rule_status ===
                          "legacy_incompatible")
                        ? "Сначала назначьте готовый совместимый нормализатор"
                        : undefined
                    }
                    onClick={() => requestAction(kind, item, "enable")}
                  >
                    Включить
                  </Button>
                )}
              </>
            )}
            <Button
              danger
              loading={lifecycle.isPending(`source:${item.id}`)}
              onClick={() => requestAction(kind, item, "delete")}
            >
              Удалить
            </Button>
          </div>
        ),
    },
  ];
  const sections = useMemo(
    () => [
      {
        kind: "kafka" as const,
        label: "Собственные источники",
        description:
          "Собственный источник связывает топик Kafka с подключением и нормализатором. Нормализатор используется для преобразования сообщений при дальнейшей обработке.",
        store: lists.kafkaSources,
      },
      {
        kind: "external" as const,
        label: "Внешние источники",
        description:
          "Внешний источник задаёт конкретный индекс, маску индексов, поток данных или маску потоков во внешнем OpenSearch или Wazuh Indexer. Доступность цели проверяется подключением.",
        store: lists.externalSources,
      },
    ],
    [lists],
  );
  const savePending =
    lifecycle.isPending("source-create") ||
    (!!editing?.source && lifecycle.isPending(`source:${editing.source.id}`));
  return (
    <main className={styles.page}>
      {focusedSource && (
        <Alert
          type="info"
          message={`Выбран источник «${focusedSource.name}» — ${focusedSource.is_enabled ? "включён" : "выключен"}. ${focusedSource.source_type === "kafka" ? focusedSource.topic_name : targetName(focusedSource)}`}
        />
      )}
      {!!focusedError && <AppRequestError error={focusedError} />}
      <div className={styles.pageToolbar}>
        <nav
          aria-label="Категория источников"
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
                aria-controls="sources-panel"
                className={styles.categoryTab}
                id={`sources-tab-${kind}`}
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
          <AppRequestError
            error={operationError}
            onRetry={() => {
              setOperationError(null);
              void Promise.all([
                lists.kafkaSources.load(),
                lists.externalSources.load(),
              ]);
            }}
          />
        </div>
      )}
      {!session.has("connections.read") && (
        <Alert
          type="info"
          message="Для выбора подключений требуются права просмотра подключений."
        />
      )}
      {sections
        .filter(({ kind }) => kind === activeKind)
        .map(({ kind, store }) => (
          <section
            aria-labelledby={`sources-tab-${kind}`}
            className={styles.section}
            id="sources-panel"
            key={kind}
            role="tabpanel"
            tabIndex={0}
          >
            <div className={styles.bar}>
              {canWrite && session.has("connections.read") && (
                <Button type="primary" onClick={() => openEdit(kind)}>
                  Добавить источник
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
                rowClassName={(item: Source) =>
                  item.id === selectedId ? "ant-table-row-selected" : ""
                }
                columns={columns(kind)}
                dataSource={store.items}
                className={styles.sourcesTable}
                tableLayout="auto"
                loading={store.loading}
                pagination={false}
                locale={{ emptyText: "Источников нет" }}
                scroll={{ x: 850 }}
              />
            </div>
            <div className={styles.tablePagination}>
              <Pagination
                current={Math.floor(store.offset / 50) + 1}
                pageSize={50}
                total={store.total}
                showSizeChanger={false}
                onChange={(page) => void store.load((page - 1) * 50)}
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
        title={editing?.source ? "Изменить источник" : "Новый источник"}
        onCancel={closeEdit}
        footer={null}
      >
        <Form
          form={form}
          layout="vertical"
          onFinish={(values) => void save(values)}
        >
          {editing?.source?.source_type === "kafka" &&
            editing.source.is_enabled && (
              <Alert
                type="info"
                message="Для смены подключения или топика сначала выключите источник. Пока доступно только переименование."
              />
            )}
          <Form.Item
            label="Имя"
            name="name"
            rules={[{ required: true, whitespace: true, max: 200 }]}
          >
            <Input maxLength={200} />
          </Form.Item>
          <Form.Item
            label="Подключение"
            name="connection_id"
            rules={[{ required: true }]}
          >
            <Select
              loading={options.connections.loading}
              disabled={
                editing?.source?.source_type === "kafka" &&
                editing.source.is_enabled
              }
              showSearch
              optionFilterProp="label"
              onChange={() =>
                form.setFieldsValue({
                  topic_name: undefined,
                  target: undefined,
                })
              }
              options={[
                ...connectionOptions,
                ...(editing?.source &&
                !connectionOptions.some(
                  (c) =>
                    c.id ===
                    (editing.source?.source_type === "kafka"
                      ? editing.source.connection_id
                      : editing.source?.external_connection_id),
                )
                  ? [
                      {
                        id:
                          editing.source.source_type === "kafka"
                            ? editing.source.connection_id
                            : editing.source.external_connection_id,
                        name:
                          knownConnections[
                            editing.source.source_type === "kafka"
                              ? editing.source.connection_id
                              : editing.source.external_connection_id
                          ]?.name ||
                          `Подключение недоступно (${editing.source.source_type === "kafka" ? editing.source.connection_id : editing.source.external_connection_id})`,
                      },
                    ]
                  : []),
              ].map((c) => ({ value: c.id, label: c.name }))}
            />
          </Form.Item>
          {connectionOptions.length < options.connections.total && (
            <Button
              loading={options.connections.loading}
              onClick={() => void options.connections.loadMore()}
            >
              Загрузить ещё подключения
            </Button>
          )}
          {!!options.connections.error && (
            <AppRequestError
              error={options.connections.error}
              onRetry={() => void options.connections.retry()}
            />
          )}
          {editing?.kind === "kafka" ? (
            <Form.Item
              label="Топик"
              name="topic_name"
              rules={[{ required: true, whitespace: true, max: 500 }]}
            >
              <Select
                disabled={
                  editing.source?.source_type === "kafka" &&
                  editing.source.is_enabled
                }
                showSearch
                optionFilterProp="label"
                options={[
                  ...new Set([
                    ...topics,
                    ...(editing.source?.source_type === "kafka"
                      ? [editing.source.topic_name]
                      : []),
                  ]),
                ].map((name) => ({ value: name, label: name }))}
              />
            </Form.Item>
          ) : (
            <>
              <Form.Item
                label="Тип цели"
                name="target_type"
                rules={[{ required: true }]}
              >
                <Select
                  onChange={() => form.setFieldValue("target", undefined)}
                  options={(Object.keys(targetLabel) as TargetType[]).map(
                    (value) => ({ value, label: targetLabel[value] }),
                  )}
                />
              </Form.Item>
              {targetType?.endsWith("pattern") ? (
                <>
                  <p className={styles.formHelp}>
                    Маска задаётся вручную: * — любое число символов, ? — один
                    символ. Совпадения в discovery не подтверждают доступность
                    цели.
                  </p>
                  <Form.Item
                    label="Маска"
                    name="target"
                    rules={[
                      {
                        required: true,
                        pattern: /^[a-z0-9][a-z0-9._*?-]*[*?][a-z0-9._*?-]*$/,
                        message:
                          "Маска должна содержать * или ? и допустимые символы.",
                      },
                    ]}
                  >
                    <Input />
                  </Form.Item>
                </>
              ) : (
                <>
                  <Form.Item
                    label="Обнаруженное имя"
                    name="target"
                    rules={[{ required: true }]}
                  >
                    <Select
                      loading={targetLoading}
                      showSearch
                      optionFilterProp="label"
                      options={[
                        ...new Set([
                          ...targets,
                          ...(editing?.source?.source_type === "external" &&
                          editing.source.target_type === targetType
                            ? [targetName(editing.source)]
                            : []),
                        ]),
                      ].map((name) => ({
                        value: name,
                        label: name,
                      }))}
                    />
                  </Form.Item>
                  <p className={styles.formHelp}>
                    Поиск выполняется среди уже загруженных вариантов. Для
                    продолжения списка используйте кнопку ниже.
                  </p>
                  {targets.length < targetTotal && (
                    <Button
                      loading={lifecycle.isPending("source-target-more")}
                      onClick={() => {
                        if (connectionId && targetType)
                          void sources.loadMoreTargets(
                            connectionId,
                            targetType,
                          );
                      }}
                    >
                      Загрузить ещё цели
                    </Button>
                  )}
                </>
              )}
              {!session.has("connections.write") && (
                <Alert
                  type="info"
                  message="Для выбора точной внешней цели нужно право изменения подключений."
                />
              )}
            </>
          )}
          {!!targetError && (
            <AppRequestError
              error={targetError}
              onRetry={() => {
                const current = form.getFieldsValue();
                form.setFieldsValue({ connection_id: undefined });
                queueMicrotask(() => form.setFieldsValue(current));
              }}
            />
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
            <Button
              disabled={savePending}
              htmlType="submit"
              loading={savePending}
              type="primary"
            >
              Сохранить
            </Button>
          </div>
        </Form>
      </Modal>
      <Modal
        open={!!normalizerFor}
        title="Назначить нормализатор"
        onCancel={() => {
          if (
            !normalizerFor ||
            lifecycle.isPending(`source:${normalizerFor.id}`)
          )
            return;
          lifecycle.cancel("normalizer-options");
          lifecycle.cancel("selected-normalizer");
          setNormalizerFor(null);
        }}
        maskClosable={
          !normalizerFor || !lifecycle.isPending(`source:${normalizerFor.id}`)
        }
        keyboard={
          !normalizerFor || !lifecycle.isPending(`source:${normalizerFor.id}`)
        }
        footer={null}
      >
        <p>
          Нормализатор назначается собственному источнику Kafka и преобразует
          сообщения топика в структуру, используемую системой.
        </p>
        <Select
          className={styles.fullWidth}
          loading={options.normalizers.loading}
          value={normalizerChoice}
          onChange={setNormalizerChoice}
          options={[
            { value: "", label: "Без нормализатора" },
            ...[
              ...(normalizerChoice &&
              knownNormalizers[normalizerChoice] &&
              !normalizers.some((item) => item.id === normalizerChoice)
                ? [knownNormalizers[normalizerChoice]]
                : []),
              ...normalizers,
            ].map((n) => ({
              value: n.id,
              label: `${n.name} · v${n.version} · ${n.rule_status === "ready" ? "готов" : "несовместим"}`,
            })),
          ]}
        />
        {normalizers.length < options.normalizers.total && (
          <Button
            loading={options.normalizers.loading}
            onClick={() => void options.normalizers.loadMore()}
          >
            Загрузить ещё
          </Button>
        )}
        {!!normalizerError && <AppRequestError error={normalizerError} />}
        {!!selectedNormalizerError && normalizerFor?.normalizer_id && (
          <AppRequestError
            error={selectedNormalizerError}
            onRetry={() =>
              void sources.retrySelectedNormalizer(normalizerFor.normalizer_id!)
            }
          />
        )}
        {!!options.normalizers.error && (
          <AppRequestError
            error={options.normalizers.error}
            onRetry={() => void options.normalizers.retry()}
          />
        )}
        <div className={`${styles.actions} ${styles.normalizerActions}`}>
          <Button
            disabled={
              !!normalizerFor &&
              lifecycle.isPending(`source:${normalizerFor.id}`)
            }
            onClick={() => setNormalizerFor(null)}
          >
            Отмена
          </Button>
          <Button
            type="primary"
            loading={
              !!normalizerFor &&
              lifecycle.isPending(`source:${normalizerFor.id}`)
            }
            onClick={() => {
              void setNormalizer(normalizerChoice || null);
            }}
          >
            Назначить
          </Button>
        </div>
      </Modal>
      <ConfirmationDialog
        open={!!confirm}
        pending={confirmPending}
        confirmDisabled={confirmNeedsRefresh}
        title={confirm?.title || ""}
        message={confirm?.message || ""}
        confirmText={confirm?.label || "Продолжить"}
        danger={confirm?.label === "Удалить"}
        onCancel={() => {
          setConfirm(null);
          setConfirmNeedsRefresh(false);
        }}
        onConfirm={() => void runConfirm()}
      >
        {!!operationError && <AppRequestError error={operationError} />}
        {confirmNeedsRefresh && (
          <Alert
            type="warning"
            message="Неизвестно, применилось ли действие. Сначала обновите список; повтор отправьте вручную после сверки."
            action={
              <Button onClick={() => void verifyConfirmOutcome()}>
                Обновить состояние
              </Button>
            }
          />
        )}
      </ConfirmationDialog>
    </main>
  );
});
