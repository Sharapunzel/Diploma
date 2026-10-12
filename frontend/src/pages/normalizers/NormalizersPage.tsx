import { useEffect, useMemo, useRef, useState } from "react";
import { InfoCircleOutlined } from "@ant-design/icons";
import { Alert, Button, Input, Modal, Pagination, Table, Tag } from "antd";
import { useNavigate } from "react-router-dom";
import { NormalizersApi } from "../../app/api/normalizers";
import type { Normalizer, KafkaSource, Page } from "../../app/api/resources";
import { ApiError } from "../../app/api/client";
import { useStores } from "../../app/providers/StoresProvider";
import { AppRequestError } from "../../app/ui/AppRequestError";
import { ConfirmationDialog } from "../../shared/ui/ConfirmationDialog";
import styles from "./Normalizers.module.css";
import { FeedbackLayer, NormalizerFeedback } from "./NotificationOverlay";

const date = (value: string) => new Date(value).toLocaleString("ru-RU");
export function NormalizersPage() {
  const { api, session } = useStores();
  const service = useMemo(() => new NormalizersApi(api), [api]);
  const navigate = useNavigate();
  const [items, setItems] = useState<Normalizer[]>([]);
  const [total, setTotal] = useState(0);
  const [offset, setOffset] = useState(0);
  const [query, setQuery] = useState("");
  const [search, setSearch] = useState("");
  const [reload, setReload] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<unknown>(null);
  const [notice, setNotice] = useState("");
  const [errorVisible, setErrorVisible] = useState(true);
  const [help, setHelp] = useState(false);
  const [expanded, setExpanded] = useState<string[]>([]);
  const [sources, setSources] = useState<Record<string, Page<KafkaSource>>>({});
  const [sourcesError, setSourcesError] = useState<Record<string, unknown>>({});
  const [sourcesLoading, setSourcesLoading] = useState<Record<string, boolean>>(
    {},
  );
  const [sourceScope, setSourceScope] = useState("");
  const sourceRequests = useRef(new Map<string, AbortController>());
  const listGeneration = useRef(0);
  const [confirm, setConfirm] = useState<Normalizer | null>(null);
  const [pending, setPending] = useState(false);
  const [deleteError, setDeleteError] = useState<unknown>(null);
  const [needsRefresh, setNeedsRefresh] = useState(false);
  const [verifying, setVerifying] = useState(false);
  const [verificationError, setVerificationError] = useState<unknown>(null);
  const [listStale, setListStale] = useState(false);
  const canWrite = session.has("normalizers.write");
  const canSources = session.has("sources.read");
  const actorLabel = (id: string | null, role: "Автор" | "Изменил") => {
    if (!id) return `${role}: неизвестно`;
    const user = session.data?.user;
    if (user?.id === id)
      return `${role}: ${user.display_name || user.username}`;
    return `${role} (ID пользователя): ${id}`;
  };
  const accessScope = `${session.version}:${session.data?.user?.id || ""}:${(session.data?.permissions || []).join(",")}`;
  const currentScope = useRef(accessScope);
  currentScope.current = accessScope;
  const visibleSources = sourceScope === accessScope ? sources : {};
  const visibleSourcesError = sourceScope === accessScope ? sourcesError : {};
  const visibleSourcesLoading =
    sourceScope === accessScope ? sourcesLoading : {};

  useEffect(() => {
    const requests = sourceRequests.current;
    requests.forEach((controller) => controller.abort());
    requests.clear();
    setSources({});
    setSourcesError({});
    setSourcesLoading({});
    setExpanded([]);
    setSourceScope(accessScope);
    return () => {
      requests.forEach((controller) => controller.abort());
      requests.clear();
    };
  }, [accessScope]);

  useEffect(() => {
    const timer = window.setTimeout(() => {
      setSearch(query);
      setOffset(0);
    }, 250);
    return () => clearTimeout(timer);
  }, [query]);
  useEffect(() => {
    const controller = new AbortController();
    const generation = ++listGeneration.current;
    setLoading(true);
    setError(null);
    setErrorVisible(true);
    service
      .list(offset, search, controller.signal)
      .then((page) => {
        if (controller.signal.aborted || generation !== listGeneration.current)
          return;
        setItems(page.items);
        setTotal(page.total);
        setListStale(false);
      })
      .catch((reason) => {
        if (!controller.signal.aborted && generation === listGeneration.current)
          setError(reason);
      })
      .finally(() => {
        if (!controller.signal.aborted && generation === listGeneration.current)
          setLoading(false);
      });
    return () => controller.abort();
  }, [service, offset, search, reload, session.version]);
  async function loadSources(id: string, nextOffset = 0) {
    if (!canSources) return;
    sourceRequests.current.get(id)?.abort();
    const controller = new AbortController();
    sourceRequests.current.set(id, controller);
    const scope = accessScope;
    const isCurrent = () =>
      !controller.signal.aborted &&
      sourceRequests.current.get(id) === controller &&
      currentScope.current === scope;
    setSourcesLoading((current) => ({ ...current, [id]: true }));
    try {
      const page = await service.sources(id, nextOffset, controller.signal);
      if (!isCurrent()) return;
      setSources((current) => ({ ...current, [id]: page }));
      setSourcesError((current) => ({ ...current, [id]: null }));
    } catch (reason) {
      if (isCurrent())
        setSourcesError((current) => ({ ...current, [id]: reason }));
    } finally {
      if (isCurrent()) {
        setSourcesLoading((current) => ({ ...current, [id]: false }));
        sourceRequests.current.delete(id);
      }
    }
  }
  function confirmedRemoval(id: string) {
    listGeneration.current++;
    setConfirm(null);
    setNeedsRefresh(false);
    setDeleteError(null);
    setVerificationError(null);
    setNotice(
      "Нормализатор удалён. Связанные источники выключены и отвязаны; история сохранена.",
    );
    setListStale(true);
    setItems((current) => current.filter((item) => item.id !== id));
    setTotal((current) => Math.max(0, current - 1));
    if (offset > 0 && items.length === 1 && items[0].id === id)
      setOffset(Math.max(0, offset - 50));
    else setReload((value) => value + 1);
  }
  async function remove() {
    if (!confirm || pending || needsRefresh) return;
    setPending(true);
    setDeleteError(null);
    try {
      await service.remove(confirm.id, confirm.version);
      confirmedRemoval(confirm.id);
    } catch (reason) {
      setDeleteError(reason);
      if (
        reason instanceof ApiError &&
        (reason.status === 0 || reason.code === "invalid_response")
      )
        setNeedsRefresh(true);
    } finally {
      setPending(false);
    }
  }
  async function verifyRemoval() {
    if (!confirm || verifying) return;
    setVerifying(true);
    setVerificationError(null);
    try {
      const current = await service.detail(confirm.id);
      setConfirm(current);
      setNeedsRefresh(false);
      setDeleteError(null);
    } catch (reason) {
      if (reason instanceof ApiError && reason.status === 404)
        confirmedRemoval(confirm.id);
      else setVerificationError(reason);
    } finally {
      setVerifying(false);
    }
  }
  const columns = [
    {
      title: "Имя и описание",
      key: "name",
      render: (_: unknown, item: Normalizer) => (
        <div className={styles.wrap}>
          <strong>{item.name}</strong>
          <br />
          <span className={styles.muted}>
            {item.description || "Без описания"}
          </span>
        </div>
      ),
    },
    { title: "Версия", dataIndex: "version", key: "version" },
    {
      title: "Статус",
      key: "status",
      render: (_: unknown, item: Normalizer) => (
        <Tag color={item.rule_status === "ready" ? "green" : "orange"}>
          {item.rule_status === "ready" ? "Готов" : "Несовместим"}
        </Tag>
      ),
    },
    {
      title: "Создан",
      key: "created",
      render: (_: unknown, item: Normalizer) => (
        <div className={styles.wrap}>
          {date(item.created_at)}
          <br />
          <span className={styles.muted}>
            {actorLabel(item.created_by_user_id, "Автор")}
          </span>
        </div>
      ),
    },
    {
      title: "Изменён",
      key: "updated",
      render: (_: unknown, item: Normalizer) => (
        <div className={styles.wrap}>
          {date(item.updated_at)}
          <br />
          <span className={styles.muted}>
            {actorLabel(item.updated_by_user_id, "Изменил")}
          </span>
        </div>
      ),
    },
    {
      title: "Действия",
      key: "actions",
      render: (_: unknown, item: Normalizer) => (
        <div className={styles.actions}>
          <Button onClick={() => navigate(`/normalizers/${item.id}`)}>
            Просмотр
          </Button>
          {canWrite && (
            <>
              <Button onClick={() => navigate(`/normalizers/${item.id}/edit`)}>
                Изменить
              </Button>
              <Button
                onClick={() => navigate(`/normalizers/new?copy=${item.id}`)}
              >
                Создать на основе
              </Button>
              <Button
                danger
                onClick={() => {
                  setConfirm(item);
                  setDeleteError(null);
                  setNeedsRefresh(false);
                }}
              >
                Удалить
              </Button>
            </>
          )}
        </div>
      ),
    },
  ];
  return (
    <main className={styles.page}>
      <FeedbackLayer>
        {notice && (
          <NormalizerFeedback
            type="success"
            message={notice}
            onClose={() => setNotice("")}
          />
        )}
        {!!error && errorVisible && (
          <div className={styles.feedbackError}>
            <AppRequestError
              error={error}
              onRetry={() => setReload((value) => value + 1)}
            />
            <Button
              aria-label="Закрыть сообщение об ошибке"
              onClick={() => setErrorVisible(false)}
            >
              ×
            </Button>
          </div>
        )}
      </FeedbackLayer>
      <section className={styles.panel}>
        {listStale && (
          <Alert
            className={styles.staleNotice}
            type="warning"
            showIcon
            message="Список ещё не сверен с сервером после удаления. Показанные строки могут быть устаревшими."
            action={
              <Button onClick={() => setReload((value) => value + 1)}>
                Повторить чтение списка
              </Button>
            }
          />
        )}
        <div className={styles.bar}>
          <Input.Search
            aria-label="Поиск по имени"
            placeholder="Поиск по имени"
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            className={styles.search}
          />
          <div className={styles.actions}>
            {canWrite && (
              <Button
                type="primary"
                onClick={() => navigate("/normalizers/new")}
              >
                Создать нормализатор
              </Button>
            )}
            <Button
              aria-label="Справка о нормализаторах"
              icon={<InfoCircleOutlined />}
              onClick={() => setHelp(true)}
            />
          </div>
        </div>
        <div className={styles.table}>
          <Table
            rowKey="id"
            columns={columns}
            tableLayout="auto"
            dataSource={items}
            loading={loading}
            pagination={false}
            scroll={{ x: 850 }}
            locale={{
              emptyText: canWrite
                ? "Нормализаторов нет. Создайте первый."
                : "Нормализаторов нет.",
            }}
            expandable={{
              expandedRowKeys: expanded,
              onExpand: (open, item) => {
                setExpanded(
                  open
                    ? [...expanded, item.id]
                    : expanded.filter((id) => id !== item.id),
                );
                if (open && canSources && !visibleSources[item.id])
                  void loadSources(item.id);
              },
              expandedRowRender: (item) => (
                <div className={styles.stack}>
                  {!canSources ? (
                    <Alert
                      type="info"
                      message="Для просмотра связанных источников требуется право sources.read. Зависимости могут существовать."
                    />
                  ) : (
                    <>
                      {visibleSourcesLoading[item.id] && (
                        <span>Загрузка источников…</span>
                      )}
                      {!!visibleSourcesError[item.id] && (
                        <AppRequestError
                          error={visibleSourcesError[item.id]}
                          onRetry={() =>
                            void loadSources(
                              item.id,
                              visibleSources[item.id]?.offset || 0,
                            )
                          }
                        />
                      )}
                      {visibleSources[item.id] && (
                        <>
                          <strong>
                            Связанные Kafka-источники:{" "}
                            {visibleSources[item.id].total}
                          </strong>
                          {visibleSources[item.id].items.map((source) => (
                            <div key={source.id}>
                              {source.name} —{" "}
                              {source.is_enabled ? "включён" : "выключен"}
                            </div>
                          ))}
                          {visibleSources[item.id].items.length <
                            visibleSources[item.id].total && (
                            <Pagination
                              current={
                                Math.floor(
                                  visibleSources[item.id].offset / 50,
                                ) + 1
                              }
                              pageSize={50}
                              total={visibleSources[item.id].total}
                              showSizeChanger={false}
                              onChange={(page) =>
                                void loadSources(item.id, (page - 1) * 50)
                              }
                            />
                          )}
                        </>
                      )}
                    </>
                  )}
                </div>
              ),
            }}
          />
        </div>
        <div className={styles.tablePagination}>
          <Pagination
            current={Math.floor(offset / 50) + 1}
            pageSize={50}
            total={total}
            showSizeChanger={false}
            onChange={(page) => setOffset((page - 1) * 50)}
          />
        </div>
      </section>
      <Modal
        open={help}
        title="О нормализаторах"
        footer={null}
        onCancel={() => setHelp(false)}
      >
        <p>
          Создайте правило с нуля, по встроенному шаблону или копии. В редакторе
          доступны блоки и полный JSON. Preview проверяет несохранённый черновик
          только на вручную введённой строке log. Служебное время проверки
          подставляется автоматически.
        </p>
        <p>
          Изменение назначенного правила затрагивает только будущие сообщения.
          Удаление выключает и отвязывает связанные источники, но не удаляет
          сохранённые события.
        </p>
      </Modal>
      <ConfirmationDialog
        open={!!confirm}
        pending={pending || verifying}
        confirmDisabled={needsRefresh}
        title="Удалить нормализатор?"
        message={`«${confirm?.name || ""}» будет удалён. Связанные Kafka-источники выключатся и потеряют назначение. Сохранённые события останутся. ${canSources ? (visibleSources[confirm?.id || ""] ? `Сейчас найдено связей: ${visibleSources[confirm?.id || ""].total}.` : "Связи ещё не загружены; их число неизвестно.") : "Нет права просмотра связей; они могут существовать."}`}
        confirmText="Удалить"
        danger
        onCancel={() => setConfirm(null)}
        onConfirm={() => void remove()}
      >
        {!!deleteError && <AppRequestError error={deleteError} />}
        {!!verificationError && <AppRequestError error={verificationError} />}
        {needsRefresh && (
          <Alert
            type="warning"
            message="Результат запроса неизвестен. Сверьте список перед ручным повтором."
            action={
              <Button
                loading={verifying}
                disabled={verifying}
                onClick={() => void verifyRemoval()}
              >
                Обновить состояние
              </Button>
            }
          />
        )}
      </ConfirmationDialog>
    </main>
  );
}
