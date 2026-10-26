import {
  useCallback,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
  type KeyboardEvent as ReactKeyboardEvent,
} from "react";
import {
  Alert,
  Button,
  Checkbox,
  Form,
  Input,
  Modal,
  Pagination,
  Select,
  Space,
  Spin,
  Table,
  Typography,
  message,
  type FormInstance,
} from "antd";
import { observer } from "mobx-react-lite";
import {
  AdministrationApi,
  type ManagedUser,
  type Mapping,
  type Page,
  type Role,
  type UserQuery,
} from "../../app/api/administration";
import { ApiError } from "../../app/api/client";
import { useStores } from "../../app/providers/StoresProvider";
import { AppRequestError } from "../../app/ui/AppRequestError";
import { ConfirmationDialog } from "../../shared/ui/ConfirmationDialog";
import styles from "./AdministrationPage.module.css";
import { OidcMethodsBlock } from "./OidcMethodsBlock";

const ADMIN_ID = "00000000-0000-4000-8000-000000000001";
const PAGE_SIZE = 20;
type Confirm = {
  title: string;
  message: string;
  label: string;
  action: () => Promise<unknown>;
  done: string;
  context: OperationContext;
};
type OperationContext = {
  reload: () => Promise<void>;
  close?: () => void;
  form?: FormInstance;
};
type OperationFailure = {
  error: unknown;
  uncertain: boolean;
  reload: () => Promise<void>;
};
type UserValues = {
  username?: string;
  password?: string;
  display_name: string;
  email?: string;
  role_id?: string;
  is_active?: boolean;
};
type MappingValues = {
  issuer: string;
  claim_name: string;
  claim_value: string;
  role_id: string;
};

function bindValidation(error: unknown, form?: FormInstance) {
  if (
    !(error instanceof ApiError) ||
    error.code !== "validation_error" ||
    !form
  )
    return;
  const details = error.details as { errors?: { loc?: unknown[] }[] } | null;
  const names = new Set(Object.keys(form.getFieldsValue()));
  const fields =
    details?.errors?.flatMap((item) => {
      const name = item.loc?.at(-1);
      return typeof name === "string" && names.has(name)
        ? [{ name, errors: ["Проверьте значение поля."] }]
        : [];
    }) ?? [];
  if (fields.length) form.setFields(fields);
}

export const AdministrationPage = observer(() => {
  const { api, session } = useStores();
  const service = useMemo(() => new AdministrationApi(api), [api]);
  const canWrite = session.has("users.write");
  const canReadUsers = session.has("users.read");
  const [users, setUsers] = useState<Page<ManagedUser> | null>(null);
  const [roles, setRoles] = useState<Role[] | null>(null);
  const [mappings, setMappings] = useState<Page<Mapping> | null>(null);
  const [methods, setMethods] = useState<{ oidc_enabled: boolean } | null>(
    null,
  );
  const [filters, setFilters] = useState<UserQuery>({
    limit: PAGE_SIZE,
    offset: 0,
  });
  const [mappingOffset, setMappingOffset] = useState(0);
  const [tab, setTab] = useState("roles");
  const [userLoading, setUserLoading] = useState(false);
  const [roleLoading, setRoleLoading] = useState(false);
  const [mappingLoading, setMappingLoading] = useState(false);
  const [userError, setUserError] = useState<unknown>(null);
  const [roleError, setRoleError] = useState<unknown>(null);
  const [mappingError, setMappingError] = useState<unknown>(null);
  const [methodsError, setMethodsError] = useState<unknown>(null);
  const [operationError, setOperationError] = useState<OperationFailure | null>(
    null,
  );
  const [pending, setPending] = useState(false);
  const [confirm, setConfirm] = useState<Confirm | null>(null);
  const [userForm, setUserForm] = useState<ManagedUser | "new" | null>(null);
  const [mappingForm, setMappingForm] = useState<Mapping | "new" | null>(null);
  const [rename, setRename] = useState<Role | null>(null);
  const [passwordUser, setPasswordUser] = useState<ManagedUser | null>(null);
  const [roleUser, setRoleUser] = useState<ManagedUser | null>(null);
  const [userFields] = Form.useForm<UserValues>();
  const [mappingFields] = Form.useForm<MappingValues>();
  const [roleFields] = Form.useForm<{
    role_id: string;
    role_managed_by_oidc: boolean;
  }>();
  const [passwordFields] = Form.useForm<{ password: string }>();
  const [renameFields] = Form.useForm<{ name: string }>();
  const mounted = useRef(false);
  const panelsViewport = useRef<HTMLDivElement>(null);
  const rolesPanel = useRef<HTMLDivElement>(null);
  const mappingsPanel = useRef<HTMLDivElement>(null);
  const previousTab = useRef(tab);
  const usersRequest = useRef(0);
  const rolesRequest = useRef(0);
  const mappingsRequest = useRef(0);
  const methodsRequest = useRef(0);
  const changeFilters = (next: UserQuery) => {
    usersRequest.current += 1;
    setFilters(next);
  };
  const changeTab = (next: "roles" | "mappings") => {
    if (next === tab) return;
    const viewport = panelsViewport.current;
    if (viewport)
      viewport.style.height = `${viewport.getBoundingClientRect().height}px`;
    setTab(next);
  };

  useLayoutEffect(() => {
    if (previousTab.current === tab) return;
    previousTab.current = tab;
    const viewport = panelsViewport.current;
    const activePanel =
      tab === "roles" ? rolesPanel.current : mappingsPanel.current;
    if (!viewport || !activePanel) return;
    const targetHeight = activePanel.scrollHeight;
    const reducedMotion = window.matchMedia?.(
      "(prefers-reduced-motion: reduce)",
    ).matches;
    if (reducedMotion) {
      viewport.style.height = "auto";
      viewport.style.overflow = "visible";
      return;
    }
    const startHeight = viewport.getBoundingClientRect().height;
    viewport.style.overflow = "hidden";
    if (Math.abs(startHeight - targetHeight) < 1) {
      viewport.style.height = "auto";
      viewport.style.overflow = "visible";
      return;
    }
    // Commit the frozen starting size before changing it, so CSS can detect
    // the height change and start a transition even when the panel switch and
    // layout effect run in the same browser frame.
    void viewport.offsetHeight;
    const targetFrame = requestAnimationFrame(() => {
      viewport.style.height = `${targetHeight}px`;
    });
    const finish = (event: TransitionEvent) => {
      if (event.propertyName !== "height") return;
      viewport.style.height = "auto";
      viewport.style.overflow = "visible";
    };
    viewport.addEventListener("transitionend", finish);
    return () => {
      cancelAnimationFrame(targetFrame);
      viewport.removeEventListener("transitionend", finish);
    };
  }, [tab]);

  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
      usersRequest.current += 1;
      rolesRequest.current += 1;
      mappingsRequest.current += 1;
      methodsRequest.current += 1;
    };
  }, []);

  const loadUsers = useCallback(async () => {
    const requestId = ++usersRequest.current;
    setUserLoading(true);
    setUserError(null);
    try {
      const result = await service.users(filters);
      if (!mounted.current || requestId !== usersRequest.current) return;
      if (result.items.length === 0 && filters.offset > 0 && result.total > 0) {
        setFilters((current) => ({
          ...current,
          offset: Math.max(
            0,
            Math.floor((result.total - 1) / PAGE_SIZE) * PAGE_SIZE,
          ),
        }));
        return;
      }
      setUsers(result);
    } catch (error) {
      if (mounted.current && requestId === usersRequest.current)
        setUserError(error);
    } finally {
      if (mounted.current && requestId === usersRequest.current)
        setUserLoading(false);
    }
  }, [service, filters]);
  const loadRoles = useCallback(async () => {
    const requestId = ++rolesRequest.current;
    setRoleLoading(true);
    setRoleError(null);
    try {
      const result = await service.roles();
      if (mounted.current && requestId === rolesRequest.current)
        setRoles(result);
    } catch (error) {
      if (mounted.current && requestId === rolesRequest.current)
        setRoleError(error);
    } finally {
      if (mounted.current && requestId === rolesRequest.current)
        setRoleLoading(false);
    }
  }, [service]);
  const loadMappings = useCallback(async () => {
    const requestId = ++mappingsRequest.current;
    setMappingLoading(true);
    setMappingError(null);
    try {
      const result = await service.mappings(PAGE_SIZE, mappingOffset);
      if (!mounted.current || requestId !== mappingsRequest.current) return;
      if (result.items.length === 0 && mappingOffset > 0 && result.total > 0) {
        setMappingOffset(
          Math.max(0, Math.floor((result.total - 1) / PAGE_SIZE) * PAGE_SIZE),
        );
        return;
      }
      setMappings(result);
    } catch (error) {
      if (mounted.current && requestId === mappingsRequest.current)
        setMappingError(error);
    } finally {
      if (mounted.current && requestId === mappingsRequest.current)
        setMappingLoading(false);
    }
  }, [service, mappingOffset]);
  const loadMethods = useCallback(async () => {
    const requestId = ++methodsRequest.current;
    setMethods(null);
    setMethodsError(null);
    try {
      const result = await service.methods();
      if (mounted.current && requestId === methodsRequest.current)
        setMethods(result);
    } catch (error) {
      if (mounted.current && requestId === methodsRequest.current)
        setMethodsError(error);
    }
  }, [service]);
  useEffect(() => {
    if (canReadUsers) void loadUsers();
  }, [canReadUsers, loadUsers]);
  useEffect(() => {
    if (canReadUsers) void loadRoles();
  }, [canReadUsers, loadRoles]);
  useEffect(() => {
    if (canReadUsers && tab === "mappings") void loadMappings();
  }, [canReadUsers, tab, loadMappings]);
  useEffect(() => {
    if (canReadUsers) void loadMethods();
  }, [canReadUsers, loadMethods]);

  const run = async (
    action: () => Promise<unknown>,
    done: string,
    context: OperationContext,
  ) => {
    if (pending) return;
    setPending(true);
    setOperationError(null);
    try {
      await action();
      context.close?.();
      message.success(done);
      setOperationError(null);
      await context.reload();
    } catch (error) {
      const uncertain =
        !(error instanceof ApiError) ||
        error.status === 0 ||
        error.status >= 500 ||
        error.code === "invalid_response";
      setOperationError({ error, uncertain, reload: context.reload });
      bindValidation(error, context.form);
      setConfirm(null);
    } finally {
      setPending(false);
    }
  };
  const requestConfirm = (value: Confirm) => {
    if (pending) return;
    setOperationError(null);
    setConfirm(value);
  };
  const operationErrorView = operationError && (
    <AppRequestError
      error={operationError.error}
      message={
        operationError.uncertain
          ? "Не удалось подтвердить результат операции. Запрос мог быть выполнен. Обновите данные и проверьте состояние перед повторным действием."
          : undefined
      }
      actionLabel={
        operationError.uncertain ? "Проверить состояние" : "Обновить данные"
      }
      onRetry={() => void operationError.reload()}
    />
  );
  const closeUser = () => {
    userFields.resetFields();
    setUserForm(null);
  };
  const closePassword = () => {
    passwordFields.resetFields();
    setPasswordUser(null);
  };
  const openUser = (user: ManagedUser | "new") => {
    userFields.resetFields();
    if (user !== "new")
      userFields.setFieldsValue({
        username: user.username ?? undefined,
        display_name: user.display_name,
        email: user.email ?? undefined,
      });
    setOperationError(null);
    setUserForm(user);
  };
  const openMapping = (mapping: Mapping | "new") => {
    mappingFields.resetFields();
    if (mapping !== "new")
      mappingFields.setFieldsValue({
        ...mapping,
        role_id: mapping.role_id ?? undefined,
      });
    setOperationError(null);
    setMappingForm(mapping);
  };
  const submitUser = (values: UserValues) => {
    if (!userForm) return;
    if (userForm === "new") {
      void run(
        () =>
          service.createUser({
            ...values,
            email: values.email || null,
            is_active: values.is_active ?? true,
          }),
        "Пользователь создан",
        { reload: loadUsers, close: closeUser, form: userFields },
      );
    } else {
      const body =
        userForm.username === null
          ? { display_name: values.display_name, email: values.email || null }
          : {
              username: values.username,
              display_name: values.display_name,
              email: values.email || null,
            };
      void run(
        () => service.updateUser(userForm.id, body),
        "Пользователь обновлён",
        { reload: loadUsers, close: closeUser, form: userFields },
      );
    }
  };
  const submitMapping = (values: MappingValues) => {
    if (!mappingForm) return;
    const action = () =>
      mappingForm === "new"
        ? service.createMapping(values)
        : service.updateMapping(mappingForm.id, values);
    const done =
      mappingForm === "new"
        ? "Сопоставление создано"
        : "Сопоставление обновлено";
    const context = {
      reload: loadMappings,
      close: () => setMappingForm(null),
      form: mappingFields,
    };
    const execute = () => run(action, done, context);
    if (values.role_id === ADMIN_ID) {
      requestConfirm({
        title: "Выдать роль Administrator",
        message:
          "Совпадающее значение claim назначит пользователю права администратора при следующем OIDC-входе.",
        label: "Подтвердить повышение",
        action,
        done,
        context,
      });
    } else void execute();
  };
  const roleOptions =
    roles?.map((role) => ({ value: role.id, label: role.name })) ?? [];
  const selfId = session.data?.user?.id;
  const handleTabKeyDown = (event: ReactKeyboardEvent<HTMLButtonElement>) => {
    const tabs = Array.from(
      event.currentTarget.parentElement?.querySelectorAll<HTMLButtonElement>(
        "[role=tab]",
      ) ?? [],
    );
    const activeIndex = tabs.indexOf(event.currentTarget);
    let nextIndex = activeIndex;
    if (event.key === "ArrowRight") nextIndex = (activeIndex + 1) % tabs.length;
    else if (event.key === "ArrowLeft")
      nextIndex = (activeIndex + tabs.length - 1) % tabs.length;
    else if (event.key === "Home") nextIndex = 0;
    else if (event.key === "End") nextIndex = tabs.length - 1;
    else return;
    event.preventDefault();
    const next = tabs[nextIndex];
    next?.focus();
    if (next?.id.endsWith("-roles")) changeTab("roles");
    if (next?.id.endsWith("-mappings")) changeTab("mappings");
  };
  const userColumns = [
    {
      title: "Пользователь",
      key: "name",
      render: (_: unknown, item: ManagedUser) => (
        <>
          <strong>{item.display_name}</strong>
          <br />
          <span className={styles.muted}>
            {item.username ?? item.email ?? item.oidc_subject}
          </span>
        </>
      ),
    },
    {
      title: "Вход",
      dataIndex: "authentication_method",
      render: (value: string) => (value === "oidc" ? "OIDC" : "Локальный"),
    },
    {
      title: "Роль",
      dataIndex: "role_id",
      render: (value: string | null) =>
        roles?.find((role) => role.id === value)?.name ?? "—",
    },
    {
      title: "Состояние",
      dataIndex: "is_active",
      render: (value: boolean) => (value ? "Активен" : "Отключён"),
    },
    {
      title: "Действия",
      key: "actions",
      render: (_: unknown, item: ManagedUser) => (
        <div className={styles.actions}>
          <Button
            size="small"
            disabled={pending}
            onClick={() => openUser(item)}
          >
            Открыть
          </Button>
          {canWrite && item.id !== selfId && (
            <>
              <Button
                size="small"
                disabled={pending}
                onClick={() => {
                  roleFields.setFieldsValue({
                    role_id: item.role_id ?? undefined,
                    role_managed_by_oidc: item.role_managed_by_oidc,
                  });
                  setRoleUser(item);
                }}
              >
                Сменить роль
              </Button>
              <Button
                size="small"
                disabled={pending}
                onClick={() =>
                  requestConfirm({
                    title: item.is_active
                      ? "Отключить пользователя"
                      : "Активировать пользователя",
                    message: item.is_active
                      ? `Доступ «${item.display_name}» прекратится; действующие сессии будут отозваны.`
                      : `«${item.display_name}» снова сможет входить в систему.`,
                    label: item.is_active ? "Отключить" : "Активировать",
                    action: () => service.active(item.id, !item.is_active),
                    done: "Состояние пользователя обновлено",
                    context: { reload: loadUsers },
                  })
                }
              >
                {item.is_active ? "Отключить" : "Активировать"}
              </Button>
              <Button
                size="small"
                danger
                disabled={pending}
                onClick={() =>
                  requestConfirm({
                    title: "Удалить пользователя",
                    message: `Учётная запись «${item.display_name}» и её сессии будут удалены.${item.authentication_method === "oidc" ? " Следующий успешный OIDC-вход может создать её снова." : ""}`,
                    label: "Удалить",
                    action: () => service.deleteUser(item.id),
                    done: "Пользователь удалён",
                    context: { reload: loadUsers },
                  })
                }
              >
                Удалить
              </Button>
            </>
          )}
          {canWrite && item.authentication_method === "local" && (
            <Button
              size="small"
              disabled={pending}
              onClick={() => {
                passwordFields.resetFields();
                setPasswordUser(item);
              }}
            >
              Новый пароль
            </Button>
          )}
        </div>
      ),
    },
  ];

  if (!canReadUsers) {
    return (
      <main className={styles.page}>
        <Typography.Title level={2}>Администрирование</Typography.Title>
        {session.has("settings.read") && <OidcMethodsBlock />}
      </main>
    );
  }

  return (
    <main className={styles.page}>
      <Typography.Title level={2}>Администрирование</Typography.Title>
      <section className={styles.block} aria-labelledby="users-heading">
        <div className={styles.heading}>
          <Typography.Title level={3} id="users-heading">
            Пользователи
          </Typography.Title>
          <Space wrap>
            <Button onClick={() => void loadUsers()} loading={userLoading}>
              Обновить
            </Button>
            {canWrite && (
              <Button
                type="primary"
                disabled={pending}
                onClick={() => openUser("new")}
              >
                Создать локального пользователя
              </Button>
            )}
          </Space>
        </div>
        <div className={styles.filters}>
          <Input.Search
            aria-label="Поиск пользователей"
            placeholder="Поиск"
            allowClear
            onSearch={(q) => changeFilters({ ...filters, q, offset: 0 })}
          />
          <Select
            aria-label="Фильтр по роли"
            value={filters.role_id ?? ""}
            options={[{ value: "", label: "Все роли" }, ...roleOptions]}
            onChange={(role_id) =>
              changeFilters({ ...filters, role_id, offset: 0 })
            }
          />
          <Select
            aria-label="Фильтр по активности"
            value={
              filters.is_active === undefined
                ? "all"
                : String(filters.is_active)
            }
            options={[
              { value: "all", label: "Все состояния" },
              { value: "true", label: "Активные" },
              { value: "false", label: "Отключённые" },
            ]}
            onChange={(value) =>
              changeFilters({
                ...filters,
                is_active: value === "all" ? undefined : value === "true",
                offset: 0,
              })
            }
          />
          <Select
            aria-label="Фильтр по способу входа"
            value={filters.authentication_method ?? "all"}
            options={[
              { value: "all", label: "Все способы входа" },
              { value: "local", label: "Локальный" },
              { value: "oidc", label: "OIDC" },
            ]}
            onChange={(value) =>
              changeFilters({
                ...filters,
                authentication_method:
                  value === "all" ? undefined : (value as "local" | "oidc"),
                offset: 0,
              })
            }
          />
        </div>
        {userError != null && (
          <AppRequestError error={userError} onRetry={() => void loadUsers()} />
        )}
        <div className={styles.table}>
          <Table
            rowKey="id"
            columns={userColumns}
            dataSource={users?.items ?? []}
            loading={userLoading}
            pagination={false}
            scroll={{ x: 800 }}
            locale={{ emptyText: "Пользователи не найдены" }}
          />
        </div>
        <Pagination
          current={filters.offset / PAGE_SIZE + 1}
          pageSize={PAGE_SIZE}
          total={users?.total ?? 0}
          showSizeChanger={false}
          onChange={(page) =>
            changeFilters({ ...filters, offset: (page - 1) * PAGE_SIZE })
          }
        />
      </section>
      <section className={styles.block} aria-labelledby="roles-heading">
        <Typography.Title level={3} id="roles-heading">
          Роли и доступ
        </Typography.Title>
        <div
          className={styles.tabs}
          role="tablist"
          aria-label="Представления доступа"
        >
          <Button
            id="administration-access-tab-roles"
            role="tab"
            aria-controls="administration-panel-roles"
            aria-selected={tab === "roles"}
            tabIndex={tab === "roles" ? 0 : -1}
            onClick={() => changeTab("roles")}
            onKeyDown={handleTabKeyDown}
          >
            Роли
          </Button>
          <Button
            id="administration-access-tab-mappings"
            role="tab"
            aria-controls="administration-panel-mappings"
            aria-selected={tab === "mappings"}
            tabIndex={tab === "mappings" ? 0 : -1}
            onClick={() => changeTab("mappings")}
            onKeyDown={handleTabKeyDown}
          >
            Сопоставления OIDC
          </Button>
        </div>
        <div
          className={styles.panelsViewport}
          data-testid="administration-panels-viewport"
          ref={panelsViewport}
        >
          <div
            className={`${styles.panel} ${tab === "roles" ? styles.panelActive : styles.panelInactive}`}
            ref={rolesPanel}
            id="administration-panel-roles"
            role="tabpanel"
            aria-labelledby="administration-access-tab-roles"
            aria-hidden={tab !== "roles"}
            inert={tab !== "roles"}
            tabIndex={tab === "roles" ? 0 : -1}
          >
            <div className={styles.heading}>
              <Typography.Text>
                Общие роли для локального и OIDC-входа
              </Typography.Text>
              <Button onClick={() => void loadRoles()} loading={roleLoading}>
                Обновить
              </Button>
            </div>
            {roleError != null && (
              <AppRequestError
                error={roleError}
                onRetry={() => void loadRoles()}
              />
            )}
            <div className={styles.table}>
              <Table
                rowKey="id"
                loading={roleLoading}
                pagination={false}
                dataSource={roles ?? []}
                locale={{ emptyText: "Роли не найдены" }}
                columns={[
                  { title: "Роль", dataIndex: "name" },
                  {
                    title: "Права",
                    dataIndex: "permissions",
                    render: (value: string[]) => value.join(", "),
                  },
                  {
                    title: "",
                    key: "action",
                    render: (_: unknown, role: Role) =>
                      canWrite && (
                        <Button
                          size="small"
                          disabled={pending}
                          onClick={() => {
                            renameFields.setFieldsValue({ name: role.name });
                            setRename(role);
                          }}
                        >
                          Переименовать
                        </Button>
                      ),
                  },
                ]}
              />
            </div>
          </div>
          <div
            className={`${styles.panel} ${tab === "mappings" ? styles.panelActive : styles.panelInactive}`}
            ref={mappingsPanel}
            id="administration-panel-mappings"
            role="tabpanel"
            aria-labelledby="administration-access-tab-mappings"
            aria-hidden={tab !== "mappings"}
            inert={tab !== "mappings"}
            tabIndex={tab === "mappings" ? 0 : -1}
          >
            <div className={styles.heading}>
              <Typography.Text>
                Issuer + claim + значение → системная роль
              </Typography.Text>
              <Space wrap>
                <Button
                  onClick={() => void loadMappings()}
                  loading={mappingLoading}
                >
                  Обновить
                </Button>
                {canWrite && (
                  <Button
                    type="primary"
                    disabled={pending}
                    onClick={() => openMapping("new")}
                  >
                    Добавить
                  </Button>
                )}
              </Space>
            </div>
            {methodsError != null && (
              <AppRequestError
                error={methodsError}
                onRetry={() => void loadMethods()}
              />
            )}
            {methods === null ? (
              <Alert type="info" showIcon message="Статус OIDC не загружен." />
            ) : methods.oidc_enabled === false ? (
              <Alert
                type="info"
                showIcon
                message="OIDC отключён. Сопоставления пока не применяются при входе."
              />
            ) : null}
            <Typography.Paragraph className={styles.muted}>
              Изменения роли применятся при следующем успешном OIDC-входе.
            </Typography.Paragraph>
            {mappingError != null && (
              <AppRequestError
                error={mappingError}
                onRetry={() => void loadMappings()}
              />
            )}
            <div className={styles.table}>
              <Table
                rowKey="id"
                loading={mappingLoading}
                pagination={false}
                dataSource={mappings?.items ?? []}
                scroll={{ x: 750 }}
                locale={{ emptyText: "Сопоставлений нет" }}
                columns={[
                  { title: "Issuer", dataIndex: "issuer" },
                  { title: "Claim", dataIndex: "claim_name" },
                  { title: "Значение", dataIndex: "claim_value" },
                  {
                    title: "Роль",
                    dataIndex: "role_id",
                    render: (id: string) =>
                      roles?.find((role) => role.id === id)?.name ?? "—",
                  },
                  {
                    title: "",
                    key: "action",
                    render: (_: unknown, mapping: Mapping) =>
                      canWrite && (
                        <Space>
                          <Button
                            size="small"
                            disabled={pending}
                            onClick={() => openMapping(mapping)}
                          >
                            Изменить
                          </Button>
                          <Button
                            size="small"
                            danger
                            disabled={pending}
                            onClick={() =>
                              requestConfirm({
                                title: "Удалить сопоставление",
                                message: `Сопоставление ${mapping.claim_name}=${mapping.claim_value} будет удалено. При следующем OIDC-входе роль может измениться.`,
                                label: "Удалить",
                                action: () => service.deleteMapping(mapping.id),
                                done: "Сопоставление удалено",
                                context: { reload: loadMappings },
                              })
                            }
                          >
                            Удалить
                          </Button>
                        </Space>
                      ),
                  },
                ]}
              />
            </div>
            <Pagination
              current={mappingOffset / PAGE_SIZE + 1}
              pageSize={PAGE_SIZE}
              total={mappings?.total ?? 0}
              showSizeChanger={false}
              onChange={(page) => {
                mappingsRequest.current += 1;
                setMappingOffset((page - 1) * PAGE_SIZE);
              }}
            />
          </div>
        </div>
      </section>
      {session.has("settings.read") && <OidcMethodsBlock />}
      <Modal
        open={userForm !== null}
        title={
          userForm === "new"
            ? "Создать локального пользователя"
            : "Пользователь"
        }
        onCancel={closeUser}
        footer={null}
        destroyOnHidden
      >
        {userForm && userForm !== "new" && (
          <Typography.Paragraph>
            Тип входа:{" "}
            {userForm.authentication_method === "oidc" ? "OIDC" : "локальный"}.
            ID: {userForm.id}
            {userForm.authentication_method === "oidc" && (
              <>
                . Issuer: {userForm.oidc_issuer}. Роль{" "}
                {userForm.role_managed_by_oidc
                  ? "управляется OIDC"
                  : "назначена вручную"}
                .
              </>
            )}
          </Typography.Paragraph>
        )}
        <Form form={userFields} layout="vertical" onFinish={submitUser}>
          {(userForm === "new" || (userForm && userForm.username !== null)) && (
            <Form.Item
              label="Имя входа"
              name="username"
              rules={[{ required: true, max: 128 }]}
            >
              <Input disabled={!canWrite} />
            </Form.Item>
          )}
          {userForm === "new" && (
            <>
              <Form.Item
                label="Пароль"
                name="password"
                rules={[{ required: true, min: 12, max: 128 }]}
              >
                <Input.Password autoComplete="new-password" />
              </Form.Item>
              <Form.Item
                label="Роль"
                name="role_id"
                rules={[{ required: true }]}
              >
                <Select options={roleOptions} />
              </Form.Item>
              <Form.Item name="is_active" valuePropName="checked" initialValue>
                <Checkbox>Активен</Checkbox>
              </Form.Item>
            </>
          )}
          <Form.Item
            label="Отображаемое имя"
            name="display_name"
            rules={[{ required: true, max: 200 }]}
          >
            <Input disabled={!canWrite} />
          </Form.Item>
          <Form.Item label="Email" name="email" rules={[{ type: "email" }]}>
            <Input disabled={!canWrite} />
          </Form.Item>
          {operationErrorView}
          {canWrite && (
            <Button
              type="primary"
              htmlType="submit"
              loading={pending}
              disabled={pending}
            >
              Сохранить
            </Button>
          )}
        </Form>
      </Modal>
      <Modal
        open={roleUser !== null}
        title="Сменить роль пользователя"
        onCancel={() => setRoleUser(null)}
        footer={null}
        destroyOnHidden
      >
        <Form
          form={roleFields}
          layout="vertical"
          onFinish={(values) => {
            if (!roleUser) return;
            const target = roleUser;
            requestConfirm({
              title: "Подтвердить смену роли",
              message: `Права «${target.display_name}» изменятся. Роль будет ${values.role_managed_by_oidc ? "обновляться при следующем OIDC-входе" : "назначена вручную"}.`,
              label: "Сменить роль",
              action: () =>
                service.role(
                  target.id,
                  values.role_id,
                  target.authentication_method === "oidc" &&
                    values.role_managed_by_oidc,
                ),
              done: "Роль пользователя обновлена",
              context: {
                reload: loadUsers,
                close: () => setRoleUser(null),
                form: roleFields,
              },
            });
          }}
        >
          <Form.Item
            name="role_id"
            label="Системная роль"
            rules={[{ required: true }]}
          >
            <Select options={roleOptions} />
          </Form.Item>
          {roleUser?.authentication_method === "oidc" && (
            <Form.Item name="role_managed_by_oidc" valuePropName="checked">
              <Checkbox>
                Управлять ролью через OIDC при следующем входе
              </Checkbox>
            </Form.Item>
          )}
          {operationErrorView}
          <Button htmlType="submit" type="primary" disabled={pending}>
            Продолжить
          </Button>
        </Form>
      </Modal>
      <Modal
        open={passwordUser !== null}
        title="Установить новый пароль"
        onCancel={closePassword}
        footer={null}
        destroyOnHidden
      >
        <Form
          form={passwordFields}
          layout="vertical"
          onFinish={(values) => {
            if (!passwordUser) return;
            const target = passwordUser;
            requestConfirm({
              title: "Сбросить пароль",
              message: `Пароль «${target.display_name}» будет заменён, действующие сессии отозваны.`,
              label: "Установить пароль",
              action: () => service.password(target.id, values.password),
              done: "Пароль обновлён",
              context: {
                reload: loadUsers,
                close: closePassword,
                form: passwordFields,
              },
            });
          }}
        >
          <Form.Item
            label="Новый пароль"
            name="password"
            rules={[{ required: true, min: 12, max: 128 }]}
          >
            <Input.Password autoComplete="new-password" />
          </Form.Item>
          {operationErrorView}
          <Button htmlType="submit" type="primary" disabled={pending}>
            Продолжить
          </Button>
        </Form>
      </Modal>
      <Modal
        open={mappingForm !== null}
        title={
          mappingForm === "new"
            ? "Новое сопоставление OIDC"
            : "Изменить сопоставление OIDC"
        }
        onCancel={() => {
          mappingFields.resetFields();
          setMappingForm(null);
        }}
        footer={null}
        destroyOnHidden
      >
        <Form form={mappingFields} layout="vertical" onFinish={submitMapping}>
          <Form.Item
            label="Issuer URL"
            name="issuer"
            rules={[{ required: true, type: "url" }]}
          >
            <Input />
          </Form.Item>
          <Form.Item
            label="Имя claim"
            name="claim_name"
            rules={[{ required: true, max: 200 }]}
          >
            <Input />
          </Form.Item>
          <Form.Item
            label="Значение claim"
            name="claim_value"
            rules={[{ required: true, max: 320 }]}
          >
            <Input />
          </Form.Item>
          <Form.Item
            label="Системная роль"
            name="role_id"
            rules={[{ required: true }]}
          >
            <Select options={roleOptions} />
          </Form.Item>
          {operationErrorView}
          <Button
            type="primary"
            htmlType="submit"
            loading={pending}
            disabled={pending}
          >
            Сохранить
          </Button>
        </Form>
      </Modal>
      <Modal
        open={rename !== null}
        title="Переименовать роль"
        onCancel={() => setRename(null)}
        footer={null}
        destroyOnHidden
      >
        <Typography.Paragraph>
          Стабильный ID и права роли сохранятся.
        </Typography.Paragraph>
        <Form
          form={renameFields}
          layout="vertical"
          onFinish={(values) => {
            if (rename)
              void run(
                () => service.renameRole(rename.id, values.name),
                "Роль переименована",
                {
                  reload: loadRoles,
                  close: () => setRename(null),
                  form: renameFields,
                },
              );
          }}
        >
          <Form.Item
            label="Название"
            name="name"
            rules={[{ required: true, max: 200 }]}
          >
            <Input />
          </Form.Item>
          {operationErrorView}
          <Button
            type="primary"
            htmlType="submit"
            loading={pending}
            disabled={pending}
          >
            Сохранить
          </Button>
        </Form>
      </Modal>
      <ConfirmationDialog
        zIndex={1200}
        open={confirm !== null}
        title={confirm?.title ?? ""}
        message={confirm?.message ?? ""}
        confirmText={confirm?.label ?? ""}
        pending={pending}
        onCancel={() => setConfirm(null)}
        onConfirm={() => {
          if (!confirm || pending) return;
          const choice = confirm;
          void run(choice.action, choice.done, {
            ...choice.context,
            close: () => {
              choice.context.close?.();
              setConfirm(null);
            },
          });
        }}
      />
      {operationError != null &&
        !userForm &&
        !mappingForm &&
        !roleUser &&
        !passwordUser &&
        !rename &&
        operationErrorView}
      {pending && (
        <span className={styles.muted}>
          <Spin size="small" /> Выполняется операция
        </span>
      )}
    </main>
  );
});
