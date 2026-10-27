import { useCallback, useEffect, useRef, useState } from "react";
import { InfoCircleOutlined } from "@ant-design/icons";
import {
  Alert,
  Button,
  Form,
  Input,
  Popover,
  Space,
  Spin,
  Typography,
  message,
} from "antd";
import { observer } from "mobx-react-lite";
import { ApiError, errorText } from "../../app/api/client";
import { useStores } from "../../app/providers/StoresProvider";
import { AppRequestError } from "../../app/ui/AppRequestError";
import { ConfirmationDialog } from "../../shared/ui/ConfirmationDialog";
import styles from "./OidcMethodsBlock.module.css";

type Config = {
  issuer_url: string;
  client_id: string;
  redirect_uri: string;
  scopes: string[];
  has_secret: boolean;
  has_ca: boolean;
};
type Status = {
  mode: "env" | "managed" | "disabled";
  env_enabled: boolean;
  version: number;
  active_version: number | null;
  active: Config | null;
  diagnostic?: string | null;
  draft: Config | null;
  checked: boolean;
  trial_succeeded: boolean;
};
type Values = {
  issuer_url: string;
  client_id: string;
  redirect_uri: string;
  scopes: string;
  client_secret?: string;
};

const confirmedRejectionStatuses: Record<string, readonly number[]> = {
  authentication_required: [401],
  permission_denied: [403],
  csrf_invalid: [403],
  validation_error: [422],
  oidc_key_unavailable: [503],
  oidc_configuration_unavailable: [503],
  oidc_public_origin_invalid: [503],
  oidc_version_conflict: [409],
  oidc_trial_required: [409],
  oidc_secret_required: [400],
  oidc_ca_invalid: [422],
  oidc_url_invalid: [422],
  oidc_https_required: [422],
  oidc_callback_invalid: [422],
  oidc_callback_origin_invalid: [422],
  oidc_tls_invalid: [422],
  oidc_hostname_mismatch: [422],
  oidc_unreachable: [422],
  oidc_metadata_invalid: [422],
  oidc_metadata_origin_invalid: [422],
  oidc_jwks_invalid: [422],
};

function isConfirmedRejection(error: unknown): error is ApiError {
  return (
    error instanceof ApiError &&
    confirmedRejectionStatuses[error.code]?.includes(error.status) === true
  );
}

function isConfig(value: unknown): value is Config {
  if (typeof value !== "object" || value === null) return false;
  const config = value as Record<string, unknown>;
  return (
    typeof config.issuer_url === "string" &&
    typeof config.client_id === "string" &&
    typeof config.redirect_uri === "string" &&
    Array.isArray(config.scopes) &&
    config.scopes.every((scope) => typeof scope === "string") &&
    typeof config.has_secret === "boolean" &&
    typeof config.has_ca === "boolean"
  );
}

function isStatus(value: unknown): value is Status {
  if (typeof value !== "object" || value === null) return false;
  const status = value as Record<string, unknown>;
  return (
    ["env", "managed", "disabled"].includes(String(status.mode)) &&
    typeof status.env_enabled === "boolean" &&
    Number.isInteger(status.version) &&
    (status.active_version === null ||
      Number.isInteger(status.active_version)) &&
    (status.active === null || isConfig(status.active)) &&
    (status.draft === null || isConfig(status.draft)) &&
    typeof status.checked === "boolean" &&
    typeof status.trial_succeeded === "boolean"
  );
}

const HelpTip = ({ label, text }: { label: string; text: string }) => (
  <Popover content={text} trigger="click">
    <Button
      type="text"
      size="small"
      icon={<InfoCircleOutlined />}
      aria-label={`Справка: ${label}`}
    />
  </Popover>
);

export const OidcMethodsBlock = observer(() => {
  const { api, session } = useStores();
  const [form] = Form.useForm<Values>();
  const [state, setState] = useState<Status | null>(null);
  const [error, setError] = useState<unknown>(null);
  const [pending, setPending] = useState<string | null>(null);
  const mutationInFlight = useRef(false);
  const caInput = useRef<HTMLInputElement>(null);
  const [confirm, setConfirm] = useState<"activate" | "disable" | null>(null);
  const [uncertain, setUncertain] = useState(false);
  const canWrite = session.has("settings.write");

  const load = useCallback(async () => {
    setPending("load");
    setError(null);
    try {
      const response = await api.request<unknown>("/oidc-provider");
      if (!isStatus(response)) throw new ApiError(200, "invalid_response");
      const next = response;
      setState(next);
      setUncertain(false);
      const config = next.draft ?? next.active;
      if (config)
        form.setFieldsValue({
          issuer_url: config.issuer_url,
          client_id: config.client_id,
          redirect_uri: config.redirect_uri,
          scopes: config.scopes.join(" "),
          client_secret: "",
        });
    } catch (cause) {
      setError(cause);
    } finally {
      setPending(null);
    }
  }, [api, form]);

  useEffect(() => {
    void load();
  }, [load]);

  useEffect(() => {
    const result = new URLSearchParams(window.location.search).get(
      "oidc_trial",
    );
    if (!result) return;
    if (result !== "success") {
      setError(new ApiError(422, "oidc_trial_failed"));
    }
    window.history.replaceState(null, "", "/administration");
  }, []);

  const run = async (
    name: string,
    action: () => Promise<Status>,
    success: string,
  ) => {
    if (mutationInFlight.current || pending || !state || uncertain) return;
    mutationInFlight.current = true;
    setPending(name);
    setError(null);
    try {
      const next = await action();
      if (!isStatus(next)) throw new ApiError(200, "invalid_response");
      setState(next);
      form.setFieldValue("client_secret", "");
      message.success(success);
    } catch (cause) {
      setError(cause);
      if (cause instanceof ApiError) {
        const details = cause.details;
        const field =
          cause.code === "oidc_secret_required"
            ? "client_secret"
            : cause.code.startsWith("oidc_callback_")
              ? "redirect_uri"
              : cause.code === "oidc_url_invalid" ||
                  cause.code === "oidc_https_required"
                ? details && typeof details === "object" && "field" in details
                  ? details.field === "redirect_uri"
                    ? "redirect_uri"
                    : "issuer_url"
                  : "issuer_url"
                : null;
        if (field)
          form.setFields([{ name: field, errors: [errorText(cause)] }]);
      }
      setUncertain(!isConfirmedRejection(cause));
    } finally {
      mutationInFlight.current = false;
      setPending(null);
      setConfirm(null);
    }
  };

  const postVersion = (path: string) =>
    api.request<Status>(path, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ version: state?.version }),
    });

  return (
    <section className={styles.block} aria-labelledby="methods-heading">
      <Typography.Title level={3} id="methods-heading">
        Способы входа
      </Typography.Title>
      <Typography.Paragraph>
        Локальный вход всегда доступен как резервный способ. Сохраните черновик,
        проверьте discovery и TLS, затем выполните пробный вход. Проверка
        соединения не подтверждает успешный вход.
      </Typography.Paragraph>
      {pending === "load" && <Spin aria-label="Загрузка способов входа" />}
      {error !== null && (
        <AppRequestError
          error={error}
          message={
            uncertain
              ? `${errorText(error)} Исход изменения неизвестен. Обновите состояние перед следующим действием.`
              : error instanceof ApiError &&
                  error.code === "oidc_key_unavailable"
                ? `${errorText(error)} Задайте постоянный EXTERNAL_SECRET_KEY в защищённом файле или корневом dev .env и обновите состояние.`
                : undefined
          }
          onRetry={() => void load()}
          actionLabel="Обновить"
        />
      )}
      {state && (
        <>
          {state.diagnostic && (
            <Alert
              type="error"
              message={errorText(new ApiError(503, state.diagnostic))}
            />
          )}
          <Typography.Paragraph>
            Источник:{" "}
            {state.mode === "env"
              ? "настройки хоста"
              : state.mode === "managed"
                ? "интерфейс"
                : "OIDC отключён"}
            . Активная версия: {state.active_version ?? "—"}.
          </Typography.Paragraph>
          {state.mode === "disabled" && (
            <Typography.Paragraph>
              Для повторного включения подготовьте черновик, проверьте
              настройки, выполните пробный вход текущей версии и активируйте
              OIDC.
            </Typography.Paragraph>
          )}
          <ol aria-label="Этапы настройки OIDC">
            <li>Черновик: {state.draft ? "сохранён" : "не создан"}</li>
            <li>
              Discovery и TLS: {state.checked ? "проверены" : "не проверены"}
            </li>
            <li>
              Пробный вход:{" "}
              {state.trial_succeeded ? "подтверждён" : "не подтверждён"}
            </li>
            <li>
              Активация:{" "}
              {state.trial_succeeded ? "доступна" : "после пробного входа"}
            </li>
          </ol>
          {state.mode === "env" && (
            <Typography.Paragraph>
              OIDC из настроек хоста:{" "}
              {state.env_enabled ? "включён" : "отключён"}.
            </Typography.Paragraph>
          )}
          {state.active && (
            <Typography.Paragraph className={styles.wrap}>
              Активный issuer: {state.active.issuer_url}. Client ID:{" "}
              {state.active.client_id}. Callback: {state.active.redirect_uri}.
              Scopes: {state.active.scopes.join(" ")}. Секрет:{" "}
              {state.active.has_secret ? "сохранён" : "отсутствует"}. CA:{" "}
              {state.active.has_ca ? "загружен" : "системное доверие"}.
            </Typography.Paragraph>
          )}
          <Form
            form={form}
            layout="vertical"
            onFinish={(values) =>
              void run(
                "save",
                () =>
                  api.request<Status>("/oidc-provider/draft", {
                    method: "PUT",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({
                      version: state.version,
                      issuer_url: values.issuer_url,
                      client_id: values.client_id,
                      redirect_uri: values.redirect_uri,
                      scopes: values.scopes.split(/\s+/).filter(Boolean),
                      client_secret: values.client_secret || null,
                    }),
                  }),
                "Черновик сохранён",
              )
            }
          >
            <Form.Item
              name="issuer_url"
              label={
                <>
                  Issuer URL{" "}
                  <HelpTip
                    label="Issuer URL"
                    text="Адрес конкретного realm в IdP, например http://localhost:8081/realms/master; корень Keycloak не подходит."
                  />
                </>
              }
              rules={[{ required: true }]}
            >
              <Input
                aria-label="Issuer URL"
                disabled={!canWrite || !!pending || uncertain}
              />
            </Form.Item>
            <Form.Item
              name="client_id"
              label={
                <>
                  Client ID{" "}
                  <HelpTip
                    label="Client ID"
                    text="Идентификатор клиента, созданного в настройках того же realm IdP."
                  />
                </>
              }
              rules={[{ required: true }]}
            >
              <Input
                aria-label="Client ID"
                disabled={!canWrite || !!pending || uncertain}
              />
            </Form.Item>
            <Form.Item
              name="redirect_uri"
              label={
                <>
                  Callback URL{" "}
                  <HelpTip
                    label="Callback URL"
                    text="Точный публичный адрес приложения; зарегистрируйте его в Valid redirect URIs клиента IdP, например http://localhost:8080/api/v1/auth/oidc/callback."
                  />
                </>
              }
              rules={[{ required: true }]}
            >
              <Input
                aria-label="Callback URL"
                disabled={!canWrite || !!pending || uncertain}
              />
            </Form.Item>
            <Form.Item
              name="scopes"
              label={
                <>
                  Scopes через пробел{" "}
                  <HelpTip
                    label="Scopes"
                    text="Разрешённые клиенту IdP области доступа через пробел; openid обязателен, например openid profile email."
                  />
                </>
              }
              initialValue="openid profile email"
              rules={[{ required: true }]}
            >
              <Input
                aria-label="Scopes через пробел"
                disabled={!canWrite || !!pending || uncertain}
              />
            </Form.Item>
            <Form.Item
              name="client_secret"
              label={
                <>
                  Client secret (
                  {state.draft?.has_secret ? "сохранён" : "отсутствует"}){" "}
                  <HelpTip
                    label="Client secret"
                    text="Секрет берётся из настроек того же клиента в IdP. Он не отображается после сохранения."
                  />
                </>
              }
              extra="Оставьте пустым, чтобы сохранить действующий секрет."
            >
              <Input.Password
                autoComplete="new-password"
                disabled={!canWrite || !!pending || uncertain}
              />
            </Form.Item>
            {canWrite && (
              <Button
                htmlType="submit"
                type="primary"
                loading={pending === "save"}
                disabled={!!pending || uncertain}
              >
                Сохранить черновик
              </Button>
            )}
          </Form>
          <Typography.Paragraph>
            {state.draft ? (
              <>
                Черновик: CA{" "}
                {state.draft.has_ca ? "загружен" : "системное доверие"}. Секрет{" "}
                {state.draft.has_secret ? "сохранён" : "отсутствует"}.
              </>
            ) : (
              "Черновик ещё не создан."
            )}
          </Typography.Paragraph>
          {canWrite && (
            <Space wrap>
              <Button
                disabled={!state.draft || !!pending || uncertain}
                loading={pending === "ca"}
                onClick={() => caInput.current?.click()}
              >
                Загрузить CA PEM
              </Button>
              <HelpTip
                label="CA PEM"
                text="Нужен только для доверия частному удостоверяющему центру HTTPS-провайдера. Загружайте сертификат CA в PEM, не сертификат сервера и не приватный ключ."
              />
              <input
                ref={caInput}
                type="file"
                accept=".pem,application/x-pem-file"
                disabled={!state.draft || !!pending || uncertain}
                className={styles.hiddenInput}
                onChange={(event) => {
                  const file = event.target.files?.[0];
                  event.target.value = "";
                  if (file && file.size <= 65536)
                    void run(
                      "ca",
                      () =>
                        api.request<Status>(
                          `/oidc-provider/draft/ca?version=${state.version}`,
                          {
                            method: "PUT",
                            headers: {
                              "Content-Type": "application/x-pem-file",
                            },
                            body: file,
                          },
                        ),
                      "CA загружен",
                    );
                  else if (file) setError(new ApiError(422, "oidc_ca_invalid"));
                }}
              />
              <Button
                disabled={!state.draft?.has_ca || !!pending || uncertain}
                loading={pending === "remove-ca"}
                onClick={() =>
                  void run(
                    "remove-ca",
                    () =>
                      api.request<Status>(
                        `/oidc-provider/draft/ca?version=${state.version}`,
                        { method: "DELETE" },
                      ),
                    "CA удалён",
                  )
                }
              >
                Удалить CA
              </Button>
              <Button
                disabled={!state.draft?.has_secret || !!pending || uncertain}
                onClick={() =>
                  void run(
                    "remove-secret",
                    () =>
                      api.request<Status>(
                        `/oidc-provider/draft/secret?version=${state.version}`,
                        { method: "DELETE" },
                      ),
                    "Секрет удалён из черновика",
                  )
                }
              >
                Удалить секрет из черновика
              </Button>
              <Button
                disabled={!state.draft || !!pending || uncertain}
                loading={pending === "check"}
                onClick={() =>
                  void run(
                    "check",
                    () => postVersion("/oidc-provider/draft/check"),
                    "Discovery и TLS проверены",
                  )
                }
              >
                Проверить настройки
              </Button>
              <Button
                disabled={!state.checked || !!pending || uncertain}
                onClick={() => {
                  if (mutationInFlight.current) return;
                  mutationInFlight.current = true;
                  setPending("trial");
                  window.location.assign(
                    `/api/v1/auth/oidc/trial/start?version=${state.version}`,
                  );
                }}
              >
                Пробный вход
              </Button>
              <Button
                type="primary"
                disabled={!state.trial_succeeded || !!pending || uncertain}
                onClick={() => setConfirm("activate")}
              >
                Активировать
              </Button>
              <Button
                danger
                disabled={
                  state.mode === "disabled" ||
                  (state.mode === "env" && !state.env_enabled) ||
                  !!pending ||
                  uncertain
                }
                onClick={() => setConfirm("disable")}
              >
                Отключить OIDC
              </Button>
            </Space>
          )}
          {canWrite && (
            <Typography.Paragraph>
              {!state.draft &&
                "Сначала сохраните черновик: тогда станут доступны CA и проверка. "}
              {state.draft &&
                !state.draft.has_ca &&
                "Удалить CA можно после его загрузки. "}
              {state.draft &&
                !state.draft.has_secret &&
                "Удалить секрет можно после его сохранения. "}
              {state.draft &&
                !state.checked &&
                "Проверьте настройки, чтобы открыть пробный вход. "}
              {state.checked &&
                !state.trial_succeeded &&
                "Выполните пробный вход текущей версии, чтобы открыть активацию. "}
              {(state.mode === "disabled" ||
                (state.mode === "env" && !state.env_enabled)) &&
                "OIDC уже отключён: действие «Отключить OIDC» недоступно."}
              {pending && "Дождитесь завершения текущей операции. "}
              {uncertain && "Сначала обновите состояние после сетевой ошибки."}
            </Typography.Paragraph>
          )}
          {state.checked && !state.trial_succeeded && (
            <Typography.Paragraph>
              Соединение проверено. Для активации нужен пробный вход текущей
              версии.
            </Typography.Paragraph>
          )}
          {state.trial_succeeded && (
            <Alert
              type="success"
              message="Пробный вход прошёл без создания сессии приложения"
            />
          )}
        </>
      )}
      <ConfirmationDialog
        open={confirm !== null}
        pending={!!pending}
        title={
          confirm === "activate" ? "Заменить активный OIDC" : "Отключить OIDC"
        }
        message={
          confirm === "activate"
            ? "После активации новый провайдер заменит текущий. Локальный вход останется доступен."
            : "Кнопка OIDC исчезнет со страницы входа. Локальный вход останется доступен."
        }
        confirmText={confirm === "activate" ? "Активировать" : "Отключить"}
        danger={confirm === "disable"}
        onCancel={() => setConfirm(null)}
        onConfirm={() =>
          void run(
            confirm || "",
            () =>
              postVersion(
                confirm === "activate"
                  ? "/oidc-provider/activate"
                  : "/oidc-provider/disable",
              ),
            confirm === "activate" ? "OIDC активирован" : "OIDC отключён",
          )
        }
      />
    </section>
  );
});
