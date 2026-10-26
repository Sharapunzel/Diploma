import { useCallback, useEffect, useRef, useState } from "react";
import {
  Alert,
  Button,
  Form,
  Input,
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

export const OidcMethodsBlock = observer(() => {
  const { api, session } = useStores();
  const [form] = Form.useForm<Values>();
  const [state, setState] = useState<Status | null>(null);
  const [error, setError] = useState<unknown>(null);
  const [pending, setPending] = useState<string | null>(null);
  const mutationInFlight = useRef(false);
  const [confirm, setConfirm] = useState<"activate" | "disable" | null>(null);
  const [uncertain, setUncertain] = useState(false);
  const canWrite = session.has("settings.write");

  const load = useCallback(async () => {
    setPending("load");
    setError(null);
    try {
      const next = await api.request<Status>("/oidc-provider");
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
      if (
        !(cause instanceof ApiError) ||
        cause.status === 0 ||
        cause.status >= 500
      )
        setUncertain(true);
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
              label="Issuer URL"
              rules={[{ required: true }]}
            >
              <Input disabled={!canWrite || !!pending || uncertain} />
            </Form.Item>
            <Form.Item
              name="client_id"
              label="Client ID"
              rules={[{ required: true }]}
            >
              <Input disabled={!canWrite || !!pending || uncertain} />
            </Form.Item>
            <Form.Item
              name="redirect_uri"
              label="Callback URL"
              rules={[{ required: true }]}
            >
              <Input disabled={!canWrite || !!pending || uncertain} />
            </Form.Item>
            <Form.Item
              name="scopes"
              label="Scopes через пробел"
              initialValue="openid profile email"
              rules={[{ required: true }]}
            >
              <Input disabled={!canWrite || !!pending || uncertain} />
            </Form.Item>
            <Form.Item
              name="client_secret"
              label={`Client secret (${state.draft?.has_secret ? "сохранён" : "отсутствует"})`}
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
              <label className={styles.upload}>
                Загрузить CA PEM
                <input
                  type="file"
                  accept=".pem,application/x-pem-file"
                  disabled={!state.draft || !!pending || uncertain}
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
                    else if (file)
                      setError(new ApiError(422, "oidc_ca_invalid"));
                  }}
                />
              </label>
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
                disabled={state.mode === "disabled" || !!pending || uncertain}
                onClick={() => setConfirm("disable")}
              >
                Отключить OIDC
              </Button>
            </Space>
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
