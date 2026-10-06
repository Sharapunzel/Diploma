import { useEffect, useRef, useState } from "react";
import { Alert, Button, Form, Input, Spin, Typography } from "antd";
import { observer } from "mobx-react-lite";
import { Navigate, useLocation, useNavigate } from "react-router-dom";
import type { Methods } from "../../app/api/dto";
import { useStores } from "../../app/providers/StoresProvider";
import { AppRequestError } from "../../app/ui/AppRequestError";
import { Logo } from "../../shared/ui/Logo";
import { ThemeToggleControl } from "../../app/ui/ThemeToggleControl";
import styles from "./LoginPage.module.css";

type Credentials = { username: string; password: string };

export const LoginPage = observer(() => {
  const { api, session } = useStores();
  const location = useLocation();
  const navigate = useNavigate();
  const [methods, setMethods] = useState<Methods | null>(null);
  const [methodsError, setMethodsError] = useState<unknown | null>(null);
  const [loginError, setLoginError] = useState<unknown | null>(null);
  const [busy, setBusy] = useState(false);
  const methodsController = useRef<AbortController | null>(null);
  const loginController = useRef<AbortController | null>(null);
  const attempt = useRef(0);
  const busyRef = useRef(false);
  const mounted = useRef(true);

  const from = (location.state as { from?: string } | null)?.from;
  const returnPath =
    from?.startsWith("/") && !from.startsWith("//") ? from : "/";

  const loadMethods = async () => {
    methodsController.current?.abort();
    const controller = new AbortController();
    methodsController.current = controller;
    setMethodsError(null);
    try {
      const result = await api.methods(controller.signal);
      if (mounted.current && !controller.signal.aborted) setMethods(result);
    } catch (error) {
      if (mounted.current && !controller.signal.aborted) setMethodsError(error);
    }
  };

  useEffect(() => {
    mounted.current = true;
    void loadMethods();
    return () => {
      mounted.current = false;
      methodsController.current?.abort();
      loginController.current?.abort();
    };
    // Requests are owned by this page instance.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [api]);

  const submit = async ({ username, password }: Credentials) => {
    if (busyRef.current) return;
    busyRef.current = true;
    setBusy(true);
    setLoginError(null);
    const currentAttempt = ++attempt.current;
    const version = session.version;
    loginController.current?.abort();
    const controller = new AbortController();
    loginController.current = controller;
    try {
      const data = await api.login(username, password, controller.signal);
      if (
        !mounted.current ||
        controller.signal.aborted ||
        currentAttempt !== attempt.current
      )
        return;
      if (session.accept(data, version))
        navigate(returnPath, { replace: true });
    } catch (error) {
      if (
        mounted.current &&
        !controller.signal.aborted &&
        currentAttempt === attempt.current
      ) {
        setLoginError(error);
      }
    } finally {
      if (mounted.current && currentAttempt === attempt.current) {
        busyRef.current = false;
        setBusy(false);
      }
    }
  };

  if (session.state === "authenticated")
    return <Navigate to={returnPath} replace />;

  return (
    <main id="main" className={styles.page} data-testid="login-page">
      <ThemeToggleControl className={styles.theme} />
      <div className={styles.scrollArea}>
        <section className={styles.card}>
          <div className={styles.brand}>
            <Logo />
            <span>Diploma</span>
          </div>
          <Typography.Title className={styles.title} level={1}>
            Вход в систему
          </Typography.Title>
          <p className={styles.intro}>
            Войдите, чтобы продолжить работу с системой.
          </p>
          {new URLSearchParams(location.search).get("error") === "oidc" && (
            <Alert
              className={styles.oidcError}
              type="error"
              showIcon
              message="Не удалось завершить вход через провайдера. Повторите попытку."
            />
          )}
          {methodsError !== null && (
            <AppRequestError
              error={methodsError}
              onRetry={() => void loadMethods()}
            />
          )}
          {!methods && methodsError === null && (
            <Spin tip="Проверка способов входа">
              <div className={styles.loadingSpace} />
            </Spin>
          )}
          {loginError !== null && <AppRequestError error={loginError} />}
          {methods?.local_enabled && (
            <Form
              autoComplete="off"
              className={styles.form}
              layout="vertical"
              onFinish={submit}
            >
              <Form.Item
                htmlFor="username"
                label="Имя пользователя"
                name="username"
                rules={[
                  { required: true, message: "Введите имя пользователя" },
                ]}
              >
                <Input autoComplete="off" id="username" name="username" />
              </Form.Item>
              <Form.Item
                htmlFor="password"
                label="Пароль"
                name="password"
                rules={[{ required: true, message: "Введите пароль" }]}
              >
                <Input.Password
                  autoComplete="off"
                  id="password"
                  name="password"
                />
              </Form.Item>
              <Button
                className={styles.submit}
                htmlType="submit"
                loading={busy}
                type="primary"
                block
              >
                Войти
              </Button>
            </Form>
          )}
          {methods?.oidc_enabled && (
            <Button
              className={styles.oidcButton}
              block
              href="/api/v1/auth/oidc/login"
            >
              Войти через провайдера
            </Button>
          )}
          {methods && !methods.local_enabled && !methods.oidc_enabled && (
            <Alert
              className={styles.info}
              type="info"
              message="Способы входа сейчас недоступны. Обратитесь к администратору."
            />
          )}
        </section>
      </div>
    </main>
  );
});
