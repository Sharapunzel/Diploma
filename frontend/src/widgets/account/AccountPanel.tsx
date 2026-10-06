import { useEffect, useRef, useState } from "react";
import { UserOutlined } from "@ant-design/icons";
import { Button } from "antd";
import { observer } from "mobx-react-lite";
import { useNavigate } from "react-router-dom";
import { useStores } from "../../app/providers/StoresProvider";
import { ConfirmationDialog } from "../../shared/ui/ConfirmationDialog";
import { AppRequestError } from "../../app/ui/AppRequestError";
import { ThemeToggleControl } from "../../app/ui/ThemeToggleControl";
import styles from "./AccountPanel.module.css";

export const AccountPanel = observer(() => {
  const { layout, session } = useStores();
  const navigate = useNavigate();
  const anchor = useRef<HTMLDivElement>(null);
  const trigger = useRef<HTMLButtonElement>(null);
  const confirmationOpenRef = useRef(false);
  const [confirmationOpen, setConfirmationOpen] = useState(false);
  const [pending, setPending] = useState(false);
  const [logoutError, setLogoutError] = useState<unknown | null>(null);
  const pendingRef = useRef(false);
  const username = session.data?.user?.username ?? "—";

  useEffect(() => {
    if (!layout.accountOpen) return;
    const closeIfOutside = (event: PointerEvent) => {
      if (
        !confirmationOpenRef.current &&
        event.target instanceof Node &&
        !anchor.current?.contains(event.target)
      )
        layout.setAccountOpen(false);
    };
    const closeOnEscape = (event: KeyboardEvent) => {
      if (event.key !== "Escape" || confirmationOpenRef.current) return;
      event.preventDefault();
      layout.setAccountOpen(false);
      trigger.current?.focus();
    };
    const closeOnFocusLeave = (event: FocusEvent) => {
      if (
        !confirmationOpenRef.current &&
        event.target instanceof Node &&
        !anchor.current?.contains(event.target)
      )
        layout.setAccountOpen(false);
    };
    document.addEventListener("pointerdown", closeIfOutside);
    document.addEventListener("keydown", closeOnEscape);
    document.addEventListener("focusin", closeOnFocusLeave);
    return () => {
      document.removeEventListener("pointerdown", closeIfOutside);
      document.removeEventListener("keydown", closeOnEscape);
      document.removeEventListener("focusin", closeOnFocusLeave);
    };
  }, [layout, layout.accountOpen]);

  const openConfirmation = () => {
    setLogoutError(null);
    confirmationOpenRef.current = true;
    setConfirmationOpen(true);
  };

  const cancelConfirmation = () => {
    confirmationOpenRef.current = false;
    setConfirmationOpen(false);
  };

  const confirmLogout = async () => {
    if (pendingRef.current) return;
    pendingRef.current = true;
    setPending(true);
    setLogoutError(null);
    try {
      await session.logout();
      confirmationOpenRef.current = false;
      setConfirmationOpen(false);
      layout.setAccountOpen(false);
      navigate("/login", { replace: true });
    } catch (error) {
      setLogoutError(error);
    } finally {
      pendingRef.current = false;
      setPending(false);
    }
  };

  return (
    <div className={styles.anchor} ref={anchor}>
      <Button
        ref={trigger}
        aria-controls="account-panel"
        aria-expanded={layout.accountOpen}
        aria-label={`Аккаунт: ${username}`}
        className={styles.trigger}
        icon={<UserOutlined />}
        onClick={() => layout.setAccountOpen(!layout.accountOpen)}
        title={username}
      >
        <span className={styles.triggerName}>{username}</span>
      </Button>
      {layout.accountOpen && (
        <section
          aria-labelledby="account-panel-title"
          className={styles.panel}
          id="account-panel"
          role="region"
        >
          <h2 className={styles.title} id="account-panel-title">
            Аккаунт
          </h2>
          <dl className={styles.identity}>
            <div>
              <dt>Пользователь</dt>
              <dd title={username}>{username}</dd>
            </div>
            <div>
              <dt>Роль</dt>
              <dd title={session.data?.role ?? "—"}>
                {session.data?.role ?? "—"}
              </dd>
            </div>
            <div>
              <dt>Способ входа</dt>
              <dd>
                {session.data?.authentication_method === "oidc"
                  ? "OIDC"
                  : "Локальный вход"}
              </dd>
            </div>
          </dl>
          <div className={styles.preference}>
            <span className={styles.preferenceLabel}>Тема</span>
            <ThemeToggleControl />
          </div>
          <Button
            className={styles.logout}
            danger
            disabled={pending}
            onClick={openConfirmation}
          >
            Выйти
          </Button>
        </section>
      )}
      <ConfirmationDialog
        open={confirmationOpen}
        pending={pending}
        title="Выйти из аккаунта?"
        message="Вы действительно хотите завершить текущий сеанс?"
        confirmText="Выйти"
        danger
        onCancel={cancelConfirmation}
        onConfirm={() => void confirmLogout()}
      >
        {logoutError !== null && <AppRequestError error={logoutError} />}
      </ConfirmationDialog>
    </div>
  );
});
