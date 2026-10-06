import { useState } from "react";
import { Alert, Button } from "antd";
import CopyOutlined from "@ant-design/icons/CopyOutlined";
import DisconnectOutlined from "@ant-design/icons/DisconnectOutlined";
import styles from "./RequestError.module.css";

export function RequestError({
  message,
  requestId,
  onRetry,
  variant = "inline",
}: {
  message: string;
  requestId?: string | null;
  onRetry?: () => void;
  variant?: "inline" | "page";
}) {
  const [copyState, setCopyState] = useState("");
  const copyRequestId = async () => {
    if (!requestId) return;
    try {
      await navigator.clipboard.writeText(requestId);
      setCopyState("Идентификатор скопирован");
    } catch {
      setCopyState("Не удалось скопировать. Выделите идентификатор вручную.");
    }
  };

  if (variant === "page") {
    return (
      <div className={styles.pageError} role="alert">
        <span aria-hidden="true" className={styles.pageIcon}>
          <DisconnectOutlined />
        </span>
        <h2>Не удалось загрузить приложение</h2>
        <p>{message}</p>
        {requestId && (
          <details className={styles.pageDetails}>
            <summary>Подробности ошибки</summary>
            <div className={styles.requestId}>
              <span>Идентификатор запроса</span>
              <code>{requestId}</code>
              <Button onClick={() => void copyRequestId()} size="small">
                Копировать
              </Button>
              {copyState && <span aria-live="polite">{copyState}</span>}
            </div>
          </details>
        )}
        {onRetry && (
          <Button onClick={onRetry} type="primary">
            Повторить
          </Button>
        )}
      </div>
    );
  }

  return (
    <Alert
      className={styles.alert}
      description={
        requestId && (
          <details className={styles.details}>
            <summary>Подробности</summary>
            <div className={styles.requestId}>
              <span>Идентификатор запроса</span>
              <code>{requestId}</code>
              <Button
                aria-label="Копировать идентификатор запроса"
                icon={<CopyOutlined />}
                onClick={() => void copyRequestId()}
                size="small"
              >
                Копировать
              </Button>
              {copyState && (
                <span aria-live="polite" className={styles.copyState}>
                  {copyState}
                </span>
              )}
            </div>
          </details>
        )
      }
      message={message}
      role="alert"
      showIcon
      type="error"
      action={onRetry && <Button onClick={onRetry}>Повторить</Button>}
    />
  );
}
