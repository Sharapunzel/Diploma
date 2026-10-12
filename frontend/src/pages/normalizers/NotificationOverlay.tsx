import { useEffect } from "react";
import { Alert } from "antd";
import type { ReactNode } from "react";
import styles from "./Normalizers.module.css";

export function NormalizerFeedback({
  message,
  type,
  onClose,
  action,
}: {
  message: string;
  type: "success" | "info" | "warning" | "error";
  onClose: () => void;
  action?: ReactNode;
}) {
  useEffect(() => {
    if (type === "error") return;
    const timer = window.setTimeout(onClose, 5000);
    return () => window.clearTimeout(timer);
  }, [message, type, onClose]);

  return (
    <Alert
      type={type}
      showIcon
      closable
      message={message}
      action={action}
      onClose={onClose}
    />
  );
}

export function FeedbackLayer({ children }: { children: ReactNode }) {
  return (
    <div className={styles.feedback} aria-live="polite">
      {children}
    </div>
  );
}
