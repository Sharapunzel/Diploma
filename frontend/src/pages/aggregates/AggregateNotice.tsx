import { useEffect, useState, type ReactNode } from "react";
import { Alert } from "antd";

/** Dismisses presentation only; validation and unresolved requests stay in their stores. */
export function AggregateNotice({
  type = "info",
  message,
  description,
  duration,
}: {
  type?: "info" | "success" | "warning" | "error";
  message: ReactNode;
  description?: ReactNode;
  duration?: number;
  role?: "alert" | "status";
}) {
  const [visible, setVisible] = useState(true);
  const delay = duration ?? (type === "success" ? 5000 : 0);
  useEffect(() => {
    setVisible(true);
    if (!delay) return;
    const timer = setTimeout(() => setVisible(false), delay);
    return () => clearTimeout(timer);
  }, [message, delay]);
  return visible ? (
    <Alert
      type={type}
      showIcon
      closable
      onClose={() => setVisible(false)}
      role={type === "error" ? "alert" : "status"}
      message={message}
      description={description}
    />
  ) : null;
}
