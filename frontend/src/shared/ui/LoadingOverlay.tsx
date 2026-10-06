import { Spin } from "antd";
import styles from "./LoadingOverlay.module.css";

export function LoadingOverlay() {
  return (
    <div
      aria-label="Загрузка"
      aria-live="polite"
      className={styles.overlay}
      data-testid="loading-overlay"
      role="status"
    >
      <Spin size="small" />
    </div>
  );
}
