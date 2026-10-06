import type { ReactNode } from "react";
import { Button, Modal } from "antd";
import styles from "./ConfirmationDialog.module.css";

export function ConfirmationDialog({
  open,
  pending = false,
  title,
  message,
  confirmText,
  danger = false,
  children,
  onCancel,
  onConfirm,
}: {
  open: boolean;
  pending?: boolean;
  title: string;
  message: string;
  confirmText: string;
  danger?: boolean;
  children?: ReactNode;
  onCancel: () => void;
  onConfirm: () => void;
}) {
  return (
    <Modal
      classNames={{
        body: styles.body,
        content: styles.content,
        header: styles.header,
      }}
      closable={!pending}
      footer={
        <div className={styles.footer}>
          <Button autoFocus disabled={pending} onClick={onCancel}>
            Отмена
          </Button>
          <Button
            className={styles.confirm}
            disabled={pending}
            loading={pending}
            onClick={onConfirm}
            danger={danger}
            type="primary"
          >
            {confirmText}
          </Button>
        </div>
      }
      keyboard={!pending}
      maskClosable={!pending}
      onCancel={onCancel}
      open={open}
      rootClassName={styles.root}
      title={title}
    >
      <div className={styles.message}>{message}</div>
      {children}
    </Modal>
  );
}
