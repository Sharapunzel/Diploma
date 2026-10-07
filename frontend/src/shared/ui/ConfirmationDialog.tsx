import { useLayoutEffect, useRef, type ReactNode } from "react";
import { Button, Modal } from "antd";
import styles from "./ConfirmationDialog.module.css";

export function ConfirmationDialog({
  open,
  pending = false,
  confirmDisabled = false,
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
  confirmDisabled?: boolean;
  title: string;
  message: string;
  confirmText: string;
  danger?: boolean;
  children?: ReactNode;
  onCancel: () => void;
  onConfirm: () => void;
}) {
  const openerRef = useRef<HTMLElement | null>(null);
  const wasOpenRef = useRef(false);
  useLayoutEffect(() => {
    if (open && !wasOpenRef.current) {
      openerRef.current =
        document.activeElement instanceof HTMLElement
          ? document.activeElement
          : null;
    }
    wasOpenRef.current = open;
  }, [open]);

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
            disabled={pending || confirmDisabled}
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
      afterOpenChange={(visible) => {
        if (!visible && wasOpenRef.current === false) {
          requestAnimationFrame(() => openerRef.current?.focus());
        }
      }}
      open={open}
      rootClassName={styles.root}
      title={title}
    >
      <div className={styles.message}>{message}</div>
      {children}
    </Modal>
  );
}
