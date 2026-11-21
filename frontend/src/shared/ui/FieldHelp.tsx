import { InfoCircleOutlined } from "@ant-design/icons";
import { Button, Popover } from "antd";
import { useState } from "react";
import styles from "./FieldHelp.module.css";

export function FieldHelp({
  text,
  name = "Описание",
}: {
  text: string;
  name?: string;
}) {
  const [open, setOpen] = useState(false);
  return (
    <Popover
      open={open}
      onOpenChange={setOpen}
      title={name}
      content={<div className={styles.content}>{text}</div>}
      trigger="click"
    >
      <Button
        type="text"
        size="small"
        className={styles.trigger}
        aria-label={`Справка: ${name}`}
        aria-expanded={open}
        icon={<InfoCircleOutlined />}
        onKeyDownCapture={(event) => {
          if (open && event.key === "Escape") {
            event.preventDefault();
            event.stopPropagation();
            setOpen(false);
          }
        }}
      />
    </Popover>
  );
}
