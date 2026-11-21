import { FieldHelp } from "../../shared/ui/FieldHelp";
import styles from "./Findings.module.css";

export function ColumnLabel({ name, help }: { name: string; help: string }) {
  return (
    <span className={styles.fieldCaption}>
      <span>{name}</span>
      <FieldHelp name={name} text={help} />
    </span>
  );
}
