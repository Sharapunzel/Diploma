import { BulbOutlined, DesktopOutlined, MoonOutlined } from "@ant-design/icons";
import { Button } from "antd";
import type { ThemeChoice } from "../types/theme";
import styles from "./ThemeToggle.module.css";

const modes: Record<ThemeChoice, { label: string; next: ThemeChoice }> = {
  system: { label: "системная", next: "light" },
  light: { label: "светлая", next: "dark" },
  dark: { label: "тёмная", next: "system" },
};

const nextLabels: Record<ThemeChoice, string> = {
  system: "светлую",
  light: "тёмную",
  dark: "системную",
};

const icons = {
  system: <DesktopOutlined />,
  light: <BulbOutlined />,
  dark: <MoonOutlined />,
};

export function ThemeToggle({
  choice,
  className = "",
  onChange,
}: {
  choice: ThemeChoice;
  className?: string;
  onChange: (choice: ThemeChoice) => void;
}) {
  const current = modes[choice];
  const next = nextLabels[choice];
  const label = `Тема: ${current.label}. Переключить на ${next}`;

  return (
    <Button
      aria-label={label}
      className={`${styles.button} ${className}`}
      data-testid="theme-toggle"
      icon={icons[choice]}
      onClick={() => onChange(current.next)}
      type="default"
    />
  );
}
