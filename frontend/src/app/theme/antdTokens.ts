import type { ThemeConfig } from "antd";

type AntdTokens = NonNullable<ThemeConfig["token"]>;

export function getAntdThemeTokens(): AntdTokens {
  const styles = getComputedStyle(document.documentElement);
  const color = (name: string) => styles.getPropertyValue(name).trim();

  return {
    fontFamily: 'Play, "Segoe UI", sans-serif',
    fontSize: 14,
    fontSizeSM: 13,
    controlHeight: 36,
    borderRadius: 8,
    colorPrimary: color("--color-accent"),
    colorPrimaryHover: color("--color-accent-hover"),
    colorPrimaryActive: color("--color-accent-strong"),
    colorLink: color("--color-accent-strong"),
    colorLinkHover: color("--color-accent-hover"),
    colorInfo: color("--color-info"),
    colorSuccess: color("--color-success"),
    colorWarning: color("--color-warning"),
    colorError: color("--color-danger"),
    colorTextLightSolid: color("--color-on-accent"),
    colorTextBase: color("--color-text"),
    colorText: color("--color-text"),
    colorTextSecondary: color("--color-text-secondary"),
    colorBgBase: color("--color-page"),
    colorBgContainer: color("--color-surface-solid"),
    colorBgElevated: color("--color-surface-solid"),
    colorBorder: color("--color-border"),
    colorBorderSecondary: color("--color-border"),
    colorFillAlter: color("--color-surface"),
    controlOutline: color("--color-focus"),
    controlOutlineWidth: 2,
  };
}
