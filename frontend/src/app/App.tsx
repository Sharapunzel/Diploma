import { useEffect, useLayoutEffect, useState } from "react";
import { ConfigProvider, theme as antdTheme, type ThemeConfig } from "antd";
import ruRU from "antd/locale/ru_RU";
import { observer } from "mobx-react-lite";
import { useStores } from "./providers/StoresProvider";
import { AppRoutes } from "./routes/routes";
import { getAntdThemeTokens } from "./theme/antdTokens";

export const App = observer(() => {
  const { theme, session } = useStores();

  const [tokens, setTokens] = useState<NonNullable<ThemeConfig["token"]>>(() =>
    getAntdThemeTokens(),
  );

  useLayoutEffect(() => {
    setTokens(getAntdThemeTokens());
  }, [theme.dark]);

  useEffect(() => {
    void session.refresh(true);
    const onVisible = () => {
      if (document.visibilityState === "visible") void session.refresh();
    };
    document.addEventListener("visibilitychange", onVisible);
    return () => document.removeEventListener("visibilitychange", onVisible);
  }, [session]);

  return (
    <ConfigProvider
      locale={ruRU}
      theme={{
        algorithm: theme.dark
          ? antdTheme.darkAlgorithm
          : antdTheme.defaultAlgorithm,
        components: {
          Button: {
            colorPrimary: tokens.colorPrimary ?? "#64f86a",
            colorPrimaryHover: tokens.colorPrimaryHover,
            colorPrimaryActive: tokens.colorPrimaryActive,
            defaultColor: tokens.colorText,
            primaryColor: tokens.colorTextLightSolid,
          },
        },
        token: tokens,
      }}
    >
      <AppRoutes />
    </ConfigProvider>
  );
});
