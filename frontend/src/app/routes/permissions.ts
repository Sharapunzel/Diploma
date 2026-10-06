type NavigationPage = {
  path: string;
  label: string;
  permission: string | null;
};

type NavigationGroup = {
  label: string;
  pages: readonly NavigationPage[];
};

export const navigationGroups = [
  {
    label: "Главная",
    pages: [{ path: "/", label: "Главная", permission: null }],
  },
  {
    label: "Обработка данных",
    pages: [
      {
        path: "/connections",
        label: "Подключения",
        permission: "connections.read",
      },
      { path: "/sources", label: "Источники", permission: "sources.read" },
      {
        path: "/normalizers",
        label: "Нормализаторы",
        permission: "normalizers.read",
      },
      { path: "/diagnostics", label: "Диагностика", permission: "events.read" },
    ],
  },
  {
    label: "Анализ данных",
    pages: [{ path: "/events", label: "События", permission: "events.read" }],
  },
  {
    label: "Управление",
    pages: [
      {
        path: "/administration",
        label: "Администрирование",
        permission: "users.read",
      },
      {
        path: "/settings",
        label: "Настройки приложения",
        permission: "settings.read",
      },
    ],
  },
] as const satisfies readonly NavigationGroup[];

export const homePage = navigationGroups[0].pages[0];

export const pages = navigationGroups
  .reduce<NavigationPage[]>((result, group) => [...result, ...group.pages], [])
  .filter(
    (page): page is NavigationPage & { permission: string } =>
      page.permission !== null,
  );

export function availableNavigationGroups(
  hasPermission: (permission: string) => boolean,
) {
  return navigationGroups
    .map((group) => ({
      label: group.label,
      pages: group.pages.filter(
        (page) => page.permission === null || hasPermission(page.permission),
      ),
    }))
    .filter((group) => group.pages.length > 0);
}
