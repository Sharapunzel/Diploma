type NavigationPage = {
  path: string;
  label: string;
  permission: string | readonly string[] | null;
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
      { path: "/agents", label: "Агенты", permission: "agents.read" },
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
        permission: ["users.read", "settings.read"],
      },
    ],
  },
] as const satisfies readonly NavigationGroup[];

export const homePage = navigationGroups[0].pages[0];

export const pages = navigationGroups
  .reduce<NavigationPage[]>((result, group) => [...result, ...group.pages], [])
  .filter(
    (
      page,
    ): page is NavigationPage & { permission: string | readonly string[] } =>
      page.permission !== null,
  );

export function canAccessPage(
  permission: NavigationPage["permission"],
  hasPermission: (permission: string) => boolean,
): boolean {
  if (permission === null) return true;
  return typeof permission === "string"
    ? hasPermission(permission)
    : permission.some(hasPermission);
}

export function availableNavigationGroups(
  hasPermission: (permission: string) => boolean,
) {
  return navigationGroups
    .map((group) => ({
      label: group.label,
      pages: group.pages.filter((page) =>
        canAccessPage(page.permission, hasPermission),
      ),
    }))
    .filter((group) => group.pages.length > 0);
}
