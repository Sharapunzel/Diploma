import { describe, expect, it } from "vitest";
import { availableNavigationGroups, homePage, pages } from "./permissions";

describe("navigation groups", () => {
  it("keeps the agreed group, route and label order in one configuration", () => {
    const groups = availableNavigationGroups(() => true);
    expect(groups.map((group) => group.label)).toEqual([
      "Главная",
      "Обработка данных",
      "Анализ данных",
      "Управление",
    ]);
    expect(
      groups.map((group) => group.pages.map((page) => page.label)),
    ).toEqual([
      ["Главная"],
      ["Подключения", "Источники", "Нормализаторы", "Диагностика"],
      ["События"],
      ["Администрирование", "Настройки приложения"],
    ]);
    expect(homePage.path).toBe("/");
    expect(pages.find((page) => page.path === "/settings")).toMatchObject({
      label: "Настройки приложения",
      permission: "settings.read",
    });
    expect(pages.find((page) => page.path === "/diagnostics")?.permission).toBe(
      "events.read",
    );
  });

  it("filters pages before dropping empty groups", () => {
    const groups = availableNavigationGroups(
      (permission) => permission === "events.read",
    );
    expect(groups.map((group) => group.label)).toEqual([
      "Главная",
      "Обработка данных",
      "Анализ данных",
    ]);
    expect(groups.map((group) => group.pages.map((page) => page.path))).toEqual(
      [["/"], ["/diagnostics"], ["/events"]],
    );
    expect(availableNavigationGroups(() => false)).toEqual([
      { label: "Главная", pages: [homePage] },
    ]);
  });
});
