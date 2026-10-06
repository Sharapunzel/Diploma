import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { ApiError } from "../api/client";
import type { Session } from "../api/dto";
import { StoresProvider } from "../providers/StoresProvider";
import { AppStores } from "../stores/AppStores";
import { Protected } from "./routes";

const session: Session = {
  authenticated: true,
  user: {
    id: "1",
    username: "alice",
    email: null,
    display_name: "Алиса Очень Длинное Имя",
  },
  role: "Переименованная роль со многими словами",
  permissions: ["events.read"],
  authentication_method: "oidc",
  csrf_token: "csrf",
};

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

function mount(stores: AppStores, permission = "events.read") {
  render(
    <StoresProvider stores={stores}>
      <MemoryRouter initialEntries={["/events"]}>
        <Routes>
          <Route
            path="/events"
            element={
              <Protected permission={permission}>
                <div>Секретный раздел</div>
              </Protected>
            }
          />
          <Route path="/login" element={<div>Страница входа</div>} />
        </Routes>
      </MemoryRouter>
    </StoresProvider>,
  );
}

describe("protected UI", () => {
  it("shows permitted groups consistently in expanded, compact and mobile navigation", async () => {
    const stores = new AppStores();
    stores.session.accept(session, stores.session.version);
    mount(stores);
    const desktop = screen.getByRole("navigation", { name: "Основное меню" });
    expect(
      Array.from(
        desktop.querySelectorAll(".ant-menu-item-group-title"),
        (item) => item.textContent?.trim(),
      ),
    ).toEqual(["Обработка данных", "Анализ данных"]);
    expect(
      within(desktop)
        .getAllByRole("link")
        .map((link) => link.getAttribute("href")),
    ).toEqual(["/", "/diagnostics", "/events"]);
    expect(
      desktop.querySelector(".ant-menu-item-group-title[tabindex]"),
    ).toBeNull();

    await userEvent.click(
      screen.getByRole("button", { name: "Свернуть меню" }),
    );
    const compact = screen.getByTestId("compact-navigation");
    expect(
      within(compact)
        .getAllByRole("link")
        .map((link) => link.getAttribute("href")),
    ).toEqual(["/", "/diagnostics", "/events"]);
    expect(
      within(compact)
        .getAllByRole("separator")
        .map((item) => item.getAttribute("aria-label")),
    ).toEqual(["Обработка данных", "Анализ данных"]);

    await userEvent.click(screen.getByRole("button", { name: "Открыть меню" }));
    const mobile = await screen.findByRole("navigation", {
      name: "Мобильное меню",
    });
    expect(
      Array.from(
        mobile.querySelectorAll(".ant-menu-item-group-title"),
        (item) => item.textContent?.trim(),
      ),
    ).toEqual(["Обработка данных", "Анализ данных"]);
    expect(
      within(mobile)
        .getAllByRole("link")
        .map((link) => link.getAttribute("href")),
    ).toEqual(["/", "/diagnostics", "/events"]);
  });

  it("does not leak content before session check and uses permissions after it", async () => {
    const stores = new AppStores();
    mount(stores);
    expect(screen.queryByText("Секретный раздел")).toBeNull();
    act(() => {
      stores.session.accept(session, stores.session.version);
    });
    expect(await screen.findByText("Секретный раздел")).toBeDefined();
    expect(
      screen.getByRole("button", { name: "Аккаунт: alice" }),
    ).toBeDefined();
  });

  it("denies a renamed role without the required permission without logging out", async () => {
    const stores = new AppStores();
    stores.session.accept(session, stores.session.version);
    mount(stores, "users.read");
    expect(
      await screen.findByText("Недостаточно прав для просмотра раздела"),
    ).toBeDefined();
    expect(screen.queryByText("Секретный раздел")).toBeNull();
    expect(stores.session.state).toBe("authenticated");
  });

  it("confirms logout, keeps the popover open and reports a safe error", async () => {
    const stores = new AppStores();
    stores.session.accept(session, stores.session.version);
    stores.api.logout = vi
      .fn()
      .mockRejectedValue(new ApiError(403, "csrf_invalid", "req-3"));
    mount(stores);
    await userEvent.click(
      screen.getByRole("button", { name: "Аккаунт: alice" }),
    );
    expect(
      await screen.findByText("Переименованная роль со многими словами"),
    ).toBeDefined();
    expect(screen.getByText("OIDC")).toBeDefined();
    expect(
      within(screen.getByRole("region", { name: "Аккаунт" })).getByText(
        "alice",
        { exact: true },
      ),
    ).toBeDefined();
    expect(screen.queryByText("Алиса Очень Длинное Имя")).toBeNull();
    const themeToggle = screen.getByRole("button", {
      name: "Тема: системная. Переключить на светлую",
    });
    expect(themeToggle).toBeDefined();
    await userEvent.click(screen.getByRole("button", { name: "Выйти" }));
    expect(stores.api.logout).not.toHaveBeenCalled();
    const dialog = await screen.findByRole("dialog", {
      name: "Выйти из аккаунта?",
    });
    expect(dialog).toBeDefined();
    expect(screen.getByRole("region", { name: "Аккаунт" })).toBeDefined();
    await userEvent.click(
      within(dialog).getByRole("button", { name: "Отмена" }),
    );
    expect(stores.api.logout).not.toHaveBeenCalled();
    expect(stores.session.state).toBe("authenticated");

    await userEvent.click(screen.getByRole("button", { name: "Выйти" }));
    const reopened = await screen.findByRole("dialog", {
      name: "Выйти из аккаунта?",
    });
    await userEvent.click(
      within(reopened).getByRole("button", { name: "Выйти" }),
    );
    expect(
      await screen.findByText("Сессия изменилась. Повторите действие."),
    ).toBeDefined();
    expect(stores.api.logout).toHaveBeenCalledTimes(1);
    await userEvent.click(screen.getByText("Подробности"));
    expect(await screen.findByText("req-3")).toBeDefined();
    expect(stores.session.state).toBe("authenticated");
  });

  it("cycles the account theme control through its three choices", async () => {
    const stores = new AppStores();
    stores.session.accept(session, stores.session.version);
    mount(stores);
    await userEvent.click(
      screen.getByRole("button", { name: "Аккаунт: alice" }),
    );
    const panel = screen.getByRole("region", { name: "Аккаунт" });
    await userEvent.click(
      within(panel).getByRole("button", {
        name: "Тема: системная. Переключить на светлую",
      }),
    );
    expect(stores.theme.choice).toBe("light");
    await userEvent.keyboard("{Enter}");
    expect(stores.theme.choice).toBe("dark");
    await userEvent.keyboard(" ");
    expect(stores.theme.choice).toBe("system");
    expect(localStorage.getItem("theme")).toBe("system");
  });

  it("closes the non-modal account panel on Escape and restores focus", async () => {
    const stores = new AppStores();
    stores.session.accept(session, stores.session.version);
    mount(stores);
    const trigger = screen.getByRole("button", { name: "Аккаунт: alice" });
    await userEvent.click(trigger);
    const logout = screen.getByRole("button", { name: "Выйти" });
    logout.focus();
    await userEvent.keyboard("{Escape}");
    expect(screen.queryByRole("region", { name: "Аккаунт" })).toBeNull();
    expect(document.activeElement).toBe(trigger);
  });

  it("opens mobile navigation and exposes keyboard collapse control", async () => {
    const stores = new AppStores();
    stores.session.accept(session, stores.session.version);
    mount(stores);
    await userEvent.click(screen.getByRole("button", { name: "Открыть меню" }));
    expect(
      await screen.findByRole("navigation", { name: "Мобильное меню" }),
    ).toBeDefined();
    const collapse = screen.getByRole("button", { name: "Свернуть меню" });
    await userEvent.click(collapse);
    expect(
      screen
        .getByRole("button", { name: "Развернуть меню" })
        .getAttribute("aria-expanded"),
    ).toBe("false");
  });
});
