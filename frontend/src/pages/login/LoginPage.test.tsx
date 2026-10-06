import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { ApiError } from "../../app/api/client";
import type { Methods, Session } from "../../app/api/dto";
import { StoresProvider } from "../../app/providers/StoresProvider";
import { AppStores } from "../../app/stores/AppStores";
import { LoginPage } from "./LoginPage";

const session: Session = {
  authenticated: true,
  user: { id: "1", username: "alice", display_name: "Алиса", email: null },
  role: "Оператор",
  permissions: ["events.read"],
  authentication_method: "local",
  csrf_token: "csrf",
};

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

function mount(methods: Promise<Methods>) {
  const stores = new AppStores();
  stores.session.state = "anonymous";
  stores.api.methods = vi.fn(() => methods);
  render(
    <StoresProvider stores={stores}>
      <MemoryRouter initialEntries={["/login"]}>
        <Routes>
          <Route path="/login" element={<LoginPage />} />
          <Route path="/" element={<div>Защищённая главная</div>} />
        </Routes>
      </MemoryRouter>
    </StoresProvider>,
  );
  return stores;
}

describe("LoginPage", () => {
  it.each([
    [true, true],
    [true, false],
    [false, true],
    [false, false],
  ])("shows local=%s and OIDC=%s exactly as reported", async (local, oidc) => {
    mount(Promise.resolve({ local_enabled: local, oidc_enabled: oidc }));
    await waitFor(() =>
      expect(screen.queryByText("Проверка способов входа")).toBeNull(),
    );
    expect(!!screen.queryByRole("button", { name: "Войти" })).toBe(local);
    expect(
      !!screen.queryByRole("link", { name: "Войти через провайдера" }),
    ).toBe(oidc);
    if (!local && !oidc)
      expect(screen.getByText(/Способы входа сейчас недоступны/)).toBeDefined();
  });

  it("shows loading, a safe methods error, then retries", async () => {
    let resolve!: (value: Methods) => void;
    const first = new Promise<Methods>((done) => {
      resolve = done;
    });
    const stores = mount(first);
    expect(screen.queryByRole("button", { name: "Войти" })).toBeNull();
    resolve({ local_enabled: true, oidc_enabled: false });
    await screen.findByRole("button", { name: "Войти" });
    cleanup();
    const second = new AppStores();
    second.session.state = "anonymous";
    second.api.methods = vi
      .fn()
      .mockRejectedValueOnce(
        new ApiError(503, "http_error", "req-1", "<script>bad</script>"),
      )
      .mockResolvedValueOnce({ local_enabled: true, oidc_enabled: false });
    render(
      <StoresProvider stores={second}>
        <MemoryRouter>
          <LoginPage />
        </MemoryRouter>
      </StoresProvider>,
    );
    await screen.findByText("Повторить");
    expect(screen.queryByText("<script>bad</script>")).toBeNull();
    await userEvent.click(screen.getByText("Подробности"));
    expect(await screen.findByText("req-1")).toBeDefined();
    await userEvent.click(screen.getByRole("button", { name: "Повторить" }));
    await screen.findByRole("button", { name: "Войти" });
    expect(second.api.methods).toHaveBeenCalledTimes(2);
    stores.dispose();
  });

  it("validates fields, submits once and reports login errors without expiring anonymous state", async () => {
    const stores = mount(
      Promise.resolve({ local_enabled: true, oidc_enabled: false }),
    );
    stores.api.login = vi.fn();
    await screen.findByRole("button", { name: "Войти" });
    await userEvent.click(screen.getByRole("button", { name: "Войти" }));
    expect(
      screen.getByLabelText("Имя пользователя").getAttribute("aria-invalid"),
    ).toBe("true");
    expect(stores.api.login).not.toHaveBeenCalled();
    stores.api.login = vi
      .fn()
      .mockRejectedValueOnce(new ApiError(401, "invalid_credentials", "req-2"))
      .mockResolvedValueOnce(session);
    await userEvent.type(screen.getByLabelText("Имя пользователя"), "alice");
    await userEvent.type(screen.getByLabelText("Пароль"), "synthetic-password");
    await userEvent.click(screen.getByRole("button", { name: "Войти" }));
    expect(
      await screen.findByText("Неверное имя пользователя или пароль."),
    ).toBeDefined();
    await userEvent.click(screen.getByText("Подробности"));
    expect(await screen.findByText("req-2")).toBeDefined();
    expect(stores.session.state).toBe("anonymous");
    await userEvent.click(screen.getByRole("button", { name: "Войти" }));
    await screen.findByText("Защищённая главная");
    expect(stores.api.login).toHaveBeenCalledTimes(2);
  });

  it("exposes theme control while loading and keeps login fields empty by default", async () => {
    let resolveMethods!: (value: Methods) => void;
    const stores = mount(
      new Promise<Methods>((resolve) => {
        resolveMethods = resolve;
      }),
    );
    expect(
      screen.getByRole("button", {
        name: "Тема: системная. Переключить на светлую",
      }),
    ).toBeDefined();
    expect(screen.queryByLabelText("Имя пользователя")).toBeNull();
    resolveMethods({ local_enabled: true, oidc_enabled: false });
    await screen.findByLabelText("Имя пользователя");
    expect(screen.getByLabelText("Имя пользователя").getAttribute("id")).toBe(
      "username",
    );
    expect(screen.getByLabelText("Имя пользователя").getAttribute("name")).toBe(
      "username",
    );
    expect(
      screen.getByLabelText("Имя пользователя").getAttribute("autocomplete"),
    ).toBe("off");
    expect(screen.getByLabelText("Пароль").getAttribute("id")).toBe("password");
    expect(screen.getByLabelText("Пароль").getAttribute("name")).toBe(
      "password",
    );
    expect(screen.getByLabelText("Пароль").getAttribute("autocomplete")).toBe(
      "off",
    );
    expect(
      (screen.getByLabelText("Имя пользователя") as HTMLInputElement).value,
    ).toBe("");
    expect((screen.getByLabelText("Пароль") as HTMLInputElement).value).toBe(
      "",
    );
    stores.dispose();
  });
});
