import { afterEach, expect, it, vi } from "vitest";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { StoresProvider } from "../../app/providers/StoresProvider";
import { AppStores } from "../../app/stores/AppStores";
import { ApiError } from "../../app/api/client";
import { AdministrationPage } from "./AdministrationPage";
import type { Mapping } from "../../app/api/administration";

const guestRole = {
  id: "00000000-0000-4000-8000-000000000002",
  name: "Guest",
  permissions: ["events.read"],
  priority: 10,
};
const adminRole = {
  id: "00000000-0000-4000-8000-000000000001",
  name: "Administrator",
  permissions: ["users.read", "users.write"],
  priority: 100,
};
const user = {
  id: "user-2",
  username: "bob",
  oidc_issuer: null,
  oidc_subject: null,
  email: null,
  display_name: "Bob",
  role_id: guestRole.id,
  role_managed_by_oidc: false,
  is_active: true,
  authentication_method: "local",
  created_at: "2026-01-01T00:00:00Z",
  updated_at: "2026-01-01T00:00:00Z",
  last_login_at: null,
};

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

function mount(
  handler?: (path: string, options?: RequestInit) => Promise<unknown>,
  selfId = "self",
) {
  const stores = new AppStores();
  stores.session.accept(
    {
      authenticated: true,
      user: {
        id: selfId,
        username: "admin",
        email: null,
        display_name: "Admin",
      },
      role: "Administrator",
      permissions: ["users.read", "users.write"],
      authentication_method: "local",
      csrf_token: "csrf",
    },
    stores.session.version,
  );
  const request = vi
    .spyOn(stores.api, "request")
    .mockImplementation(async (path, options) => {
      const handled = await handler?.(path, options);
      if (handled !== undefined) return handled as never;
      if (path.startsWith("/users?"))
        return { items: [user], total: 1, limit: 20, offset: 0 } as never;
      if (path === "/roles") return [adminRole, guestRole] as never;
      if (path === "/auth/methods")
        return { local_enabled: true, oidc_enabled: false } as never;
      if (path.startsWith("/oidc-role-mappings?"))
        return { items: [], total: 0, limit: 20, offset: 0 } as never;
      return {} as never;
    });
  render(
    <StoresProvider stores={stores}>
      <MemoryRouter>
        <AdministrationPage />
      </MemoryRouter>
    </StoresProvider>,
  );
  return request;
}

function chooseOption(dialog: HTMLElement, field: string, option: string) {
  fireEvent.mouseDown(within(dialog).getByRole("combobox", { name: field }));
  const item = screen
    .getAllByText(option)
    .find((element) =>
      element.classList.contains("ant-select-item-option-content"),
    );
  if (!item) throw new Error(`Option not found: ${option}`);
  fireEvent.click(item);
}

it("loads user filters from the server and keeps OIDC tab independent", async () => {
  const request = mount();
  expect(await screen.findByText("Bob")).toBeDefined();
  await userEvent.type(
    screen.getByRole("searchbox", { name: "Поиск пользователей" }),
    "bob{enter}",
  );
  await waitFor(() =>
    expect(request.mock.calls.some(([path]) => path.includes("q=bob"))).toBe(
      true,
    ),
  );
  const rolesTab = screen.getByRole("tab", { name: "Роли" });
  rolesTab.focus();
  fireEvent.keyDown(rolesTab, { key: "ArrowRight" });
  const mappingTab = screen.getByRole("tab", { name: "Сопоставления OIDC" });
  const rolesPanel = document.getElementById("administration-panel-roles");
  const mappingPanel = document.getElementById("administration-panel-mappings");
  expect(rolesTab.getAttribute("aria-controls")).toBe(rolesPanel?.id);
  expect(rolesPanel?.getAttribute("aria-labelledby")).toBe(rolesTab.id);
  expect(rolesPanel?.getAttribute("aria-hidden")).toBe("true");
  expect(mappingTab.getAttribute("aria-controls")).toBe(mappingPanel?.id);
  expect(mappingPanel?.getAttribute("aria-labelledby")).toBe(mappingTab.id);
  expect(mappingPanel?.getAttribute("aria-hidden")).toBe("false");
  expect(await screen.findByText(/OIDC отключён/)).toBeDefined();
  expect(
    request.mock.calls.some(([path]) =>
      path.startsWith("/oidc-role-mappings?"),
    ),
  ).toBe(true);
});

it("requires confirmation before disabling a user and does not send on cancel", async () => {
  const request = mount();
  expect(await screen.findByText("Bob")).toBeDefined();
  await userEvent.click(screen.getByRole("button", { name: "Отключить" }));
  const dialog = await screen.findByRole("dialog", {
    name: "Отключить пользователя",
  });
  expect(
    request.mock.calls.some(([path]) => path.includes("/deactivate")),
  ).toBe(false);
  await userEvent.click(within(dialog).getByRole("button", { name: "Отмена" }));
  expect(
    request.mock.calls.some(([path]) => path.includes("/deactivate")),
  ).toBe(false);
  await userEvent.click(screen.getByRole("button", { name: "Отключить" }));
  await userEvent.click(
    within(
      await screen.findByRole("dialog", { name: "Отключить пользователя" }),
    ).getByRole("button", { name: "Отключить" }),
  );
  await waitFor(() =>
    expect(
      request.mock.calls.some(([path]) => path.includes("/deactivate")),
    ).toBe(true),
  );
});

it("confirms an OIDC mapping that grants Administrator before posting", async () => {
  const request = mount();
  await screen.findByText("Bob");
  await userEvent.click(
    screen.getByRole("tab", { name: "Сопоставления OIDC" }),
  );
  await userEvent.click(screen.getByRole("button", { name: "Добавить" }));
  const dialog = await screen.findByRole("dialog", {
    name: "Новое сопоставление OIDC",
  });
  fireEvent.change(
    within(dialog).getByRole("textbox", { name: "Issuer URL" }),
    { target: { value: "https://idp.test" } },
  );
  fireEvent.change(within(dialog).getByRole("textbox", { name: "Имя claim" }), {
    target: { value: "groups" },
  });
  fireEvent.change(
    within(dialog).getByRole("textbox", { name: "Значение claim" }),
    { target: { value: "admins" } },
  );
  fireEvent.mouseDown(
    within(dialog).getByRole("combobox", { name: "Системная роль" }),
  );
  fireEvent.click(
    screen
      .getAllByText("Administrator")
      .find((element) =>
        element.classList.contains("ant-select-item-option-content"),
      )!,
  );
  await waitFor(() =>
    expect(within(dialog).getByText("Administrator")).toBeDefined(),
  );
  await userEvent.click(
    within(dialog).getByRole("button", { name: "Сохранить" }),
  );
  const confirmation = (
    await screen.findByText("Выдать роль Administrator")
  ).closest<HTMLElement>("[role=dialog]")!;
  expect(
    request.mock.calls.some(
      ([path, options]) =>
        path === "/oidc-role-mappings" && options?.method === "POST",
    ),
  ).toBe(false);
  fireEvent.click(within(confirmation).getByRole("button", { name: "Отмена" }));
  expect(
    request.mock.calls.some(
      ([path, options]) =>
        path === "/oidc-role-mappings" && options?.method === "POST",
    ),
  ).toBe(false);
}, 30_000);

it("keeps an Administrator mapping form editable after a failed confirmed request", async () => {
  let mutationCalls = 0;
  let mappingReads = 0;
  mount((path) => {
    if (path.startsWith("/oidc-role-mappings?")) {
      mappingReads += 1;
      return Promise.resolve({ items: [], total: 0, limit: 20, offset: 0 });
    }
    if (path === "/oidc-role-mappings") {
      mutationCalls += 1;
      return Promise.reject(new ApiError(503, "service_unavailable"));
    }
    return Promise.resolve(undefined);
  });
  await screen.findByText("Bob");
  await userEvent.click(
    screen.getByRole("tab", { name: "Сопоставления OIDC" }),
  );
  await userEvent.click(screen.getByRole("button", { name: "Добавить" }));
  const form = await screen.findByRole("dialog", {
    name: "Новое сопоставление OIDC",
  });
  await userEvent.type(
    within(form).getByRole("textbox", { name: "Issuer URL" }),
    "https://idp.test",
  );
  await userEvent.type(
    within(form).getByRole("textbox", { name: "Имя claim" }),
    "groups",
  );
  await userEvent.type(
    within(form).getByRole("textbox", { name: "Значение claim" }),
    "admins",
  );
  chooseOption(form, "Системная роль", "Administrator");
  await userEvent.click(
    within(form).getByRole("button", { name: "Сохранить" }),
  );
  const confirmation = (
    await screen.findByText("Выдать роль Administrator")
  ).closest<HTMLElement>("[role=dialog]")!;
  fireEvent.click(
    within(confirmation).getByRole("button", { name: "Подтвердить повышение" }),
  );
  expect(await screen.findByText(/Запрос мог быть выполнен/)).toBeDefined();
  expect(mutationCalls).toBe(1);
  expect(
    (
      within(form).getByRole("textbox", {
        name: "Issuer URL",
      }) as HTMLInputElement
    ).value,
  ).toBe("https://idp.test");
  const previousReads = mappingReads;
  fireEvent.click(screen.getByRole("button", { name: "Проверить состояние" }));
  await waitFor(() => expect(mappingReads).toBeGreaterThan(previousReads));
  expect(mutationCalls).toBe(1);
}, 60_000);

it("ignores a stale user list response after a newer server search", async () => {
  let resolveFirst!: (value: unknown) => void;
  let userCalls = 0;
  const oldUser = { ...user, display_name: "Old response" };
  const newUser = { ...user, display_name: "Current response" };
  mount((path) => {
    if (!path.startsWith("/users?")) return Promise.resolve(undefined);
    userCalls += 1;
    if (userCalls === 1)
      return new Promise((resolve) => {
        resolveFirst = resolve;
      });
    return Promise.resolve({
      items: [newUser],
      total: 1,
      limit: 20,
      offset: 0,
    });
  });
  const search = screen.getByRole("searchbox", { name: "Поиск пользователей" });
  await userEvent.type(search, "current{enter}");
  expect(await screen.findByText("Current response")).toBeDefined();
  resolveFirst({ items: [oldUser], total: 1, limit: 20, offset: 0 });
  await waitFor(() => expect(screen.queryByText("Old response")).toBeNull());
  expect(screen.getByText("Current response")).toBeDefined();
});

it("retries the OIDC methods request after its own failure", async () => {
  let methodCalls = 0;
  mount((path) => {
    if (path === "/auth/methods") {
      methodCalls += 1;
      return methodCalls === 1
        ? Promise.reject(new ApiError(503, "service_unavailable"))
        : Promise.resolve({ local_enabled: true, oidc_enabled: true });
    }
    return Promise.resolve(undefined);
  });
  await userEvent.click(
    screen.getByRole("tab", { name: "Сопоставления OIDC" }),
  );
  expect(await screen.findByText("Статус OIDC не загружен.")).toBeDefined();
  await userEvent.click(screen.getByRole("button", { name: "Повторить" }));
  await waitFor(() => expect(methodCalls).toBe(2));
  expect(screen.queryByText("Статус OIDC не загружен.")).toBeNull();
  expect(screen.queryByText(/OIDC отключён/)).toBeNull();
});

it("checks state after an unknown mutation result without resending it", async () => {
  let mutationCalls = 0;
  let reads = 0;
  mount((path) => {
    if (path.startsWith("/users?")) {
      reads += 1;
      return Promise.resolve({ items: [user], total: 1, limit: 20, offset: 0 });
    }
    if (path.endsWith("/deactivate")) {
      mutationCalls += 1;
      return Promise.reject(new ApiError(0, "network_error"));
    }
    return Promise.resolve(undefined);
  });
  await screen.findByText("Bob");
  await userEvent.click(screen.getByRole("button", { name: "Отключить" }));
  const dialog = await screen.findByRole("dialog", {
    name: "Отключить пользователя",
  });
  await userEvent.click(
    within(dialog).getByRole("button", { name: "Отключить" }),
  );
  expect(await screen.findByText(/Запрос мог быть выполнен/)).toBeDefined();
  expect(mutationCalls).toBe(1);
  const previousReads = reads;
  await userEvent.click(
    screen.getByRole("button", { name: "Проверить состояние" }),
  );
  await waitFor(() => expect(reads).toBeGreaterThan(previousReads));
  expect(mutationCalls).toBe(1);
});

it("labels a confirmed mutation refusal and only reloads data on request", async () => {
  let mutationCalls = 0;
  let reads = 0;
  mount((path) => {
    if (path.startsWith("/users?")) {
      reads += 1;
      return Promise.resolve({ items: [user], total: 1, limit: 20, offset: 0 });
    }
    if (path.endsWith("/deactivate")) {
      mutationCalls += 1;
      return Promise.reject(new ApiError(409, "self_administration_forbidden"));
    }
    return Promise.resolve(undefined);
  });
  await screen.findByText("Bob");
  await userEvent.click(screen.getByRole("button", { name: "Отключить" }));
  const dialog = await screen.findByRole("dialog", {
    name: "Отключить пользователя",
  });
  await userEvent.click(
    within(dialog).getByRole("button", { name: "Отключить" }),
  );
  expect(mutationCalls).toBe(1);
  await userEvent.click(
    screen.getByRole("button", { name: "Обновить данные" }),
  );
  await waitFor(() => expect(reads).toBeGreaterThan(1));
  expect(mutationCalls).toBe(1);
}, 60_000);

it("returns to the last existing user page after deleting its final row", async () => {
  let total = 21;
  const userReads: string[] = [];
  let deleteCalls = 0;
  const lastPageUser = {
    ...user,
    id: "user-21",
    display_name: "Last page user",
  };
  mount((path, options) => {
    if (path.startsWith("/users?")) {
      userReads.push(path);
      const offset = new URLSearchParams(path.slice(path.indexOf("?") + 1)).get(
        "offset",
      );
      return Promise.resolve({
        items: offset === "20" ? (total === 20 ? [] : [lastPageUser]) : [user],
        total,
        limit: 20,
        offset: Number(offset ?? 0),
      });
    }
    if (path === "/users/user-21" && options?.method === "DELETE") {
      deleteCalls += 1;
      total = 20;
      return Promise.resolve(undefined);
    }
    return Promise.resolve(undefined);
  });
  await screen.findByText("Bob");
  await userEvent.click(
    document.querySelector(".ant-pagination-item-2") as HTMLElement,
  );
  await waitFor(() =>
    expect(userReads.some((path) => path.includes("offset=20"))).toBe(true),
  );
  const loadedLastPageUser = await screen.findByText("Last page user");
  const lastPageRow = loadedLastPageUser.closest("tr");
  expect(lastPageRow).not.toBeNull();
  const deleteButton = within(lastPageRow as HTMLElement).getByRole("button", {
    name: "Удалить",
  }) as HTMLButtonElement;
  await waitFor(() => expect(deleteButton.disabled).toBe(false));
  await userEvent.click(deleteButton);
  const confirmation = await screen.findByRole("dialog", {
    name: "Удалить пользователя",
  });
  await userEvent.click(
    within(confirmation).getByRole("button", { name: "Удалить" }),
  );
  await waitFor(() =>
    expect(
      userReads.filter((path) => path.includes("offset=0")).length,
    ).toBeGreaterThan(1),
  );
  expect(deleteCalls).toBe(1);
  expect(screen.getByText("Bob")).toBeDefined();
});

it("creates, edits and deletes users with the supported methods and payloads", async () => {
  const mutations: { path: string; method: string; body: unknown }[] = [];
  const request = mount((path, options) => {
    if (options?.method && options.method !== "GET")
      mutations.push({
        path,
        method: options.method,
        body: options.body ? JSON.parse(String(options.body)) : undefined,
      });
    return Promise.resolve(undefined);
  });
  await screen.findByText("Bob");

  await userEvent.click(
    screen.getByRole("button", { name: "Создать локального пользователя" }),
  );
  const create = await screen.findByRole("dialog", {
    name: "Создать локального пользователя",
  });
  await userEvent.type(
    within(create).getByRole("textbox", { name: "Имя входа" }),
    "alice",
  );
  await userEvent.type(
    within(create).getByLabelText("Пароль"),
    "local-password-long",
  );
  await userEvent.type(
    within(create).getByRole("textbox", { name: "Отображаемое имя" }),
    "Alice",
  );
  chooseOption(create, "Роль", "Guest");
  await userEvent.click(
    within(create).getByRole("button", { name: "Сохранить" }),
  );
  await waitFor(() =>
    expect(
      mutations.some(
        (item) => item.path === "/users" && item.method === "POST",
      ),
    ).toBe(true),
  );
  expect(mutations.find((item) => item.path === "/users")?.body).toMatchObject({
    username: "alice",
    display_name: "Alice",
    role_id: guestRole.id,
  });

  await userEvent.click(screen.getByRole("button", { name: "Открыть" }));
  const edit = await screen.findByRole("dialog", { name: "Пользователь" });
  const displayName = within(edit).getByRole("textbox", {
    name: "Отображаемое имя",
  });
  await userEvent.clear(displayName);
  await userEvent.type(displayName, "Bob Updated");
  await userEvent.click(
    within(edit).getByRole("button", { name: "Сохранить" }),
  );
  await waitFor(() =>
    expect(
      mutations.some(
        (item) => item.path === "/users/user-2" && item.method === "PATCH",
      ),
    ).toBe(true),
  );
  expect(
    mutations.find((item) => item.path === "/users/user-2")?.body,
  ).toMatchObject({ display_name: "Bob Updated" });

  await userEvent.click(screen.getByRole("button", { name: "Удалить" }));
  const confirmation = await screen.findByRole("dialog", {
    name: "Удалить пользователя",
  });
  await userEvent.click(
    within(confirmation).getByRole("button", { name: "Удалить" }),
  );
  await waitFor(() =>
    expect(
      mutations.some(
        (item) => item.path === "/users/user-2" && item.method === "DELETE",
      ),
    ).toBe(true),
  );
  expect(
    request.mock.calls.some(
      ([path, options]) =>
        path === "/users/user-2" && options?.method === "DELETE",
    ),
  ).toBe(true);
}, 60_000);

it("confirms password reset, sends the secret once and clears the form after success", async () => {
  const mutations: { path: string; method?: string; body?: string }[] = [];
  mount((path, options) => {
    if (options?.method)
      mutations.push({
        path,
        method: options.method,
        body: String(options.body ?? ""),
      });
    return Promise.resolve(undefined);
  });
  await screen.findByText("Bob");
  await userEvent.click(screen.getByRole("button", { name: "Новый пароль" }));
  const form = await screen.findByRole("dialog", {
    name: "Установить новый пароль",
  });
  const password = within(form).getByLabelText("Новый пароль");
  await userEvent.type(password, "replacement-password-long");
  await userEvent.click(
    within(form).getByRole("button", { name: "Продолжить" }),
  );
  const confirmation = (
    await screen.findByText("Сбросить пароль")
  ).closest<HTMLElement>("[role=dialog]")!;
  fireEvent.click(
    within(confirmation).getByRole("button", { name: "Установить пароль" }),
  );
  await waitFor(() =>
    expect(
      mutations.filter((item) => item.path === "/users/user-2/password"),
    ).toHaveLength(1),
  );
  expect(
    mutations.find((item) => item.path === "/users/user-2/password")?.body,
  ).toContain("replacement-password-long");
  await waitFor(() =>
    expect(
      screen.queryByRole("dialog", { name: "Установить новый пароль" }),
    ).toBeNull(),
  );
  expect(screen.queryByDisplayValue("replacement-password-long")).toBeNull();
}, 60_000);

it("confirms role changes and preserves stable role identity when renaming", async () => {
  const mutations: { path: string; method?: string; body?: unknown }[] = [];
  mount((path, options) => {
    if (options?.method)
      mutations.push({
        path,
        method: options.method,
        body: options.body ? JSON.parse(String(options.body)) : undefined,
      });
    return Promise.resolve(undefined);
  });
  await screen.findByText("Bob");
  await userEvent.click(screen.getByRole("button", { name: "Сменить роль" }));
  const roleForm = await screen.findByRole("dialog", {
    name: "Сменить роль пользователя",
  });
  chooseOption(roleForm, "Системная роль", "Administrator");
  await userEvent.click(
    within(roleForm).getByRole("button", { name: "Продолжить" }),
  );
  const confirmation = (
    await screen.findByText("Подтвердить смену роли")
  ).closest<HTMLElement>("[role=dialog]")!;
  fireEvent.click(
    within(confirmation).getByRole("button", { name: "Сменить роль" }),
  );
  await waitFor(() =>
    expect(
      mutations.some(
        (item) => item.path === "/users/user-2/role" && item.method === "PUT",
      ),
    ).toBe(true),
  );
  expect(
    mutations.find((item) => item.path === "/users/user-2/role")?.body,
  ).toEqual({ role_id: adminRole.id, role_managed_by_oidc: false });
}, 60_000);

it("renames a system role through its stable ID without changing permissions", async () => {
  const mutations: { path: string; method?: string; body?: unknown }[] = [];
  let currentRoles = [adminRole, guestRole];
  mount((path, options) => {
    if (path === "/roles") return Promise.resolve(currentRoles);
    if (options?.method)
      mutations.push({
        path,
        method: options.method,
        body: options.body ? JSON.parse(String(options.body)) : undefined,
      });
    if (path === `/roles/${adminRole.id}` && options?.method === "PATCH") {
      currentRoles = currentRoles.map((role) =>
        role.id === adminRole.id ? { ...role, name: "Admin renamed" } : role,
      );
      return Promise.resolve(currentRoles[0]);
    }
    return Promise.resolve(undefined);
  });
  await screen.findByText("Bob");
  const renameButton = screen.getAllByRole("button", {
    name: "Переименовать",
  })[0];
  await userEvent.click(renameButton);
  const rename = await screen.findByRole("dialog", {
    name: "Переименовать роль",
  });
  const name = within(rename).getByRole("textbox", { name: "Название" });
  await userEvent.clear(name);
  await userEvent.type(name, "Admin renamed");
  await userEvent.click(
    within(rename).getByRole("button", { name: "Сохранить" }),
  );
  await waitFor(() =>
    expect(
      mutations.some(
        (item) =>
          item.path === `/roles/${adminRole.id}` && item.method === "PATCH",
      ),
    ).toBe(true),
  );
  expect(
    mutations.find((item) => item.path === `/roles/${adminRole.id}`)?.body,
  ).toEqual({ name: "Admin renamed" });
  expect(await screen.findByText("Admin renamed")).toBeDefined();
  expect(screen.getByText(/users\.read, users\.write/)).toBeDefined();
}, 60_000);

it("creates, updates and deletes OIDC mappings with explicit requests", async () => {
  const mutations: { path: string; method?: string; body?: unknown }[] = [];
  let mappings: Mapping[] = [];
  mount((path, options) => {
    if (path.startsWith("/oidc-role-mappings?"))
      return Promise.resolve({
        items: mappings,
        total: mappings.length,
        limit: 20,
        offset: 0,
      });
    if (options?.method) {
      const body = options.body ? JSON.parse(String(options.body)) : undefined;
      mutations.push({ path, method: options.method, body });
      if (options.method === "POST" && path === "/oidc-role-mappings") {
        mappings = [{ id: "mapping-1", ...(body as Omit<Mapping, "id">) }];
        return Promise.resolve(mappings[0]);
      }
      if (options.method === "PATCH") {
        mappings = mappings.map((item) =>
          item.id === "mapping-1"
            ? { ...item, ...(body as Partial<Mapping>) }
            : item,
        );
        return Promise.resolve(mappings[0]);
      }
      if (options.method === "DELETE") {
        mappings = [];
        return Promise.resolve(undefined);
      }
    }
    return Promise.resolve(undefined);
  });
  await screen.findByText("Bob");
  await userEvent.click(
    screen.getByRole("tab", { name: "Сопоставления OIDC" }),
  );
  await userEvent.click(screen.getByRole("button", { name: "Добавить" }));
  const create = await screen.findByRole("dialog", {
    name: "Новое сопоставление OIDC",
  });
  await userEvent.type(
    within(create).getByRole("textbox", { name: "Issuer URL" }),
    "https://issuer.example.test",
  );
  await userEvent.type(
    within(create).getByRole("textbox", { name: "Имя claim" }),
    "groups",
  );
  await userEvent.type(
    within(create).getByRole("textbox", { name: "Значение claim" }),
    "analysts",
  );
  chooseOption(create, "Системная роль", "Guest");
  await userEvent.click(
    within(create).getByRole("button", { name: "Сохранить" }),
  );
  await waitFor(() =>
    expect(
      mutations.some(
        (item) => item.path === "/oidc-role-mappings" && item.method === "POST",
      ),
    ).toBe(true),
  );
  expect(mutations.find((item) => item.method === "POST")?.body).toMatchObject({
    issuer: "https://issuer.example.test",
    claim_name: "groups",
    claim_value: "analysts",
    role_id: guestRole.id,
  });
  await screen.findByText("analysts");
  await userEvent.click(screen.getByRole("button", { name: "Изменить" }));
  const edit = await screen.findByRole("dialog", {
    name: "Изменить сопоставление OIDC",
  });
  const claimValue = within(edit).getByRole("textbox", {
    name: "Значение claim",
  });
  await userEvent.clear(claimValue);
  await userEvent.type(claimValue, "analysts-updated");
  await userEvent.click(
    within(edit).getByRole("button", { name: "Сохранить" }),
  );
  await waitFor(() =>
    expect(
      mutations.some(
        (item) =>
          item.path === "/oidc-role-mappings/mapping-1" &&
          item.method === "PATCH",
      ),
    ).toBe(true),
  );
  expect(mutations.find((item) => item.method === "PATCH")?.body).toMatchObject(
    { claim_value: "analysts-updated" },
  );
  await userEvent.click(
    within(screen.getByRole("region", { name: "Роли и доступ" })).getByRole(
      "button",
      { name: "Удалить" },
    ),
  );
  const confirmation = (
    await screen.findByText("Удалить сопоставление")
  ).closest<HTMLElement>("[role=dialog]")!;
  fireEvent.click(
    within(confirmation).getByRole("button", { name: "Удалить" }),
  );
  await waitFor(() =>
    expect(
      mutations.some(
        (item) =>
          item.path === "/oidc-role-mappings/mapping-1" &&
          item.method === "DELETE",
      ),
    ).toBe(true),
  );
}, 90_000);

it("hides forbidden self and OIDC user actions", async () => {
  mount((path) => {
    if (path.startsWith("/users?"))
      return Promise.resolve({
        items: [{ ...user, id: "self" }],
        total: 1,
        limit: 20,
        offset: 0,
      });
    return Promise.resolve(undefined);
  });
  await screen.findByText("Bob");
  expect(screen.queryByRole("button", { name: "Сменить роль" })).toBeNull();
  expect(screen.queryByRole("button", { name: "Отключить" })).toBeNull();
  expect(screen.queryByRole("button", { name: "Удалить" })).toBeNull();
  cleanup();
  mount((path) => {
    if (path.startsWith("/users?"))
      return Promise.resolve({
        items: [
          {
            ...user,
            id: "oidc-1",
            username: null,
            authentication_method: "oidc",
            oidc_issuer: "https://issuer",
            oidc_subject: "subject",
          },
        ],
        total: 1,
        limit: 20,
        offset: 0,
      });
    return Promise.resolve(undefined);
  });
  await screen.findByText("Bob");
  expect(screen.queryByRole("button", { name: "Новый пароль" })).toBeNull();
  await userEvent.click(screen.getByRole("button", { name: "Открыть" }));
  const details = await screen.findByRole("dialog", { name: "Пользователь" });
  expect(
    within(details).queryByRole("textbox", { name: "Имя входа" }),
  ).toBeNull();
  expect(within(details).getByText(/OIDC/)).toBeDefined();
}, 60_000);
