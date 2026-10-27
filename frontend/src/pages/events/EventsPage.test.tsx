import { afterEach, expect, it, vi } from "vitest";
import {
  cleanup,
  render,
  screen,
  waitFor,
  within,
} from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { StoresProvider } from "../../app/providers/StoresProvider";
import { AppStores } from "../../app/stores/AppStores";
import { ApiError } from "../../app/api/client";
import { EventsPage, inputValue } from "./EventsPage";

function renderSearchPage(
  search: (call: number) => Promise<unknown> = async () => ({
    items: [],
    has_more: false,
    next_cursor: null,
  }),
) {
  const stores = new AppStores();
  stores.session.data = {
    authenticated: true,
    user: {
      id: "reader",
      username: "reader",
      email: null,
      display_name: "Reader",
    },
    role: "reader",
    permissions: ["events.read", "sources.read"],
    authentication_method: "local",
    csrf_token: "csrf",
  };
  stores.session.state = "authenticated";
  let searchCalls = 0;
  const request = vi.fn(async (path: string) => {
    if (path.startsWith("/sources?"))
      return {
        items: [
          {
            id: "local",
            name: "Synthetic local",
            source_type: "kafka",
            is_enabled: false,
          },
        ],
        total: 1,
        limit: 50,
        offset: 0,
      };
    if (path.startsWith("/external-sources?"))
      return { items: [], total: 0, limit: 50, offset: 0 };
    if (path.includes("/fields"))
      return {
        items: [{ name: "message", type: "text", operators: ["contains"] }],
        total: 1,
        limit: 100,
        offset: 0,
      };
    if (path.endsWith("/search")) return search(++searchCalls);
    throw new Error(`Unexpected request ${path}`);
  });
  stores.api.request = request as typeof stores.api.request;
  render(
    <StoresProvider stores={stores}>
      <EventsPage />
    </StoresProvider>,
  );
  return { stores, request, user: userEvent.setup() };
}

async function chooseLocalSource(user: ReturnType<typeof userEvent.setup>) {
  await user.click(await screen.findByRole("combobox", { name: "Источник" }));
  await user.click(await screen.findByText(/Synthetic local/));
  await screen.findByRole("table");
}

it("applies a search only after the server responds successfully", async () => {
  const { stores, user, request } = renderSearchPage();
  await chooseLocalSource(user);
  await waitFor(() =>
    expect(
      request.mock.calls.filter(([path]) => path.endsWith("/search")),
    ).toHaveLength(1),
  );
  await user.click(screen.getByRole("button", { name: "Фильтры и поля" }));
  const dialog = screen.getByRole("dialog", { name: "Фильтры и поля" });
  await user.click(within(dialog).getByText("Новые сначала"));
  await user.click(await screen.findByText("Старые сначала", { exact: true }));
  await user.click(within(dialog).getByRole("button", { name: "Искать" }));
  await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
  expect(screen.getByText("Порядок: Старые сначала")).not.toBeNull();
  expect(
    request.mock.calls.filter(([path]) => path.endsWith("/search")),
  ).toHaveLength(2);
  stores.dispose();
});

it("keeps the filter draft and previous results after an indexed server rejection", async () => {
  const { stores, user } = renderSearchPage(async (call) => {
    if (call === 1)
      return {
        items: [
          {
            id: "previous",
            source_id: "local",
            source_type: "kafka",
            event_timestamp: "2026-01-01T00:00:00Z",
            fields: { message: "previous result" },
          },
        ],
        has_more: false,
        next_cursor: null,
      };
    throw new ApiError(422, "event_filter_invalid", null, null, { index: 0 });
  });
  await chooseLocalSource(user);
  await screen
    .findByRole("table")
    .then((table) => within(table).findByText("previous result"));
  await user.click(screen.getByRole("button", { name: "Фильтры и поля" }));
  const dialog = screen.getByRole("dialog", { name: "Фильтры и поля" });
  await user.click(
    within(dialog).getByRole("button", { name: "Добавить условие" }),
  );
  await user.click(
    within(dialog).getByRole("combobox", { name: "Поле условия" }),
  );
  const messageOptions = await screen.findAllByText("message", { exact: true });
  await user.click(messageOptions[messageOptions.length - 1]);
  await user.click(
    within(dialog).getByRole("combobox", { name: "Оператор условия" }),
  );
  const containsOptions = await screen.findAllByText("Содержит", {
    exact: true,
  });
  await user.click(containsOptions[containsOptions.length - 1]);
  await user.type(within(dialog).getByPlaceholderText("Значение"), "short");
  await user.click(within(dialog).getByRole("button", { name: "Искать" }));
  expect(
    await within(dialog).findByText(/Источник отклонил это условие/),
  ).not.toBeNull();
  expect(within(dialog).getByPlaceholderText("Значение")).toHaveProperty(
    "value",
    "short",
  );
  expect(
    within(screen.getByRole("table")).getByText("previous result"),
  ).not.toBeNull();
  expect(screen.getByText("Условий: 0")).not.toBeNull();
  stores.dispose();
});

it("shows safe operation feedback for a search error without an index", async () => {
  const { stores, user } = renderSearchPage(async (call) => {
    if (call === 1) return { items: [], has_more: false, next_cursor: null };
    throw new ApiError(0, "network_error");
  });
  await chooseLocalSource(user);
  await user.click(screen.getByRole("button", { name: "Фильтры и поля" }));
  const dialog = screen.getByRole("dialog", { name: "Фильтры и поля" });
  await waitFor(() =>
    expect(
      within(dialog).getByRole("button", { name: "Искать" }),
    ).toHaveProperty("disabled", false),
  );
  await user.click(within(dialog).getByRole("button", { name: "Искать" }));
  expect(await within(dialog).findByText(/Поиск не применён/)).not.toBeNull();
  expect(screen.queryByText(/network_error/)).toBeNull();
  stores.dispose();
});

it("keeps the applied search when the filter dialog closes without applying", async () => {
  const { stores, user, request } = renderSearchPage();
  await chooseLocalSource(user);
  const searchCount = request.mock.calls.filter(([path]) =>
    path.endsWith("/search"),
  ).length;
  await user.click(screen.getByRole("button", { name: "Фильтры и поля" }));
  const dialog = screen.getByRole("dialog", { name: "Фильтры и поля" });
  await waitFor(() =>
    expect(
      within(dialog).getByRole("button", { name: "Искать" }),
    ).toHaveProperty("disabled", false),
  );
  await user.click(within(dialog).getByText("Новые сначала"));
  await user.click(await screen.findByText("Старые сначала", { exact: true }));
  await user.click(within(dialog).getByRole("button", { name: "Close" }));
  await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
  expect(screen.getByText("Порядок: Новые сначала")).not.toBeNull();
  expect(
    request.mock.calls.filter(([path]) => path.endsWith("/search")),
  ).toHaveLength(searchCount);
  stores.dispose();
});

it("confirms four exact deletion scopes and never sends on cancellation", async () => {
  const stores = new AppStores();
  stores.session.data = {
    authenticated: true,
    user: {
      id: "admin",
      username: "admin",
      email: null,
      display_name: "Admin",
    },
    role: "admin",
    permissions: ["events.read", "events.delete", "sources.read"],
    authentication_method: "local",
    csrf_token: "csrf",
  };
  stores.session.state = "authenticated";
  const changes: { path: string; body?: unknown }[] = [];
  let failure: "network" | "invalid" | null = null;
  const request = vi.fn(async (path: string, options?: RequestInit) => {
    if (path.startsWith("/sources?"))
      return {
        items: [
          {
            id: "local",
            name: "Synthetic local",
            source_type: "kafka",
            is_enabled: false,
          },
        ],
        total: 1,
        limit: 50,
        offset: 0,
      };
    if (path.startsWith("/external-sources?"))
      return { items: [], total: 0, limit: 50, offset: 0 };
    if (path.includes("/fields"))
      return { items: [], total: 0, limit: 100, offset: 0 };
    if (path.endsWith("/search"))
      return {
        items: [
          {
            id: "opaque-id",
            source_id: "local",
            source_type: "kafka",
            event_timestamp: "2026-01-01T00:00:00Z",
            fields: {},
            normalization_status: "complete",
          },
        ],
        has_more: false,
        next_cursor: null,
      };
    if (
      path.includes("/events/opaque-id") ||
      path === "/parsed-logs/actions/delete"
    ) {
      if (failure === "network") throw new ApiError(0, "network_error");
      if (failure === "invalid") return {};
      changes.push({
        path,
        body: options?.body ? JSON.parse(String(options.body)) : undefined,
      });
      return path === "/parsed-logs/actions/delete"
        ? { deleted_count: 2 }
        : undefined;
    }
    throw new Error(`Unexpected ${path}`);
  });
  stores.api.request = request as typeof stores.api.request;
  render(
    <StoresProvider stores={stores}>
      <EventsPage />
    </StoresProvider>,
  );
  const user = userEvent.setup();
  await user.click(await screen.findByRole("combobox", { name: "Источник" }));
  await user.click(await screen.findByText(/Synthetic local/));
  const table = await screen.findByRole("table");
  await user.click(within(table).getByRole("button", { name: "Удалить" }));
  expect(changes).toHaveLength(0);
  await user.click(
    within(screen.getByRole("dialog")).getByRole("button", { name: "Отмена" }),
  );
  expect(changes).toHaveLength(0);
  await user.click(within(table).getByRole("button", { name: "Удалить" }));
  await user.click(
    within(screen.getByRole("dialog")).getByRole("button", { name: "Удалить" }),
  );
  await waitFor(() => expect(changes).toHaveLength(1));
  expect(changes[0]).toEqual({
    path: "/events/sources/local/events/opaque-id",
    body: undefined,
  });
  await user.click(screen.getByRole("button", { name: "Удаление событий" }));
  await user.click(
    screen.getByRole("button", { name: "Удалить события источника" }),
  );
  await user.click(
    within(screen.getByRole("dialog")).getByRole("button", { name: "Удалить" }),
  );
  await waitFor(() => expect(changes).toHaveLength(2));
  expect(changes[1].body).toEqual({ source_id: "local" });
  await user.click(screen.getByRole("button", { name: "Удаление событий" }));
  await user.click(
    screen.getByRole("button", { name: "Взять период применённого поиска" }),
  );
  await user.click(
    screen.getByRole("button", { name: "Удалить источник за период" }),
  );
  await user.click(
    within(screen.getByRole("dialog")).getByRole("button", { name: "Удалить" }),
  );
  await waitFor(() => expect(changes).toHaveLength(3));
  expect(changes[2].body).toEqual({
    source_id: "local",
    timestamp_from: expect.any(String),
    timestamp_to: expect.any(String),
  });
  expect(
    (changes[2].body as { timestamp_from: string }).timestamp_from.startsWith(
      "2025-",
    ),
  ).toBe(false);
  await user.click(screen.getByRole("button", { name: "Удаление событий" }));
  await user.click(
    screen.getByRole("button", { name: "Удалить все локальные за период" }),
  );
  await user.click(
    within(screen.getByRole("dialog")).getByRole("button", { name: "Удалить" }),
  );
  await waitFor(() => expect(changes).toHaveLength(4));
  expect(changes[3].body).toEqual({
    timestamp_from: expect.any(String),
    timestamp_to: expect.any(String),
  });
  expect(await screen.findByText("Удалено событий: 2.")).not.toBeNull();
  failure = "network";
  await user.click(screen.getByRole("button", { name: "Удаление событий" }));
  await user.click(
    screen.getByRole("button", { name: "Удалить события источника" }),
  );
  await user.click(
    within(screen.getByRole("dialog")).getByRole("button", { name: "Удалить" }),
  );
  expect(
    await screen.findByText("Результат удаления неизвестен"),
  ).not.toBeNull();
  expect(screen.queryByRole("dialog")).toBeNull();
  await user.click(screen.getByRole("button", { name: "Удаление событий" }));
  expect(
    screen
      .getByRole("button", { name: "Удалить события источника" })
      .hasAttribute("disabled"),
  ).toBe(true);
  await user.click(screen.getByRole("button", { name: "Обновить события" }));
  await waitFor(() =>
    expect(
      screen
        .getByRole("button", { name: "Удалить события источника" })
        .hasAttribute("disabled"),
    ).toBe(false),
  );
  failure = "invalid";
  await user.click(
    screen.getByRole("button", { name: "Удалить события источника" }),
  );
  await user.click(
    within(screen.getByRole("dialog")).getByRole("button", { name: "Удалить" }),
  );
  expect(
    await screen.findByText("Результат удаления неизвестен"),
  ).not.toBeNull();
  stores.dispose();
}, 60_000);

it("validates numeric precision, list limits and external date formats", () => {
  const integer = { name: "event.code", type: "long", operators: ["eq", "in"] };
  expect(inputValue("12", integer, "eq")).toBe(12);
  expect(() => inputValue("1.5", integer, "eq")).toThrow();
  expect(() => inputValue("9007199254740993", integer, "eq")).toThrow();
  expect(() => inputValue("nonsense", integer, "eq")).toThrow();
  expect(() =>
    inputValue(
      Array.from({ length: 21 }, (_, i) => String(i)).join(","),
      integer,
      "in",
    ),
  ).toThrow();
  const date = {
    name: "event.created",
    type: "date",
    operators: ["eq"],
    date_formats: ["epoch_millis"],
  };
  expect(inputValue("2026-01-01T00:00", date, "eq")).toBe(
    new Date("2026-01-01T00:00").getTime(),
  );
  expect(() =>
    inputValue(
      "2026-01-01T00:00",
      { ...date, date_formats: ["basic_date"] },
      "eq",
    ),
  ).toThrow();
});

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

it("lets a reader inspect a disabled local source without delete actions", async () => {
  const stores = new AppStores();
  stores.session.data = {
    authenticated: true,
    user: {
      id: "reader",
      username: "reader",
      email: null,
      display_name: "Reader",
    },
    role: "guest",
    permissions: ["events.read", "sources.read"],
    authentication_method: "local",
    csrf_token: "csrf",
  };
  stores.session.state = "authenticated";
  const source = {
    id: "local",
    source_type: "kafka",
    name: "Synthetic local",
    is_enabled: false,
  };
  const external = {
    id: "external",
    source_type: "external",
    name: "Synthetic external",
    is_enabled: false,
  };
  const longMessage = "Длинное значение ".repeat(12);
  const request = vi.fn((path: string) => {
    if (path.startsWith("/sources?"))
      return Promise.resolve({
        items: [source],
        total: 1,
        limit: 50,
        offset: 0,
      });
    if (path.startsWith("/external-sources?"))
      return Promise.resolve({
        items: [external],
        total: 1,
        limit: 50,
        offset: 0,
      });
    if (path.includes("/fields"))
      return Promise.resolve({
        items: [
          { name: "message", type: "text", operators: ["eq", "contains"] },
        ],
        total: 1,
        limit: 100,
        offset: 0,
      });
    if (path.endsWith("/events/opaque"))
      return Promise.resolve({
        id: "opaque",
        source_id: "local",
        source_type: "kafka",
        event_timestamp: "2026-10-12T10:00:00Z",
        fields: { message: longMessage },
        details: { raw: longMessage, metadata: { source: "test" } },
      });
    if (path.endsWith("/search"))
      return Promise.resolve({
        items: [
          {
            id: "opaque",
            source_id: "local",
            source_type: "kafka",
            event_timestamp: "2026-10-12T10:00:00Z",
            fields: { message: longMessage },
            normalization_status: "partial",
          },
        ],
        has_more: false,
        next_cursor: null,
      });
    return Promise.reject(new Error(`Unexpected request ${path}`));
  });
  stores.api.request = request as typeof stores.api.request;
  render(
    <StoresProvider stores={stores}>
      <EventsPage />
    </StoresProvider>,
  );
  const user = userEvent.setup();
  await user.click(await screen.findByRole("combobox", { name: "Источник" }));
  expect(await screen.findByText(/Synthetic local/)).not.toBeNull();
  expect(screen.queryByText(/Synthetic external/)).toBeNull();
  await user.click(screen.getByText(/Synthetic local/));
  const table = await screen.findByRole("table");
  expect(within(table).getByText(/Длинное значение/)).not.toBeNull();
  expect(within(table).getByText("partial")).not.toBeNull();
  expect(screen.queryByRole("button", { name: /Удалить/ })).toBeNull();
  expect(
    within(table).getByRole("button", { name: "Развернуть" }),
  ).not.toBeNull();
  await user.click(within(table).getByRole("button", { name: "Развернуть" }));
  expect(
    within(table).getByRole("button", { name: "Свернуть" }),
  ).not.toBeNull();
  const searches = request.mock.calls.filter(([path]) =>
    path.endsWith("/search"),
  ).length;
  await user.click(screen.getByRole("button", { name: "Фильтры и поля" }));
  expect(screen.getByRole("dialog", { name: "Фильтры и поля" })).not.toBeNull();
  await user.click(
    within(screen.getByRole("dialog", { name: "Фильтры и поля" })).getByRole(
      "button",
      { name: "Close" },
    ),
  );
  await waitFor(() =>
    expect(screen.queryByRole("dialog", { name: "Фильтры и поля" })).toBeNull(),
  );
  expect(
    request.mock.calls.filter(([path]) => path.endsWith("/search")),
  ).toHaveLength(searches);
  await user.click(within(table).getByRole("button", { name: "Открыть" }));
  const card = await screen.findByRole("dialog", { name: "Событие" });
  expect(within(card).getByText("Исходная запись")).not.toBeNull();
  expect(
    within(card).getAllByRole("button", { name: "Развернуть" }).length,
  ).toBeGreaterThan(0);
  await user.click(within(card).getByRole("button", { name: "Close" }));
  await waitFor(() =>
    expect(screen.queryByRole("dialog", { name: "Событие" })).toBeNull(),
  );
  await waitFor(() =>
    expect(request.mock.calls.some(([path]) => path.endsWith("/search"))).toBe(
      true,
    ),
  );
  stores.dispose();
});
