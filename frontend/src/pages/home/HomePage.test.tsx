import { afterEach, expect, it, vi } from "vitest";
import {
  act,
  cleanup,
  render,
  screen,
  waitFor,
  within,
} from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { runInAction } from "mobx";
import { StoresProvider } from "../../app/providers/StoresProvider";
import { AppStores } from "../../app/stores/AppStores";
import { HomePage } from "./HomePage";

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

function mount(permissions: string[], request: () => Promise<unknown>) {
  const stores = new AppStores();
  stores.session.accept(
    {
      authenticated: true,
      user: { id: "1", username: "alice", email: null, display_name: "Alice" },
      role: "custom",
      permissions,
      authentication_method: "local",
      csrf_token: "csrf",
    },
    stores.session.version,
  );
  vi.spyOn(stores.api, "request").mockImplementation(
    request as AppStores["api"]["request"],
  );
  render(
    <StoresProvider stores={stores}>
      <MemoryRouter>
        <HomePage />
      </MemoryRouter>
    </StoresProvider>,
  );
  return stores;
}

it("keeps navigation available and hides forbidden inventory blocks", async () => {
  mount(["sources.read", "events.read"], async () => ({
    captured_at: "2026-10-21T12:00:00Z",
    sources: {
      own_enabled: 2,
      own_disabled: 1,
      own_archived: 1,
      external_enabled: 3,
      external_disabled: 0,
    },
  }));
  const processing = screen.getByRole("region", { name: "Обработка данных" });
  expect(
    within(processing)
      .getByRole("link", { name: /Источники/ })
      .getAttribute("href"),
  ).toBe("/sources");
  expect(
    within(processing)
      .getByRole("link", { name: /Диагностика/ })
      .getAttribute("href"),
  ).toBe("/diagnostics");
  expect(within(processing).queryByRole("link", { name: /Агенты/ })).toBeNull();
  expect(
    await screen.findByRole("heading", { name: "Собственные источники" }),
  ).toBeDefined();
  expect(screen.queryByRole("heading", { name: "Подключения" })).toBeNull();
  expect(
    screen.getByRole("img", {
      name: /включённые 2, выключенные 1, архивные 1/,
    }),
  ).toBeDefined();
});

it("renders exact source proportions and accessible values for a mixed distribution", async () => {
  mount(["sources.read"], async () => ({
    captured_at: "2026-10-21T12:00:00Z",
    sources: {
      own_enabled: 2,
      own_disabled: 1,
      own_archived: 1,
      external_enabled: 0,
      external_disabled: 0,
    },
  }));
  const chart = await screen.findByRole("img", {
    name: /включённые 2, выключенные 1, архивные 1/,
  });
  const segments = Array.from(chart.querySelectorAll("rect"));
  expect(segments.map((segment) => segment.getAttribute("width"))).toEqual([
    "50",
    "25",
    "25",
  ]);
  expect(segments.map((segment) => segment.getAttribute("x"))).toEqual([
    "0",
    "50",
    "75",
  ]);
  expect(
    Array.from(chart.closest("article")!.querySelectorAll("strong")).map(
      (value) => value.textContent,
    ),
  ).toEqual(["2", "1", "1"]);
});

it("omits zero source categories from the chart without adding separators", async () => {
  mount(["sources.read"], async () => ({
    captured_at: "2026-10-21T12:00:00Z",
    sources: {
      own_enabled: 3,
      own_disabled: 0,
      own_archived: 2,
      external_enabled: 0,
      external_disabled: 0,
    },
  }));
  const chart = await screen.findByRole("img", {
    name: /включённые 3, выключенные 0, архивные 2/,
  });
  const segments = Array.from(chart.querySelectorAll("rect"));
  expect(segments).toHaveLength(2);
  expect(segments.map((segment) => segment.getAttribute("width"))).toEqual([
    "60",
    "40",
  ]);
  expect(segments.map((segment) => segment.getAttribute("x"))).toEqual([
    "0",
    "60",
  ]);
});

it("shows an empty state instead of a chart for a zero source total", async () => {
  mount(["sources.read"], async () => ({
    captured_at: "2026-10-21T12:00:00Z",
    sources: {
      own_enabled: 0,
      own_disabled: 0,
      own_archived: 0,
      external_enabled: 0,
      external_disabled: 0,
    },
  }));
  expect(await screen.findByText("Источников пока нет.")).toBeDefined();
  expect(screen.queryByRole("img")).toBeNull();
});

it("refreshes the inventory values and snapshot timestamp", async () => {
  const request = vi
    .fn()
    .mockResolvedValueOnce({
      captured_at: "2026-10-21T12:00:00Z",
      normalizers: 2,
    })
    .mockResolvedValueOnce({
      captured_at: "2026-10-21T12:05:00Z",
      normalizers: 7,
    });
  mount(["normalizers.read"], request);
  await screen.findByText("2", { selector: "strong" });
  const timestamp = document.querySelector("time")!;
  expect(timestamp.getAttribute("dateTime")).toBe("2026-10-21T12:00:00Z");
  await userEvent.click(
    screen.getByRole("button", { name: "Обновить показатели" }),
  );
  await waitFor(() => {
    expect(screen.getByText("7", { selector: "strong" })).toBeDefined();
    expect(timestamp.getAttribute("dateTime")).toBe("2026-10-21T12:05:00Z");
  });
  expect(request).toHaveBeenCalledTimes(2);
});

it("shows navigation links allowed by each partial permission set", async () => {
  const cases: [string, string[]][] = [
    ["agents.read", ["/agents"]],
    ["connections.read", ["/connections"]],
    ["sources.read", ["/sources"]],
    ["normalizers.read", ["/normalizers"]],
    ["events.read", ["/diagnostics", "/events"]],
  ];
  for (const [permission, expectedHrefs] of cases) {
    cleanup();
    mount([permission], async () => ({ captured_at: "2026-10-21T12:00:00Z" }));
    await waitFor(() => {
      expect(
        screen
          .getAllByRole("link")
          .map((link) => link.getAttribute("href"))
          .sort(),
      ).toEqual([...expectedHrefs].sort());
    });
  }
});

it("shows a safe error and retries manually", async () => {
  const request = vi
    .fn()
    .mockRejectedValueOnce(new Error("private error"))
    .mockResolvedValueOnce({
      captured_at: "2026-10-21T12:00:00Z",
    });
  mount([], request);
  expect(await screen.findByText("Не удалось выполнить запрос.")).toBeDefined();
  await userEvent.click(
    screen.getByRole("button", { name: "Обновить показатели" }),
  );
  expect(
    await screen.findByText(/Инвентарные показатели недоступны/),
  ).toBeDefined();
  expect(request).toHaveBeenCalledTimes(2);
  expect(screen.queryByText("private error")).toBeNull();
  expect(screen.queryByRole("link")).toBeNull();
});

it("drops the previous inventory when permissions change", async () => {
  const request = vi
    .fn()
    .mockResolvedValueOnce({
      captured_at: "2026-10-21T12:00:00Z",
      sources: {
        own_enabled: 1,
        own_disabled: 0,
        own_archived: 0,
        external_enabled: 0,
        external_disabled: 0,
      },
    })
    .mockResolvedValueOnce({ captured_at: "2026-10-21T12:01:00Z" });
  const stores = mount(["sources.read"], request);
  expect(
    await screen.findByRole("heading", { name: "Собственные источники" }),
  ).toBeDefined();
  act(() =>
    runInAction(() => {
      stores.session.data = { ...stores.session.data!, permissions: [] };
    }),
  );
  expect(
    screen.queryByRole("heading", { name: "Собственные источники" }),
  ).toBeNull();
  expect(
    await screen.findByText(/Инвентарные показатели недоступны/),
  ).toBeDefined();
});
