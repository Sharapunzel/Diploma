import { cleanup, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { afterEach, expect, it, vi } from "vitest";
import { ApiError } from "../../app/api/client";
import { StoresProvider } from "../../app/providers/StoresProvider";
import { AppStores } from "../../app/stores/AppStores";
import { findingEventsPath } from "../../features/events/findingLink";
import { EventsRoute } from "./EventsPage";

const sourceId = "00000000-0000-4000-8000-000000000055";
const from = "2026-11-12T20:00:00.123456Z";
const to = "2026-11-12T20:00:00.123457Z";
const url = findingEventsPath({ sourceId, from, to });

function setup(initial = url, unavailable = false) {
  const stores = new AppStores();
  const request = vi.fn<
    (path: string, options?: RequestInit) => Promise<unknown>
  >(async (path) => {
    if (path === `/sources/${sourceId}`) {
      if (unavailable) throw new ApiError(404, "source_not_found");
      return {
        id: sourceId,
        name: "Источник за первой страницей",
        source_type: "kafka",
        is_enabled: false,
      };
    }
    if (path.includes("/fields"))
      return {
        items: [{ name: "message", type: "text", operators: ["contains"] }],
        total: 1,
      };
    if (path.endsWith("/search"))
      return { items: [], has_more: false, next_cursor: null };
    if (path.includes("sources?")) return { items: [], total: 0 };
    throw new Error(`Unexpected event request ${path}`);
  });
  stores.api.request = request as typeof stores.api.request;
  render(
    <StoresProvider stores={stores}>
      <MemoryRouter initialEntries={[initial]}>
        <Routes>
          <Route path="/events" element={<EventsRoute />} />
        </Routes>
      </MemoryRouter>
    </StoresProvider>,
  );
  return { request, user: userEvent.setup() };
}

afterEach(cleanup);

it("loads the source by UUID and preserves a one-microsecond range on first search, filters and refresh", async () => {
  const { request, user } = setup();
  await screen.findByText("В этом диапазоне событий нет");
  expect(request.mock.calls[0][0]).toBe(`/sources/${sourceId}`);
  expect(
    request.mock.calls.some(([path]) => path.startsWith("/sources?")),
  ).toBe(false);
  await user.click(screen.getByRole("button", { name: "Фильтры и поля" }));
  await user.click(screen.getByRole("button", { name: "Искать" }));
  await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
  await user.click(screen.getByRole("button", { name: "Обновить" }));
  const searches = request.mock.calls.filter(([path]) =>
    path.endsWith("/search"),
  );
  expect(searches).toHaveLength(3);
  for (const [path, options] of searches) {
    expect(path).toBe(`/events/sources/${sourceId}/search`);
    expect(JSON.parse(String(options?.body))).toMatchObject({
      timestamp_from: from,
      timestamp_to: to,
    });
  }
  await user.click(screen.getByRole("link", { name: "Обычный поиск" }));
  expect(
    await screen.findByText("Выберите источник для просмотра событий."),
  ).toBeTruthy();
});

it.each([
  [url, true, "Исходный локальный источник недоступен."],
  [
    findingEventsPath({ sourceId, from: to, to: from }),
    false,
    "Некорректная ссылка на события находки.",
  ],
] as const)(
  "never issues a broad or unrelated search when the link cannot resolve",
  async (initial, unavailable, message) => {
    const { request } = setup(initial, unavailable);
    expect(await screen.findByText(message)).toBeTruthy();
    expect(request.mock.calls.map(([path]) => path)).toEqual(
      unavailable ? [`/sources/${sourceId}`] : [],
    );
  },
);
