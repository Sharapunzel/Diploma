import { afterEach, expect, it, vi } from "vitest";
import {
  cleanup,
  render,
  screen,
  waitFor,
  within,
} from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, useLocation } from "react-router-dom";
import { ApiError } from "../../app/api/client";
import { StoresProvider } from "../../app/providers/StoresProvider";
import { AppStores } from "../../app/stores/AppStores";
import { ConnectionsPage } from "./ConnectionsPage";

afterEach(cleanup);

const connections = [
  {
    id: "connection-a",
    name: "Connection A",
    bootstrap_servers: ["kafka-a:9092"],
    created_at: "2026-01-01T00:00:00Z",
    updated_at: "2026-01-01T00:00:00Z",
  },
  {
    id: "connection-b",
    name: "Connection B",
    bootstrap_servers: ["kafka-b:9092"],
    created_at: "2026-01-01T00:00:00Z",
    updated_at: "2026-01-01T00:00:00Z",
  },
];

function LocationProbe() {
  const location = useLocation();
  return <output aria-label="Current location">{location.search}</output>;
}

function renderPage(initialEntry = "/connections?id=connection-a") {
  const stores = new AppStores();
  stores.session.data = {
    authenticated: true,
    user: { id: "user", username: "user", email: null, display_name: "User" },
    role: "admin",
    permissions: ["connections.read", "sources.read"],
    authentication_method: "local",
    csrf_token: "csrf",
  };
  stores.session.state = "authenticated";
  stores.api.request = vi.fn(async (path: string) => {
    if (path.startsWith("/kafka-connections?"))
      return {
        items: connections,
        total: connections.length,
        limit: 50,
        offset: 0,
      };
    if (path.startsWith("/external-connections?"))
      return { items: [], total: 0, limit: 50, offset: 0 };
    if (path === "/kafka-connections/connection-a") return connections[0];
    if (path === "/kafka-connections/connection-b") return connections[1];
    if (
      path.startsWith("/kafka-connections/") &&
      path.endsWith("/topics?include_internal=false")
    )
      return { items: [], total: 0 };
    if (path.startsWith("/sources?"))
      return { items: [], total: 0, limit: 50, offset: 0 };
    if (path === "/kafka-connections/missing")
      throw new ApiError(404, "not_found");
    throw new Error(`Unexpected API request ${path}`);
  }) as typeof stores.api.request;

  render(
    <StoresProvider stores={stores}>
      <MemoryRouter initialEntries={[initialEntry]}>
        <LocationProbe />
        <ConnectionsPage />
      </MemoryRouter>
    </StoresProvider>,
  );
  return stores;
}

it("keeps the selected card and URL aligned when navigating from an addressed connection", async () => {
  const user = userEvent.setup();
  renderPage();
  expect(
    await screen.findByRole("dialog", {
      name: /Наборы данных для подключения «Connection A»/,
    }),
  ).toBeTruthy();

  const rowB = await screen.findByRole("row", { name: /Connection B/ });
  await user.click(within(rowB).getByRole("button", { name: "Наборы" }));

  const dialogB = await screen.findByRole("dialog", {
    name: /Наборы данных для подключения «Connection B»/,
  });
  expect(screen.getByLabelText("Current location").textContent).toBe(
    "?id=connection-b",
  );
  await user.click(within(dialogB).getByRole("button", { name: "Close" }));
  await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
  expect(screen.getByLabelText("Current location").textContent).toBe("");
});

it("explains an unknown addressed connection without opening a dead card", async () => {
  renderPage("/connections?id=missing");
  expect(
    (await screen.findByRole("alert")).textContent?.includes(
      "Подключение могло быть удалено или у вас нет доступа к нему.",
    ),
  ).toBe(true);
  expect(screen.queryByRole("dialog")).toBeNull();
});
