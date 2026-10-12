import { afterEach, expect, it, vi } from "vitest";
import { act, cleanup, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import type { Session } from "../../app/api/dto";
import { StoresProvider } from "../../app/providers/StoresProvider";
import { AppStores } from "../../app/stores/AppStores";
import { NormalizersPage } from "./NormalizersPage";

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

it("labels the same actor across different normalizers without presenting it as a normalizer ID", async () => {
  const stores = new AppStores();
  const actorId = "9171185b-6e64-4a54-8a4f-ee04d619649a";
  stores.session.data = {
    authenticated: true,
    user: { id: actorId, username: "dima", display_name: "Dima", email: null },
    role: "demo",
    permissions: ["normalizers.read"],
    authentication_method: "local",
    csrf_token: "csrf",
  };
  stores.session.state = "authenticated";
  stores.api.request = vi.fn((path: string) => {
    if (path.startsWith("/normalizers?"))
      return Promise.resolve({
        items: ["Syslog", "IIS"].map((name, index) => ({
          id: `rule-${index}`,
          name,
          description: null,
          version: 1,
          rule_status: "ready",
          created_at: "2026-01-01T00:00:00Z",
          updated_at: "2026-01-01T00:00:00Z",
          created_by_user_id: actorId,
          updated_by_user_id: actorId,
        })),
        total: 2,
        limit: 50,
        offset: 0,
      });
    return Promise.resolve(null);
  }) as typeof stores.api.request;
  render(
    <StoresProvider stores={stores}>
      <MemoryRouter>
        <NormalizersPage />
      </MemoryRouter>
    </StoresProvider>,
  );
  await screen.findByText("Syslog");
  expect(screen.getAllByText("Автор: Dima")).toHaveLength(2);
  expect(screen.getAllByText("Изменил: Dima")).toHaveLength(2);
  expect(screen.queryByText(actorId)).toBeNull();
  stores.dispose();
});

// Several user clicks, rerenders and a controlled pending request are exercised in one scenario.
it("isolates pending source responses when permissions change", async () => {
  const stores = new AppStores();
  const session: Session = {
    authenticated: true,
    user: { id: "user", username: "demo", display_name: "Demo", email: null },
    role: "demo",
    permissions: ["normalizers.read", "sources.read"],
    authentication_method: "local",
    csrf_token: "csrf",
  };
  stores.session.data = session;
  stores.session.state = "authenticated";
  let resolveOld!: (value: unknown) => void;
  const oldSources = new Promise((resolve) => {
    resolveOld = resolve;
  });
  let sourceReads = 0;
  stores.api.request = vi.fn((path: string) => {
    if (path.startsWith("/normalizers?"))
      return Promise.resolve({
        items: [
          {
            id: "one",
            name: "Rule",
            description: null,
            version: 1,
            rule_status: "ready",
            created_at: "2026-01-01T00:00:00Z",
            updated_at: "2026-01-01T00:00:00Z",
            created_by_user_id: null,
            updated_by_user_id: null,
          },
        ],
        total: 1,
        limit: 50,
        offset: 0,
      });
    if (path.startsWith("/sources?")) {
      sourceReads++;
      return sourceReads === 1
        ? oldSources
        : Promise.resolve({
            items: [{ id: "new", name: "New source", is_enabled: true }],
            total: 1,
            limit: 50,
            offset: 0,
          });
    }
    return Promise.resolve(null);
  }) as typeof stores.api.request;
  const view = (key: number) => (
    <StoresProvider stores={stores}>
      <MemoryRouter key={key}>
        <NormalizerList />
      </MemoryRouter>
    </StoresProvider>
  );
  // The key belongs to the child only; the page stays mounted on rerender.
  function NormalizerList() {
    return <NormalizersPage />;
  }
  const mounted = render(view(0));
  await screen.findByText("Rule");
  await userEvent.click(screen.getByRole("button", { name: /Expand row/ }));
  await screen.findByText("Загрузка источников…");
  act(() => {
    stores.session.data = { ...session, permissions: ["normalizers.read"] };
  });
  mounted.rerender(view(0));
  await screen.findByText(/требуется право sources.read/);
  await act(async () => {
    resolveOld({
      items: [{ id: "old", name: "Old source" }],
      total: 1,
      limit: 50,
      offset: 0,
    });
  });
  expect(screen.queryByText("Old source")).toBeNull();
  act(() => {
    stores.session.data = session;
  });
  mounted.rerender(view(0));
  await userEvent.click(screen.getByRole("button", { name: /Expand row/ }));
  await waitFor(() => expect(screen.getByText(/New source/)).toBeDefined());
  expect(screen.queryByText("Old source")).toBeNull();
  stores.dispose();
}, 12000);
