import { afterEach, expect, it, vi } from "vitest";
import { cleanup, render, screen, within } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { runInAction } from "mobx";
import { AppStores } from "../../app/stores/AppStores";
import { StoresProvider } from "../../app/providers/StoresProvider";
import {
  aggregateFixture,
  executionFixture,
} from "../../app/api/aggregateTestData";
import { AggregatesPage } from "./AggregatesPage";

const owned: AppStores[] = [];
afterEach(() => {
  cleanup();
  owned.splice(0).forEach((store) => store.dispose());
});

it.each(["computed", "uncomputed"] as const)(
  "keeps lifecycle and window outcome separate for %s in the aggregate list",
  async (status) => {
    const stores = new AppStores();
    owned.push(stores);
    const item = aggregateFixture({
      last_execution: executionFixture({
        origin: "online",
        window_index: 0,
        calculation_status: status,
        calculation_reason:
          status === "uncomputed" ? "execution_budget_exceeded" : null,
        measured_through:
          status === "uncomputed" ? "finalization" : "before_commit",
      }),
    });
    runInAction(() => {
      stores.session.data = {
        authenticated: true,
        user: { id: "u", username: "u", email: null, display_name: "U" },
        role: "admin",
        permissions: ["aggregates.read"],
        authentication_method: "local",
        csrf_token: "csrf",
      };
      stores.session.state = "authenticated";
      stores.api.request = vi
        .fn()
        .mockResolvedValue({ items: [item], total: 1 });
    });
    render(
      <StoresProvider stores={stores}>
        <MemoryRouter>
          <AggregatesPage />
        </MemoryRouter>
      </StoresProvider>,
    );
    const link = await screen.findByRole("link", { name: item.name });
    const row = within(link.closest("tr")!);
    expect(row.getByText("Активен", { exact: true })).toBeTruthy();
    expect(link.getAttribute("href")).toBe(`/aggregates/${item.id}`);
    if (status === "uncomputed") {
      expect(row.getByText("Последнее окно не рассчитано")).toBeTruthy();
      expect(row.getByText(/исчерпан бюджет времени/)).toBeTruthy();
    } else expect(row.queryByText("Последнее окно не рассчитано")).toBeNull();
  },
);
