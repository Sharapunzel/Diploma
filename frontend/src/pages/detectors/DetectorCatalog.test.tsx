import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { MemoryRouter } from "react-router-dom";
import { AppStores } from "../../app/stores/AppStores";
import { StoresProvider } from "../../app/providers/StoresProvider";
import { ApiError } from "../../app/api/client";
import { id } from "../../features/detectors/testData";
import { DetectorCreatePage } from "./DetectorCreatePage";

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});
it("keeps a failed query when Select closes and explicitly retries that query", async () => {
  const stores = new AppStores();
  stores.session.accept(
    {
      authenticated: true,
      user: { id, username: "admin", email: null, display_name: "Admin" },
      role: "admin",
      permissions: ["detectors.read", "detectors.write"],
      authentication_method: "local",
      csrf_token: "synthetic",
    },
    0,
  );
  const queries: string[] = [];
  let fail = true;
  vi.spyOn(stores.api, "request").mockImplementation(async (path) => {
    const q = new URL(path, "https://example.test").searchParams.get("q") ?? "";
    queries.push(q);
    if (q && fail) {
      fail = false;
      throw new ApiError(503, "temporary_unavailable");
    }
    return { items: [], total: 0 } as never;
  });
  try {
    render(
      <StoresProvider stores={stores}>
        <MemoryRouter>
          <DetectorCreatePage />
        </MemoryRouter>
      </StoresProvider>,
    );
    await waitFor(() => expect(queries).toEqual([""]));
    const combo = screen.getByRole("combobox", { name: "Активный агрегат" });
    fireEvent.mouseDown(combo);
    fireEvent.change(combo, { target: { value: "Далёкий" } });
    await screen.findByRole("button", { name: "Повторить" });
    fireEvent.keyDown(combo, { key: "Escape", code: "Escape" });
    await waitFor(() => expect(stores.detectors.draft.query).toBe("Далёкий"));
    fireEvent.click(screen.getByRole("button", { name: "Повторить" }));
    await waitFor(() => expect(queries).toEqual(["", "Далёкий", "Далёкий"]));
    expect(stores.detectors.draft.query).toBe("Далёкий");
  } finally {
    cleanup();
    stores.dispose();
  }
});
