import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { MemoryRouter } from "react-router-dom";
import { AppStores } from "../../app/stores/AppStores";
import { StoresProvider } from "../../app/providers/StoresProvider";
import { ApiError } from "../../app/api/client";
import { fixture, id } from "../../features/detectors/testData";
import { DetectorsPage } from "./DetectorsPage";

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});
it("edits filter drafts locally, retains failed drafts, applies at offset zero and resets explicitly", async () => {
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
  const queries: URLSearchParams[] = [];
  let fail = true;
  vi.spyOn(stores.api, "request").mockImplementation(async (path) => {
    const query = new URL(path, "https://example.test").searchParams;
    queries.push(query);
    if (query.get("aggregate_id") && fail)
      throw new ApiError(503, "http_error");
    return {
      items: [
        {
          ...fixture(),
          name: query.has("aggregate_id")
            ? "Отфильтрованный детектор"
            : "Начальный детектор",
        },
      ],
      total: 1,
      limit: 25,
      offset: Number(query.get("offset")),
    } as never;
  });
  try {
    render(
      <StoresProvider stores={stores}>
        <MemoryRouter>
          <DetectorsPage />
        </MemoryRouter>
      </StoresProvider>,
    );
    await screen.findByRole("link", { name: "Начальный детектор" });
    fireEvent.click(screen.getByRole("button", { name: "Фильтры" }));
    let dialog = await screen.findByRole("dialog");
    fireEvent.change(
      within(dialog).getByRole("textbox", { name: "UUID агрегата" }),
      { target: { value: id } },
    );
    fireEvent.click(within(dialog).getByRole("button", { name: "Отмена" }));
    await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
    expect(queries).toHaveLength(1);
    expect(stores.detectors.list.aggregate).toBe("");
    fireEvent.click(screen.getByRole("button", { name: "Фильтры" }));
    dialog = await screen.findByRole("dialog");
    expect(
      (
        within(dialog).getByRole("textbox", {
          name: "UUID агрегата",
        }) as HTMLInputElement
      ).value,
    ).toBe("");
    fireEvent.change(
      within(dialog).getByRole("textbox", { name: "UUID агрегата" }),
      { target: { value: id } },
    );
    fireEvent.click(within(dialog).getByRole("button", { name: "Применить" }));
    await waitFor(() =>
      expect(stores.detectors.list.error).toBeInstanceOf(ApiError),
    );
    expect(
      screen.queryByRole("link", { name: "Начальный детектор" }),
    ).toBeNull();
    expect(
      (
        within(screen.getByRole("dialog")).getByRole("textbox", {
          name: "UUID агрегата",
        }) as HTMLInputElement
      ).value,
    ).toBe(id);
    expect(stores.detectors.list.aggregate).toBe("");
    fail = false;
    await waitFor(() =>
      expect(
        (
          within(screen.getByRole("dialog")).getByRole("button", {
            name: "Применить",
          }) as HTMLButtonElement
        ).disabled,
      ).toBe(false),
    );
    fireEvent.click(
      within(screen.getByRole("dialog")).getByRole("button", {
        name: "Применить",
      }),
    );
    await screen.findByRole("link", { name: "Отфильтрованный детектор" });
    await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
    expect(queries.at(-1)?.get("offset")).toBe("0");
    expect(stores.detectors.list.aggregate).toBe(id);
    fireEvent.click(screen.getByRole("button", { name: "Фильтры" }));
    dialog = await screen.findByRole("dialog");
    fireEvent.click(within(dialog).getByRole("button", { name: "Сбросить" }));
    expect(stores.detectors.list.aggregate).toBe(id);
    expect(queries).toHaveLength(3);
    fireEvent.click(within(dialog).getByRole("button", { name: "Применить" }));
    await screen.findByRole("link", { name: "Начальный детектор" });
    expect(queries.at(-1)?.has("aggregate_id")).toBe(false);
  } finally {
    stores.dispose();
  }
});
