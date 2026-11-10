import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { MemoryRouter, Route, Routes, useNavigate } from "react-router-dom";
import { runInAction } from "mobx";
import { AppStores } from "../../app/stores/AppStores";
import { StoresProvider } from "../../app/providers/StoresProvider";
import { ApiError } from "../../app/api/client";
import type { DetectorsApi } from "../../app/api/detectors";
import { CommandStore } from "../../features/detectors/Workspace";
import { fixture, id, receipt } from "../../features/detectors/testData";
import { DetectorCardPage } from "./DetectorCardPage";
import { CommandFeedback } from "./Parts";

const disposers: (() => void)[] = [];
afterEach(() => {
  cleanup();
  disposers.splice(0).forEach((dispose) => dispose());
  vi.restoreAllMocks();
});
function admin(stores: AppStores) {
  stores.session.accept(
    {
      authenticated: true,
      user: { id, username: "admin", email: null, display_name: "Admin" },
      role: "admin",
      permissions: ["detectors.read", "detectors.write"],
      authentication_method: "local",
      csrf_token: "synthetic",
    },
    stores.session.version,
  );
}
function Navigation() {
  const navigate = useNavigate();
  return (
    <>
      <button onClick={() => void navigate(-1)}>Назад в истории</button>
      <button onClick={() => void navigate(1)}>Вперёд в истории</button>
    </>
  );
}

it.each(["back", "actor", "permission"])(
  "invalidates a stop confirmation on %s and requires a new explicit target",
  async (change) => {
    const stores = new AppStores();
    disposers.push(() => stores.dispose());
    admin(stores);
    const b = "00000000-0000-4000-8000-000000000456";
    const posts: string[] = [];
    const stopped = new Set<string>();
    vi.spyOn(stores.api, "request").mockImplementation(
      async (path, options) => {
        const target = path.split("/")[2];
        if (options?.method === "POST") {
          posts.push(path);
          stopped.add(target);
          return { ...receipt("stop"), detector_id: target } as never;
        }
        return {
          ...fixture(),
          id: target,
          name: target === id ? "Детектор A" : "Детектор B",
          execution_state: stopped.has(target) ? "stopped" : "running",
          model_state: "ready",
        } as never;
      },
    );
    render(
      <StoresProvider stores={stores}>
        <MemoryRouter
          initialEntries={[`/detectors/${b}`, `/detectors/${id}`]}
          initialIndex={1}
        >
          <Navigation />
          <Routes>
            <Route path="/detectors/:id" element={<DetectorCardPage />} />
          </Routes>
        </MemoryRouter>
      </StoresProvider>,
    );
    await screen.findByRole("heading", { name: "Детектор A", level: 2 });
    fireEvent.click(screen.getByRole("button", { name: "Остановить" }));
    expect((await screen.findByRole("dialog")).textContent).toContain(
      `Детектор «Детектор A» (${id})`,
    );
    if (change === "back")
      fireEvent.click(screen.getByRole("button", { name: "Назад в истории" }));
    else
      await act(async () => {
        if (change === "actor") admin(stores);
        else
          runInAction(() => {
            stores.session.data!.permissions = ["detectors.read"];
          });
      });
    await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
    expect(posts).toEqual([]);
    if (change !== "back") return;
    await screen.findByRole("heading", { name: "Детектор B", level: 2 });
    fireEvent.click(screen.getByRole("button", { name: "Вперёд в истории" }));
    await screen.findByRole("heading", { name: "Детектор A", level: 2 });
    expect(screen.queryByRole("dialog")).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "Назад в истории" }));
    await screen.findByRole("heading", { name: "Детектор B", level: 2 });
    fireEvent.click(screen.getByRole("button", { name: "Остановить" }));
    fireEvent.click(
      within(await screen.findByRole("dialog")).getByRole("button", {
        name: "Отмена",
      }),
    );
    await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
    expect(posts).toEqual([]);
    fireEvent.click(screen.getByRole("button", { name: "Остановить" }));
    const dialog = await screen.findByRole("dialog");
    expect(dialog.textContent).toContain("Детектор B");
    fireEvent.click(within(dialog).getByRole("button", { name: "Остановить" }));
    await waitFor(() => expect(posts).toEqual([`/detectors/${b}/stop`]));
  },
);

it.each([503, 409])(
  "does not confirm a new refused/unknown command with an old receipt (%i)",
  async (status) => {
    const api = {
      command: vi
        .fn()
        .mockResolvedValueOnce(receipt("start"))
        .mockRejectedValueOnce(new ApiError(status, "http_error"))
        .mockResolvedValueOnce(receipt("stop")),
      card: vi
        .fn()
        .mockResolvedValueOnce(fixture())
        .mockRejectedValue(new ApiError(503, "http_error")),
    };
    const store = new CommandStore(api as unknown as DetectorsApi, id);
    disposers.push(() => store.dispose());
    await store.send("start");
    render(<CommandFeedback store={store} />);
    await act(async () => {
      await store.send("stop");
      await store.load();
    });
    expect(screen.queryByText(/Команда подтверждена\. Детектор/)).toBeNull();
    expect(screen.queryByText(/Команда запуска подтверждена/)).toBeNull();
    expect(store.receipt).toBeNull();
    if (status === 503) {
      expect(screen.getByText(/Результат команды неизвестен:/)).not.toBeNull();
      const before = api.command.mock.calls[1];
      fireEvent.click(
        screen.getByRole("button", { name: "Повторить исходную команду" }),
      );
      await screen.findByText(/Команда подтверждена\. Детектор/);
      expect(api.command.mock.calls[2].slice(0, 4)).toEqual(before.slice(0, 4));
      expect(store.receipt?.action).toBe("stop");
      expect(api.command).toHaveBeenCalledTimes(3);
    } else
      expect(
        screen.queryByRole("button", { name: "Повторить исходную команду" }),
      ).toBeNull();
  },
);

it("shows the current command's valid receipt with failed GET and retries only reading", async () => {
  const api = {
    command: vi.fn().mockResolvedValue(receipt("stop")),
    card: vi.fn().mockRejectedValue(new ApiError(503, "http_error")),
  };
  const store = new CommandStore(api as unknown as DetectorsApi, id);
  disposers.push(() => store.dispose());
  render(<CommandFeedback store={store} />);
  await act(async () => {
    await store.send("stop");
  });
  expect(screen.getByText(/Команда подтверждена\. Детектор/)).not.toBeNull();
  expect(screen.queryByText(/Результат команды неизвестен:/)).toBeNull();
  await act(async () => {
    await store.load();
  });
  expect(api.command).toHaveBeenCalledTimes(1);
});
