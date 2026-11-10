import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
} from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { AppStores } from "../../app/stores/AppStores";
import { StoresProvider } from "../../app/providers/StoresProvider";
import { ApiError } from "../../app/api/client";
import { fixture, id } from "../../features/detectors/testData";
import { DetectorCardPage } from "./DetectorCardPage";

afterEach(() => {
  cleanup();
  vi.useRealTimers();
  vi.restoreAllMocks();
  Object.defineProperty(document, "hidden", {
    configurable: true,
    value: false,
  });
});

it("stops polling after a read error, resumes after explicit refresh, and pauses on hidden/unmounted views", async () => {
  vi.useFakeTimers();
  Object.defineProperty(document, "hidden", {
    configurable: true,
    value: false,
  });
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
  let reads = 0;
  vi.spyOn(stores.api, "request").mockImplementation(async (path) => {
    if (path !== `/detectors/${id}`) throw new Error("Unexpected request");
    reads++;
    if (reads === 2) throw new ApiError(503, "http_error");
    return { ...fixture(), execution_state: "running", model_state: "ready" };
  });
  const view = render(
    <StoresProvider stores={stores}>
      <MemoryRouter initialEntries={[`/detectors/${id}`]}>
        <Routes>
          <Route path="/detectors/:id" element={<DetectorCardPage />} />
        </Routes>
      </MemoryRouter>
    </StoresProvider>,
  );
  await act(async () => {
    await Promise.resolve();
  });
  expect(reads).toBe(1);
  await act(async () => {
    await vi.advanceTimersByTimeAsync(3000);
  });
  expect(reads).toBe(2);
  await act(async () => {
    await vi.advanceTimersByTimeAsync(9000);
  });
  expect(reads).toBe(2);
  await act(async () => {
    fireEvent.click(screen.getByRole("button", { name: "Обновить состояние" }));
  });
  expect(reads).toBe(3);
  await act(async () => {
    await vi.advanceTimersByTimeAsync(3000);
  });
  expect(reads).toBe(4);
  await act(async () => {
    Object.defineProperty(document, "hidden", {
      configurable: true,
      value: true,
    });
    document.dispatchEvent(new Event("visibilitychange"));
  });
  await act(async () => {
    await vi.advanceTimersByTimeAsync(9000);
  });
  expect(reads).toBe(4);
  await act(async () => {
    Object.defineProperty(document, "hidden", {
      configurable: true,
      value: false,
    });
    document.dispatchEvent(new Event("visibilitychange"));
  });
  expect(reads).toBe(5);
  view.unmount();
  await act(async () => {
    await vi.advanceTimersByTimeAsync(9000);
  });
  expect(reads).toBe(5);
});
