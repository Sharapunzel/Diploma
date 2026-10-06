import { cleanup, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiError } from "../api/client";
import { AppRequestError } from "./AppRequestError";

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("AppRequestError", () => {
  it("maps unknown errors to a safe message", () => {
    render(<AppRequestError error={new Error("private traceback")} />);
    expect(screen.getByRole("alert").textContent).toContain(
      "Не удалось выполнить запрос.",
    );
    expect(screen.getByRole("alert").textContent).not.toContain(
      "private traceback",
    );
  });

  it("maps network failures without exposing server text", () => {
    render(
      <AppRequestError
        error={new ApiError(0, "network_error", null, "private network detail")}
      />,
    );
    const alert = screen.getByRole("alert");
    expect(alert.textContent).toContain("Нет соединения с сервером.");
    expect(alert.textContent).not.toContain("private network detail");
  });

  it("keeps safe API text and request id without exposing server details", async () => {
    const user = userEvent.setup();
    render(
      <AppRequestError
        error={
          new ApiError(
            503,
            "unavailable",
            "request-safe-id",
            "private server message",
            { trace: "private trace" },
          )
        }
      />,
    );
    const alert = screen.getByRole("alert");
    expect(alert.textContent).toContain("Сервер временно недоступен.");
    expect(alert.textContent).not.toMatch(
      /private server message|private trace/,
    );
    await user.click(screen.getByText("Подробности"));
    expect(screen.getByText("request-safe-id").closest("details")?.open).toBe(
      true,
    );
  });

  it("retains request retry callback", async () => {
    const retry = vi.fn();
    render(<AppRequestError error={new Error()} onRetry={retry} />);
    await screen.getByRole("button", { name: "Повторить" }).click();
    expect(retry).toHaveBeenCalledOnce();
  });
});
