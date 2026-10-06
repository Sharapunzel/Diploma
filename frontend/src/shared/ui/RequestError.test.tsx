import { cleanup, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { RequestError } from "./RequestError";

const clipboardDescriptor = Object.getOwnPropertyDescriptor(
  navigator,
  "clipboard",
);

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
  if (clipboardDescriptor) {
    Object.defineProperty(navigator, "clipboard", clipboardDescriptor);
  } else {
    Reflect.deleteProperty(navigator, "clipboard");
  }
});

describe("RequestError", () => {
  it("renders inline safe message, request id, and retries on demand", async () => {
    const retry = vi.fn();
    const user = userEvent.setup();
    render(
      <RequestError
        message="Нет соединения с сервером."
        requestId="request-123"
        onRetry={retry}
      />,
    );
    expect(screen.getByRole("alert").textContent).toContain(
      "Нет соединения с сервером.",
    );
    await user.click(screen.getByText("Подробности"));
    expect(screen.getByText("request-123").closest("details")?.open).toBe(true);
    await user.click(screen.getByRole("button", { name: "Повторить" }));
    expect(retry).toHaveBeenCalledOnce();
  });

  it("copies the real request id when the clipboard is available", async () => {
    const user = userEvent.setup();
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(navigator, "clipboard", {
      configurable: true,
      value: { writeText },
    });
    render(<RequestError message="Ошибка запроса." requestId="request-789" />);

    await user.click(screen.getByText("Подробности"));
    await user.click(
      screen.getByRole("button", { name: "Копировать идентификатор запроса" }),
    );
    expect(writeText).toHaveBeenCalledExactlyOnceWith("request-789");
    expect(await screen.findByText("Идентификатор скопирован")).toBeDefined();
  });

  it("renders page variant and copies request id with manual-copy fallback", async () => {
    const user = userEvent.setup();
    Object.defineProperty(navigator, "clipboard", {
      configurable: true,
      value: { writeText: vi.fn().mockRejectedValue(new Error("blocked")) },
    });
    render(
      <RequestError
        message="Нет соединения с сервером."
        requestId="request-456"
        variant="page"
      />,
    );
    expect(
      screen.getByRole("heading", { name: "Не удалось загрузить приложение" }),
    ).toBeDefined();
    await user.click(screen.getByText("Подробности ошибки"));
    expect(screen.getByText("request-456").closest("details")?.open).toBe(true);
    await user.click(screen.getByRole("button", { name: "Копировать" }));
    expect(
      await screen.findByText(/Выделите идентификатор вручную/),
    ).toBeDefined();
  });
});
