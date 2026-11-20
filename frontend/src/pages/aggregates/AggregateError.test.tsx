import { afterEach, expect, it } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { ApiError } from "../../app/api/client";
import { AggregateError } from "./AggregateError";

afterEach(cleanup);

it("distinguishes an expired client wait from a confirmed server budget refusal", () => {
  const view = render(
    <AggregateError error={new ApiError(0, "preview_wait_timeout")} />,
  );
  expect(
    screen.getByText(/не подтверждает остановку вычисления на сервере/),
  ).toBeTruthy();
  expect(screen.queryByText(/Точка не сохранена/)).toBeNull();
  view.rerender(
    <AggregateError error={new ApiError(422, "execution_budget_exceeded")} />,
  );
  expect(screen.getByText(/Точка не сохранена/)).toBeTruthy();
  expect(
    screen.queryByText(/не подтверждает остановку вычисления на сервере/),
  ).toBeNull();
});
it("focuses the rule with its Russian accessible name when an error is outside a visual node", () => {
  render(
    <>
      <textarea aria-label="Полное правило JSON" defaultValue="{}" />
      <AggregateError
        error={
          new ApiError(422, "invalid_rule", null, null, {
            path: "aggregate_rule",
          })
        }
      />
    </>,
  );
  fireEvent.click(screen.getByRole("button", { name: "aggregate_rule" }));
  expect(document.activeElement).toBe(
    screen.getByLabelText("Полное правило JSON"),
  );
});
it("explains the resource factor in Russian without exposing the server message", () => {
  render(
    <AggregateError
      error={
        new ApiError(422, "resource_budget_exceeded", "request", "private", {
          factor: "operator_cost",
        })
      }
    />,
  );
  expect(
    screen.getByText(/Фактор ограничения: Сложность операций/),
  ).toBeTruthy();
  expect(screen.queryByText("private")).toBeNull();
});

it.each([
  ["automatic", "Значения очищены автоматически."],
  ["manual", "Значения очищены ручной командой."],
  [null, "Способ очистки прежнего окна неизвестен."],
] as const)(
  "explains cleared410 cause %s separately from a not-ready result",
  (cause, message) => {
    const date = cause === null ? null : "2026-11-16T00:00:00Z";
    render(
      <AggregateError
        error={
          new ApiError(410, "aggregate_point_cleared", "clear-window", null, {
            clear_reason: cause,
            cleared_at: date,
          })
        }
      />,
    );
    expect(screen.getByText((text) => text.startsWith(message))).toBeTruthy();
    expect(
      screen.queryByText(/Сохранённый результат окна ещё не готов/),
    ).toBeNull();
    if (date)
      expect(screen.getByText(date).getAttribute("dateTime")).toBe(date);
    else expect(screen.getByText(/Время очистки неизвестно/)).toBeTruthy();
  },
);
