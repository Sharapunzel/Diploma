import { afterEach, expect, it } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { ApiError } from "../../app/api/client";
import { AggregateError } from "./AggregateError";

afterEach(cleanup);
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
