import { afterEach, expect, it, vi } from "vitest";
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { AggregatesApi } from "../../app/api/aggregates";
import { ApiError } from "../../app/api/client";
import { parseAggregate } from "../../app/api/aggregateJson";
import {
  aggregateFixture,
  aggregateId,
  autoCleanupFixture,
  missingAutoCleanupFixture,
} from "../../app/api/aggregateTestData";
import { CardStore } from "../../features/aggregates/CardStore";
import { AutoCleanupPanel } from "./AutoCleanupPanel";

const owned: CardStore[] = [];
afterEach(() => {
  cleanup();
  owned.splice(0).forEach((store) => store.dispose());
});
function setup(summary = autoCleanupFixture()) {
  const api = {
    card: vi.fn().mockResolvedValue(aggregateFixture()),
    command: vi.fn(),
    cleanup: vi.fn(),
  };
  const store = new CardStore(api as unknown as AggregatesApi, aggregateId);
  store.card = aggregateFixture({ auto_cleanup: summary });
  owned.push(store);
  render(<AutoCleanupPanel store={store} zone="UTC" />);
  return { store, api };
}
it("shows missing state as unknown and only reads after an explicit retry, without an auto-depth control or mutation", async () => {
  const { api } = setup(missingAutoCleanupFixture());
  expect(screen.getByRole("alert").textContent).toContain(
    "Служебное состояние хранения отсутствует",
  );
  expect(screen.getAllByText("неизвестно").length).toBeGreaterThan(2);
  expect(screen.queryByRole("textbox")).toBeNull();
  expect(screen.queryByRole("spinbutton")).toBeNull();
  expect(screen.queryByRole("checkbox")).toBeNull();
  fireEvent.click(
    screen.getByRole("button", { name: "Обновить состояние хранения" }),
  );
  await waitFor(() => expect(api.card).toHaveBeenCalledTimes(1));
  await waitFor(() => expect(screen.queryByRole("alert")).toBeNull());
  expect(api.command).not.toHaveBeenCalled();
  expect(api.cleanup).not.toHaveBeenCalled();
});
it("explains all-model protection without presenting a future unread suffix as already retained payload", () => {
  setup(
    autoCleanupFixture({
      status: "protected",
      reason: "unconsumed_points",
      reserve_windows: 512,
      protected_total: 11,
      protected_from_index: 1000,
      retained_from_index: 0,
      retained_to_index: 519,
      protected_by: Array.from({ length: 10 }, (_, index) => ({
        detector_id: `00000000-0000-4000-8000-${String(index).padStart(12, "0")}`,
        name: `Остановленная модель ${index}`,
        reason: "unconsumed_points" as const,
        protected_from_index: 1000,
        protected_to_index: null,
      })),
    }),
  );
  expect(
    screen.getByLabelText("Модели, защищающие историю").children,
  ).toHaveLength(10);
  expect(
    screen.getByText(/Показаны первые 10 моделей; защита проверяется для всех/),
  ).toBeTruthy();
  expect(
    screen.getAllByText(/Сохраняются ещё не потреблённые моделью окна/).length,
  ).toBeGreaterThan(0);
  expect(screen.queryByRole("button", { name: /Очистить/ })).toBeNull();
});
it("keeps counters and revision exact and displays a scoped GET error without claiming background success", () => {
  const { store } = setup(
    autoCleanupFixture({
      auto_cleared_total: parseAggregate("9007199254740993"),
      history_revision: parseAggregate("9223372036854775807"),
    }),
  );
  expect(screen.getByText("9007199254740993", { selector: "dd" })).toBeTruthy();
  expect(
    screen.getByText("9223372036854775807", { selector: "dd" }),
  ).toBeTruthy();
  act(() => {
    store.autoCleanupError = new ApiError(503, "aggregate_database_error");
  });
  expect(
    screen.getByText(/Не удалось обновить состояние хранения/),
  ).toBeTruthy();
  expect(screen.queryByText(/Очистка завершена/)).toBeNull();
  expect(screen.queryByRole("dialog")).toBeNull();
});
