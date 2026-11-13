import { afterEach, expect, it } from "vitest";
import { cleanup, render, screen, within } from "@testing-library/react";
import type { Preview } from "../../app/api/aggregates";
import { parseAggregate, type JsonObject } from "../../app/api/aggregateJson";
import { BudgetView } from "./BudgetView";
import { FeatureTable, ResultView } from "./ResultView";

afterEach(cleanup);

it("keeps exact output values and missing values distinct in the styled preview", () => {
  const result = parseAggregate(`{
    "rule_hash":"hash", "start_utc":"2026-11-01T00:00:00.123456+05:00",
    "end_utc":"2026-11-01T00:05:00.123456+05:00", "scheduled_at":"2026-11-01T00:05:00.123456+05:00",
    "observed_rows":2,"event_count":2,"event_time_min":null,"event_time_max":null,
    "features":[
      {"key":"huge","value":9007199254740993,"reason":null,"valid_count":1,"invalid_count":0},
      {"key":"fraction","value":1.0,"reason":null,"valid_count":1,"invalid_count":0},
      {"key":"empty","value":null,"reason":"no_values","valid_count":0,"invalid_count":0}
    ],
    "completeness":"unknown","completeness_reason":"preview_snapshot_without_coverage_proof",
    "time_basis":"availability_at","budget":{"estimated_rows":2,"time_guarantee":false}
  }`) as unknown as Preview;
  render(<ResultView result={result} zone="UTC" />);
  const values = within(
    screen.getByRole("region", { name: "Значения признаков" }),
  );
  expect(
    within(values.getByRole("row", { name: /^huge / })).getByRole("cell", {
      name: "9007199254740993",
    }),
  ).toBeTruthy();
  expect(
    within(values.getByRole("row", { name: /^fraction / })).getByRole("cell", {
      name: "1.0",
    }),
  ).toBeTruthy();
  const empty = within(values.getByRole("row", { name: /^empty / }));
  expect(empty.getByRole("cell", { name: "Нет значения" })).toBeTruthy();
  expect(
    empty.getByRole("cell", { name: /Нет подходящих значений/ }),
  ).toBeTruthy();
  expect(screen.getByText("Полнота неизвестна")).toBeTruthy();
  expect(
    screen.getByText("По времени доступности сохранённых событий"),
  ).toBeTruthy();
  expect(screen.getByText(result.start_utc, { exact: true })).toBeTruthy();
});

it("presents Russian resource bricks without rounding precise numbers or hiding zero", () => {
  const budget = parseAggregate(
    '{"estimated_rows":0,"estimated_memory_bytes":9007199254740993,"operator_weight":1.0,"time_guarantee":false}',
  ) as JsonObject;
  render(<BudgetView budget={budget} />);
  const resources = within(
    screen.getByRole("list", { name: "Ресурсы расчёта" }),
  );
  const items = resources.getAllByRole("listitem");
  expect(items.map((item) => item.textContent)).toEqual([
    "Записей в окне: 0",
    "Оценка памяти, байт: 9007199254740993",
    "Вес операций: 1.0",
  ]);
  expect(screen.queryByText("time_guarantee")).toBeNull();
  expect(screen.getByText(/не гарантия своевременного расчёта/)).toBeTruthy();
});

it("does not invent zero-valued outputs for an old point with unavailable metadata", () => {
  render(<FeatureTable features={null} />);
  expect(
    screen.getByText(
      /У этой старой точки отсутствуют доступные значения признаков/,
    ),
  ).toBeTruthy();
  expect(screen.queryByRole("table")).toBeNull();
});

it("keeps all zero-valued resource estimates visible without treating time_guarantee as a resource", () => {
  render(
    <BudgetView
      budget={{
        estimated_rows: 0,
        source_rows: 0,
        cost: 0,
        estimated_memory_bytes: 0,
        time_guarantee: false,
      }}
    />,
  );
  const resources = within(
    screen.getByRole("list", { name: "Ресурсы расчёта" }),
  );
  expect(
    resources.getAllByRole("listitem").map((item) => item.textContent),
  ).toEqual([
    "Записей в окне: 0",
    "Сохранённых записей источника: 0",
    "Расчётная стоимость: 0",
    "Оценка памяти, байт: 0",
  ]);
  expect(screen.queryByText("time_guarantee")).toBeNull();
  expect(screen.getByText(/не гарантия своевременного расчёта/)).toBeTruthy();
});
