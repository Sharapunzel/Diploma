import { afterEach, expect, it } from "vitest";
import { cleanup, render, screen, within } from "@testing-library/react";
import type { Preview } from "../../app/api/aggregates";
import { parseAggregate, type JsonObject } from "../../app/api/aggregateJson";
import { BudgetView } from "./BudgetView";
import { FeatureTable, ResultView } from "./ResultView";
import {
  previewFixture,
  executionFixture,
  executionPolicyFixture,
} from "../../app/api/aggregateTestData";

afterEach(cleanup);

it("updates draft thresholds without inventing a measured duration", () => {
  const view = render(
    <BudgetView duration="5" budget={{ estimated_rows: 0 }} />,
  );
  const thresholds = within(
    screen.getByRole("region", { name: "Пороги расчёта" }),
  );
  expect(thresholds.getByText("4 с")).toBeTruthy();
  expect(thresholds.getByText("5 с")).toBeTruthy();
  expect(
    screen.getByText(
      /Ещё не измерено. Оценка ресурсов не является прогнозом времени/,
    ),
  ).toBeTruthy();
  expect(
    screen.getByText("Записей в окне:").parentElement?.textContent,
  ).toContain("0");
  view.rerender(<BudgetView duration="300" />);
  expect(thresholds.getByText("30 с")).toBeTruthy();
  expect(thresholds.queryByText("4 с")).toBeNull();
});

it("does not reuse a previous policy when an explicit draft duration is invalid", () => {
  const view = render(
    <BudgetView duration="" policy={executionPolicyFixture()} />,
  );
  const thresholds = within(
    screen.getByRole("region", { name: "Пороги расчёта" }),
  );
  expect(
    thresholds.getByText("Укажите корректную длительность окна"),
  ).toBeTruthy();
  expect(thresholds.queryByText("30 с")).toBeNull();
  view.rerender(<BudgetView policy={executionPolicyFixture()} />);
  expect(thresholds.getByText("30 с")).toBeTruthy();
  view.rerender(<BudgetView duration="5" policy={executionPolicyFixture()} />);
  expect(thresholds.getByText("4 с")).toBeTruthy();
  expect(thresholds.queryByText("30 с")).toBeNull();
});

it("labels a precommit measurement without presenting unknown ACK latency as zero", () => {
  render(
    <BudgetView
      duration={300}
      execution={executionFixture({
        origin: "online",
        window_index: 0,
        measured_through: "before_commit",
        elapsed_seconds: 6,
      })}
    />,
  );
  const actual = within(
    screen.getByRole("region", { name: "Последний фактический расчёт" }),
  );
  expect(actual.getByText("Рассчитано")).toBeTruthy();
  expect(actual.getByText("6 с")).toBeTruthy();
  expect(
    actual.getByText(
      /до сохранения результата; запись и её подтверждение не включены/,
    ),
  ).toBeTruthy();
  expect(
    actual.getByText(/Полная online-задержка.*Ещё не измерено/),
  ).toBeTruthy();
  expect(actual.getByText(/Мягкий ориентир: Ещё не измерено/)).toBeTruthy();
  expect(actual.queryByText("Не превышен")).toBeNull();
});

it("does not turn a soft exceedance into an uncomputed result", () => {
  render(
    <BudgetView
      duration={300}
      execution={executionFixture({
        origin: "online",
        window_index: 0,
        elapsed_seconds: 1,
        online_latency_seconds: 7,
        soft_latency_exceeded: true,
      })}
    />,
  );
  const actual = within(
    screen.getByRole("region", { name: "Последний фактический расчёт" }),
  );
  expect(actual.getByText("Рассчитано")).toBeTruthy();
  expect(
    actual.getByText(
      /Превышен; вычисленные значения сохраняют прежнюю пригодность/,
    ),
  ).toBeTruthy();
  expect(actual.queryByText("Не рассчитано")).toBeNull();
});

it("does not imply available numeric data when an uncomputed window also exceeds the soft target", () => {
  render(
    <BudgetView
      duration={300}
      execution={executionFixture({
        calculation_status: "uncomputed",
        calculation_reason: "execution_budget_exceeded",
        origin: "online",
        window_index: 0,
        measured_through: "finalization",
        elapsed_seconds: 31,
        online_latency_seconds: 40,
        soft_latency_exceeded: true,
      })}
    />,
  );
  const actual = within(
    screen.getByRole("region", { name: "Последний фактический расчёт" }),
  );
  expect(actual.getByText("Не рассчитано")).toBeTruthy();
  expect(actual.getByText(/Превышен; исход окна указан выше/)).toBeTruthy();
  expect(actual.queryByText(/вычисленные значения сохраняют/)).toBeNull();
});

it("separates a mutable out-of-process preview from received quality and delivery", () => {
  render(
    <ResultView
      result={previewFixture({
        in_process: false,
        snapshot_metadata: {
          delivery_coverage: "not_attested",
          facts: { exclusions: { event_time_fallback: 2 } },
        },
      })}
      zone="UTC"
    />,
  );
  expect(
    screen.getByRole("heading", {
      name: "Предварительный расчёт — результат не сохранён",
    }),
  ).toBeTruthy();
  expect(screen.getByText(/Диагностика вне процесса/)).toBeTruthy();
  expect(
    screen.getByText("Полученный снимок без известных нарушений"),
  ).toBeTruthy();
  expect(
    screen.getByText(/доставка всех журналов не подтверждена/),
  ).toBeTruthy();
  expect(
    screen.getByRole("list", { name: "Исключения времени снимка" }).textContent,
  ).toContain("2");
  expect(screen.queryByText("Полнота подтверждена")).toBeNull();
});

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
