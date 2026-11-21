import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { afterEach, expect, it, vi } from "vitest";
import { ApiError } from "../../app/api/client";
import type {
  FindingCard,
  FindingDetector,
  FindingRow,
} from "../../app/api/findings";
import { StoresProvider } from "../../app/providers/StoresProvider";
import { AppStores } from "../../app/stores/AppStores";
import { FindingsPage } from "./FindingsPage";
import { Timeline } from "./Timeline";

const id = "00000000-0000-4000-8000-000000000042";
const source = "00000000-0000-4000-8000-000000000059";
const start = "2026-11-12T20:00:00.123456Z";
const end = "2026-11-12T20:00:00.123457Z";
const fixture: FindingCard = {
  id,
  detector_id: id,
  detector_name: "Детектор А",
  source_name: "Источник А",
  time_basis: "event_time",
  window_start: start,
  window_end: "2026-11-12T20:02:00.123456Z",
  grade: 0.8,
  confidence: 0.75,
  late: true,
  accelerated_used: true,
  aggregate_name: "Агрегат А",
  source_id: source,
  event_range: { timestamp_from: start, timestamp_to: end },
  event_range_reason: null,
  aggregation_started_at: start,
  aggregation_completed_at: end,
  evaluation_started_at: end,
  evaluation_completed_at: end,
  expected_origin: "aws_rcf",
  features: [
    {
      key: "first",
      name: "Первый",
      actual: "1152921504606846977",
      expected: "1152921504606846976.75",
      attribution: 3,
    },
    {
      key: "second",
      name: "Второй",
      actual: "1",
      expected: "0.000000000000001",
      attribution: 1,
    },
    {
      key: "third",
      name: "Третий",
      actual: "0",
      expected: null,
      attribution: 0,
    },
  ],
};
function setup(
  card = fixture,
  failFirst = true,
  rows: FindingRow[] = [fixture],
  offPage?: FindingRow,
) {
  const stores = new AppStores();
  let first = true;
  const request = vi.fn(async (path: string) => {
    const all = offPage ? [...rows, offPage] : rows;
    if (path === "/findings/detectors") return all;
    if (path === `/findings/${id}`) return card;
    const chart = path.startsWith("/findings/timeline?");
    if (chart || path.startsWith("/findings?")) {
      if (!chart && first && failFirst) {
        first = false;
        throw new ApiError(503, "service_unavailable");
      }
      const query = new URLSearchParams(path.split("?")[1]);
      const filtered = query.get("grade_min") === "0.9";
      const nextPage =
        !chart &&
        offPage &&
        (query.get("anchor_id") === offPage.id || query.get("offset") === "25");
      return {
        items: filtered ? [] : chart ? all : nextPage ? [offPage] : rows,
        total: filtered ? 0 : offPage ? 26 : rows.length,
        limit: chart ? 100 : 25,
        offset: nextPage ? 25 : 0,
      };
    }
    throw new Error(`Unexpected findings request ${path}`);
  });
  stores.api.request = request as typeof stores.api.request;
  const user = userEvent.setup();
  render(
    <StoresProvider stores={stores}>
      <MemoryRouter>
        <FindingsPage />
      </MemoryRouter>
    </StoresProvider>,
  );
  return { request, user };
}

afterEach(cleanup);

it("scales overlapping windows by duration and groups the time bases by detector", () => {
  const second = {
    ...fixture,
    id: source,
    detector_id: source,
    detector_name: "Детектор Б",
    window_start: "2026-11-12T20:01:00.123456Z",
    window_end: "2026-11-12T20:05:00.123456Z",
  };
  const legacy = {
    ...second,
    id: "00000000-0000-4000-8000-000000000061",
    time_basis: "availability_at",
  } as const;
  const detectors = [fixture, second];
  const chart = (
    items: FindingRow[],
    options: FindingDetector[] = detectors,
  ) => (
    <Timeline
      items={items}
      detectors={options}
      highlighted={null}
      onSelect={vi.fn()}
    />
  );
  const items = [fixture, second, legacy];
  const { rerender } = render(chart(items));
  expect(screen.getByRole("heading", { name: "Детекторы" })).toBeTruthy();
  const legend = screen.getByRole("list", {
    name: "Детекторы и основание времени",
  });
  const secondLegend = within(legend).getByText("Детектор Б").parentElement!;
  expect(
    within(secondLegend).getByText("Время события (ECS @timestamp)"),
  ).toBeTruthy();
  expect(
    within(secondLegend).getByText("Время доступности данных"),
  ).toBeTruthy();
  fireEvent.click(
    within(secondLegend).getByRole("button", {
      name: "Справка: Время доступности данных",
    }),
  );
  expect(screen.getByText(/Когда запись сохранилась/)).toBeTruthy();

  const mark = (row: FindingRow) =>
    document.getElementById("finding-window-" + row.id)!;
  const first = mark(fixture);
  const middle = mark(second);
  const last = mark(legacy);
  expect(first.getAttribute("aria-label")).not.toMatch(/:\d{2}\.\d+/);
  expect(first.style.width).toBe("40%");
  expect(middle.style.left).toBe("20%");
  expect(middle.style.width).toBe("80%");
  expect(last.style.left).toBe(middle.style.left);
  expect(last.style.width).toBe(middle.style.width);
  expect(
    new Set([first.style.top, middle.style.top, last.style.top]).size,
  ).toBe(3);
  const color = middle.style.backgroundColor;
  expect(color).not.toBe(first.style.backgroundColor);
  expect(color).toBe(last.style.backgroundColor);
  expect(middle.style.backgroundImage).not.toBe(last.style.backgroundImage);

  rerender(chart([legacy]));
  expect(mark(legacy).style.backgroundColor).toBe(color);
  rerender(
    chart(items, [
      fixture,
      { ...second, detector_name: fixture.detector_name },
    ]),
  );
  expect(within(legend).getAllByText(fixture.detector_name!)).toHaveLength(2);
  expect(legend.textContent).not.toContain(source);
});

it("shows bare names and explains rows without special evaluation flags", async () => {
  await act(async () => {
    setup(fixture, false, [
      fixture,
      {
        ...fixture,
        id: source,
        detector_id: source,
        late: false,
        accelerated_used: false,
      },
    ]);
  });
  const table = screen.getByRole("region", { name: "Список находок" });
  expect(
    within(table)
      .getByRole("columnheader", { name: /^Окно/ })
      .getAttribute("aria-sort"),
  ).toBe("descending");
  expect(
    within(table).getByRole("columnheader", { name: /^Отметки/ }),
  ).toBeTruthy();
  expect(
    within(table).getAllByRole("button", {
      name: fixture.detector_name!,
    }),
  ).toHaveLength(2);
  expect(within(table).getByText("Без отметок")).toBeTruthy();
  expect(within(table).getByText("Запоздалая оценка")).toBeTruthy();
});

it("jumps to the finding on another table page and back to its window", async () => {
  const newer = {
    ...fixture,
    id: source,
    detector_id: source,
    detector_name: "Более новый детектор",
    window_start: "2026-11-12T21:00:00.000Z",
    window_end: "2026-11-12T21:02:00.000Z",
  };
  const { request } = setup(fixture, true, [newer], fixture);
  expect(
    screen
      .getByRole("region", { name: "Таймлайн находок" })
      .getAttribute("aria-busy"),
  ).toBe("true");
  const table = screen.getByRole("region", { name: "Список находок" });
  expect(
    await within(table).findByText("Не удалось загрузить находки"),
  ).toBeTruthy();
  fireEvent.click(within(table).getByRole("button", { name: "Повторить" }));
  const timeline = screen.getByRole("region", { name: "Таймлайн находок" });
  const column = await within(timeline).findByRole("button", {
    name: /^Находка: Детектор А/,
  });
  fireEvent.click(column);
  await waitFor(() => {
    const row = document.getElementById("finding-row-" + id);
    expect(row?.getAttribute("data-highlighted")).toBe("true");
    expect(document.activeElement).toBe(row);
  });
  expect(
    request.mock.calls.some(
      ([path]) =>
        path.startsWith("/findings?") &&
        new URLSearchParams(path.split("?")[1]).get("anchor_id") === id,
    ),
  ).toBe(true);
  expect(
    screen.getByTitle("2").classList.contains("ant-pagination-item-active"),
  ).toBe(true);
  const link = await within(table).findByRole("button", {
    name: fixture.detector_name!,
  });
  fireEvent.click(link);
  const dialog = await screen.findByRole("dialog", {
    name: "Подробности находки",
  });
  expect(await within(dialog).findByText("1152921504606846977")).toBeTruthy();
  expect(
    within(dialog)
      .getAllByRole("heading")
      .map((heading) => heading.textContent),
  ).toEqual([
    "Степень аномальности",
    "Уверенность оценки",
    "Вклад признаков",
    "Значения признаков",
    expect.stringMatching(/^Как получена находка/),
    expect.stringMatching(/^Исходные события/),
  ]);
  expect(within(dialog).getByText("1152921504606846976.75")).toBeTruthy();
  expect(within(dialog).getByText("Запоздалая оценка")).toBeTruthy();
  const flow = within(dialog).getByRole("list", { name: "Поток данных" });
  expect(
    within(flow)
      .getAllByRole("listitem")
      .map((item) => item.textContent),
  ).toEqual(["ИсточникИсточник А", "АгрегатАгрегат А", "ДетекторДетектор А"]);
  expect(within(flow).queryByText("Основание времени")).toBeNull();
  expect(within(dialog).getByText("Основание времени")).toBeTruthy();
  expect(
    within(dialog).getByText(/Использован ускоренный прогрев/),
  ).toBeTruthy();
  const legend = within(dialog).getByRole("list", {
    name: "Доли вкладов признаков",
  });
  expect(legend.getAttribute("tabindex")).toBe("0");
  for (const [name, percentage] of [
    ["Первый", "75"],
    ["Второй", "25"],
    ["Третий", "0"],
  ])
    expect(within(legend).getByText(name).parentElement?.textContent).toContain(
      percentage,
    );
  const href = within(dialog)
    .getByRole("link", { name: "Перейти к событиям" })
    .getAttribute("href")!;
  expect(new URLSearchParams(href.split("?")[1]).get("timestamp_to")).toBe(end);
  fireEvent.click(within(dialog).getByRole("button", { name: /^Закрыть$/ }));
  expect(
    screen.getByTitle("2").classList.contains("ant-pagination-item-active"),
  ).toBe(true);
  vi.useFakeTimers();
  try {
    fireEvent.click(within(table).getByRole("button", { name: "На графике" }));
    expect(column.getAttribute("data-highlighted")).toBe("true");
    expect(document.activeElement).toBe(column);
    act(() => vi.advanceTimersByTime(1999));
    expect(column.getAttribute("data-highlighted")).toBe("true");
    act(() => vi.advanceTimersByTime(1));
    expect(column.getAttribute("data-highlighted")).toBeNull();
  } finally {
    vi.useRealTimers();
  }
});

it("describes filter fields with keyboard help and keeps their values editable after Escape", async () => {
  const { user } = setup(fixture, false);
  fireEvent.click(screen.getByRole("button", { name: /^Фильтры$/ }));
  const hint = await screen.findByRole("button", {
    name: "Справка: Начало периода",
  });
  hint.focus();
  await user.keyboard("{Enter}");
  expect(
    await screen.findByText(
      "Находки с началом окна в этот момент и позже попадают в выборку.",
    ),
  ).toBeTruthy();
  expect(hint.getAttribute("aria-expanded")).toBe("true");
  await user.keyboard("{Escape}");
  expect(hint.getAttribute("aria-expanded")).toBe("false");
  fireEvent.change(
    screen.getByRole("spinbutton", { name: "Степень аномальности, от" }),
    { target: { value: "0.9" } },
  );
  fireEvent.click(screen.getByRole("button", { name: "Применить" }));
  expect(
    await screen.findByText("Находок по выбранным фильтрам нет"),
  ).toBeTruthy();
});

it.each([
  [null, "В снимке нет полного набора вкладов RCF."],
  [0, "Все сохранённые вклады равны нулю."],
] as const)(
  "explains unavailable attribution (%s) and the missing range",
  async (attribution, label) => {
    await act(async () => {
      setup(
        {
          ...fixture,
          event_range: null,
          event_range_reason: "unavailable",
          expected_origin: "preceding_median",
          features: fixture.features.map((feature) => ({
            ...feature,
            attribution,
          })),
        },
        false,
      );
    });
    await act(async () => {
      fireEvent.click(
        screen.getByRole("button", { name: fixture.detector_name! }),
      );
    });
    const dialog = await screen.findByRole("dialog", {
      name: "Подробности находки",
    });
    expect(await within(dialog).findByText(label)).toBeTruthy();
    expect(
      within(dialog).queryByRole("list", { name: "Доли вкладов признаков" }),
    ).toBeNull();
    expect(
      within(dialog).getByText(/медиане предшествующих входов/),
    ).toBeTruthy();
    expect(
      within(dialog).getByText(/отсутствует источник или точный диапазон/),
    ).toBeTruthy();
    expect(
      within(dialog).queryByRole("link", { name: "Перейти к событиям" }),
    ).toBeNull();
  },
);
