import { afterEach, expect, it, vi } from "vitest";
import {
  cleanup,
  act,
  fireEvent,
  render,
  screen,
  within,
} from "@testing-library/react";
import { runInAction } from "mobx";
import type { Aggregate, Point } from "../../app/api/aggregates";
import { parseAggregate } from "../../app/api/aggregateJson";
import { AppStores } from "../../app/stores/AppStores";
import { StoresProvider } from "../../app/providers/StoresProvider";
import { PointsView } from "./PointsView";

const owned: AppStores[] = [];
afterEach(() => {
  cleanup();
  owned.splice(0).forEach((stores) => stores.dispose());
});

function point(id: string, start: number, value = "1"): Point {
  const minute = (n: number) =>
    `2026-01-01T00:${String(n).padStart(2, "0")}:00Z`;
  return {
    id,
    kind: "working",
    start_utc: minute(start),
    end_utc: minute(start + 1),
    scheduled_at: minute(start + 1),
    actual_started_at: minute(start + 1),
    actual_completed_at: minute(start + 1),
    completeness: "complete",
    completeness_reason: "confirmed",
    time_basis: "availability_at",
    event_count: 1,
    event_time_min: null,
    event_time_max: null,
    features: [
      {
        key: "n",
        value: parseAggregate(value),
        reason: value === "null" ? "no_values" : null,
        valid_count: value === "null" ? 0 : 1,
        invalid_count: 0,
      },
    ],
  } as Point;
}

function setup(
  points: Point[],
  initial: {
    historicalPoint?: Point | null;
    selectionKey?: string;
    refresh?: number;
  } = {},
) {
  const stores = new AppStores();
  owned.push(stores);
  const request = vi.fn(async (path: string) => {
    const params = new URL(path, "https://test.invalid").searchParams;
    const offset = Number(params.get("offset"));
    const limit = Number(params.get("limit"));
    return {
      items: points.slice(offset, offset + limit),
      total: points.length,
    };
  });
  runInAction(() => {
    stores.session.data = {
      authenticated: true,
      user: { id: "u", username: "u", email: null, display_name: "U" },
      role: "admin",
      permissions: ["aggregates.read"],
      authentication_method: "local",
      csrf_token: "csrf",
    };
    stores.session.state = "authenticated";
    stores.api.request = request as typeof stores.api.request;
  });
  const aggregate = { id: "aggregate" } as Aggregate;
  const draw = (props: typeof initial) => (
    <StoresProvider stores={stores}>
      <PointsView aggregate={aggregate} zone="UTC" refresh={0} {...props} />
    </StoresProvider>
  );
  const view = render(draw(initial));
  return {
    request,
    rerender: (props: typeof initial) => view.rerender(draw(props)),
    unmount: view.unmount,
  };
}

it("requests three history rows per server page and keeps exact values in the table", async () => {
  const { request } = setup([
    point("a", 0, "9007199254740993"),
    point("b", 1),
    point("c", 2),
    point("d", 3),
    point("e", 4),
  ]);
  await screen.findByRole("button", { name: "Выбрать точку a" });
  expect(
    screen.getAllByRole("button", { name: /^Выбрать точку / }),
  ).toHaveLength(3);
  expect(request).toHaveBeenLastCalledWith(
    "/aggregates/aggregate/points?limit=3&offset=0",
    expect.anything(),
  );
  expect(screen.getByText("9007199254740993", { exact: true })).toBeTruthy();
  expect(screen.queryByRole("img", { name: /^График n / })).toBeNull();
  expect(
    screen.queryByRole("button", { name: "Показать окна текущей страницы" }),
  ).toBeNull();
  fireEvent.click(screen.getByTitle("2"));
  await screen.findByRole("button", { name: "Выбрать точку d" });
  expect(
    screen.getAllByRole("button", { name: /^Выбрать точку / }),
  ).toHaveLength(2);
  expect(screen.queryByRole("button", { name: "Выбрать точку a" })).toBeNull();
  expect(request).toHaveBeenLastCalledWith(
    "/aggregates/aggregate/points?limit=3&offset=3",
    expect.anything(),
  );
  expect(screen.queryByRole("img", { name: /^График n / })).toBeNull();
  fireEvent.click(screen.getByRole("button", { name: "Выбрать точку d" }));
  const details = await screen.findByRole("region", {
    name: "Подробности выбранной точки",
  });
  expect(details.textContent).toContain("2026-01-01T00:03:00Z");
  expect(details.textContent).toContain("2026-01-01T00:04:00Z");
  expect(screen.queryByRole("img", { name: /^График n / })).toBeNull();
});

it("summarizes three outputs and opens all exact values, reasons and counts through the remaining-features action", async () => {
  const compact = point("compact", 0);
  compact.features = [
    {
      key: "zero",
      value: parseAggregate("0"),
      reason: null,
      valid_count: 1,
      invalid_count: 0,
    },
    {
      key: "missing",
      value: null,
      reason: "no_values",
      valid_count: 0,
      invalid_count: 0,
    },
    {
      key: "decimal",
      value: parseAggregate("1.0"),
      reason: null,
      valid_count: 1,
      invalid_count: 0,
    },
    {
      key: "precise",
      value: parseAggregate("9007199254740993"),
      reason: null,
      valid_count: 2,
      invalid_count: 0,
    },
    {
      key: "other",
      value: parseAggregate("3"),
      reason: null,
      valid_count: 1,
      invalid_count: 0,
    },
  ];
  setup([compact]);
  await screen.findByRole("button", { name: "Выбрать точку compact" });
  const table = within(screen.getByRole("region", { name: "Таблица точек" }));
  expect(table.getByText("0", { exact: true })).toBeTruthy();
  expect(table.getByText("Нет значения", { exact: true })).toBeTruthy();
  expect(table.getByText("1.0", { exact: true })).toBeTruthy();
  expect(table.queryByText("9007199254740993", { exact: true })).toBeNull();
  fireEvent.click(
    table.getByRole("button", { name: "Ещё 2 признака точки compact" }),
  );
  const details = within(
    await screen.findByRole("region", { name: "Подробности выбранной точки" }),
  );
  expect(details.getByRole("cell", { name: "9007199254740993" })).toBeTruthy();
  const missing = within(details.getByRole("row", { name: /^missing / }));
  expect(missing.getByRole("cell", { name: "Нет значения" })).toBeTruthy();
  expect(
    missing.getByRole("cell", { name: /Нет подходящих значений/ }),
  ).toBeTruthy();
  const precise = within(details.getByRole("row", { name: /^precise / }));
  expect(precise.getByRole("cell", { name: "2" })).toBeTruthy();
  expect(screen.queryByRole("img", { name: /^График n / })).toBeNull();
});

it("uses the calculated window's real time axis and never invents a connecting line for one point", async () => {
  const historical = { ...point("historical", 10, "5"), kind: "historical" };
  setup([point("a", 0)], {
    selectionKey: historical.start_utc,
    historicalPoint: historical,
  });
  const svg = await screen.findByRole("img", {
    name: "График n выбранного окна",
  });
  const connections = svg.querySelector('[data-layer="connections"]')!;
  const markers = svg.querySelector('[data-layer="markers"]')!;
  expect(connections.querySelectorAll("line")).toHaveLength(0);
  expect(markers.querySelectorAll("rect")).toHaveLength(1);
  expect(
    connections.compareDocumentPosition(markers) &
      Node.DOCUMENT_POSITION_FOLLOWING,
  ).toBeTruthy();
  expect(svg.getAttribute("data-time-low")).toBe(
    String(Date.parse(historical.start_utc)),
  );
  expect(svg.getAttribute("data-time-high")).toBe(
    String(Date.parse(historical.end_utc)),
  );
  expect(svg.textContent).toContain("00:11:00");
  expect(svg.textContent).toContain("2026-01-01");
});

it("keeps a null historical output explicit without replacing it with a zero-valued marker", async () => {
  const historical = { ...point("missing", 10, "null"), kind: "historical" };
  setup([point("a", 0)], {
    selectionKey: historical.start_utc,
    historicalPoint: historical,
  });
  const graph = await screen.findByRole("region", {
    name: "График сохранённых точек",
  });
  expect(
    await within(graph).findByRole("cell", { name: "Нет значения" }),
  ).toBeTruthy();
  expect(within(graph).queryByRole("img", { name: /^График n / })).toBeNull();
  expect(graph.textContent).toContain(historical.start_utc);
  expect(graph.textContent).toContain(historical.end_utc);
});

it("labels the half-second tick of a one-second window without repeating a whole-second label", async () => {
  const historical = {
    ...point("short", 0),
    kind: "historical",
    end_utc: "2026-01-01T00:00:01Z",
  };
  setup([point("a", 2)], {
    selectionKey: historical.start_utc,
    historicalPoint: historical,
  });
  const svg = await screen.findByRole("img", {
    name: "График n выбранного окна",
  });
  expect(svg.querySelector("text")).toBeNull();
  expect(screen.getByLabelText("Подписи временной оси").textContent).toContain(
    "00:00:00.500",
  );
});

it("keeps the single point inside a narrow canvas and updates its physical geometry on resize", async () => {
  let watcher: {
    target: Element;
    callback: ResizeObserverCallback;
    observer: ResizeObserver;
  } | null = null;
  const currentWatcher = () => {
    expect(watcher).not.toBeNull();
    return watcher!;
  };
  class PlotResizeObserver {
    constructor(private callback: ResizeObserverCallback) {}
    observe(target: Element) {
      if (target.getAttribute("aria-label") === "Временная шкала графика") {
        watcher = {
          target,
          callback: this.callback,
          observer: this as unknown as ResizeObserver,
        };
      }
    }
    unobserve() {}
    disconnect = vi.fn();
  }
  vi.stubGlobal("ResizeObserver", PlotResizeObserver);
  try {
    const historical = { ...point("historical", 10, "5"), kind: "historical" };
    const { unmount } = setup([point("a", 0)], {
      selectionKey: historical.start_utc,
      historicalPoint: historical,
    });
    const svg = await screen.findByRole("img", {
      name: "График n выбранного окна",
    });
    const resize = (width: number) => {
      const active = currentWatcher();
      act(() =>
        active.callback(
          [
            {
              target: active.target,
              contentRect: { width },
            } as ResizeObserverEntry,
          ],
          active.observer,
        ),
      );
    };
    for (const width of [230, 420]) {
      resize(width);
      expect(svg.getAttribute("viewBox")).toBe(`0 0 ${width} 220`);
      const marker = svg.querySelector('[data-layer="markers"] rect')!;
      const left = Number(marker.getAttribute("x"));
      const right = left + Number(marker.getAttribute("width"));
      expect(left).toBeGreaterThanOrEqual(0);
      expect(right).toBeLessThanOrEqual(width);
      expect(Number(marker.getAttribute("width"))).toBe(10);
    }
    const active = currentWatcher();
    unmount();
    expect(active.observer.disconnect).toHaveBeenCalled();
  } finally {
    vi.unstubAllGlobals();
  }
});

it("shows the returned historical window and clears it when its date or result is cleared", async () => {
  const historical = { ...point("historical", 10, "5"), kind: "historical" };
  const { rerender } = setup(
    [point("a", 0), point("b", 1), point("c", 2), point("d", 3)],
    { selectionKey: historical.start_utc },
  );
  await screen.findByRole("button", { name: "Выбрать точку a" });
  expect(screen.queryByRole("img", { name: /^График n / })).toBeNull();
  rerender({ selectionKey: historical.start_utc, historicalPoint: historical });
  await screen.findByRole("img", { name: "График n выбранного окна" });
  expect(screen.getByLabelText("Границы временной оси").textContent).toContain(
    historical.start_utc,
  );
  expect(screen.getByLabelText("Границы временной оси").textContent).toContain(
    historical.end_utc,
  );
  rerender({
    selectionKey: historical.start_utc,
    historicalPoint: historical,
    refresh: 1,
  });
  await screen.findByRole("img", { name: "График n выбранного окна" });
  expect(screen.getByLabelText("Границы временной оси").textContent).toContain(
    historical.start_utc,
  );
  fireEvent.click(screen.getByTitle("2"));
  await screen.findByRole("button", { name: "Выбрать точку d" });
  expect(
    screen.getByRole("img", { name: "График n выбранного окна" }),
  ).toBeTruthy();
  expect(screen.getByLabelText("Границы временной оси").textContent).toContain(
    historical.start_utc,
  );
  rerender({
    selectionKey: "2026-01-01T00:12:00Z",
    historicalPoint: historical,
  });
  expect(screen.queryByRole("img", { name: /^График n / })).toBeNull();
  rerender({ selectionKey: "", historicalPoint: historical });
  expect(screen.queryByRole("img", { name: /^График n / })).toBeNull();
  expect(
    screen.queryByRole("region", { name: "Подробности выбранной точки" }),
  ).toBeNull();
  rerender({ selectionKey: "", historicalPoint: { ...historical } });
  expect(screen.queryByRole("img", { name: /^График n / })).toBeNull();
  const next = { ...point("next", 12, "7"), kind: "historical" };
  rerender({ selectionKey: next.start_utc, historicalPoint: null });
  expect(screen.queryByRole("img", { name: /^График n / })).toBeNull();
  rerender({ selectionKey: next.start_utc, historicalPoint: next });
  await screen.findByRole("img", { name: "График n выбранного окна" });
  expect(screen.getByLabelText("Границы временной оси").textContent).toContain(
    next.start_utc,
  );
  expect(
    screen.getByLabelText("Границы временной оси").textContent,
  ).not.toContain(historical.start_utc);
  rerender({ selectionKey: next.start_utc, historicalPoint: null });
  expect(screen.queryByRole("img", { name: /^График n / })).toBeNull();
});
