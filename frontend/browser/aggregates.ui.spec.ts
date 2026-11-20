import { expect, test, type Locator, type Page } from "@playwright/test";
import { readFileSync } from "node:fs";
import { calendarInputValue } from "./aggregates.visual";

const config = process.env.AGGREGATE_BROWSER_CONFIG
  ? (JSON.parse(readFileSync(process.env.AGGREGATE_BROWSER_CONFIG, "utf8")) as {
      username: string;
      password: string;
      sources: string[];
      anchor: string;
    })
  : null;
if (!config && process.env.BROWSER_FIXTURES_REQUIRED)
  throw new Error("Required aggregate fixture missing");
test.skip(!config, "Uses synthetic fixtures from end-to-end --aggregate-only.");
test.setTimeout(90_000);
async function login(page: Page) {
  page.setDefaultTimeout(15_000);
  await page.goto("/aggregates");
  await page
    .getByRole("textbox", { name: "Имя пользователя" })
    .fill(config!.username);
  await page.getByLabel("Пароль").fill(config!.password);
  await page.getByRole("button", { name: "Войти", exact: true }).click();
  await expect(
    page.getByRole("button", { name: "Создать агрегат", exact: true }),
  ).toBeVisible();
}
// Capture only normal viewport content. Oversized budget ancestors are never
// resized, unclipped or stitched; each small target must fit the real scrollport.
type DiagnosticShots = { count: number; limit: number };
async function diagnosticVisibility(targets: Locator) {
  return targets.evaluateAll((elements) => {
    const rectangle = (box: DOMRect) => ({
      left: box.left,
      top: box.top,
      right: box.right,
      bottom: box.bottom,
      width: box.width,
      height: box.height,
    });
    const viewport = { width: innerWidth, height: innerHeight };
    const targets = elements.map((element, index) => ({
      index,
      rect: rectangle(element.getBoundingClientRect()),
    }));
    const port = document.querySelector<HTMLElement>(
      '[data-testid="workspace-content"]',
    );
    if (!port || !port.offsetWidth || !port.offsetHeight)
      return { viewport, scrollport: null, clip: null, targets, visible: [] };
    const frame = port.getBoundingClientRect();
    const scaleX = frame.width / port.offsetWidth;
    const scaleY = frame.height / port.offsetHeight;
    const clientLeft = frame.left + port.clientLeft * scaleX;
    const clientTop = frame.top + port.clientTop * scaleY;
    const left = Math.max(0, clientLeft);
    const top = Math.max(0, clientTop);
    const right = Math.min(innerWidth, clientLeft + port.clientWidth * scaleX);
    const bottom = Math.min(
      innerHeight,
      clientTop + port.clientHeight * scaleY,
    );
    const visible = targets.flatMap(({ index, rect: box }) => {
      return box.width > 0 &&
        box.height > 0 &&
        box.left >= left &&
        box.top >= top &&
        box.right <= right &&
        box.bottom <= bottom
        ? [index]
        : [];
    });
    return {
      viewport,
      scrollport: {
        rect: rectangle(frame),
        clientWidth: port.clientWidth,
        clientHeight: port.clientHeight,
        scrollTop: port.scrollTop,
        scrollLeft: port.scrollLeft,
        scrollHeight: port.scrollHeight,
        scrollWidth: port.scrollWidth,
        scaleX,
        scaleY,
      },
      clip: { left, top, right, bottom },
      targets,
      visible,
    };
  });
}
async function visibleDiagnosticTargets(targets: Locator): Promise<number[]> {
  return (await diagnosticVisibility(targets)).visible;
}
async function scrollDiagnosticTarget(target: Locator) {
  await target.evaluate((element) => {
    element.scrollIntoView({
      block: "start",
      inline: "nearest",
      behavior: "instant",
    });
    const port = document.querySelector<HTMLElement>(
      '[data-testid="workspace-content"]',
    );
    if (port)
      port.scrollTop = Math.max(
        0,
        Math.min(port.scrollTop - 8, port.scrollHeight - port.clientHeight),
      );
  });
  await expect(target).toBeVisible();
  await expect
    .poll(async () => {
      const state = await diagnosticVisibility(target);
      return state.visible.length === 1 && state.visible[0] === 0
        ? null
        : state;
    })
    .toBeNull();
}
async function diagnosticViewport(
  page: Page,
  target: Locator,
  name: string,
  shots: DiagnosticShots,
) {
  await scrollDiagnosticTarget(target);
  expect(++shots.count).toBeLessThanOrEqual(shots.limit);
  await page.screenshot({
    path: test.info().outputPath(`${name}.png`),
    fullPage: false,
    animations: "disabled",
  });
}
async function diagnosticParts(
  page: Page,
  targets: Locator,
  stem: string,
  part: string,
  shots: DiagnosticShots,
  overview = false,
) {
  const count = await targets.count();
  expect(count).toBeGreaterThan(0);
  expect(count).toBeLessThanOrEqual(part === "resources" ? 13 : 6);
  const covered = new Set<number>();
  for (let chunk = 0; chunk < count && covered.size < count; chunk++) {
    const index = Array.from({ length: count }, (_, i) => i).find(
      (i) => !covered.has(i),
    )!;
    const target = targets.nth(index);
    await scrollDiagnosticTarget(target);
    const visible = await visibleDiagnosticTargets(targets);
    expect(visible).toContain(index);
    const last = Array.from({ length: count }, (_, i) => i).every(
      (i) => covered.has(i) || visible.includes(i),
    );
    const suffix =
      part === "resources"
        ? chunk === 0
          ? last
            ? "resources-first-last"
            : "resources-first"
          : last
            ? "resources-last"
            : `resources-middle-${chunk}`
        : `${part}${chunk ? `-${chunk + 1}` : ""}`;
    expect(++shots.count).toBeLessThanOrEqual(shots.limit);
    const name = overview && chunk === 0 ? stem : `${stem}-${suffix}`;
    await page.screenshot({
      path: test.info().outputPath(`${name}.png`),
      fullPage: false,
      animations: "disabled",
    });
    for (const i of visible) {
      await expect(targets.nth(i)).toBeVisible();
      covered.add(i);
    }
  }
  expect(covered.size).toBe(count);
}

const MOCK = "00000000-0000-4000-8000-000000000041";
const rule = { features: [{ key: "events", name: "События", op: "count" }] };
test("shows measured budget diagnostics and a terminal uncomputed lookup without a zero graph", async ({
  page,
}) => {
  await login(page);
  const at = (milliseconds: number) =>
    new Date(Date.parse(config!.anchor) + milliseconds).toISOString();
  const policy = {
    policy_version: "window_execution_v1",
    soft_latency_seconds: 5,
    hard_budget_seconds: 0.8,
  };
  const execution = {
    ...policy,
    calculation_status: "uncomputed",
    calculation_reason: "execution_budget_exceeded",
    attempt_id: "00000000-0000-4000-8000-000000000049",
    origin: "online",
    window_index: 1,
    start_utc: at(1000),
    end_utc: at(2000),
    started_at: at(3000),
    execution_deadline_at: at(3800),
    measured_at: at(3900),
    measured_through: "finalization",
    elapsed_seconds: 0.9,
    queue_seconds: null,
    online_latency_seconds: null,
    soft_latency_exceeded: null,
    timings: {},
  };
  const successExecution = {
    ...execution,
    calculation_status: "computed",
    calculation_reason: null,
    attempt_id: "00000000-0000-4000-8000-000000000048",
    window_index: 2,
    start_utc: at(2000),
    end_utc: at(3000),
    started_at: at(4000),
    execution_deadline_at: at(4800),
    measured_at: at(4200),
    measured_through: "before_commit",
    elapsed_seconds: 0.2,
  };
  const computed = (index: number, value: number) => ({
    id: `00000000-0000-4000-8000-00000000005${index}`,
    aggregate_id: MOCK,
    kind: "working",
    origin: index ? "online" : "initial_history",
    window_index: index,
    calculation_status: "computed",
    calculation_reason: null,
    execution: index === 2 ? successExecution : null,
    start_utc: at(index * 1000),
    end_utc: at((index + 1) * 1000),
    scheduled_at: at((index + 1) * 1000),
    actual_started_at: at((index + 2) * 1000 + 100),
    actual_completed_at: at((index + 2) * 1000 + 200),
    close_not_before_at: index ? at((index + 2) * 1000) : null,
    snapshot_reference_at: at((index + 2) * 1000 + 100),
    payload_state: "present",
    snapshot_metadata: { delivery_coverage: "not_attested" },
    evidence_refs: {},
    event_count: value,
    event_time_min: at(index * 1000 + 500),
    event_time_max: at(index * 1000 + 500),
    features: [
      {
        key: "events",
        value,
        reason: null,
        valid_count: value,
        invalid_count: 0,
      },
    ],
    completeness: "complete",
    completeness_reason: "received_event_snapshot",
    time_basis: "event_time",
  });
  const terminal = {
    ...computed(1, 0),
    calculation_status: "uncomputed",
    calculation_reason: "execution_budget_exceeded",
    execution,
    actual_started_at: at(3000),
    actual_completed_at: at(3900),
    event_count: null,
    event_time_min: null,
    event_time_max: null,
    features: null,
    completeness: null,
    completeness_reason: null,
    snapshot_reference_at: null,
    snapshot_metadata: null,
    evidence_refs: null,
  };
  const aggregate = {
    ...card(),
    created_at: at(1000),
    bootstrap_at: at(1000),
    online_start_index: 1,
    execution_policy: policy,
    last_execution: successExecution,
    next_index: 3,
    next_scheduled_at: at(4000),
    next_close_not_before_at: at(5000),
  };
  let lookups = 0;
  await page.route(`**/api/v1/aggregates/${MOCK}**`, async (route) => {
    const url = route.request().url();
    if (url.endsWith("/historical-points")) {
      lookups++;
      return route.fulfill({ json: terminal });
    }
    if (url.includes("/points?"))
      return route.fulfill({
        json: { items: [computed(2, 3), terminal, computed(0, 1)], total: 3 },
      });
    if (route.request().method() !== "GET")
      throw new Error("Unexpected mutation in diagnostic UI scenario");
    return route.fulfill({ json: aggregate });
  });
  await page.goto(`/aggregates/${MOCK}`);
  const budget = page
    .getByRole("region", { name: "Оценка ресурсов", exact: true })
    .first();
  await expect(
    budget.getByRole("heading", { name: "Пороги", exact: true }),
  ).toBeVisible();
  await expect(
    budget.getByRole("heading", {
      name: "Последний фактический расчёт",
      exact: true,
    }),
  ).toBeVisible();
  await expect(budget.getByText("Рассчитано", { exact: true })).toBeVisible();
  await expect(
    budget.getByText(/Полная online-задержка.*Ещё не измерено/),
  ).toBeVisible();
  const graph = page.getByRole("region", {
    name: "График сохранённых точек",
    exact: true,
  });
  await expect(graph.locator('[data-layer="markers"] circle')).toHaveCount(2);
  await expect(graph.locator('[data-layer="connections"] line')).toHaveCount(0);
  await page
    .getByLabel("Начало сохранённого окна", { exact: true })
    .fill(calendarInputValue(at(1000)));
  await page
    .getByRole("button", { name: "Просмотреть сохранённое окно", exact: true })
    .click();
  await expect(
    page.getByText("Сохранённое окно загружено. Новый расчёт не выполнялся.", {
      exact: true,
    }),
  ).toBeVisible();
  await expect(graph.getByRole("img", { name: /^График / })).toHaveCount(0);
  await expect(graph.locator('[data-layer="markers"] circle')).toHaveCount(0);
  await expect(graph.locator('[data-layer="connections"] line')).toHaveCount(0);
  await expect(
    graph.getByRole("status").filter({
      hasText: /^Нет числовых значений для графика выбранного признака\./,
    }),
  ).toBeVisible();
  await expect(graph.getByText(/автоматического пересчёта нет/)).toBeVisible();
  await page
    .getByRole("button", { name: `Выбрать точку ${terminal.id}`, exact: true })
    .click();
  const detail = page.getByRole("region", {
    name: "Подробности выбранной точки",
    exact: true,
  });
  await expect(
    detail.getByText("Не рассчитано", { exact: true }).last(),
  ).toBeVisible();
  await expect(
    detail.getByText(
      /Значения, состав событий и опоздания для него недоступны/,
    ),
  ).toBeVisible();
  await expect(detail.getByText(/Диапазон ECS @timestamp/)).toHaveCount(0);
  for (const theme of ["light", "dark"]) {
    await page.getByRole("button", { name: /^Аккаунт:/ }).click();
    for (
      let i = 0;
      i < 3 &&
      (await page.locator("html").getAttribute("data-theme")) !== theme;
      i++
    )
      await page.getByTestId("theme-toggle").click();
    await expect(page.locator("html")).toHaveAttribute("data-theme", theme);
    await page.keyboard.press("Escape");
    for (const frame of [
      { width: 320, height: 720, zoom: "1" },
      { width: 2560, height: 720, zoom: "1" },
      { width: 1280, height: 360, zoom: "1" },
      { width: 800, height: 600, zoom: "2" },
    ]) {
      await page.setViewportSize({ width: frame.width, height: frame.height });
      await page.evaluate((zoom) => {
        document.documentElement.style.zoom = zoom;
      }, frame.zoom);
      const shots = { count: 0, limit: 10 };
      const stem = `diagnostics-budget-${theme}-${frame.width}-${frame.height}-zoom${frame.zoom}`;
      await diagnosticViewport(
        page,
        budget.getByRole("heading", {
          name: "Оценка ресурсов",
          level: 4,
          exact: true,
        }),
        stem,
        shots,
      );
      await diagnosticParts(
        page,
        budget
          .getByRole("region", {
            name: "Пороги расчёта",
            exact: true,
          })
          .locator(":scope > h5, :scope > p"),
        stem,
        "thresholds",
        shots,
      );
      await diagnosticParts(
        page,
        budget
          .getByRole("region", {
            name: "Последний фактический расчёт",
            exact: true,
          })
          .locator(":scope > h5, :scope > p"),
        stem,
        "lastactual",
        shots,
      );
      await diagnosticParts(
        page,
        detail.getByRole("status").locator(":scope > p"),
        `diagnostics-uncomputed-${theme}-${frame.width}-${frame.height}-zoom${frame.zoom}`,
        "details",
        shots,
        true,
      );
      expect(
        await page.evaluate(
          () => document.documentElement.scrollWidth <= innerWidth,
        ),
      ).toBe(true);
    }
  }
  await page.evaluate(() => {
    document.documentElement.style.zoom = "";
  });
  expect(lookups).toBe(1);
});
test("copy refuses deletion that would invalidate unchanged forward references", async ({
  page,
}) => {
  await login(page);
  for (const marker of ["auxiliary", "formula", "auxiliary-formula"]) {
    const document: { features: object[]; auxiliary?: object[] } = {
      features: [
        { key: "b", name: "B", op: "derive", expr: { ref: "a" } },
        { key: "a", name: "A", op: "count" },
      ],
    };
    const c = {
      key: "c",
      name: "C",
      op: marker === "auxiliary" ? "count" : "formula",
      ...(marker === "auxiliary" ? {} : { expr: { value: 1 } }),
    };
    if (marker === "formula") document.features.push(c);
    else document.auxiliary = [c];
    await page.route(`**/api/v1/aggregates/guard-${marker}/copy`, (route) =>
      route.fulfill({
        json: {
          source_id: config!.sources[0],
          schedule: {
            boundary: config!.anchor,
            timezone: "UTC",
            duration_seconds: 300,
          },
          aggregate_rule: document,
        },
      }),
    );
    await page.goto(`/aggregates/new?copy=guard-${marker}`);
    await page
      .getByLabel("Имя агрегата", { exact: true })
      .fill("Проверка удаления");
    const button = page.getByRole("button", {
      name: "Удалить признак c",
      exact: true,
    });
    await expect(
      page.getByRole("button", { name: "Инструкция для ИИ", exact: true }),
    ).toBeEnabled();
    await expect(
      page.getByText("Действие изменит правила ссылок", { exact: false }),
    ).toHaveCount(0);
    await button.click();
    await expect(page.getByRole("alert")).toContainText(
      "Сначала исправьте порядок",
    );
    await expect(button).toBeFocused();
    await expect(page.getByRole("dialog")).toHaveCount(0);
    await page.getByText("JSON", { exact: true }).click();
    const text = page.getByLabel("Полное правило JSON", { exact: true });
    expect(JSON.parse(await text.inputValue())).toEqual(document);
    const corrected = {
      ...document,
      features: [
        document.features[1],
        document.features[0],
        ...document.features.slice(2),
      ],
    };
    await text.fill(JSON.stringify(corrected));
    await page.getByText("Визуальный", { exact: true }).click();
    await expect(
      page.getByText("Сначала исправьте порядок", { exact: false }),
    ).toHaveCount(0);
    await button.click();
    await expect(button).toHaveCount(0);
    await page.getByText("JSON", { exact: true }).click();
    const result = JSON.parse(await text.inputValue());
    expect(result.features.map((f: { key: string }) => f.key)).toEqual([
      "a",
      "b",
    ]);
    expect(result.auxiliary).toBeUndefined();
    page.once("dialog", (dialog) => dialog.accept());
  }
});
test("saved card keeps rule and history readable while operator metadata retries fail and then recover", async ({
  page,
}) => {
  await login(page);
  let attempts = 0;
  let release!: () => void;
  await page.route("**/api/v1/aggregates/operators?*", async (route) => {
    attempts++;
    if (attempts <= 2)
      return route.fulfill({
        status: 503,
        json: { code: "internal_error", message: "private detail" },
      });
    await new Promise<void>((resolve) => {
      release = resolve;
    });
    await route.continue();
  });
  await page.route(`**/api/v1/aggregates/${MOCK}**`, (route) =>
    route.fulfill({
      json: route.request().url().includes("/points")
        ? { items: [], total: 0 }
        : card(),
    }),
  );
  await page.goto(`/aggregates/${MOCK}`);
  await expect(
    page.getByText("Не удалось загрузить каталог операций.", { exact: false }),
  ).toBeVisible();
  await expect(
    page.getByRole("region", { name: "Признаки", exact: true }),
  ).toContainText("События");
  await expect(
    page.getByRole("region", { name: "Сохранённые точки", exact: true }),
  ).toBeVisible();
  await expect(
    page.getByRole("button", { name: "Копировать JSON", exact: true }),
  ).toBeEnabled();
  const instruction = page.getByRole("button", {
    name: "Инструкция для ИИ",
    exact: true,
  });
  await expect(instruction).toBeDisabled();
  const retry = page.getByRole("button", {
    name: "Повторить загрузку операций",
  });
  await retry.click();
  await expect(retry).toBeVisible();
  expect(attempts).toBe(2);
  await retry.click();
  await expect(
    page.getByLabel("Загрузка каталога операций", { exact: true }),
  ).toBeVisible();
  await expect(instruction).toBeDisabled();
  expect(attempts).toBe(3);
  release();
  await expect(instruction).toBeEnabled();
  await expect(retry).toHaveCount(0);
  await expect(page.getByText("private detail")).toHaveCount(0);
});
function card() {
  return {
    id: MOCK,
    name: "Очень длинное имя агрегата ".repeat(12),
    source_id: config!.sources[0],
    source_name: "Тестовый источник",
    source_type: "kafka",
    aggregate_rule: rule,
    rule_hash: "a".repeat(64),
    schedule_id: "00000000-0000-4000-8000-000000000042",
    anchor_utc: config!.anchor,
    duration_seconds: 1,
    execution_policy: {
      policy_version: "window_execution_v1",
      soft_latency_seconds: 5,
      hard_budget_seconds: 0.8,
    },
    last_execution: null,
    original_boundary: config!.anchor,
    timezone: "UTC",
    created_at: new Date().toISOString(),
    created_by: "00000000-0000-4000-8000-000000000043",
    state: "active",
    archived_at: null,
    deleted_at: null,
    deleted_by: null,
    semantic_basis: "event_time",
    execution_mode: "current",
    execution_reason: null,
    bootstrap_at: config!.anchor,
    online_start_index: 0,
    paused: false,
    execution_generation: 0,
    active_cleanup_id: null,
    auto_cleanup: {
      policy_version: "model_history_v1",
      status: "scheduled",
      reason: null,
      checked_at: null,
      next_check_at: new Date().toISOString(),
      reserve_windows: null,
      keep_from_index: null,
      protected_from_index: null,
      auto_cleared_total: 0,
      history_revision: 0,
      last_cleared_at: null,
      retained_from_index: null,
      retained_to_index: null,
      protected_total: null,
      protected_by: [],
    },
    next_close_not_before_at: config!.anchor,
    diagnostics: {
      exclusions: {},
      late_count: 0,
      late_start: null,
      late_end: null,
      late_reason: null,
    },
    next_scheduled_at: config!.anchor,
    next_index: 1,
    skipped_ranges: [],
    lag_seconds: 0,
    last_error: null,
    last_error_at: null,
  };
}
test("retired snapshots retain their original basis and copy into a new event-time draft", async ({
  page,
}) => {
  await login(page);
  const retired = {
    ...card(),
    semantic_basis: "availability_v1",
    execution_mode: "retired",
    execution_reason: "aggregate_time_semantics_retired",
    auto_cleanup: {
      ...card().auto_cleanup,
      status: "inactive",
      reason: "aggregate_time_semantics_retired",
    },
    bootstrap_at: null,
    online_start_index: null,
    next_close_not_before_at: null,
  };
  let mutations = 0;
  await page.route(`**/api/v1/aggregates/${MOCK}**`, (route) => {
    if (route.request().method() !== "GET") {
      mutations++;
      return route.abort();
    }
    if (route.request().url().endsWith("/copy"))
      return route.fulfill({
        json: {
          source_id: retired.source_id,
          schedule: {
            boundary: retired.original_boundary,
            timezone: retired.timezone,
            duration_seconds: retired.duration_seconds,
          },
          aggregate_rule: retired.aggregate_rule,
        },
      });
    if (route.request().url().includes("/points"))
      return route.fulfill({
        json: {
          total: 1,
          items: [
            {
              id: "00000000-0000-4000-8000-000000000044",
              aggregate_id: MOCK,
              kind: "working",
              calculation_status: "computed",
              calculation_reason: null,
              execution: null,
              origin: "legacy",
              window_index: 0,
              start_utc: config!.anchor,
              end_utc: new Date(
                Date.parse(config!.anchor) + 1000,
              ).toISOString(),
              scheduled_at: config!.anchor,
              actual_started_at: config!.anchor,
              actual_completed_at: config!.anchor,
              snapshot_reference_at: null,
              close_not_before_at: null,
              payload_state: "present",
              snapshot_metadata: {},
              evidence_refs: {},
              event_count: 1,
              event_time_min: config!.anchor,
              event_time_max: config!.anchor,
              time_basis: "availability_at",
              completeness: "unknown",
              completeness_reason: "input_coverage_unavailable",
              features: [
                {
                  key: "events",
                  value: 1,
                  reason: null,
                  valid_count: 1,
                  invalid_count: 0,
                },
              ],
            },
          ],
        },
      });
    return route.fulfill({ json: retired });
  });
  await page.goto(`/aggregates/${MOCK}`);
  await expect(
    page.getByText(/Прежний расчёт остановлен\. Сохранённые результаты/),
  ).toBeVisible();
  const points = page.getByRole("region", {
    name: "Сохранённые точки",
    exact: true,
  });
  await expect(points).toContainText("времени доступности");
  for (const name of [
    "Пауза",
    "Продолжить",
    "Возобновить",
    "Архивировать",
    "Просмотреть сохранённое окно",
  ])
    await expect(page.getByRole("button", { name, exact: true })).toHaveCount(
      0,
    );
  await expect(
    page.getByRole("button", { name: "Копировать JSON", exact: true }),
  ).toBeEnabled();
  await page.getByRole("button", { name: "Копировать", exact: true }).click();
  await expect(page).toHaveURL(new RegExp(`/aggregates/new\\?copy=${MOCK}$`));
  await expect(
    page.getByLabel("Начало истории и сетка окон", { exact: true }),
  ).not.toHaveValue("");
  await expect(page.getByLabel("Имя агрегата", { exact: true })).toHaveValue(
    "",
  );
  expect(mutations).toBe(0);
});
test("responsive unified initial-history/online points, pagination, not-ready lookup, confirmation cancel and keyboard focus", async ({
  page,
}) => {
  await login(page);
  const at = (milliseconds: number) =>
    new Date(Date.parse(config!.anchor) + milliseconds).toISOString();
  const a = {
    ...card(),
    created_at: at(1000),
    bootstrap_at: at(1000),
    online_start_index: 1,
    next_index: 2,
    next_scheduled_at: at(3000),
    next_close_not_before_at: at(4000),
  };
  let mutations = 0;
  const offsets: string[] = [];
  const point = (i: number) => ({
    id: `00000000-0000-4000-8000-00000000000${i + 1}`,
    aggregate_id: MOCK,
    kind: "working",
    calculation_status: "computed",
    calculation_reason: null,
    execution: null,
    origin: i ? "online" : "initial_history",
    window_index: i,
    snapshot_reference_at: at(i ? 3100 : 1100),
    close_not_before_at: i ? at(3000) : null,
    payload_state: "present",
    snapshot_metadata: {
      quality_basis: "received_event_snapshot",
      delivery_coverage: "not_attested",
    },
    evidence_refs: {},
    start_utc: at(i * 1000),
    end_utc: at((i + 1) * 1000),
    scheduled_at: at((i + 1) * 1000),
    actual_started_at: at(i ? 3100 : 1100),
    actual_completed_at: at(i ? 3200 : 1200),
    event_count: 0,
    features: [
      {
        key: "events",
        value: i ? null : 0,
        reason: i ? "no_values" : null,
        valid_count: 0,
        invalid_count: 0,
      },
      {
        key: "fraction",
        value: i ? 0.2 : 0.1,
        reason: null,
        valid_count: 1,
        invalid_count: 0,
      },
      {
        key: "extreme",
        value: i ? 1e308 : -1e308,
        reason: null,
        valid_count: 1,
        invalid_count: 0,
      },
    ],
    completeness: i ? "interrupted" : "unknown",
    completeness_reason: "Нет доказанного покрытия",
    time_basis: "event_time",
    event_time_min: null,
    event_time_max: null,
  });
  await page.route(`**/api/v1/aggregates/${MOCK}**`, async (route) => {
    const url = route.request().url();
    if (url.includes("/points")) {
      expect(new URL(url).searchParams.get("limit")).toBe("3");
      offsets.push(new URL(url).searchParams.get("offset") || "0");
      return route.fulfill({
        json: {
          total: 51,
          items: [point(1), point(0)],
        },
      });
    }
    if (route.request().method() !== "GET") {
      mutations++;
      if (url.endsWith("/historical-points") && mutations === 2)
        return route.fulfill({ json: point(1) });
      return route.fulfill({
        status: 409,
        json: {
          code: "canonical_point_not_ready",
          message: "internal",
          request_id: "safe-request",
          details: null,
        },
      });
    }
    return route.fulfill({ json: a });
  });
  await page.goto(`/aggregates/${MOCK}`);
  await expect(
    page.getByRole("region", { name: "Сохранённые точки" }),
  ).toContainText("Полнота неизвестна");
  await expect(
    page.getByRole("img", { name: /^График .* загруженной страницы$/ }),
  ).toBeVisible();
  await page
    .getByRole("button", {
      name: "Выбрать точку 00000000-0000-4000-8000-000000000001",
    })
    .click();
  await expect(
    page.getByRole("region", { name: "Подробности выбранной точки" }),
  ).toBeVisible();
  await expect(
    page.getByRole("img", { name: /^График .* загруженной страницы$/ }),
  ).toBeVisible();
  await page
    .getByLabel("Пояс отображения", { exact: true })
    .fill("Europe/Berlin");
  await page
    .getByTestId("workspace-content")
    .getByRole("button", { name: "Архивировать", exact: true })
    .click();
  await page.keyboard.press("Escape");
  await expect(
    page
      .getByTestId("workspace-content")
      .getByRole("button", { name: "Архивировать", exact: true }),
  ).toBeFocused();
  expect(mutations).toBe(0);
  await page
    .getByRole("button", { name: "Удалить агрегат", exact: true })
    .click();
  await page
    .getByRole("dialog")
    .getByRole("button", { name: "Отмена", exact: true })
    .click();
  expect(mutations).toBe(0);
  await page.getByLabel("Пояс отображения", { exact: true }).fill("UTC");
  await page
    .getByLabel("Начало сохранённого окна", { exact: true })
    .fill(
      calendarInputValue(
        new Date(Date.parse(config!.anchor) + 1000).toISOString(),
      ),
    );
  await page
    .getByRole("button", { name: "Просмотреть сохранённое окно", exact: true })
    .click();
  await expect(
    page.getByText(
      "Сохранённый результат окна ещё не готов. Обновите состояние или выберите другое окно.",
      { exact: true },
    ),
  ).toBeVisible();
  await page
    .getByLabel("Пояс отображения", { exact: true })
    .fill("Europe/Berlin");
  await page.getByTitle("Вперед", { exact: true }).click();
  expect(offsets).toContain("3");
  await expect(
    page.getByRole("img", { name: /^График .* загруженной страницы$/ }),
  ).toBeVisible();
  await page
    .getByRole("button", { name: "Просмотреть сохранённое окно", exact: true })
    .click();
  await expect(
    page.getByText("Сохранённое окно загружено. Новый расчёт не выполнялся.", {
      exact: true,
    }),
  ).toBeVisible();
  expect(mutations).toBe(2);
  const featurePicker = page.getByRole("combobox", {
    name: "Признак графика",
    exact: true,
  });
  await featurePicker.focus();
  await featurePicker.press("ArrowDown");
  await page.getByText("fraction", { exact: true }).last().click();
  const plot = page.getByRole("img", {
    name: "График fraction выбранного окна",
    exact: true,
  });
  await expect(plot).toHaveAttribute("data-domain-low", "0.2");
  await expect(plot).toHaveAttribute("data-domain-high", "0.2");
  await expect(plot.locator('[data-layer="markers"] circle')).toHaveAttribute(
    "cy",
    "100",
  );
  await expect(plot.locator('[data-layer="connections"] line')).toHaveCount(0);
  await expect(plot).toHaveAttribute(
    "data-time-low",
    String(Date.parse(config!.anchor) + 1000),
  );
  await expect(plot).toHaveAttribute(
    "data-time-high",
    String(Date.parse(config!.anchor) + 2000),
  );
  await featurePicker.focus();
  await featurePicker.press("ArrowDown");
  await page.getByText("extreme", { exact: true }).last().click();
  const extremes = page.getByRole("img", {
    name: "График extreme выбранного окна",
    exact: true,
  });
  expect(
    await extremes.evaluate((el) =>
      [...el.querySelectorAll("circle,rect,line")].every((node) =>
        ["x", "y", "cx", "cy", "x1", "x2", "y1", "y2"].every(
          (attr) =>
            !node.hasAttribute(attr) ||
            Number.isFinite(Number(node.getAttribute(attr))),
        ),
      ),
    ),
  ).toBe(true);
  for (const theme of ["light", "dark"]) {
    await page.getByRole("button", { name: /^Аккаунт:/ }).click();
    for (
      let i = 0;
      i < 3 &&
      (await page.locator("html").getAttribute("data-theme")) !== theme;
      i++
    )
      await page.getByTestId("theme-toggle").click();
    await expect(page.locator("html")).toHaveAttribute("data-theme", theme);
    await page.keyboard.press("Escape");
    for (const width of [320, 1920, 2560]) {
      await page.setViewportSize({ width, height: 650 });
      const graph = page.getByRole("region", {
        name: "График сохранённых точек",
        exact: true,
      });
      await graph.scrollIntoViewIfNeeded();
      const marker = graph.locator('[data-layer="markers"] circle');
      const canvas = graph.locator("svg[data-time-low]");
      await expect
        .poll(async () => {
          const dot = await marker.boundingBox();
          const frame = await canvas.boundingBox();
          return (
            !!dot &&
            !!frame &&
            dot.x >= frame.x &&
            dot.x + dot.width <= frame.x + frame.width
          );
        })
        .toBe(true);
      await page.screenshot({
        path: test.info().outputPath(`graph-${theme}-${width}.png`),
        fullPage: false,
      });
      await expect
        .poll(() =>
          page.evaluate(
            () => document.documentElement.scrollWidth <= innerWidth,
          ),
        )
        .toBe(true);
      const table = page.getByRole("region", {
        name: "Таблица точек",
        exact: true,
      });
      const widths = await table
        .locator("table > thead")
        .first()
        .locator("th")
        .evaluateAll((nodes) =>
          nodes.map((n) => (n as HTMLElement).offsetWidth),
        );
      expect(widths[1]).toBeGreaterThanOrEqual(240);
      expect(widths[2]).toBeGreaterThanOrEqual(200);
      expect(widths[3]).toBeGreaterThanOrEqual(80);
      const labels = page.getByLabel("Границы временной оси", { exact: true });
      expect(
        await labels.evaluate((el) =>
          parseFloat(getComputedStyle(el).fontSize),
        ),
      ).toBeGreaterThanOrEqual(14);
      if (width === 320) {
        await table.focus();
        await table.press("ArrowRight");
        await expect
          .poll(() => table.evaluate((el) => el.scrollLeft))
          .toBeGreaterThan(0);
      }
      await expect
        .poll(() =>
          page
            .getByRole("region", { name: "Сохранённые точки" })
            .evaluate((el) => el.scrollWidth <= el.clientWidth),
        )
        .toBe(true);
      await page.screenshot({
        path: test.info().outputPath(`${theme}-${width}.png`),
        fullPage: true,
      });
    }
  }
  await page.setViewportSize({ width: 640, height: 700 });
  await page.evaluate(() => {
    document.documentElement.style.zoom = "2";
  });
  await expect
    .poll(() =>
      page.evaluate(() => document.documentElement.scrollWidth <= innerWidth),
    )
    .toBe(true);
  await expect(
    page.getByLabel("Границы временной оси", { exact: true }),
  ).toContainText("Europe/Berlin");
  await page.screenshot({
    path: test.info().outputPath("points-zoom-200.png"),
    fullPage: true,
  });
  await page.evaluate(() => {
    document.documentElement.style.zoom = "";
  });
  await page.getByLabel("Начало сохранённого окна", { exact: true }).fill("");
  await expect(
    page.getByRole("img", { name: /^График .* загруженной страницы$/ }),
  ).toBeVisible();
  await expect(
    page.getByRole("region", { name: "Подробности выбранной точки" }),
  ).toHaveCount(0);
  expect(mutations).toBe(2);
});

test("delayed copy blocks editing until ready, retries loading explicitly, and is ignored after leaving", async ({
  page,
}) => {
  await login(page);
  let releaseFirst!: () => void,
    releaseRetry!: () => void,
    releaseLate!: () => void;
  const first = new Promise<void>((r) => {
      releaseFirst = r;
    }),
    retry = new Promise<void>((r) => {
      releaseRetry = r;
    }),
    late = new Promise<void>((r) => {
      releaseLate = r;
    });
  let calls = 0;
  const input = {
    source_id: config!.sources[0],
    schedule: {
      boundary: config!.anchor,
      timezone: "UTC",
      duration_seconds: 1,
    },
    aggregate_rule: {
      features: [{ key: "original", name: "Оригинал", op: "count" }],
    },
  };
  await page.route("**/api/v1/aggregates/delayed/copy", async (route) => {
    calls++;
    if (calls === 1) {
      await first;
      return route.fulfill({
        status: 503,
        json: {
          code: "http_error",
          message: "unavailable",
          request_id: "copy-request",
          details: null,
        },
      });
    }
    await retry;
    return route.fulfill({ json: input });
  });
  await page.goto("/aggregates/new?copy=delayed");
  const loader = page.getByRole("region", { name: "Загрузка копии" });
  await expect(loader).toHaveAttribute("aria-busy", "true");
  await expect(page.getByLabel("Имя агрегата", { exact: true })).toHaveCount(0);
  await expect(
    page.getByLabel("Полное правило JSON", { exact: true }),
  ).toHaveCount(0);
  releaseFirst();
  await loader.getByRole("button", { name: "Повторить", exact: true }).click();
  await expect(loader).toHaveAttribute("aria-busy", "true");
  await expect(page.getByLabel("Имя агрегата", { exact: true })).toHaveCount(0);
  releaseRetry();
  await page.getByLabel("Имя агрегата", { exact: true }).fill("Мой ввод");
  await page.getByText("JSON", { exact: true }).click();
  await expect(
    page.getByLabel("Полное правило JSON", { exact: true }),
  ).toHaveValue(/original/);
  const edited =
    '{"features":[{"key":"edited","name":"Мой признак","op":"count"}]}';
  await page.getByLabel("Полное правило JSON", { exact: true }).fill(edited);
  await expect(
    page.getByLabel("Полное правило JSON", { exact: true }),
  ).toHaveValue(edited);
  expect(calls).toBe(2);
  await page.getByRole("link", { name: "← К агрегатам", exact: true }).click();
  await page
    .getByRole("dialog")
    .getByRole("button", { name: "Покинуть", exact: true })
    .click();
  await page.route("**/api/v1/aggregates/late/copy", async (route) => {
    await late;
    await route.fulfill({ json: input }).catch(() => {});
  });
  await page.goto("/aggregates/new?copy=late");
  await expect(
    page.getByRole("region", { name: "Загрузка копии" }),
  ).toBeVisible();
  await page.getByRole("link", { name: "← К агрегатам", exact: true }).click();
  releaseLate();
  await expect(page).toHaveURL(/\/aggregates$/);
  await expect(page.getByLabel("Имя агрегата", { exact: true })).toHaveCount(0);
});
test("resource errors retain JSON, clipboard fallback, invalid JSON stays editable, unknown creation retries exact body/key", async ({
  page,
}) => {
  await login(page);
  await page
    .getByRole("button", { name: "Создать агрегат", exact: true })
    .click();
  const vectorBlock = page.locator(
    'fieldset[aria-label="Выходной вектор и полное правило"]',
  );
  const diagnosticsBlock = page.locator(
    'fieldset[aria-label="Проверка и диагностика"]',
  );
  await expect(vectorBlock).toBeDisabled();
  await expect(diagnosticsBlock).toHaveAttribute("inert", "");
  for (const label of [
    "Имя агрегата",
    "Длительность, секунды",
    "Пояс отображения",
    "Начало истории и сетка окон",
  ])
    await expect(page.getByLabel(label, { exact: true })).toHaveAttribute(
      "aria-required",
      "true",
    );
  await expect(
    page.getByRole("combobox", { name: "Собственный источник", exact: true }),
  ).toHaveAttribute("aria-required", "true");
  await page.getByLabel("Имя агрегата", { exact: true }).fill("Проверка");
  await expect(vectorBlock).toBeDisabled();
  await page
    .getByRole("combobox", { name: "Собственный источник", exact: true })
    .click();
  const source = (
    await (await page.request.get("/api/v1/sources?limit=50&offset=0")).json()
  ).items.find((s: { id: string }) => s.id === config!.sources[0]);
  await page.getByTitle(source.name, { exact: true }).click();
  await page.getByLabel("Пояс отображения", { exact: true }).fill("UTC");
  await page.getByLabel("Длительность, секунды", { exact: true }).fill("1");
  await page
    .getByLabel("Начало истории и сетка окон", { exact: true })
    .fill(calendarInputValue(config!.anchor));
  await expect(vectorBlock).toBeEnabled();
  await expect(diagnosticsBlock).toBeEnabled();
  await page
    .getByLabel("Начало окна для расчёта", { exact: true })
    .fill(calendarInputValue(config!.anchor));
  await page.getByText("JSON", { exact: true }).click();
  await page.getByLabel("Полное правило JSON", { exact: true }).fill("{");
  await page.getByText("Визуальный", { exact: true }).click();
  await expect(
    page.getByLabel("Полное правило JSON", { exact: true }),
  ).toHaveValue("{");
  const text = JSON.stringify(rule);
  await page.getByLabel("Полное правило JSON", { exact: true }).fill(text);
  await page.evaluate(() =>
    Object.defineProperty(navigator, "clipboard", {
      value: { writeText: () => Promise.reject(new Error("denied")) },
      configurable: true,
    }),
  );
  await page
    .getByRole("button", { name: "Инструкция для ИИ", exact: true })
    .click();
  await expect(page.getByLabel("Текст для ручного копирования")).toBeVisible();
  await expect(page.getByText(/Буфер обмена недоступен/)).toBeVisible();
  await expect(page.getByRole("dialog")).toHaveCount(0);
  let fail = true;
  await page.route("**/api/v1/aggregates/validate", async (route) => {
    if (fail)
      return route.fulfill({
        status: 422,
        json: {
          code: "resource_budget_exceeded",
          message: "SQL internal",
          request_id: "request-test",
          details: { path: "features[0]", factor: "operator_cost" },
        },
      });
    return route.continue();
  });
  await page
    .getByRole("button", { name: "Проверить правило", exact: true })
    .click();
  await expect(page.getByText(/Расчёт превышает бюджет/)).toBeVisible();
  await expect(
    page.getByLabel("Полное правило JSON", { exact: true }),
  ).toHaveValue(text);
  for (const width of [320, 1920, 2560]) {
    await page.setViewportSize({ width, height: 650 });
    await expect
      .poll(() =>
        page.evaluate(() => document.documentElement.scrollWidth <= innerWidth),
      )
      .toBe(true);
    await page.screenshot({
      path: test.info().outputPath(`editor-${width}.png`),
      fullPage: true,
    });
  }
  await expect(page.getByText("SQL internal")).toHaveCount(0);
  await page
    .getByRole("button", { name: "Закрыть сообщение об ошибке", exact: true })
    .click();
  await expect(page.getByText(/Расчёт превышает бюджет/)).toHaveCount(0);
  await expect(
    page.getByLabel("Полное правило JSON", { exact: true }),
  ).toHaveValue(text);
  fail = false;
  await page
    .getByRole("button", { name: "Проверить правило", exact: true })
    .click();
  await expect(
    page.getByText("Правило и расписание проверены сервером.", { exact: true }),
  ).toBeVisible();
  const validationNotice = page
    .locator(".ant-alert")
    .filter({ hasText: "Правило и расписание проверены сервером." });
  await validationNotice
    .getByRole("button", { name: /close|закрыть/i })
    .click();
  await expect(
    page.getByText("Правило и расписание проверены сервером.", { exact: true }),
  ).toHaveCount(0);
  await expect(
    page.getByRole("button", { name: "Предварительный расчёт", exact: true }),
  ).toBeEnabled();
  await page
    .getByRole("button", { name: "Предварительный расчёт", exact: true })
    .click();
  await expect(
    page.getByRole("region", { name: "Предварительный результат" }),
  ).toBeVisible();
  const resourceBudget = page.getByRole("region", {
    name: "Оценка ресурсов",
    exact: true,
  });
  const resourceItems = resourceBudget
    .getByRole("list", { name: "Ресурсы расчёта", exact: true })
    .getByRole("listitem");
  await expect(resourceItems).toHaveCount(13);
  for (const value of await resourceItems.locator("strong").all())
    await expect(value).not.toBeEmpty();
  await expect(
    resourceBudget.getByText(/предварительный расчёт завершён/),
  ).toBeVisible();
  const editorViewport = page.viewportSize()!;
  try {
    for (const theme of ["light", "dark"]) {
      await page.getByRole("button", { name: /^Аккаунт:/ }).click();
      for (
        let i = 0;
        i < 3 &&
        (await page.locator("html").getAttribute("data-theme")) !== theme;
        i++
      )
        await page.getByTestId("theme-toggle").click();
      await expect(page.locator("html")).toHaveAttribute("data-theme", theme);
      await page.keyboard.press("Escape");
      for (const frame of [
        { width: 320, height: 720, zoom: "1" },
        { width: 2560, height: 720, zoom: "1" },
        { width: 1280, height: 360, zoom: "1" },
        { width: 800, height: 600, zoom: "2" },
      ]) {
        await page.setViewportSize({
          width: frame.width,
          height: frame.height,
        });
        await page.evaluate((zoom) => {
          document.documentElement.style.zoom = zoom;
        }, frame.zoom);
        const shots = { count: 0, limit: 16 };
        const stem = `diagnostics-resources-${theme}-${frame.width}-${frame.height}-zoom${frame.zoom}`;
        await expect(resourceItems).toHaveCount(13);
        for (const value of await resourceItems.locator("strong").all())
          await expect(value).not.toBeEmpty();
        await diagnosticViewport(
          page,
          resourceBudget.getByRole("heading", {
            name: "Оценка ресурсов",
            level: 4,
            exact: true,
          }),
          stem,
          shots,
        );
        await diagnosticParts(
          page,
          resourceBudget
            .getByRole("region", {
              name: "Пороги расчёта",
              exact: true,
            })
            .locator(":scope > h5, :scope > p"),
          stem,
          "thresholds",
          shots,
        );
        await diagnosticParts(page, resourceItems, stem, "resources", shots);
        await diagnosticParts(
          page,
          resourceBudget
            .getByRole("region", {
              name: "Последний фактический расчёт",
              exact: true,
            })
            .locator(":scope > h5, :scope > p"),
          stem,
          "lastactual",
          shots,
        );
        expect(
          await page.evaluate(
            () => document.documentElement.scrollWidth <= innerWidth,
          ),
        ).toBe(true);
      }
    }
  } finally {
    await page.evaluate(() => {
      document.documentElement.style.zoom = "";
    });
    await page.setViewportSize(editorViewport);
  }
  const attempts: { body: string | null; key: string | undefined }[] = [];
  let emptyCreate = false;
  await page.route("**/api/v1/aggregates", async (route) => {
    if (route.request().method() !== "POST") return route.continue();
    attempts.push({
      body: route.request().postData(),
      key: route.request().headers()["idempotency-key"],
    });
    if (emptyCreate) {
      if (attempts.length === 3)
        return route.fulfill({
          status: 201,
          body: "",
          headers: { "X-Request-ID": "empty-create" },
        });
      return route.fulfill({ status: 201, json: card() });
    }
    return route.abort("failed");
  });
  await page
    .getByRole("button", { name: "Создать агрегат", exact: true })
    .click();
  await expect(page.getByText(/Результат создания неизвестен/)).toBeVisible();
  await page
    .getByRole("button", {
      name: "Повторить создание с тем же ключом",
      exact: true,
    })
    .click();
  await expect.poll(() => attempts.length).toBe(2);
  expect(attempts[1]).toEqual(attempts[0]);
  await page.getByRole("link", { name: "← К агрегатам", exact: true }).click();
  await page
    .getByRole("dialog")
    .getByRole("button", { name: "Отмена", exact: true })
    .click();
  await expect(
    page.getByLabel("Полное правило JSON", { exact: true }),
  ).toHaveValue(text);
  emptyCreate = true;
  await page
    .getByRole("button", {
      name: "Повторить создание с тем же ключом",
      exact: true,
    })
    .click();
  await expect.poll(() => attempts.length).toBe(3);
  await expect(page.getByText(/Результат создания неизвестен/)).toBeVisible();
  await expect(
    page.getByLabel("Полное правило JSON", { exact: true }),
  ).toHaveValue(text);
  await expect(page).toHaveURL(/\/aggregates\/new$/);
  expect(attempts[2]).toEqual(attempts[0]);
  await page.route(`**/api/v1/aggregates/${MOCK}**`, (route) =>
    route.fulfill({
      json: route.request().url().includes("/points")
        ? { items: [], total: 0 }
        : card(),
    }),
  );
  await page
    .getByRole("button", {
      name: "Повторить создание с тем же ключом",
      exact: true,
    })
    .click();
  await expect(page).toHaveURL(new RegExp(`/aggregates/${MOCK}$`));
  expect(attempts[3]).toEqual(attempts[0]);
  await expect(
    page.getByText("Агрегат создан. Правило и расписание сохранены.", {
      exact: true,
    }),
  ).toBeVisible();
});
test("guest direct routes do not request aggregate data", async ({ page }) => {
  await login(page);
  const session = await (await page.request.get("/api/v1/auth/session")).json();
  await page.route("**/api/v1/auth/session", (route) =>
    route.fulfill({
      json: {
        ...session,
        permissions: ["events.read", "findings.read"],
        role: "Guest",
      },
    }),
  );
  let requests = 0;
  await page.route("**/api/v1/aggregates**", (route) => {
    requests++;
    return route.abort();
  });
  for (const path of [
    "/aggregates",
    "/aggregates/new",
    `/aggregates/${MOCK}`,
  ]) {
    await page.goto(path);
    await expect(
      page.getByText("Недостаточно прав для просмотра раздела", {
        exact: true,
      }),
    ).toBeVisible();
  }
  expect(requests).toBe(0);
});

for (const [status, code] of [
  [500, "internal_error"],
  [503, "aggregate_database_error"],
] as const) {
  test(`card ${status} ${code} stays unknown until a successful manual read`, async ({
    page,
  }) => {
    await login(page);
    const a = card();
    let mutations = 0,
      reads = 0;
    await page.route(`**/api/v1/aggregates/${MOCK}**`, async (route) => {
      if (route.request().url().includes("/points"))
        return route.fulfill({ json: { items: [], total: 0 } });
      if (route.request().method() === "DELETE") {
        mutations++;
        return route.fulfill({
          status,
          json: {
            code,
            message: "private database traceback",
            request_id: "mutation-request",
            details: null,
          },
        });
      }
      reads++;
      if (reads === 3 || reads === 4)
        return route.fulfill({
          status: reads === 3 ? 200 : 204,
          body: "",
          headers: { "X-Request-ID": `empty-read-${reads}` },
        });
      if (reads === 2)
        return route.fulfill({
          status: 503,
          json: {
            code: "aggregate_database_error",
            message: "private database traceback",
            request_id: "read-request",
            details: null,
          },
        });
      return route.fulfill({
        json: { ...a, state: reads > 2 ? "deleted" : "active" },
      });
    });
    await page.goto(`/aggregates/${MOCK}`);
    const remove = page.getByRole("button", {
      name: "Удалить агрегат",
      exact: true,
    });
    await remove.click();
    await page
      .getByRole("dialog")
      .getByRole("button", { name: "Удалить", exact: true })
      .click();
    const warning = page.getByText(/Результат изменения неизвестен/);
    await expect(warning).toBeVisible();
    await expect(page.getByRole("dialog")).toHaveCount(0);
    await expect(
      page
        .getByTestId("workspace-content")
        .getByText("Сервер временно недоступен.", { exact: true }),
    ).toBeVisible();
    await expect(page.getByText("private database traceback")).toHaveCount(0);
    await expect(remove).toBeDisabled();
    await expect(
      page.getByRole("button", { name: "Архивировать", exact: true }),
    ).toBeDisabled();
    await expect(
      page.getByRole("button", {
        name: "Просмотреть сохранённое окно",
        exact: true,
      }),
    ).toBeEnabled(); // Canonical lookup is read-only; unknown mutations stay fenced.
    expect(mutations).toBe(1);
    expect(reads).toBe(1);
    const refresh = page.getByRole("button", {
      name: "Обновить состояние",
      exact: true,
    });
    await refresh.click();
    await expect.poll(() => reads).toBe(2);
    await expect(
      page
        .getByTestId("workspace-content")
        .getByText("read-request", { exact: true }),
    ).toHaveCount(1);
    await expect(refresh).toBeEnabled();
    await expect(warning).toBeVisible();
    await expect(remove).toBeDisabled();
    for (const read of [3, 4]) {
      await refresh.click();
      await expect(
        page
          .getByTestId("workspace-content")
          .getByText(`empty-read-${read}`, { exact: true }),
      ).toHaveCount(1);
      await expect(warning).toBeVisible();
      await expect(remove).toBeDisabled();
      await expect(page.getByText(a.name, { exact: true })).toBeVisible();
      expect(mutations).toBe(1);
    }
    await refresh.click();
    await expect(
      page.getByText(
        "Агрегат удалён. Новые расчёты окончательно остановлены, сохранённая история доступна.",
        { exact: true },
      ),
    ).toBeVisible();
    await expect(warning).toHaveCount(0);
    await expect(remove).toHaveCount(0);
    expect(mutations).toBe(1);
    expect(reads).toBe(5);
  });
}

test("card refresh cannot cancel deletion and an earlier read cannot restore active state", async ({
  page,
}) => {
  await login(page);
  const a = card();
  let releaseRead!: () => void, releaseDelete!: () => void;
  const readGate = new Promise<void>((r) => {
      releaseRead = r;
    }),
    mutationGate = new Promise<void>((r) => {
      releaseDelete = r;
    });
  let reads = 0,
    deletes = 0;
  await page.route(`**/api/v1/aggregates/${MOCK}**`, async (route) => {
    if (route.request().url().includes("/points"))
      return route.fulfill({ json: { items: [], total: 0 } });
    if (route.request().method() === "DELETE") {
      deletes++;
      await mutationGate;
      return route.fulfill({ json: { ...a, state: "deleted" } });
    }
    reads++;
    if (reads > 1) await readGate;
    return route.fulfill({ json: a }).catch(() => {});
  });
  await page.goto(`/aggregates/${MOCK}`);
  const refresh = page.getByRole("button", {
    name: "Обновить состояние",
    exact: true,
  });
  await refresh.click();
  await expect.poll(() => reads).toBe(2);
  await page
    .getByRole("button", { name: "Удалить агрегат", exact: true })
    .click();
  await page
    .getByRole("dialog")
    .getByRole("button", { name: "Удалить", exact: true })
    .click();
  await expect.poll(() => deletes).toBe(1);
  await expect(refresh).toBeDisabled();
  await expect(
    page.getByRole("button", { name: "Обновить точки", exact: true }),
  ).toBeEnabled();
  releaseDelete();
  await expect(
    page.getByText(
      "Агрегат удалён. Новые расчёты окончательно остановлены, сохранённая история доступна.",
      { exact: true },
    ),
  ).toBeVisible();
  releaseRead();
  await expect(
    page.getByRole("button", { name: "Удалить агрегат", exact: true }),
  ).toHaveCount(0);
  await expect(refresh).toBeEnabled();
  expect(deletes).toBe(1);
});
