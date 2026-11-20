import { readFileSync, renameSync, writeFileSync } from "node:fs";
import { randomUUID } from "node:crypto";
import { expect, test, type Page } from "@playwright/test";
import { Deadline, withPhaseEnd } from "./deadline";
const path = process.env.DETECTOR_BROWSER_CONFIG;
if (!path)
  throw new Error(
    "Detector live requires the isolated browser profile fixture",
  );
const config = JSON.parse(readFileSync(path, "utf8")) as {
  username: string;
  guest: string;
  password: string;
  aggregate: string;
  source: string;
  anchor: string;
  runner_timeline?: Record<string, unknown>;
  auto_cleanup: {
    run_id: string;
    aggregate: string;
    source: string;
    anchor: string;
    seed_windows: number;
    old_point_id: string;
    old_window_index: number;
    old_payloads: number;
    old_snapshots: number;
    old_point: { completeness: string; payload_state: string };
    clock_at: string;
    clock_file: string;
  };
};
if (!config.aggregate || !config.username || !config.guest)
  throw new Error("Incomplete detector live fixture");
test.setTimeout(180_000);
const precedingTests: {
  title: string;
  duration_observed_ms: number;
  status: string | undefined;
}[] = [];
// eslint-disable-next-line no-empty-pattern -- Playwright requires a fixture pattern; no fixture is requested.
test.afterEach(({}, info) => {
  // Public duration as observed in this hook; final reporter duration can be longer.
  const item = {
    title: info.title,
    duration_observed_ms: info.duration,
    status: info.status,
  };
  if (precedingTests.length < 6) precedingTests.push(item);
  try {
    console.log("Detector live framework duration: " + JSON.stringify(item));
  } catch {
    // Metadata must not change the test outcome.
  }
});
async function login(page: Page, guest = false) {
  const response = await page.goto("/detectors");
  expect(response?.ok()).toBeTruthy();
  await page
    .getByRole("textbox", { name: "Имя пользователя" })
    .fill(guest ? config.guest : config.username);
  await page.getByLabel("Пароль", { exact: true }).fill(config.password);
  await page.getByRole("button", { name: "Войти", exact: true }).click();
  await expect(page).toHaveURL(/\/detectors$/);
  if (!guest)
    await expect(
      page.getByRole("button", { name: "Создать детектор", exact: true }),
    ).toBeVisible();
}
async function create(
  page: Page,
  name: string,
  accelerated = false,
  aggregateName = "Браузерный агрегат",
  sample = "8",
  expectedSource = config.source,
) {
  await page.goto("/detectors/new");
  const combo = page.getByRole("combobox", { name: "Активный агрегат" });
  const search = page.waitForResponse((response) => {
    const url = new URL(response.url());
    return (
      url.pathname === "/api/v1/aggregates" &&
      url.searchParams.get("q") === aggregateName &&
      url.searchParams.get("limit") === "10" &&
      url.searchParams.get("state") === "active"
    );
  });
  await combo.fill(aggregateName);
  const searched = await search;
  expect(searched.ok()).toBeTruthy();
  const chosen = (await searched.json()).items.find(
    (item: { name: string }) => item.name === aggregateName,
  );
  expect(chosen).toBeTruthy();
  expect(chosen.semantic_basis).toBe("event_time");
  expect(chosen.execution_mode).toBe("current");
  expect(chosen.paused).toBe(false);
  expect(chosen.source_id).toBe(expectedSource);
  if (aggregateName === "Браузерный агрегат")
    expect(chosen.id).toBe(config.aggregate);
  await page
    .locator(".ant-select-dropdown:visible")
    .getByTitle(aggregateName, { exact: true })
    .click();
  await expect(
    page.getByRole("region", { name: "Выбранный агрегат" }),
  ).toContainText(aggregateName);
  await page.getByLabel("Имя детектора", { exact: true }).fill(name);
  if (accelerated) {
    await page.getByText("Ускоренный прогрев", { exact: true }).click();
    await page.getByLabel("Шаг прогрева, секунды", { exact: true }).fill("1");
  }
  await page.getByText("Дополнительные настройки RCF", { exact: true }).click();
  await page.getByLabel("Число деревьев", { exact: true }).fill("20");
  await page.getByLabel("Размер выборки", { exact: true }).fill(sample);
  await page.getByLabel("Порог начала оценки", { exact: true }).fill(sample);
  await page
    .getByRole("button", { name: "Создать без запуска", exact: true })
    .click();
  await expect(page).toHaveURL(/\/detectors\/[0-9a-f-]{36}$/);
  const created = page.url().split("/").at(-1)!;
  const saved = await card(page, created);
  expect(saved.aggregate_id).toBe(chosen.id);
  expect(saved.source_id).toBe(chosen.source_id);
  return created;
}
async function card(page: Page, id: string, budget?: Deadline) {
  const response = await page.request.get(
    `/api/v1/detectors/${id}`,
    budget === undefined ? undefined : { timeout: budget.remaining() },
  );
  budget?.remaining();
  expect(response.ok()).toBeTruthy();
  const result = await response.json();
  budget?.remaining();
  expect(result.aggregate_semantic_basis).toBe("event_time");
  expect(result.aggregate_execution_mode).toBe("current");
  expect(typeof result.aggregate_paused).toBe("boolean");
  if (result.next_scheduled_at !== null) {
    expect(result.next_close_not_before_at).not.toBeNull();
    expect(
      Date.parse(result.next_close_not_before_at) -
        Date.parse(result.next_scheduled_at),
    ).toBe(Math.min(result.duration_seconds, 300) * 1000);
  }
  for (const window of [result.last_consumed, result.last_evaluated]) {
    if (window === null) continue;
    expect(window.time_basis).toBe("event_time");
    expect(window.origin).toBe("online");
    expect(Date.parse(window.scheduled_at)).toBe(Date.parse(window.end_utc));
    expect(
      Date.parse(window.close_not_before_at) - Date.parse(window.end_utc),
    ).toBe(Math.min(result.duration_seconds, 300) * 1000);
  }
  return result;
}
test("real creation, ordinary warmup, durable stop/resume and independent models", async ({
  page,
}) => {
  await login(page);
  const ordinary = await create(page, "Обычный браузерный детектор");
  expect((await card(page, ordinary)).execution_state).toBe("created");
  await page.reload();
  await expect(
    page.getByRole("button", { name: "Запустить", exact: true }),
  ).toBeVisible();
  await page.getByRole("button", { name: "Запустить", exact: true }).click();
  await expect
    .poll(async () => (await card(page, ordinary)).execution_state, {
      timeout: 45_000,
    })
    .toBe("running");
  await expect(
    page.getByText("Исполнение разрешено", { exact: true }),
  ).toBeVisible({ timeout: 10_000 });
  for (const [scheme, width] of [
    ["light", 1280],
    ["dark", 320],
  ] as const) {
    await page.emulateMedia({ colorScheme: scheme });
    await page.setViewportSize({ width, height: 720 });
    await expect(page.locator("html")).toHaveAttribute("data-theme", scheme);
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth,
      ),
    ).toBe(true);
    await page.screenshot({
      path: test.info().outputPath(`detector-live-${scheme}.png`),
      fullPage: true,
      animations: "disabled",
    });
  }
  await page.setViewportSize({ width: 1280, height: 720 });
  await page.emulateMedia({ colorScheme: "light" });
  const accelerated = await create(page, "Второй независимый детектор", false);
  expect((await card(page, accelerated)).accelerated_used).toBe(false);
  await expect(
    page.getByRole("img", { name: "Использован ускоренный прогрев" }),
  ).toHaveCount(0);
  await page.getByRole("button", { name: "Запустить", exact: true }).click();
  await expect
    .poll(async () => (await card(page, accelerated)).execution_state, {
      timeout: 45_000,
    })
    .toBe("running");
  const warm = await card(page, accelerated);
  expect(warm.step_seconds).toBe(2);
  expect(warm.duration_seconds).toBe(2);
  expect(warm.warmup.eligible_coverage_seconds).toBeGreaterThanOrEqual(32);
  expect(warm.accelerated_used).toBe(false);
  await page.reload();
  await expect(
    page.getByRole("img", { name: "Использован ускоренный прогрев" }),
  ).toHaveCount(0);
  await page.getByRole("button", { name: "Остановить", exact: true }).click();
  await page
    .getByRole("dialog")
    .getByRole("button", { name: "Отмена", exact: true })
    .click();
  await expect(page.getByRole("dialog")).not.toBeVisible();
  expect((await card(page, accelerated)).execution_state).toBe("running");
  await page.getByRole("button", { name: "Остановить", exact: true }).click();
  await page
    .getByRole("dialog")
    .getByRole("button", { name: "Остановить", exact: true })
    .click();
  await expect
    .poll(async () => (await card(page, accelerated)).execution_state)
    .toBe("stopped");
  const stopped = await card(page, accelerated);
  const cutoff = stopped.warmup.cutoff;
  const consumed = stopped.last_consumed;
  // Wait for actual ready aggregate points, rather than assuming elapsed time is evidence.
  await expect
    .poll(
      async () => {
        const r = await page.request.get(
          `/api/v1/aggregates/${config.aggregate}/points?limit=50&offset=0`,
        );
        return (await r.json()).items.filter(
          (p: {
            kind: string;
            end_utc: string;
            completeness: string;
            time_basis: string;
            origin: string;
          }) =>
            p.kind === "working" &&
            p.time_basis === "event_time" &&
            p.origin === "online" &&
            p.completeness === "complete" &&
            p.end_utc > (consumed?.end_utc ?? cutoff),
        ).length;
      },
      { timeout: 30_000 },
    )
    .toBeGreaterThan(1);
  expect((await card(page, accelerated)).last_consumed).toEqual(consumed);
  expect((await card(page, ordinary)).execution_state).toBe("running");
  await page.getByRole("button", { name: "Запустить", exact: true }).click();
  await expect
    .poll(async () => (await card(page, accelerated)).execution_state, {
      timeout: 30_000,
    })
    .toBe("running");
  await expect
    .poll(async () => (await card(page, accelerated)).last_consumed?.end_utc, {
      timeout: 30_000,
    })
    .not.toBe(consumed?.end_utc);
  const resumed = await card(page, accelerated);
  expect(resumed.warmup.cutoff).toBe(cutoff);
  expect(resumed.recovery.reason).toBe("manual_resume");
  expect(resumed.accelerated_used).toBe(false);
  await page.goto("/detectors");
  await expect(
    page.getByRole("link", {
      name: "Второй независимый детектор",
      exact: true,
    }),
  ).toBeVisible();
  await page.reload();
  await expect(
    page.getByRole("link", {
      name: "Обычный браузерный детектор",
      exact: true,
    }),
  ).toBeVisible();
});
test("real overlapping accelerated warmup retains provenance through stop and reload", async ({
  page,
}) => {
  await login(page);
  const identity = await create(page, "Ускоренный браузерный детектор", true);
  expect((await card(page, identity)).accelerated_used).toBe(false);
  await expect(
    page.getByRole("img", { name: "Использован ускоренный прогрев" }),
  ).toHaveCount(0);
  await page.getByRole("button", { name: "Запустить", exact: true }).click();
  await expect
    .poll(async () => (await card(page, identity)).execution_state, {
      timeout: 45_000,
    })
    .toBe("running");
  const warm = await card(page, identity);
  expect(warm.step_seconds).toBe(1);
  expect(warm.duration_seconds).toBe(2);
  expect(warm.warmup.eligible_coverage_seconds).toBeGreaterThanOrEqual(16);
  expect(warm.accelerated_used).toBe(true);
  await page.reload();
  await expect(
    page.getByRole("img", { name: "Использован ускоренный прогрев" }),
  ).toBeVisible();
  await page.getByRole("button", { name: "Остановить", exact: true }).click();
  await page
    .getByRole("dialog")
    .getByRole("button", { name: "Остановить", exact: true })
    .click();
  await expect
    .poll(async () => (await card(page, identity)).execution_state)
    .toBe("stopped");
  await page.reload();
  await expect(
    page.getByRole("img", { name: "Использован ускоренный прогрев" }),
  ).toBeVisible();
});
test("actual guest direct routes never request detector or aggregate data", async ({
  page,
}) => {
  const forbidden: string[] = [];
  page.on("request", (r) => {
    if (/\/api\/v1\/(detectors|aggregates)(\/|\?)/.test(r.url()))
      forbidden.push(r.url());
  });
  await login(page, true);
  await expect(
    page.getByText("Недостаточно прав для просмотра раздела", { exact: true }),
  ).toBeVisible();
  await expect(
    page.getByRole("link", { name: "Детекторы", exact: true }),
  ).toHaveCount(0);
  await page.goto("/detectors/new");
  await expect(
    page.getByText("Недостаточно прав для просмотра раздела", { exact: true }),
  ).toBeVisible();
  await page.goto(`/detectors/${config.aggregate}`);
  await expect(
    page.getByText("Недостаточно прав для просмотра раздела", { exact: true }),
  ).toBeVisible();
  expect(forbidden).toEqual([]);
});
test("insufficient history stays failed until a new explicit start", async ({
  page,
}) => {
  await login(page);
  const session = await (await page.request.get("/api/v1/auth/session")).json();
  const headers = { "X-CSRF-Token": session.csrf_token };
  let anchorSeconds = Math.floor(Date.now() / 1000) - 2;
  if (anchorSeconds % 2 === Math.floor(Date.parse(config.anchor) / 1000) % 2)
    anchorSeconds--;
  const anchor = new Date(anchorSeconds * 1000).toISOString();
  const body = {
    source_id: config.source,
    aggregate_rule: {
      features: [{ key: "n", name: "Число событий", op: "count" }],
    },
    schedule: { boundary: anchor, timezone: "UTC", duration_seconds: 2 },
  };
  const checked = await page.request.post("/api/v1/aggregates/validate", {
    headers,
    data: body,
  });
  expect(checked.ok()).toBeTruthy();
  const preview = {
    ...body,
    window_start_utc: anchor,
    time_basis: "event_time",
  };
  expect(
    (
      await page.request.post("/api/v1/aggregates/preview", {
        headers,
        data: preview,
      })
    ).ok(),
  ).toBeTruthy();
  const aggregate = await page.request.post("/api/v1/aggregates", {
    headers: { ...headers, "Idempotency-Key": crypto.randomUUID() },
    data: {
      ...preview,
      name: "Новая история браузера",
      rule_hash: (await checked.json()).rule_hash,
    },
  });
  expect(aggregate.ok()).toBeTruthy();
  const aid = (await aggregate.json()).id;
  const detector = await create(
    page,
    "Ожидание ручного повтора",
    false,
    "Новая история браузера",
    "8",
  );
  await page.getByRole("button", { name: "Запустить", exact: true }).click();
  await expect
    .poll(async () => (await card(page, detector)).execution_state, {
      timeout: 30_000,
    })
    .toBe("failed");
  await page
    .getByRole("button", { name: "Обновить состояние", exact: true })
    .click();
  await expect(page.getByText(/Недостаточно истории/).first()).toBeVisible();
  const failed = await card(page, detector);
  await expect
    .poll(
      async () => {
        const r = await page.request.get(
          `/api/v1/aggregates/${aid}/points?limit=50&offset=0`,
        );
        const points = (await r.json()).items;
        let consecutive = 0;
        let previous: number | undefined;
        for (const point of points) {
          if (
            point.kind !== "working" ||
            point.time_basis !== "event_time" ||
            point.origin !== "online" ||
            point.completeness !== "complete" ||
            (previous !== undefined && point.window_index !== previous - 1)
          )
            break;
          previous = point.window_index;
          consecutive++;
        }
        return consecutive;
      },
      { timeout: 60_000 },
    )
    .toBeGreaterThanOrEqual(18);
  const still = await card(page, detector);
  expect(still.execution_state).toBe("failed");
  expect(still.start_request_id).toBe(failed.start_request_id);
  await page.getByRole("button", { name: "Запустить", exact: true }).click();
  await expect
    .poll(async () => (await card(page, detector)).execution_state, {
      timeout: 40_000,
    })
    .toBe("running");
  expect((await card(page, detector)).start_request_id).not.toBe(
    failed.start_request_id,
  );
});

test("real aggregate pause catches up and cleanup protects every detector then permanently clears payload", async ({
  page,
}) => {
  await login(page);
  const first = await create(page, "Защита очистки — модель A");
  await page.getByRole("button", { name: "Запустить", exact: true }).click();
  await expect
    .poll(async () => (await card(page, first)).execution_state, {
      timeout: 45_000,
    })
    .toBe("running");
  const second = await create(page, "Защита очистки — модель B");
  await page.getByRole("button", { name: "Запустить", exact: true }).click();
  await expect
    .poll(async () => (await card(page, second)).execution_state, {
      timeout: 45_000,
    })
    .toBe("running");

  async function aggregateCard() {
    const response = await page.request.get(
      "/api/v1/aggregates/" + config.aggregate,
    );
    expect(response.ok()).toBeTruthy();
    return response.json();
  }
  async function pauseAggregate() {
    await page.getByRole("button", { name: "Пауза", exact: true }).click();
    const dialog = page.getByRole("dialog");
    await expect(dialog).toContainText(config.aggregate);
    await dialog.getByRole("button", { name: "Пауза", exact: true }).click();
    await expect.poll(async () => (await aggregateCard()).paused).toBe(true);
    await expect(
      page.getByRole("button", { name: "Продолжить", exact: true }),
    ).toBeEnabled();
  }
  async function stopDetector(identity: string) {
    await page.goto("/detectors/" + identity);
    await page.getByRole("button", { name: "Остановить", exact: true }).click();
    const dialog = page.getByRole("dialog");
    await expect(dialog).toContainText(identity);
    await dialog
      .getByRole("button", { name: "Остановить", exact: true })
      .click();
    await expect
      .poll(async () => (await card(page, identity)).execution_state)
      .toBe("stopped");
  }
  type CleanupPoint = {
    id: string;
    window_index: number;
    calculation_status: "computed" | "uncomputed";
    payload_state: "present";
  };
  async function points(limit = 100, offset = 0) {
    const response = await page.request.get(
      "/api/v1/aggregates/" +
        config.aggregate +
        "/points?limit=" +
        limit +
        "&offset=" +
        offset,
    );
    expect(response.ok()).toBeTruthy();
    return (await response.json()) as { total: number; items: CleanupPoint[] };
  }
  async function pausedPoints() {
    const firstPage = await points();
    const items = [...firstPage.items];
    while (items.length < firstPage.total) {
      const page = await points(100, items.length);
      expect(page.total).toBe(firstPage.total);
      expect(page.items.length).toBeGreaterThan(0);
      items.push(...page.items);
    }
    expect(new Set(items.map(({ id }) => id)).size).toBe(firstPage.total);
    return {
      total: firstPage.total,
      payloads: items.filter(
        (point) =>
          point.calculation_status === "computed" &&
          point.payload_state === "present",
      ),
      uncomputed: items.filter(
        (point) => point.calculation_status === "uncomputed",
      ),
    };
  }

  try {
    await stopDetector(second);
    const stoppedSecond = await card(page, second);
    await page.goto("/aggregates/" + config.aggregate);
    await pauseAggregate();
    const paused = await aggregateCard();
    const pauseObservedAt = Date.now();
    await page.reload();
    await expect(
      page.getByRole("button", { name: "Продолжить", exact: true }),
    ).toBeVisible();
    // Exercise a real pause across several fixed close deadlines; readiness
    // below is still proved by canonical worker results, never by this clock.
    await expect
      .poll(() => Date.now() - pauseObservedAt, { timeout: 15_000 })
      .toBeGreaterThanOrEqual(paused.duration_seconds * 3000);
    const reloadedPause = await aggregateCard();
    expect(reloadedPause.paused).toBe(true);
    expect(reloadedPause.next_index).toBe(paused.next_index);
    expect(reloadedPause.next_close_not_before_at).toBe(
      paused.next_close_not_before_at,
    );
    await page.getByRole("button", { name: "Продолжить", exact: true }).click();
    await expect.poll(async () => (await aggregateCard()).paused).toBe(false);
    await expect
      .poll(async () => (await aggregateCard()).next_index, { timeout: 30_000 })
      .toBeGreaterThan(paused.next_index + 1);
    await expect
      .poll(async () => (await card(page, first)).last_consumed?.index ?? -1, {
        timeout: 30_000,
      })
      .toBeGreaterThanOrEqual(paused.next_index + 1);
    expect((await card(page, first)).recovery.reason).toBe("aggregate_resume");
    expect((await card(page, second)).execution_state).toBe("stopped");
    expect((await card(page, second)).last_consumed).toEqual(
      stoppedSecond.last_consumed,
    );

    const keep = page.getByRole("textbox", {
      name: "Оставить последние N точек",
      exact: true,
    });
    await keep.fill("0");
    const blockedResponse = page.waitForResponse((response) => {
      const url = new URL(response.url());
      return (
        url.pathname.endsWith("/points/cleanup-preview") &&
        url.searchParams.get("keep_last") === "0"
      );
    });
    await page
      .getByRole("button", { name: "Проверить защиту истории", exact: true })
      .click();
    const blocked = await (await blockedResponse).json();
    expect(blocked.can_cleanup).toBe(false);
    expect(
      blocked.blockers.some(
        (item: { detector_id: string }) => item.detector_id === first,
      ),
    ).toBe(true);
    expect(
      blocked.blockers.some(
        (item: { detector_id: string }) => item.detector_id === second,
      ),
    ).toBe(true);
    const visiblePlan = page.getByLabel("План защиты истории", { exact: true });
    for (const [identity, name] of [
      [first, "Защита очистки — модель A"],
      [second, "Защита очистки — модель B"],
    ]) {
      const blocker = visiblePlan
        .getByRole("listitem")
        .filter({ hasText: identity });
      await expect(blocker).toBeVisible();
      await expect(blocker).toContainText(name);
    }
    await expect(
      page.getByRole("button", { name: "Очистить историю", exact: true }),
    ).toBeDisabled();
    const session = await (
      await page.request.get("/api/v1/auth/session")
    ).json();
    const refused = await page.request.post(
      "/api/v1/aggregates/" + config.aggregate + "/points/cleanup",
      {
        headers: {
          "X-CSRF-Token": session.csrf_token,
          "Idempotency-Key": crypto.randomUUID(),
        },
        data: { keep_last: 0 },
      },
    );
    expect(refused.status()).toBe(409);

    const list = await page.request.get(
      "/api/v1/detectors?limit=100&offset=0&aggregate_id=" + config.aggregate,
    );
    expect(list.ok()).toBeTruthy();
    const all = await list.json();
    expect(all.total).toBe(all.items.length);
    expect(all.items.length).toBeGreaterThanOrEqual(2);
    for (const detector of all.items) {
      if (detector.execution_state === "stopped") continue;
      expect(["requested", "starting", "running"]).toContain(
        detector.execution_state,
      );
      await stopDetector(detector.id);
    }
    for (const detector of all.items)
      expect((await card(page, detector.id)).execution_state).toBe("stopped");
    const modelProjections = await Promise.all(
      all.items.map(async (detector: { id: string }) => {
        const value = await card(page, detector.id);
        return {
          id: value.id,
          last_consumed: value.last_consumed,
          last_evaluated: value.last_evaluated,
        };
      }),
    );

    await page.goto("/aggregates/" + config.aggregate);
    await pauseAggregate();
    const beforeRevision = (await aggregateCard()).auto_cleanup
      .history_revision;
    const before = await pausedPoints();
    expect(before.total).toBeGreaterThan(0);
    const planResponse = await page.request.get(
      "/api/v1/aggregates/" +
        config.aggregate +
        "/points/cleanup-preview?keep_last=0",
    );
    expect(planResponse.ok()).toBeTruthy();
    const legal = await planResponse.json();
    expect(legal.maximum_delete).toBeGreaterThan(0);
    expect(legal.total_points).toBe(before.payloads.length);
    expect(before.total).toBe(
      before.payloads.length + before.uncomputed.length,
    );
    expect((await aggregateCard()).auto_cleanup.history_revision).toBe(
      beforeRevision,
    );
    const oldest = before.payloads.at(-1)!;
    // A UI smoke proves a fixed payload portion, rather than imposing a bulk
    // throughput bound on all history accumulated during preceding visual tests.
    const plannedDelete = Math.min(8, legal.maximum_delete);
    const requestedKeep = legal.total_points - plannedDelete;
    expect(requestedKeep).toBeGreaterThanOrEqual(legal.minimum_keep);
    await keep.fill(String(requestedKeep));
    const allowedResponse = page.waitForResponse((response) => {
      const url = new URL(response.url());
      return (
        url.pathname.endsWith("/points/cleanup-preview") &&
        url.searchParams.get("keep_last") === String(requestedKeep)
      );
    });
    await page
      .getByRole("button", { name: "Проверить защиту истории", exact: true })
      .click();
    const allowed = await (await allowedResponse).json();
    console.log(
      "Manual cleanup plan: " +
        JSON.stringify({
          revision: beforeRevision,
          list_total: before.total,
          computed: before.payloads.length,
          uncomputed: before.uncomputed.length,
          preview_total: legal.total_points,
          keep: requestedKeep,
          expected_delete: plannedDelete,
          preview_delete: allowed.planned_delete,
        }),
    );
    expect(allowed.can_cleanup).toBe(true);
    expect(allowed.total_points).toBe(legal.total_points);
    expect(allowed.requested_keep).toBe(requestedKeep);
    expect(allowed.planned_delete).toBe(plannedDelete);
    await page
      .getByRole("button", { name: "Очистить историю", exact: true })
      .click();
    const dialog = page.getByRole("dialog");
    await expect(dialog).toContainText(config.aggregate);
    const receiptResponse = page.waitForResponse(
      (response) =>
        response.request().method() === "POST" &&
        response.url().endsWith("/points/cleanup"),
    );
    await dialog.getByRole("button", { name: "Очистить", exact: true }).click();
    const accepted = await receiptResponse;
    expect(accepted.status()).toBe(202);
    const receipt = await accepted.json();
    expect(receipt.cleanup_operation_id).toBeTruthy();
    expect(receipt.cleanup_operation_id).not.toBe(receipt.operation_id);
    let deletedPoints: number | undefined;
    await expect
      .poll(
        async () => {
          const response = await page.request.get(
            "/api/v1/aggregates/" +
              config.aggregate +
              "/cleanup-operations/" +
              receipt.cleanup_operation_id,
          );
          expect(response.ok()).toBeTruthy();
          const operation = await response.json();
          deletedPoints = operation.deleted_points;
          return operation.status;
        },
        { timeout: 30_000 },
      )
      .toBe("succeeded");
    await expect(
      page.getByRole("region", { name: "Состояние очистки" }),
    ).toContainText("Завершена", { timeout: 10_000 });
    expect(deletedPoints).toBe(plannedDelete);
    const after = await pausedPoints();
    const afterRevision = (await aggregateCard()).auto_cleanup.history_revision;
    console.log(
      "Manual cleanup counts: " +
        JSON.stringify({
          revision_before: beforeRevision,
          revision_after: afterRevision,
          list_before: before.total,
          list_after: after.total,
          computed_before: before.payloads.length,
          uncomputed_before: before.uncomputed.length,
          computed_after: after.payloads.length,
          uncomputed_after: after.uncomputed.length,
          preview_total: allowed.total_points,
          keep: requestedKeep,
          removed: deletedPoints,
        }),
    );
    expect(Number(afterRevision) - Number(beforeRevision)).toBe(plannedDelete);
    expect(after.total).toBe(before.total - allowed.planned_delete);
    expect(after.payloads).toEqual(before.payloads.slice(0, requestedKeep));
    expect(after.uncomputed).toEqual(before.uncomputed);
    const cleared = await page.request.get(
      "/api/v1/aggregates/" +
        config.aggregate +
        "/points/by-index/" +
        oldest.window_index,
    );
    expect(cleared.status()).toBe(410);
    expect((await cleared.json()).code).toBe("aggregate_point_cleared");
    await page.reload();
    await expect(
      page.getByRole("button", { name: "Продолжить", exact: true }),
    ).toBeVisible();
    expect((await points()).total).toBe(after.total);
    expect(
      (
        await page.request.get(
          "/api/v1/aggregates/" + config.aggregate + "/points/" + oldest.id,
        )
      ).status(),
    ).toBe(410);
    for (const model of modelProjections) {
      const value = await card(page, model.id);
      expect(value.last_consumed).toEqual(model.last_consumed);
      expect(value.last_evaluated).toEqual(model.last_evaluated);
    }
    const afterCleanup = await aggregateCard();
    await page.getByRole("button", { name: "Продолжить", exact: true }).click();
    await expect.poll(async () => (await aggregateCard()).paused).toBe(false);
    await expect
      .poll(async () => (await aggregateCard()).next_index, { timeout: 30_000 })
      .toBeGreaterThan(afterCleanup.next_index);
    for (const detector of all.items)
      expect((await card(page, detector.id)).execution_state).toBe("stopped");
    expect(
      (
        await page.request.get(
          "/api/v1/aggregates/" +
            config.aggregate +
            "/points/by-index/" +
            oldest.window_index,
        )
      ).status(),
    ).toBe(410);
  } finally {
    // Restore only this profile-owned synthetic aggregate when no durable
    // cleanup fence forbids it. Never retry a mutation or start a detector.
    try {
      const current = await aggregateCard();
      if (current.paused && !current.active_cleanup_id) {
        const response = await page.request.get("/api/v1/auth/session");
        if (response.ok()) {
          const session = await response.json();
          await page.request.post(
            "/api/v1/aggregates/" + config.aggregate + "/unpause",
            {
              headers: {
                "X-CSRF-Token": session.csrf_token,
                "Idempotency-Key": crypto.randomUUID(),
              },
            },
          );
        }
      }
    } catch {
      // Profile teardown still owns every resource after a failed browser path.
    }
  }
});

test("automatic history collection preserves reserve, both real warmups and a stopped model's unread windows", async ({
  page,
}) => {
  const autoBudget = new Deadline(720_000);
  test.setTimeout(autoBudget.remaining());
  function phaseReport(
    phase: string,
    state: string,
    budget: Deadline,
    facts: Record<string, unknown>,
  ) {
    console.log(
      "Automatic history phase: " +
        JSON.stringify({
          phase,
          state,
          clock_domain: "playwright_worker_monotonic",
          phase_started_monotonic_ms: budget.started,
          ...budget.snapshot(),
          ...facts,
        }),
    );
  }
  const auto = config.auto_cleanup;
  try {
    console.log(
      "Automatic history entry timeline: " +
        JSON.stringify({
          runner: config.runner_timeline ?? null,
          preceding_same_file_tests: precedingTests,
          preceding_durations_are_not_wall_time: true,
          global_remaining_at_auto_entry_seconds: null,
          unknown_reason:
            "runner_and_Playwright_have_no_shared_monotonic_origin_or_entry_transport",
          runner_to_test_start_lag_seconds: null,
        }),
    );
  } catch {
    // Bounded existing metadata only; no request or polling for a new clock.
  }
  expect(auto).toBeTruthy();
  expect(auto.seed_windows).toBeGreaterThan(512);
  // These are captured by the runner before collection, from real canonical
  // rows. Its final read-only check also proves physical payload/snapshot GC.
  expect(auto.old_payloads).toBe(1);
  expect(auto.old_snapshots).toBe(1);
  expect(auto.old_point.completeness).toBe("complete");
  expect(auto.old_point.payload_state).toBe("present");
  await login(page);
  const manualCleanupRequests: string[] = [];
  page.on("request", (request) => {
    if (
      request.method() === "POST" &&
      /\/api\/v1\/aggregates\/[^/]+\/points\/cleanup(?:\?|$)/.test(
        request.url(),
      )
    )
      manualCleanupRequests.push(request.url());
  });
  async function aggregateCard(budget = autoBudget) {
    const response = await page.request.get(
      `/api/v1/aggregates/${auto.aggregate}`,
      { timeout: budget.remaining() },
    );
    budget.remaining();
    expect(response.ok()).toBeTruthy();
    const result = await response.json();
    budget.remaining();
    return result;
  }
  type PhaseMarker = {
    run_id: string;
    aggregate_id: string;
    nonce: string;
    phase: "generation";
    state: "begin" | "end";
    outcome?: "complete" | "failed";
  };
  function writeClock(value: string, generation_phase?: PhaseMarker) {
    // Only the profile-owned UUID is controlled. The real writer, WAL,
    // snapshot classifier, model implementation and wall-clock budgets run.
    const next = `${auto.clock_file}.next`;
    writeFileSync(
      next,
      JSON.stringify({
        aggregates: { [auto.aggregate]: value },
        ...(generation_phase ? { generation_phase } : {}),
      }),
    );
    renameSync(next, auto.clock_file);
  }
  function advanceClock(value: string, marker?: PhaseMarker) {
    writeClock(value, marker);
    console.log(
      "Automatic history clock advance: " +
        JSON.stringify({ at: new Date().toISOString(), clock_at: value }),
    );
  }
  async function start(identity: string, parent = autoBudget) {
    const budget = parent.child(45_000);
    await budget.run(async () => {
      await page.goto(`/detectors/${identity}`, {
        timeout: budget.remaining(),
      });
      await page
        .getByRole("button", { name: "Запустить", exact: true })
        .click({ timeout: budget.remaining() });
      await expect
        .poll(
          async () => (await card(page, identity, budget)).execution_state,
          { timeout: budget.remaining() },
        )
        .toBe("running");
    });
  }
  async function stop(identity: string) {
    await page.goto(`/detectors/${identity}`);
    await page.getByRole("button", { name: "Остановить", exact: true }).click();
    await page
      .getByRole("dialog")
      .getByRole("button", { name: "Остановить", exact: true })
      .click();
    await expect
      .poll(async () => (await card(page, identity)).execution_state)
      .toBe("stopped");
  }
  let previousOldPointStatus: number | null = null;
  async function oldPointStatus() {
    const response = await page.request.get(
      `/api/v1/aggregates/${auto.aggregate}/points/${auto.old_point_id}`,
    );
    const status = response.status();
    if (status !== previousOldPointStatus) {
      previousOldPointStatus = status;
      console.log(
        "Automatic history old point HTTP: " +
          JSON.stringify({
            at: new Date().toISOString(),
            aggregate_id: auto.aggregate,
            point_id: auto.old_point_id,
            status,
          }),
      );
    }
    if (status !== 410) return status;
    const result = await response.json();
    expect(result.code).toBe("aggregate_point_cleared");
    expect(result.details.clear_reason).toBe("automatic");
    expect(result.details.clear_policy_version).toBe("model_history_v1");
    expect(Number.isFinite(Date.parse(result.details.cleared_at))).toBe(true);
    return status;
  }
  await page.goto(`/aggregates/${auto.aggregate}`);
  const panel = page.getByRole("region", {
    name: "Автоматическое хранение истории",
    exact: true,
  });
  await expect(panel).toBeVisible();
  // The new auto scenario includes the normal physical 60s maintenance cadence
  // and up to 20s for its bounded work; the poll still returns as soon as GC does.
  await expect.poll(oldPointStatus, { timeout: 80_000 }).toBe(410);
  await expect
    .poll(async () => (await aggregateCard()).auto_cleanup.auto_cleared_total)
    .toBeGreaterThanOrEqual(auto.seed_windows - 512);
  const collected = await aggregateCard();
  expect(collected.paused).toBe(false);
  expect(collected.state).toBe("active");
  expect(collected.auto_cleanup.reserve_windows).toBe(512);
  expect(collected.auto_cleanup.retained_to_index).toBe(519);
  expect(collected.auto_cleanup.retained_from_index).toBe(8);
  await page.reload();
  await expect(panel).toContainText("Автоматически очищено точек");
  expect(await oldPointStatus()).toBe(410);

  // A 512-input warmup uses the policy's actual retained reserve, rather than
  // proving only an eight-input model on an otherwise untested large history.
  const ordinary = await create(
    page,
    "Автоистория — обычная модель",
    false,
    "Автоматическая история браузера",
    "256",
    auto.source,
  );
  await start(ordinary);
  const ordinaryWarm = await card(page, ordinary);
  expect(ordinaryWarm.warmup.eligible_windows).toBeGreaterThanOrEqual(512);
  expect(ordinaryWarm.accelerated_used).toBe(false);
  const accelerated = await create(
    page,
    "Автоистория — ускоренная модель",
    true,
    "Автоматическая история браузера",
    "256",
    auto.source,
  );
  await start(accelerated);
  const acceleratedWarm = await card(page, accelerated);
  expect(acceleratedWarm.warmup.eligible_windows).toBeGreaterThanOrEqual(512);
  expect(acceleratedWarm.accelerated_used).toBe(true);

  advanceClock(new Date(Date.parse(auto.clock_at) + 4000).toISOString());
  await expect
    .poll(
      async () => (await card(page, ordinary)).last_evaluated?.index ?? -1,
      {
        timeout: 30_000,
      },
    )
    .toBeGreaterThanOrEqual(520);
  await stop(ordinary);
  const stopped = await card(page, ordinary);
  expect(stopped.last_consumed.kind).toBe("evaluated");
  const consumed = stopped.last_consumed.index;
  expect(consumed).toBe(520);
  const lastIndex = consumed + 520;
  // Index10 is safely inside B512 at L520 and becomes an early GC candidate
  // after the jump. Observing this selected point exercises real background
  // refresh without waiting for every old payload to be collected.
  const visibleIndex = 10;
  const visiblePage = Math.floor((consumed - visibleIndex) / 3) + 1;
  const visiblePointResponse = await page.request.get(
    `/api/v1/aggregates/${auto.aggregate}/points/by-index/${visibleIndex}`,
  );
  expect(visiblePointResponse.ok()).toBeTruthy();
  const visiblePoint = await visiblePointResponse.json();
  await page.goto(`/aggregates/${auto.aggregate}`);
  await page.getByTitle(String(visiblePage), { exact: true }).click();
  await page
    .getByRole("button", {
      name: `Выбрать точку ${visiblePoint.id}`,
      exact: true,
    })
    .click();
  const selectedDetails = page.getByRole("region", {
    name: "Подробности выбранной точки",
    exact: true,
  });
  await expect(selectedDetails).toBeVisible();
  const generation = autoBudget.child(360_000);
  let generatedIndex: number | null = null;
  const generationFacts = () => ({
    expected_windows: 520,
    last_index: lastIndex,
    next_index: generatedIndex,
  });
  phaseReport("generation", "starting", generation, generationFacts());
  const phaseScope = {
    run_id: auto.run_id,
    aggregate_id: auto.aggregate,
    nonce: randomUUID(),
    phase: "generation" as const,
  };
  const generationClock = new Date(
    Date.parse(auto.anchor) + (consumed + 522) * 2000,
  ).toISOString();
  let generationJumpCommitted = false;
  try {
    await withPhaseEnd(
      () =>
        generation.run(async () => {
          advanceClock(generationClock, { ...phaseScope, state: "begin" });
          generationJumpCommitted = true;
          await expect
            .poll(
              async () => {
                generatedIndex = (await aggregateCard(generation)).next_index;
                return generatedIndex;
              },
              { timeout: generation.remaining() },
            )
            .toBeGreaterThan(lastIndex);
        }),
      (outcome) => {
        if (generationJumpCommitted) {
          // Same logical value, in finally before protection/resume; no new jump.
          writeClock(generationClock, { ...phaseScope, state: "end", outcome });
        }
      },
      (error) => {
        console.log(
          "Automatic generation end marker unavailable: " +
            (error instanceof Error ? error.name : "UnknownError"),
        );
      },
    );
    phaseReport("generation", "complete", generation, generationFacts());
  } catch (error) {
    phaseReport("generation", "failed", generation, generationFacts());
    throw error;
  }
  const beforeProtectionCheck = (await aggregateCard()).auto_cleanup;
  const nextCheckAt = Date.parse(beforeProtectionCheck.next_check_at);
  expect(Number.isFinite(nextCheckAt)).toBe(true);
  // Maintenance retains its real 60s cadence. Wait for the already scheduled
  // check, using the server's physical Date, then verify a new protection plan
  // after all 520 windows exist. A pre-jump summary cannot prove this case.
  await expect
    .poll(
      async () => {
        const response = await page.request.get(
          `/api/v1/aggregates/${auto.aggregate}`,
        );
        expect(response.ok()).toBeTruthy();
        const summary = (await response.json()).auto_cleanup;
        return (
          summary.checked_at !== beforeProtectionCheck.checked_at ||
          Date.parse(response.headers().date) >= nextCheckAt
        );
      },
      { timeout: 60_000 },
    )
    .toBe(true);
  await expect
    .poll(
      async () => {
        const summary = (await aggregateCard()).auto_cleanup;
        return (
          summary.checked_at !== beforeProtectionCheck.checked_at &&
          summary.protected_by.some(
            (model: {
              detector_id: string;
              reason: string;
              protected_from_index: number;
            }) =>
              model.detector_id === ordinary &&
              model.reason === "unconsumed_points" &&
              model.protected_from_index === consumed + 1,
          )
        );
      },
      { timeout: 20_000 },
    )
    .toBe(true);
  const protectedCard = await aggregateCard();
  expect(
    protectedCard.auto_cleanup.retained_to_index - consumed,
  ).toBeGreaterThan(512);
  const unread = await page.request.get(
    `/api/v1/aggregates/${auto.aggregate}/points/by-index/${consumed + 1}`,
  );
  expect(unread.ok()).toBeTruthy();
  expect((await unread.json()).payload_state).toBe("present");
  await expect
    .poll(
      async () =>
        (
          await page.request.get(
            `/api/v1/aggregates/${auto.aggregate}/points/${visiblePoint.id}`,
          )
        ).status(),
      { timeout: 20_000 },
    )
    .toBe(410);
  // Keep this same visible card mounted: its scoped GET/revision refresh must
  // remove the cleared selection and reload the current page without a manual
  // refresh, navigation, cleanup command or success notification.
  await expect(selectedDetails).toHaveCount(0, { timeout: 10_000 });
  const latestPointResponse = await page.request.get(
    `/api/v1/aggregates/${auto.aggregate}/points/by-index/${lastIndex - (visiblePage - 1) * 3}`,
  );
  expect(latestPointResponse.ok()).toBeTruthy();
  const latestPoint = await latestPointResponse.json();
  await expect(
    page.getByRole("button", {
      name: `Выбрать точку ${latestPoint.id}`,
      exact: true,
    }),
  ).toBeVisible({ timeout: 10_000 });
  expect((await card(page, ordinary)).last_consumed).toEqual(
    stopped.last_consumed,
  );
  await expect(panel).toContainText("Автоистория — обычная модель");
  for (const theme of ["light", "dark"] as const) {
    await page.emulateMedia({ colorScheme: theme });
    await expect(page.locator("html")).toHaveAttribute("data-theme", theme);
    for (const [width, height, zoom] of [
      [320, 720, ""],
      [2560, 720, ""],
      [1280, 360, ""],
      [800, 600, "2"],
    ] as const) {
      await page.setViewportSize({ width, height });
      await page.evaluate((value) => {
        document.documentElement.style.zoom = value;
      }, zoom);
      await panel.scrollIntoViewIfNeeded();
      expect(
        await page.evaluate(
          () => document.documentElement.scrollWidth <= innerWidth,
        ),
      ).toBe(true);
      await page.screenshot({
        path: test
          .info()
          .outputPath(
            `auto-history-${theme}-${width}-${height}-${zoom || "100"}.png`,
          ),
        fullPage: false,
        animations: "disabled",
      });
    }
  }
  await page.evaluate(() => {
    document.documentElement.style.zoom = "";
  });
  await page.setViewportSize({ width: 1280, height: 720 });
  await page.emulateMedia({ colorScheme: "light" });
  const resume = autoBudget.child(300_000);
  const resumedIndices: Record<string, number | null> = {
    ordinary: null,
    accelerated: null,
  };
  phaseReport("resume", "starting", resume, resumedIndices);
  try {
    await resume.run(async () => {
      await start(ordinary, resume);
      await expect
        .poll(
          async () => {
            resumedIndices.ordinary =
              (await card(page, ordinary, resume)).last_evaluated?.index ?? -1;
            return resumedIndices.ordinary;
          },
          {
            timeout: resume.remaining(),
          },
        )
        .toBe(lastIndex);
      const resumed = await card(page, ordinary, resume);
      expect(resumed.last_consumed.kind).toBe("evaluated");
      expect(resumed.last_consumed.index).toBe(lastIndex);
      expect((await aggregateCard(resume)).next_index).toBe(
        resumed.last_consumed.index + 1,
      );
      expect(resumed.warmup.cutoff).toBe(stopped.warmup.cutoff);
      expect(resumed.recovery.reason).toBe("manual_resume");
      expect(resumed.accelerated_used).toBe(false);
      await expect
        .poll(
          async () => {
            resumedIndices.accelerated =
              (await card(page, accelerated, resume)).last_evaluated?.index ??
              -1;
            return resumedIndices.accelerated;
          },
          {
            timeout: resume.remaining(),
          },
        )
        .toBe(lastIndex);
      const independentlyRunning = await card(page, accelerated, resume);
      expect(independentlyRunning.execution_state).toBe("running");
      expect(independentlyRunning.accelerated_used).toBe(true);
    });
    phaseReport("resume", "complete", resume, resumedIndices);
  } catch (error) {
    phaseReport("resume", "failed", resume, resumedIndices);
    throw error;
  }
  expect(await oldPointStatus()).toBe(410);
  expect(manualCleanupRequests).toEqual([]);
});
