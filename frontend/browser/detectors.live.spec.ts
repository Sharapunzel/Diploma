import { readFileSync } from "node:fs";
import { expect, test, type Page } from "@playwright/test";
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
};
if (!config.aggregate || !config.username || !config.guest)
  throw new Error("Incomplete detector live fixture");
test.setTimeout(180_000);
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
  expect(chosen.source_id).toBe(config.source);
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
async function card(page: Page, id: string) {
  const response = await page.request.get(`/api/v1/detectors/${id}`);
  expect(response.ok()).toBeTruthy();
  return response.json();
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
          (p: { kind: string; end_utc: string; completeness: string }) =>
            p.kind === "working" &&
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
    time_basis: "availability_at",
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
        return (await r.json()).items.filter(
          (p: { kind: string; completeness: string }) =>
            p.kind === "working" && p.completeness === "complete",
        ).length;
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
