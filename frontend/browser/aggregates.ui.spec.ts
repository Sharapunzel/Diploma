import { expect, test, type Page } from "@playwright/test";
import { readFileSync } from "node:fs";

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
const rule = { features: [{ key: "events", name: "События", op: "count" }] };
function card() {
  return {
    id: "mock",
    name: "Очень длинное имя агрегата ".repeat(12),
    source_id: config!.sources[0],
    source_name: "Тестовый источник",
    source_type: "kafka",
    aggregate_rule: rule,
    rule_hash: "a".repeat(64),
    schedule_id: "s",
    anchor_utc: config!.anchor,
    duration_seconds: 1,
    original_boundary: config!.anchor,
    timezone: "UTC",
    created_at: new Date().toISOString(),
    created_by: "user",
    state: "active",
    next_scheduled_at: config!.anchor,
    next_index: 1,
    skipped_ranges: [],
    lag_seconds: 0,
    last_error: null,
    last_error_at: null,
  };
}
test("responsive points, mixed kinds, pagination, capture refusal, confirmation cancel and keyboard focus", async ({
  page,
}) => {
  await login(page);
  const a = card();
  let mutations = 0;
  const offsets: string[] = [];
  await page.route("**/api/v1/aggregates/mock**", async (route) => {
    const url = route.request().url();
    if (url.includes("/points")) {
      offsets.push(new URL(url).searchParams.get("offset") || "0");
      return route.fulfill({
        json: {
          total: 51,
          items: ["working", "historical"].map((kind, i) => ({
            id: `${kind}1`,
            aggregate_id: "mock",
            kind,
            start_utc: config!.anchor,
            end_utc: new Date(Date.parse(config!.anchor) + 1000).toISOString(),
            scheduled_at: config!.anchor,
            actual_started_at: config!.anchor,
            actual_completed_at: config!.anchor,
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
            time_basis: "availability_at",
            event_time_min: null,
            event_time_max: null,
          })),
        },
      });
    }
    if (route.request().method() !== "GET") {
      mutations++;
      return route.fulfill({
        status: 422,
        json: {
          code: "historical_time_basis_unknown",
          message: "internal",
          request_id: "safe-request",
          details: null,
        },
      });
    }
    return route.fulfill({ json: a });
  });
  await page.goto("/aggregates/mock");
  await expect(
    page.getByRole("region", { name: "Сохранённые точки" }),
  ).toContainText("Полнота неизвестна");
  await page.getByRole("button", { name: "Выбрать точку working1" }).click();
  await expect(
    page.getByRole("region", { name: "Подробности выбранной точки" }),
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
  await page
    .getByLabel("Начало исторического окна", { exact: true })
    .fill(config!.anchor);
  await page
    .getByRole("button", { name: "Рассчитать историческое окно", exact: true })
    .click();
  await expect(
    page.getByText(/Основание времени истории не доказано/),
  ).toBeVisible();
  await page.getByTitle("Вперед", { exact: true }).click();
  expect(offsets).toContain("50");
  const featurePicker = page.getByRole("combobox", {
    name: "Признак графика",
    exact: true,
  });
  await featurePicker.focus();
  await featurePicker.press("ArrowDown");
  await page.getByText("fraction", { exact: true }).last().click();
  const plot = page.getByRole("img", {
    name: "График fraction загруженной страницы",
    exact: true,
  });
  await expect(plot).toHaveAttribute("data-domain-low", "0.1");
  await expect(plot).toHaveAttribute("data-domain-high", "0.2");
  await expect(plot.locator("circle")).toHaveAttribute("cy", "170");
  await expect(plot.locator("rect")).toHaveAttribute("y", "25");
  await featurePicker.focus();
  await featurePicker.press("ArrowDown");
  await page.getByText("extreme", { exact: true }).last().click();
  const extremes = page.getByRole("img", {
    name: "График extreme загруженной страницы",
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
    for (const width of [320, 1920]) {
      await page.setViewportSize({ width, height: 650 });
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
  await expect(page.getByLabel("aggregate_rule", { exact: true })).toHaveCount(
    0,
  );
  releaseFirst();
  await loader.getByRole("button", { name: "Повторить", exact: true }).click();
  await expect(loader).toHaveAttribute("aria-busy", "true");
  await expect(page.getByLabel("Имя агрегата", { exact: true })).toHaveCount(0);
  releaseRetry();
  await page.getByLabel("Имя агрегата", { exact: true }).fill("Мой ввод");
  await page.getByText("JSON", { exact: true }).click();
  await expect(page.getByLabel("aggregate_rule", { exact: true })).toHaveValue(
    /original/,
  );
  const edited =
    '{"features":[{"key":"edited","name":"Мой признак","op":"count"}]}';
  await page.getByLabel("aggregate_rule", { exact: true }).fill(edited);
  await expect(page.getByLabel("aggregate_rule", { exact: true })).toHaveValue(
    edited,
  );
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
  await page.getByLabel("Имя агрегата", { exact: true }).fill("Проверка");
  await page
    .getByRole("combobox", { name: "Собственный источник", exact: true })
    .click();
  const source = (
    await (await page.request.get("/api/v1/sources?limit=50&offset=0")).json()
  ).items.find((s: { id: string }) => s.id === config!.sources[0]);
  await page.getByTitle(source.name, { exact: true }).click();
  await page.getByLabel("Длительность, секунды", { exact: true }).fill("1");
  await page
    .getByLabel("Исходная граница", { exact: true })
    .fill(config!.anchor);
  await page
    .getByLabel("Начало preview-окна", { exact: true })
    .fill(config!.anchor);
  await page.getByText("JSON", { exact: true }).click();
  await page.getByLabel("aggregate_rule", { exact: true }).fill("{");
  await page.getByText("Визуальный", { exact: true }).click();
  await expect(page.getByLabel("aggregate_rule", { exact: true })).toHaveValue(
    "{",
  );
  const text = JSON.stringify(rule);
  await page.getByLabel("aggregate_rule", { exact: true }).fill(text);
  await page.evaluate(() =>
    Object.defineProperty(navigator, "clipboard", {
      value: { writeText: () => Promise.reject(new Error("denied")) },
      configurable: true,
    }),
  );
  await page
    .getByRole("button", { name: "Промпт для ИИ", exact: true })
    .click();
  await page
    .getByRole("button", { name: "Копировать инструкцию", exact: true })
    .click();
  await expect(page.getByText(/Буфер обмена недоступен/)).toBeVisible();
  await page.keyboard.press("Escape");
  await expect(
    page.getByRole("button", { name: "Промпт для ИИ", exact: true }),
  ).toBeFocused();
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
  await expect(page.getByLabel("aggregate_rule", { exact: true })).toHaveValue(
    text,
  );
  for (const width of [320, 1920]) {
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
  fail = false;
  await page
    .getByRole("button", { name: "Проверить правило", exact: true })
    .click();
  await expect(
    page.getByText("Правило и расписание проверены сервером.", { exact: true }),
  ).toBeVisible();
  await page
    .getByRole("button", { name: "Предварительный расчёт", exact: true })
    .click();
  await expect(
    page.getByRole("region", { name: "Предварительный результат" }),
  ).toBeVisible();
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
  await expect(page.getByLabel("aggregate_rule", { exact: true })).toHaveValue(
    text,
  );
  emptyCreate = true;
  await page
    .getByRole("button", {
      name: "Повторить создание с тем же ключом",
      exact: true,
    })
    .click();
  await expect.poll(() => attempts.length).toBe(3);
  await expect(page.getByText(/Результат создания неизвестен/)).toBeVisible();
  await expect(page.getByLabel("aggregate_rule", { exact: true })).toHaveValue(
    text,
  );
  await expect(page).toHaveURL(/\/aggregates\/new$/);
  expect(attempts[2]).toEqual(attempts[0]);
  await page.route("**/api/v1/aggregates/mock**", (route) =>
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
  await expect(page).toHaveURL(/\/aggregates\/mock$/);
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
  for (const path of ["/aggregates", "/aggregates/new", "/aggregates/mock"]) {
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
    await page.route("**/api/v1/aggregates/mock**", async (route) => {
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
    await page.goto("/aggregates/mock");
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
        name: "Рассчитать историческое окно",
        exact: true,
      }),
    ).toBeDisabled();
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
      await expect(
        page.getByRole("heading", { name: a.name, exact: true }),
      ).toBeVisible();
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
  await page.route("**/api/v1/aggregates/mock**", async (route) => {
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
  await page.goto("/aggregates/mock");
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
