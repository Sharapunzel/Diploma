import { readFileSync } from "node:fs";
import { expect, test, type Page } from "@playwright/test";
const path = process.env.DETECTOR_BROWSER_CONFIG;
if (!path)
  throw new Error("Detector UI requires the isolated browser profile fixture");
const config = JSON.parse(readFileSync(path, "utf8")) as {
  username: string;
  password: string;
  aggregate: string;
  source: string;
  anchor: string;
};
const id = "00000000-0000-4000-8000-000000000123";
async function loginAt(page: Page, path: string) {
  await page.goto(path);
  await page
    .getByRole("textbox", { name: "Имя пользователя" })
    .fill(config.username);
  await page.getByLabel("Пароль", { exact: true }).fill(config.password);
  await page.getByRole("button", { name: "Войти", exact: true }).click();
  await expect(page.getByRole("button", { name: /^Аккаунт:/ })).toBeVisible();
}
async function screenShot(page: Page, name: string) {
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= innerWidth,
    ),
  ).toBe(true);
  await page.screenshot({
    path: test.info().outputPath(name + ".png"),
    fullPage: true,
    animations: "disabled",
  });
}
function card() {
  return {
    id,
    name: "Длинное имя детектора ".repeat(8),
    aggregate_id: config.aggregate,
    aggregate_name: "Агрегат ".repeat(20),
    aggregate_state: "active",
    source_id: config.source,
    source_name: "Источник",
    created_at: config.anchor,
    execution_state: "running",
    model_state: "ready",
    accelerated: true,
    accelerated_used: true,
    reason: null,
    last_consumed: null,
    last_evaluated: null,
    backlog: null,
    lag_seconds: null,
    config: {
      dimensions: 1,
      trees: 100,
      sample_size: 256,
      output_after: 256,
      seed: 42,
    },
    feature_keys: ["n"],
    feature_names: ["События"],
    anchor_utc: config.anchor,
    duration_seconds: 2,
    step_seconds: 1,
    revision: 0,
    execution_generation: 1,
    start_request_id: null,
    start_requested_at: null,
    execution_started_at: null,
    stopped_at: null,
    updated_at: config.anchor,
    last_failure: null,
    warmup: null,
    recovery: null,
    next_scheduled_at: null,
    waiting_for_point: null,
  };
}
test("mock creation retains frozen exact payload/key across navigation and waits for explicit replay", async ({
  page,
}) => {
  const requests: { key: string; body: string }[] = [];
  await page.route("**/api/v1/detectors", async (route) => {
    if (route.request().method() !== "POST") return route.continue();
    requests.push({
      key: route.request().headers()["idempotency-key"],
      body: route.request().postData()!,
    });
    if (requests.length === 1)
      return route.fulfill({
        status: 201,
        body: "{}",
        contentType: "application/json",
      });
    return route.fulfill({
      status: 201,
      json: {
        operation_id: id,
        detector_id: id,
        action: "create",
        recorded_at: config.anchor,
        execution_generation: 0,
        start_request_id: null,
      },
    });
  });
  await page.route(`**/api/v1/detectors/${id}`, (route) =>
    route.fulfill({
      json: { ...card(), execution_state: "created", accelerated_used: false },
    }),
  );
  await page.goto("/detectors/new");
  await page
    .getByRole("textbox", { name: "Имя пользователя" })
    .fill(config.username);
  await page.getByLabel("Пароль", { exact: true }).fill(config.password);
  await page.getByRole("button", { name: "Войти", exact: true }).click();
  await page.getByRole("combobox", { name: "Активный агрегат" }).click();
  await page.getByTitle("Браузерный агрегат", { exact: true }).click();
  await page
    .getByLabel("Имя детектора", { exact: true })
    .fill("Точный черновик");
  await page.getByText("Дополнительные настройки RCF", { exact: true }).click();
  await page
    .getByLabel("Начальное значение генератора", { exact: true })
    .fill("-9223372036854775808");
  await page
    .getByRole("button", { name: "Создать без запуска", exact: true })
    .click();
  await expect(
    page.getByRole("button", {
      name: "Повторить исходную команду",
      exact: true,
    }),
  ).toBeVisible();
  await expect(
    page.getByLabel("Имя детектора", { exact: true }),
  ).toBeDisabled();
  expect(requests).toHaveLength(1);
  expect(requests[0].body).toContain('"seed":-9223372036854775808');
  await page
    .getByRole("link", { name: "К списку детекторов", exact: true })
    .click();
  await page
    .getByRole("dialog")
    .getByRole("button", { name: "Перейти", exact: true })
    .click();
  await page
    .getByRole("button", { name: "Создать детектор", exact: true })
    .click();
  await expect(page.getByLabel("Имя детектора", { exact: true })).toHaveValue(
    "Точный черновик",
  );
  expect(requests).toHaveLength(1);
  await page
    .getByRole("button", { name: "Повторить исходную команду", exact: true })
    .click();
  await expect(page).toHaveURL(new RegExp(`/detectors/${id}$`));
  expect(requests[1]).toEqual(requests[0]);
  await expect(
    page.getByRole("button", { name: "Запустить", exact: true }),
  ).toBeVisible();
  expect(requests).toHaveLength(2);
});
test("mock unknown stop, explicit replay, confirmation and responsive themes", async ({
  page,
}) => {
  let calls = 0;
  const keys: string[] = [];
  const body = card();
  await page.route(`**/api/v1/detectors/${id}**`, async (route) => {
    if (route.request().method() === "POST") {
      calls++;
      keys.push(route.request().headers()["idempotency-key"]);
      if (calls === 1) return route.abort("failed");
      body.execution_state = "stopped";
      return route.fulfill({
        json: {
          operation_id: id,
          detector_id: id,
          action: "stop",
          recorded_at: config.anchor,
          execution_generation: 2,
          start_request_id: null,
        },
      });
    }
    return route.fulfill({
      body: JSON.stringify(body).replace(
        '"seed":42',
        '"seed":9223372036854775807',
      ),
      contentType: "application/json",
    });
  });
  await page.goto(`/detectors/${id}`);
  await page
    .getByRole("textbox", { name: "Имя пользователя" })
    .fill(config.username);
  await page.getByLabel("Пароль", { exact: true }).fill(config.password);
  await page.getByRole("button", { name: "Войти", exact: true }).click();
  await expect(
    page.getByText("9223372036854775807", { exact: true }),
  ).toBeVisible();
  await page.getByRole("button", { name: "Остановить", exact: true }).click();
  await expect(page.getByRole("dialog")).toContainText(id);
  await screenShot(page, "stop-target-confirmation");
  await page
    .getByRole("dialog")
    .getByRole("button", { name: "Отмена", exact: true })
    .press("Escape");
  await expect(page.getByRole("dialog")).not.toBeVisible();
  expect(calls).toBe(0);
  await page.getByRole("button", { name: "Остановить", exact: true }).click();
  await page
    .getByRole("dialog")
    .getByRole("button", { name: "Остановить", exact: true })
    .click();
  await expect(
    page.getByText(
      "Результат команды неизвестен: сервер мог принять изменение.",
      { exact: true },
    ),
  ).toBeVisible();
  expect(calls).toBe(1);
  await page
    .getByRole("button", { name: "Повторить исходную команду", exact: true })
    .click();
  await expect(
    page.getByRole("button", { name: "Запустить", exact: true }),
  ).toBeVisible();
  expect(keys[0]).toBe(keys[1]);
  await page.getByRole("combobox", { name: "Часовой пояс" }).focus();
  await page.getByRole("combobox", { name: "Часовой пояс" }).press("ArrowDown");
  await page.getByTitle("Europe/Moscow", { exact: true }).click();
  await expect(page.locator(".ant-select-dropdown:visible")).toHaveCount(0);
  expect(calls).toBe(2);
  for (const theme of ["light", "dark"] as const) {
    await page.emulateMedia({ colorScheme: theme });
    for (const width of [320, 1280, 2560]) {
      await page.setViewportSize({ width, height: 720 });
      expect(
        await page.evaluate(
          () => document.documentElement.scrollWidth <= innerWidth,
        ),
      ).toBe(true);
      await page.screenshot({
        path: test.info().outputPath(`detector-${theme}-${width}.png`),
        fullPage: true,
        animations: "disabled",
      });
      await page
        .getByRole("button", { name: "Справка: Часовой пояс", exact: true })
        .focus();
      await expect(page.getByRole("tooltip")).toBeVisible();
      await screenShot(page, `detector-help-${theme}-${width}`);
      await page.getByRole("combobox", { name: "Часовой пояс" }).focus();
    }
  }
});

test("mock nonempty table and modal filters preserve drafts, failures and keyboard cancellation", async ({
  page,
}) => {
  test.setTimeout(90_000);
  const queries: string[] = [];
  let refuse = true;
  await page.route("**/api/v1/detectors?**", async (route) => {
    const url = new URL(route.request().url());
    queries.push(url.search);
    if (url.searchParams.has("aggregate_id") && refuse)
      return route.fulfill({
        status: 503,
        json: { code: "temporary_unavailable", message: "synthetic" },
      });
    return route.fulfill({
      json: {
        items: [
          {
            ...card(),
            name: url.searchParams.has("aggregate_id")
              ? "Отфильтрованный детектор с длинным именем ".repeat(4)
              : "Исходный детектор с длинным именем ".repeat(4),
          },
        ],
        total: 1,
        limit: 25,
        offset: 0,
      },
    });
  });
  await loginAt(page, "/detectors");
  await expect(page.getByRole("table")).toBeVisible();
  const trigger = page.getByRole("button", { name: "Фильтры", exact: true });
  await trigger.focus();
  await page.keyboard.press("Enter");
  await page.getByRole("textbox", { name: "UUID агрегата" }).fill(id);
  await page.keyboard.press("Escape");
  await expect(page.getByRole("dialog")).not.toBeVisible();
  await expect(trigger).toBeFocused();
  expect(queries).toHaveLength(1);
  await trigger.click();
  await expect(
    page.getByRole("textbox", { name: "UUID агрегата" }),
  ).toHaveValue("");
  await page.getByRole("textbox", { name: "UUID агрегата" }).fill(id);
  await page
    .getByRole("dialog")
    .getByRole("button", { name: "Применить", exact: true })
    .click();
  await expect(page.getByRole("dialog").getByRole("alert")).toBeVisible();
  await expect(
    page.getByRole("textbox", { name: "UUID агрегата" }),
  ).toHaveValue(id);
  await expect(
    page.getByRole("link", { name: /^Исходный детектор/ }),
  ).toHaveCount(0);
  refuse = false;
  await page
    .getByRole("dialog")
    .getByRole("button", { name: "Применить", exact: true })
    .click();
  await expect(page.getByRole("dialog")).not.toBeVisible();
  await expect(
    page.getByRole("link", { name: /^Отфильтрованный детектор/ }),
  ).toBeVisible();
  expect(new URLSearchParams(queries.at(-1)).get("offset")).toBe("0");
  for (const scheme of ["light", "dark"] as const) {
    await page.emulateMedia({ colorScheme: scheme });
    for (const width of [320, 1280, 2560]) {
      await page.setViewportSize({ width, height: 720 });
      await screenShot(page, `list-${scheme}-${width}`);
      await trigger.click();
      await expect(
        page.getByRole("textbox", { name: "UUID агрегата" }),
      ).toHaveValue(id);
      await screenShot(page, `filters-${scheme}-${width}`);
      await page
        .getByRole("dialog")
        .getByRole("button", { name: "Отмена", exact: true })
        .click();
      await expect(page.getByRole("dialog")).not.toBeVisible();
    }
  }
  // Half the CSS viewport models the layout at 200% browser zoom.
  await page.setViewportSize({ width: 640, height: 160 });
  await trigger.click();
  await screenShot(page, "filters-short-200-percent");
  await page
    .getByRole("dialog")
    .getByRole("button", { name: "Применить", exact: true })
    .scrollIntoViewIfNeeded();
  await screenShot(page, "filters-short-200-percent-actions");
  const before = queries.length;
  await page
    .getByRole("dialog")
    .getByRole("button", { name: "Сбросить", exact: true })
    .click();
  expect(queries).toHaveLength(before);
  await page
    .getByRole("dialog")
    .getByRole("button", { name: "Применить", exact: true })
    .click();
  await expect(page.getByRole("dialog")).not.toBeVisible();
  await expect(
    page.getByRole("link", { name: /^Исходный детектор/ }),
  ).toBeVisible();
  expect(new URLSearchParams(queries.at(-1)).has("aggregate_id")).toBe(false);
});

test("mock single aggregate combobox searches beyond fifty and keeps selection, errors and exact frozen intent", async ({
  page,
}) => {
  test.setTimeout(120_000);
  await loginAt(page, "/detectors");
  const response = await page.request.get(
    `/api/v1/aggregates/${config.aggregate}`,
  );
  expect(response.ok()).toBeTruthy();
  const aggregate = await response.json();
  const catalog = Array.from({ length: 61 }, (_, index) => ({
    ...aggregate,
    id: `00000000-0000-4000-8000-${String(1000 + index).padStart(12, "0")}`,
    name:
      index === 55
        ? "Далёкий агрегат с длинным именем для серверного поиска"
        : index >= 59
          ? "Одинаковый агрегат"
          : `Агрегат ${index}`,
    source_name: `Источник ${index}`,
  }));
  const target = catalog[55];
  const searches: string[] = [];
  let failNext = false;
  let oldRelease: (() => void) | undefined;
  const oldResponse = new Promise<void>((resolve) => {
    oldRelease = resolve;
  });
  await page.route("**/api/v1/aggregates?**", async (route) => {
    const params = new URL(route.request().url()).searchParams;
    expect(params.get("limit")).toBe("10");
    expect(params.get("offset")).toBe("0");
    expect(params.get("state")).toBe("active");
    const q = params.get("q") ?? "";
    searches.push(q);
    if (q === "старый") await oldResponse;
    if (failNext) {
      failNext = false;
      return route.fulfill({
        status: 503,
        json: { code: "temporary_unavailable", message: "synthetic" },
      });
    }
    const found =
      q === "старый"
        ? catalog
        : catalog.filter((item) =>
            item.name.toLocaleLowerCase().includes(q.toLocaleLowerCase()),
          );
    return route.fulfill({
      json: { items: found.slice(0, 10), total: found.length },
    });
  });
  const commands: { body: string; key: string }[] = [];
  await page.route("**/api/v1/detectors", async (route) => {
    if (route.request().method() !== "POST") return route.continue();
    commands.push({
      body: route.request().postData()!,
      key: route.request().headers()["idempotency-key"],
    });
    return route.fulfill({
      status: 503,
      json: { code: "temporary_unavailable", message: "synthetic" },
    });
  });
  await page.goto("/detectors/new");
  const combo = page.getByRole("combobox", { name: "Активный агрегат" });
  await expect(page.getByText(/Показаны первые 10/)).toBeVisible();
  await combo.fill("Далёкий");
  await expect(
    page
      .locator(".ant-select-dropdown:visible")
      .getByTitle(target.name, { exact: true }),
  ).toBeVisible();
  await combo.press("ArrowDown");
  await combo.press("Enter");
  const selected = page.getByRole("region", { name: "Выбранный агрегат" });
  await expect(selected).toContainText(target.name);
  await expect(selected).toContainText(target.source_name);
  await combo.fill("старый");
  await expect.poll(() => searches.includes("старый")).toBe(true);
  await combo.fill("Одинаковый");
  await expect(
    page.getByText("Источник: Источник 59", { exact: true }),
  ).toBeVisible();
  await expect(
    page.getByText("Источник: Источник 60", { exact: true }),
  ).toBeVisible();
  oldRelease!();
  await expect(selected).toContainText(target.name);
  await expect(
    page.getByText("Источник: Источник 59", { exact: true }),
  ).toBeVisible();
  await expect(
    page.getByText("Источник: Источник 60", { exact: true }),
  ).toBeVisible();
  await combo.fill("нет такого имени");
  await expect(
    page.getByText("Активных агрегатов не найдено", { exact: true }),
  ).toBeVisible();
  failNext = true;
  await combo.fill("Далёкий");
  await expect(
    page.getByText("Поиск недоступен. Повторите запрос.", { exact: true }),
  ).toBeVisible();
  await page.getByRole("button", { name: "Повторить", exact: true }).click();
  await expect.poll(() => searches.slice(-2)).toEqual(["Далёкий", "Далёкий"]);
  await combo.focus();
  await combo.press("ArrowDown");
  await expect(
    page
      .locator(".ant-select-dropdown:visible")
      .getByTitle(target.name, { exact: true }),
  ).toBeVisible();
  await combo.press("Escape");
  await expect(selected).toContainText(target.name);
  await combo.fill("");
  await expect.poll(() => searches.at(-1)).toBe("");
  await combo.press("Escape");
  await page
    .getByRole("textbox", { name: "Имя детектора" })
    .fill("Детектор с длинным именем ".repeat(6));
  await page
    .getByRole("button", { name: "Справка: Ускоренный прогрев", exact: true })
    .click();
  await expect(
    page.getByRole("checkbox", { name: "Ускоренный прогрев", exact: true }),
  ).not.toBeChecked();
  expect(commands).toHaveLength(0);
  await page
    .getByRole("checkbox", { name: "Ускоренный прогрев", exact: true })
    .check();
  await page.getByText("Дополнительные настройки RCF", { exact: true }).click();
  await page
    .getByLabel("Начальное значение генератора", { exact: true })
    .fill("9223372036854775807");
  for (const scheme of ["light", "dark"] as const) {
    await page.emulateMedia({ colorScheme: scheme });
    for (const width of [320, 1280, 2560]) {
      await page.setViewportSize({ width, height: 720 });
      await page
        .getByRole("textbox", { name: "Имя детектора" })
        .scrollIntoViewIfNeeded();
      await screenShot(page, `create-${scheme}-${width}`);
      await page
        .getByRole("button", { name: "Справка: Активный агрегат", exact: true })
        .focus();
      await expect(page.getByRole("tooltip")).toBeVisible();
      await screenShot(page, `create-help-${scheme}-${width}`);
      await page
        .getByLabel("Начальное значение генератора", { exact: true })
        .focus();
      await screenShot(page, `create-settings-${scheme}-${width}`);
    }
  }
  await page.setViewportSize({ width: 640, height: 160 });
  await screenShot(page, "create-short-200-percent");
  await page
    .getByRole("button", { name: "Создать без запуска", exact: true })
    .click();
  await expect(
    page.getByRole("button", {
      name: "Повторить исходную команду",
      exact: true,
    }),
  ).toBeVisible();
  expect(commands).toHaveLength(1);
  expect(commands[0].body).toContain(`"aggregate_id":"${target.id}"`);
  expect(commands[0].body).toContain('"seed":9223372036854775807');
  expect(commands[0].body).toContain('"step_seconds":1');
  await expect(combo).toBeDisabled();
});
