import { expect, test, type Page } from "@playwright/test";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import {
  calendarInputValue,
  inspectAggregateScreen,
} from "./aggregates.visual";
import { parse, stringify } from "lossless-json";

const configPath = process.env.AGGREGATE_BROWSER_CONFIG;
type SourceFixture = { sources: string[]; anchor: string };
type Config = {
  username: string;
  password: string;
  empty_source: string;
  result: string;
  saved_window?: SourceFixture;
} & SourceFixture;
export const config: Config | null = configPath
  ? JSON.parse(readFileSync(configPath, "utf8"))
  : null;
if (!config && process.env.BROWSER_FIXTURES_REQUIRED)
  throw new Error("Required aggregate fixture missing");
test.skip(
  !config,
  "Run through end-to-end --aggregate-only; fixtures are owned by the profile runner.",
);
test.beforeEach(({ page }) => {
  page.on("response", async (response) => {
    if (
      response.status() < 500 ||
      !response.url().includes("/api/v1/aggregates") ||
      response.request().method() !== "POST"
    )
      return;
    try {
      const body = await response.json();
      if (
        ["capture_unavailable", "availability_capture_unavailable"].includes(
          body.code,
        )
      ) {
        const reason =
          typeof body.details?.reason === "string" &&
          /^[a-z_]+$/.test(body.details.reason)
            ? body.details.reason
            : "unspecified";
        console.log(
          `Aggregate capture refused: ${body.code}; reason=${reason}`,
        );
      }
    } catch {
      /* The existing UI assertions still report malformed responses. */
    }
  });
});
test.afterEach(async ({ page }) => {
  if (!config) return;
  const sessionResponse = await page.request.get("/api/v1/auth/session");
  const session = await sessionResponse.json();
  if (!session.authenticated || session.user?.username !== config.username)
    return;
  let offset = 0,
    total = 1;
  while (offset < total) {
    const response = await page.request.get(
      `/api/v1/aggregates?limit=100&offset=${offset}`,
    );
    expect(response.ok()).toBe(true);
    const pageData = await response.json();
    total = pageData.total;
    for (const aggregate of pageData.items) {
      if (
        aggregate.created_by === session.user.id &&
        [...config.sources, ...(config.saved_window?.sources ?? [])].includes(
          aggregate.source_id,
        ) &&
        aggregate.state === "active"
      ) {
        const archived = await page.request.post(
          `/api/v1/aggregates/${aggregate.id}/archive`,
          { headers: { "X-CSRF-Token": session.csrf_token } },
        );
        expect(archived.ok()).toBe(true);
      }
    }
    if (!pageData.items.length && offset < total)
      throw new Error("Incomplete owned aggregate cleanup page");
    offset += pageData.items.length;
  }
});
export async function login(page: Page) {
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
export async function fillBase(
  page: Page,
  name: string,
  sourceIndex = 0,
  fixture: SourceFixture = config!,
) {
  await page.getByLabel("Имя агрегата", { exact: true }).fill(name);
  await page
    .getByRole("combobox", { name: "Собственный источник", exact: true })
    .click();
  const response = await page.request.get("/api/v1/sources?limit=50&offset=0");
  const source = (await response.json()).items.find(
    (s: { id: string }) => s.id === fixture.sources[sourceIndex],
  );
  await page.getByTitle(source.name, { exact: true }).click();
  await page.getByLabel("Пояс отображения", { exact: true }).fill("UTC");
  await page.getByLabel("Длительность, секунды", { exact: true }).fill("1");
  await page
    .getByLabel("Начало истории и сетка окон", { exact: true })
    .fill(calendarInputValue(fixture.anchor));
  await page
    .getByLabel("Начало окна для расчёта", { exact: true })
    .fill(calendarInputValue(fixture.anchor));
}
export async function checkPreview(page: Page) {
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
  await expect(
    page.getByRole("region", { name: "Оценка ресурсов", exact: true }),
  ).toHaveCount(1);
}
test("real UI creates on two sources, previews, preserves exact values and original identity through copy/lifecycle/history", async ({
  page,
}) => {
  test.setTimeout(120_000);
  const fixture = config!.saved_window;
  if (!fixture || fixture.sources.length !== 2) {
    throw new Error("The saved-window fixture requires two prepared sources");
  }
  const expectedStart = new Date(fixture.anchor).toISOString();
  const owned: string[] = existsSync(config!.result)
    ? JSON.parse(readFileSync(config!.result, "utf8"))
    : [];
  await login(page);
  for (let index = 0; index < 2; index++) {
    await page
      .getByRole("button", { name: "Создать агрегат", exact: true })
      .click();
    await fillBase(page, `UI ${index}`, index, fixture);
    // The modal keeps the exact numeric draft isolated until Apply.
    if (!index) {
      await page
        .getByRole("button", {
          name: "Добавить признак",
          exact: true,
        })
        .click();
      const modal = page.getByRole("dialog");
      const method = modal.getByRole("combobox", {
        name: "Операция входа",
        exact: true,
      });
      await method.focus();
      await method.press("ArrowDown");
      await page
        .getByRole("option", { name: /^Формула по именованным признакам/ })
        .click();
      await modal
        .getByLabel("Формула: константа", { exact: true })
        .fill("9007199254740993");
      await modal
        .getByRole("button", { name: "Применить признак", exact: true })
        .click();
    }
    await page.getByText("JSON", { exact: true }).click();
    if (index)
      await page
        .getByLabel("Полное правило JSON", { exact: true })
        .fill(
          '{"where":{"op":"in","field":"http.request.method","value":["GET","POST"]},"features":[{"key":"events","name":"События","op":"count"},{"key":"bytes","name":"Байты","op":"sum","field":"network.bytes"},{"key":"ratio","name":"Отношение","op":"derive","expr":{"op":"ratio","args":[{"ref":"bytes"},{"ref":"events"}]}},{"key":"precise","name":"Точное","op":"derive","expr":{"value":9007199254740993}}]}',
        );
    else {
      const rule = parse(
        await page
          .getByLabel("Полное правило JSON", { exact: true })
          .inputValue(),
      ) as { where?: unknown };
      rule.where = { op: "eq", field: "event.outcome", value: "failure" };
      await page
        .getByLabel("Полное правило JSON", { exact: true })
        .fill(stringify(rule)!);
      await page.getByText("Визуальный", { exact: true }).click();
    }
    await checkPreview(page);
    await page.screenshot({
      path: test.info().outputPath(`editor-${index}.png`),
      fullPage: true,
    });
    await expect(
      page.getByRole("region", { name: "Предварительный результат" }),
    ).toContainText("9007199254740993");
    const created = page.waitForResponse(
      (r) =>
        r.url().endsWith("/api/v1/aggregates") &&
        r.request().method() === "POST",
    );
    await page
      .getByRole("button", { name: "Создать агрегат", exact: true })
      .click();
    const response = await created;
    expect(response.status()).toBe(201);
    expect(response.request().postData()).toContain("9007199254740993");
    expect(JSON.parse(response.request().postData()!).time_basis).toBe(
      "event_time",
    );
    const original = await response.json();
    expect(original.semantic_basis).toBe("event_time");
    expect(original.execution_mode).toBe("current");
    expect(original.source_id).toBe(fixture.sources[index]);
    expect(original.duration_seconds).toBe(1);
    // All possible remaining one-second windows in this test fit the
    // 512-index reserve measured from the last closure.
    expect(Number(original.online_start_index) + 120).toBeLessThan(512);
    const originalRule = stringify(
      (parse(await response.text()) as { aggregate_rule: unknown })
        .aggregate_rule,
    );
    owned.push(original.id);
    writeFileSync(config!.result, JSON.stringify(owned));
    await expect(page).toHaveURL(new RegExp(`/aggregates/${original.id}$`));
    await page
      .getByRole("button", { name: "Обновить точки", exact: true })
      .click();
    const aggregatePath = `/api/v1/aggregates/${original.id}`;
    let pointId = "";
    await expect
      .poll(async () => {
        const point = await page.request.get(
          `${aggregatePath}/points/by-index/0`,
          {
            timeout: 5_000,
          },
        );
        const body = await point.json();
        if (point.status() === 409 && body.code === "canonical_point_not_ready")
          return "";
        expect(point.status(), "Exact archived window must have payload").toBe(
          200,
        );
        expect(body).toMatchObject({
          aggregate_id: original.id,
          window_index: 0,
          origin: "initial_history",
          calculation_status: "computed",
        });
        expect(new Date(body.start_utc).toISOString()).toBe(expectedStart);
        pointId = body.id;
        return pointId;
      })
      .toMatch(/^[a-f\d-]{36}$/i);
    await page
      .getByRole("button", { name: "Обновить точки", exact: true })
      .click();
    await page
      .getByRole("button", { name: /^Выбрать точку / })
      .first()
      .click();
    await expect(
      page.getByRole("region", { name: "Подробности выбранной точки" }),
    ).toContainText("9007199254740993");
    await expect(
      page.getByRole("img", { name: /^График .* загруженной страницы$/ }),
    ).toBeVisible();
    await page.screenshot({
      path: test.info().outputPath(`card-${index}.png`),
      fullPage: true,
    });
    await page.getByLabel("Пояс отображения", { exact: true }).fill("UTC");
    await page
      .getByLabel("Начало сохранённого окна", { exact: true })
      .fill(calendarInputValue(fixture.anchor));
    const historicalResponse = page.waitForResponse(
      (result) =>
        new URL(result.url()).pathname ===
          `${aggregatePath}/historical-points` &&
        result.request().method() === "POST",
      { timeout: 5_000 },
    );
    const notice = page.getByText(
      "Сохранённое окно загружено. Новый расчёт не выполнялся.",
      { exact: true },
    );
    await Promise.all([
      (async () => {
        await page
          .getByRole("button", {
            name: "Просмотреть сохранённое окно",
            exact: true,
          })
          .click();
        await expect(notice).toBeVisible();
      })(),
      (async () => {
        const result = await historicalResponse;
        const body = await result.json();
        console.log(
          "Saved aggregate window: " +
            JSON.stringify({
              aggregate: original.id,
              index: 0,
              start: expectedStart,
              status: result.status(),
              expectedPoint: pointId,
              ...(result.ok()
                ? { returnedPoint: body.id }
                : {
                    code: [
                      "canonical_point_not_ready",
                      "aggregate_point_cleared",
                    ].includes(body.code)
                      ? body.code
                      : "other",
                  }),
            }),
        );
        expect(result.status()).toBe(200);
        expect(
          new Date(
            result.request().postDataJSON().window_start_utc,
          ).toISOString(),
        ).toBe(expectedStart);
        expect(body).toMatchObject({
          id: pointId,
          aggregate_id: original.id,
          window_index: 0,
          origin: "initial_history",
          calculation_status: "computed",
        });
        expect(new Date(body.start_utc).toISOString()).toBe(expectedStart);
      })(),
    ]);
    await expect(
      page.getByRole("img", { name: /^График .* выбранного окна$/ }),
    ).toHaveAttribute("data-time-low", String(Date.parse(expectedStart)));
    await page.getByLabel("Начало сохранённого окна", { exact: true }).fill("");
    await expect(
      page.getByRole("img", { name: /^График .* загруженной страницы$/ }),
    ).toBeVisible();
    await page.getByRole("button", { name: "Копировать", exact: true }).click();
    await expect(page).toHaveURL(/\/aggregates\/new\?copy=/);
    await page.getByLabel("Имя агрегата", { exact: true }).fill("Копия");
    await page.getByLabel("Пояс отображения", { exact: true }).fill("UTC");
    await page
      .getByLabel("Начало окна для расчёта", { exact: true })
      .fill(calendarInputValue(fixture.anchor));
    await checkPreview(page);
    const copied = page.waitForResponse(
      (r) =>
        r.url().endsWith("/api/v1/aggregates") &&
        r.request().method() === "POST",
    );
    await page
      .getByRole("button", { name: "Создать агрегат", exact: true })
      .click();
    const copiedResponse = await copied;
    expect(copiedResponse.request().postData()).toContain("9007199254740993");
    const copy = await copiedResponse.json();
    expect(copy.id).not.toBe(original.id);
    owned.push(copy.id);
    writeFileSync(config!.result, JSON.stringify(owned));
    const unchangedResponse = await page.request.get(
      `/api/v1/aggregates/${original.id}`,
    );
    const unchanged = await unchangedResponse.json();
    expect(
      stringify(
        (parse(await unchangedResponse.text()) as { aggregate_rule: unknown })
          .aggregate_rule,
      ),
    ).toBe(originalRule);
    for (const k of [
      "id",
      "aggregate_rule",
      "source_id",
      "anchor_utc",
      "duration_seconds",
    ])
      expect(unchanged[k]).toEqual(original[k]);
    await expect(page).toHaveURL(new RegExp(`/aggregates/${copy.id}$`));
    await page
      .getByRole("button", { name: "Архивировать", exact: true })
      .click();
    await page
      .getByRole("dialog")
      .getByRole("button", { name: "Отмена", exact: true })
      .click();
    await expect(page.getByRole("dialog")).toHaveCount(0);
    await expect(
      page
        .getByTestId("workspace-content")
        .getByRole("button", { name: "Архивировать", exact: true }),
    ).toBeFocused();
    await page
      .getByRole("button", { name: "Архивировать", exact: true })
      .click();
    await page
      .getByRole("dialog")
      .getByRole("button", { name: "Архивировать", exact: true })
      .click();
    await expect(page.getByRole("dialog")).toHaveCount(0);
    const resumed = page.waitForResponse(
      (r) =>
        r.url().endsWith(`/api/v1/aggregates/${copy.id}/resume`) &&
        r.request().method() === "POST",
    );
    await page
      .getByRole("button", { name: "Возобновить", exact: true })
      .click();
    expect((await resumed).ok()).toBe(true);
    await expect(
      page
        .getByTestId("workspace-content")
        .getByRole("button", { name: "Архивировать", exact: true }),
    ).toBeVisible();
    await expect(
      page
        .getByRole("button", { name: "Архивировать", exact: true })
        .getByRole("img", { name: "loading" }),
    ).toHaveCount(0);
    await page
      .getByRole("button", { name: "Удалить агрегат", exact: true })
      .click();
    const deleteDialog = page.getByRole("dialog", {
      name: "Удалить агрегат?",
      exact: true,
    });
    await expect(deleteDialog).toBeVisible();
    await expect(
      deleteDialog.getByRole("img", { name: "loading", exact: true }),
    ).toHaveCount(0);
    const confirmDelete = deleteDialog.getByRole("button", {
      name: "Удалить",
      exact: true,
    });
    await expect(confirmDelete).toBeEnabled();
    await confirmDelete.click();
    await expect(
      page.getByText(
        "Агрегат удалён. Новые расчёты окончательно остановлены, сохранённая история доступна.",
        { exact: true },
      ),
    ).toBeVisible();
    await page.goto("/aggregates");
  }
});

test("extended vector, separate catalog/prompt, lossless file exchange and production direct routes", async ({
  page,
}) => {
  test.setTimeout(300_000);
  await login(page);
  await page
    .getByRole("button", { name: "Создать агрегат", exact: true })
    .click();
  await fillBase(
    page,
    "Расширенный агрегат — " + crypto.randomUUID() + " длинное имя ".repeat(7),
  );
  await inspectAggregateScreen(page, "create");
  const received: string[] = [];
  await page.evaluate(() =>
    Object.defineProperty(navigator, "clipboard", {
      configurable: true,
      value: {
        writeText: (text: string) => {
          (window as unknown as { copied: string }).copied = text;
          return Promise.resolve();
        },
      },
    }),
  );
  await page
    .getByRole("button", { name: "Инструкция для ИИ", exact: true })
    .click();
  await expect(
    page.getByText("Инструкция скопирована.", { exact: true }),
  ).toBeVisible();
  const prompt = await page.evaluate(
    () => (window as unknown as { copied: string }).copied,
  );
  expect(prompt).toContain("До уточнения задавай вопросы");
  expect(prompt).not.toContain(config!.sources[0]);
  await expect(page.getByRole("dialog")).toHaveCount(0);
  await page
    .getByRole("button", { name: "Скопировать поля источника", exact: true })
    .click();
  await expect(
    page.getByText("Полный каталог полей скопирован.", { exact: true }),
  ).toBeVisible();
  const fields = await page.evaluate(
    () => (window as unknown as { copied: string }).copied,
  );
  expect(fields).toContain("source.bytes");
  expect(fields).toContain("Обнаружено в событиях");
  expect(fields).toContain("Предусмотрено нормализатором");
  const catalog = await (
    await page.request.get(
      `/api/v1/aggregates/sources/${config!.empty_source}/field-catalog`,
    )
  ).json();
  expect(catalog.observation.rows).toBe(0);
  expect(
    catalog.items.some((f: { name: string }) => f.name === "user.name"),
  ).toBe(true);
  const extended =
    '{"where":{"op":"eq","field":"user.name","value":"Andrew"},"auxiliary":[{"key":"logons","name":"Входы","op":"count","where":{"op":"eq","field":"event.action","value":"logon"}},{"key":"internal","name":"Внутренняя зависимость","op":"formula","expr":{"ref":"logons"}}],"features":[{"key":"ratio","name":"Первичное отношение","op":"formula","expr":{"op":"divide","args":[{"aggregate":{"op":"sum","field":"source.bytes"}},{"aggregate":{"op":"sum","field":"destination.bytes"}}]}},{"key":"mixed","name":"Производный выход","op":"formula","expr":{"op":"add","args":[{"ref":"internal"},{"aggregate":{"op":"count","where":{"op":"eq","field":"event.action","value":"logoff"}}}]}},{"key":"exact","name":"Точное целое","op":"derive","expr":{"value":9007199254740993}}]}';
  await page.getByLabel("Файл правила JSON").setInputFiles({
    name: "aggregate.json",
    mimeType: "application/json",
    buffer: Buffer.from(extended),
  });
  await page
    .getByRole("dialog")
    .getByRole("button", { name: "Отмена", exact: true })
    .click();
  await page.getByText("JSON", { exact: true }).click();
  await expect(
    page.getByLabel("Полное правило JSON", { exact: true }),
  ).not.toHaveValue(/logons/);
  await page.getByLabel("Файл правила JSON").setInputFiles({
    name: "aggregate.json",
    mimeType: "application/json",
    buffer: Buffer.from(extended),
  });
  await page
    .getByRole("dialog")
    .getByRole("button", { name: "Заменить правило", exact: true })
    .click();
  await expect(
    page.getByLabel("Полное правило JSON", { exact: true }),
  ).toHaveValue(extended);
  const download = page.waitForEvent("download");
  await page
    .getByRole("button", { name: "Скачать .json", exact: true })
    .click();
  const saved = await download;
  const stream = await saved.createReadStream();
  for await (const chunk of stream!) received.push(chunk.toString());
  expect(stringify(parse(received.join("")))).toBe(stringify(parse(extended)));
  await page.getByText("Визуальный", { exact: true }).click();
  const featureList = page.getByRole("region", {
    name: "Признаки",
    exact: true,
  });
  const rowNames = () =>
    featureList
      .getByRole("button", { name: / · / })
      .evaluateAll((buttons) =>
        buttons.map((button) => button.getAttribute("aria-label")),
      );
  const originalRows = await rowNames();
  const readWire = async () => {
    await page.getByText("JSON", { exact: true }).click();
    const value = JSON.parse(
      await page
        .getByLabel("Полное правило JSON", { exact: true })
        .inputValue(),
    );
    await page.getByText("Визуальный", { exact: true }).click();
    return value;
  };
  const originalKeys = (await readWire()).features.map(
    (f: { key: string }) => f.key,
  );
  await expect(
    featureList.getByRole("button", { name: "Выше", exact: true }),
  ).toHaveCount(0);
  await expect(
    featureList.getByRole("button", { name: "Ниже", exact: true }),
  ).toHaveCount(0);
  await expect(featureList.locator("article details")).toHaveCount(0);
  await page
    .getByRole("switch", { name: "ratio: включён в выход", exact: true })
    .click();
  await expect(
    page.getByRole("switch", { name: "ratio: включён в выход", exact: true }),
  ).not.toBeChecked();
  expect(await rowNames()).toEqual(originalRows);
  await expect(featureList).toContainText("Первичное отношение");
  expect(
    (await readWire()).features.map((f: { key: string }) => f.key),
  ).toEqual(originalKeys.filter((key: string) => key !== "ratio"));
  await featureList
    .getByRole("button", { name: "Производный выход · mixed", exact: true })
    .click();
  await page
    .getByRole("dialog")
    .getByLabel("Название признака", { exact: true })
    .fill("Производный выход B2");
  await page
    .getByRole("button", { name: "Применить признак", exact: true })
    .click();
  await expect(page.getByRole("dialog")).toHaveCount(0);
  originalRows[originalRows.indexOf("Производный выход · mixed")] =
    "Производный выход B2 · mixed";
  expect(
    (await readWire()).features.find((f: { key: string }) => f.key === "mixed")
      .name,
  ).toBe("Производный выход B2");
  await page
    .getByRole("button", { name: "Добавить признак", exact: true })
    .click();
  await page
    .getByRole("dialog")
    .getByLabel("Название признака", { exact: true })
    .fill("Временный счётчик");
  await page
    .getByRole("button", { name: "Применить признак", exact: true })
    .click();
  await expect(page.getByRole("dialog")).toHaveCount(0);
  expect(
    (await readWire()).features.map((f: { key: string }) => f.key),
  ).toEqual([
    ...originalKeys.filter((key: string) => key !== "ratio"),
    "feature_1",
  ]);
  await page
    .getByRole("button", { name: "Удалить признак feature_1", exact: true })
    .click();
  expect(
    (await readWire()).features.map((f: { key: string }) => f.key),
  ).toEqual(originalKeys.filter((key: string) => key !== "ratio"));
  await page
    .getByRole("switch", { name: "ratio: включён в выход", exact: true })
    .click();
  expect(await rowNames()).toEqual(originalRows);
  expect(
    (await readWire()).features.map((f: { key: string }) => f.key),
  ).toEqual(originalKeys);
  await expect(
    page.getByRole("switch", { name: "ratio: включён в выход", exact: true }),
  ).toBeChecked();
  const outputHelp = page.getByRole("button", {
    name: "Справка: Выход признака ratio",
    exact: true,
  });
  await outputHelp.focus();
  await outputHelp.press("Enter");
  await expect(outputHelp).toHaveAttribute("aria-expanded", "true");
  await expect(page.getByRole("tooltip")).toBeVisible();
  await outputHelp.press("Escape");
  await expect(outputHelp).toHaveAttribute("aria-expanded", "false");
  await page
    .getByRole("button", { name: /Первичное отношение · ratio/ })
    .click();
  await expect(
    page.getByRole("dialog", { name: "Признак", exact: true }),
  ).toBeVisible();
  await expect(
    page.getByRole("radio", { name: "Первичный", exact: true }),
  ).toBeChecked();
  await expect(
    page.getByRole("dialog").getByLabel("Формула признака"),
  ).toContainText("sum(source.bytes)");
  await inspectAggregateScreen(page, "primary-modal");
  // The new editor binds the operation directly to its AST. Exercise keyboard
  // selection on a separate new count draft so the imported ratio stays intact.
  await page
    .getByRole("dialog")
    .getByRole("button", { name: "Отмена", exact: true })
    .click();
  await page
    .getByRole("button", { name: "Добавить признак", exact: true })
    .click();
  // A searchable ECS input must keep usable width, and options explain their meaning.
  const primary = page.getByRole("dialog", { name: "Признак", exact: true });
  const operationInput = primary.getByRole("combobox", {
    name: "Операция входа",
    exact: true,
  });
  await operationInput.focus();
  await operationInput.press("ArrowDown");
  const selectedCount = page.getByRole("option", {
    name: /^Количество событий/,
  });
  await expect(selectedCount).toHaveAttribute("aria-selected", "true");
  // rc-select restores the active selected option in a deferred effect on open.
  // Wait for that browser frame before navigating; otherwise a later reset can
  // replace sum with count between the active assertion and Enter (R39-7-1).
  await operationInput.evaluate(
    () =>
      new Promise<void>((resolve) =>
        requestAnimationFrame(() => requestAnimationFrame(() => resolve())),
      ),
  );
  const selectedCountId = await selectedCount.getAttribute("id");
  expect(selectedCountId).toBeTruthy();
  await expect(operationInput).toHaveAttribute(
    "aria-activedescendant",
    selectedCountId!,
  );
  await expect(page.getByRole("option", { name: /^Сумма / })).toContainText(
    "Сумма значений числового поля",
  );
  // Complete the keyboard selection before moving focus: clicking an option
  // schedules rc-select's mouse focus restoration, which can steal ECS focus.
  const sumOptionId = await page
    .getByRole("option", { name: /^Сумма / })
    .getAttribute("id");
  expect(sumOptionId).toBeTruthy();
  const operationCount = await page.getByRole("option").count();
  for (
    let index = 0;
    index < operationCount &&
    (await operationInput.getAttribute("aria-activedescendant")) !==
      sumOptionId;
    index++
  )
    await operationInput.press("ArrowDown");
  await expect(operationInput).toHaveAttribute(
    "aria-activedescendant",
    sumOptionId!,
  );
  await operationInput.press("Enter");
  await expect(operationInput).toHaveAttribute("aria-expanded", "false");
  await expect(operationInput).toBeFocused();
  await expect(
    primary
      .locator(".ant-select")
      .filter({
        has: page.getByRole("combobox", {
          name: "Операция входа",
          exact: true,
        }),
      })
      .locator(".ant-select-selection-item"),
  ).toHaveText("Сумма");
  const ecs = primary
    .getByRole("combobox", { name: "Поле ECS", exact: true })
    .first();
  const catalogResponse = page.waitForResponse(
    (response) => {
      const url = new URL(response.url());
      return (
        url.pathname.endsWith(`/sources/${config!.sources[0]}/field-catalog`) &&
        url.searchParams.get("q") === "source.bytes"
      );
    },
    { timeout: 5000 },
  );
  await ecs.focus();
  await ecs.fill("source.bytes");
  await expect(ecs).toHaveValue("source.bytes");
  await ecs.press("ArrowDown");
  await expect(ecs).toHaveAttribute("aria-expanded", "true");
  const [searchedCatalogResponse] = await Promise.all([
    catalogResponse,
    expect(page.getByRole("option", { name: /source.bytes ·/ })).toBeVisible(),
  ]);
  expect(searchedCatalogResponse.ok()).toBe(true);
  expect((await searchedCatalogResponse.json()).items).toEqual(
    expect.arrayContaining([expect.objectContaining({ name: "source.bytes" })]),
  );
  await expect(ecs).toHaveValue("source.bytes");
  await expect(ecs).toBeFocused();
  const ecsWidth = await ecs.evaluate(
    (input) => input.closest(".ant-select")!.getBoundingClientRect().width,
  );
  expect(ecsWidth).toBeGreaterThan(500);
  await page.screenshot({
    path: test.info().outputPath("primary-modal-ecs-search.png"),
    fullPage: false,
  });
  await ecs.press("Enter");
  await expect(ecs).toHaveAttribute("aria-expanded", "false");
  await expect(
    primary
      .locator(".ant-select")
      .filter({
        has: page.getByRole("combobox", { name: "Поле ECS", exact: true }),
      })
      .first()
      .locator(".ant-select-selection-item"),
  ).toHaveAttribute("title", "source.bytes");
  const fieldHelp = primary
    .getByRole("button", { name: "Справка: Поле ECS", exact: true })
    .first();
  await fieldHelp.focus();
  await fieldHelp.press("Enter");
  await expect(page.getByRole("tooltip")).toContainText("совместимость");
  await expect(page.getByRole("tooltip")).toBeVisible();
  await expect
    .poll(() =>
      page
        .getByRole("tooltip")
        .evaluate(
          (el) => getComputedStyle(el.closest(".ant-popover") ?? el).opacity,
        ),
    )
    .toBe("1");
  await page.screenshot({
    path: test.info().outputPath("primary-modal-help.png"),
    fullPage: false,
    animations: "disabled",
  });
  await fieldHelp.press("Escape");
  await expect(primary).toBeVisible();
  await expect(page.getByRole("tooltip")).not.toBeVisible();
  await ecs.focus();
  await page
    .getByRole("dialog")
    .getByRole("button", { name: "Отмена", exact: true })
    .click();
  await page
    .getByRole("button", { name: /Производный выход B2 · mixed/ })
    .click();
  await expect(
    page.getByRole("radio", { name: "Производный", exact: true }),
  ).toBeChecked();
  await expect(
    page.getByRole("dialog").getByLabel("Формула признака"),
  ).toContainText("internal");
  await inspectAggregateScreen(page, "derived-modal");
  await page
    .getByRole("dialog")
    .getByRole("button", { name: "Применить признак", exact: true })
    .click();
  await page
    .getByLabel("Пояс отображения", { exact: true })
    .fill("Asia/Yekaterinburg");
  await page
    .getByLabel("Начало истории и сетка окон", { exact: true })
    .fill(
      calendarInputValue(
        new Date(Date.parse(config!.anchor) + 4 * 3600000).toISOString(),
      ),
    );
  // Changing the process may propose its new first completed window. Choose
  // explicitly the seeded event interval on the new grid, in this display zone.
  await page
    .getByLabel("Начало окна для расчёта", { exact: true })
    .fill(
      calendarInputValue(
        new Date(Date.parse(config!.anchor) + 5 * 3600000).toISOString(),
      ),
    );
  const previewResponse = page.waitForResponse(
    (r) =>
      r.url().endsWith("/api/v1/aggregates/preview") &&
      r.request().method() === "POST",
  );
  await checkPreview(page);
  const previewBody = await (await previewResponse).json();
  expect(previewBody.features.map((f: { key: string }) => f.key)).toEqual(
    originalKeys,
  );
  expect(
    previewBody.features.find((f: { key: string }) => f.key === "ratio").value,
  ).toBe(2);
  expect(
    previewBody.features.find((f: { key: string }) => f.key === "mixed").value,
  ).toBe(1);
  await inspectAggregateScreen(page, "diagnostics");
  const result = page.getByRole("region", {
    name: "Предварительный результат",
  });
  await expect(result).toContainText("9007199254740993");
  const zone = page.getByLabel("Пояс отображения", { exact: true });
  let mutations = 0;
  const count = (r: import("@playwright/test").Request) => {
    if (r.method() === "POST") mutations++;
  };
  page.on("request", count);
  await zone.fill("America/New_York");
  await expect(result).toBeVisible();
  expect(mutations).toBe(0);
  page.off("request", count);
  const created = page.waitForResponse(
    (r) =>
      r.url().endsWith("/api/v1/aggregates") && r.request().method() === "POST",
  );
  await page
    .getByRole("button", { name: "Создать агрегат", exact: true })
    .click();
  const response = await created;
  expect(response.status()).toBe(201);
  const original = await response.json();
  const submitted = JSON.parse(response.request().postData()!);
  expect(submitted.time_basis).toBe("event_time");
  expect(submitted.schedule.timezone).toBe("Asia/Yekaterinburg");
  expect(submitted.schedule.boundary).toMatch(/\+05:00$/);
  const owned = existsSync(config!.result)
    ? JSON.parse(readFileSync(config!.result, "utf8"))
    : [];
  owned.push(original.id);
  writeFileSync(config!.result, JSON.stringify(owned));
  await expect(page).toHaveURL(new RegExp(`/aggregates/${original.id}$`));
  const preview = JSON.parse(
    await (await page.request.get(`/api/v1/aggregates/${original.id}`)).text(),
  );
  expect(preview.aggregate_rule.auxiliary).toHaveLength(2);
  await page.reload();
  await expect(
    page.getByRole("heading", { name: /1. Параметры процесса/ }),
  ).toBeVisible();
  await inspectAggregateScreen(page, "saved-card");
  for (const path of [
    "/aggregates",
    `/aggregates/${original.id}`,
    `/aggregates/new?copy=${original.id}`,
  ]) {
    expect((await page.goto(path))!.status()).toBe(200);
    if (path.includes("copy=")) page.once("dialog", (d) => d.accept());
    await page.reload();
    if (path.includes("copy="))
      await expect(
        page.getByLabel("Имя агрегата", { exact: true }),
      ).toHaveValue("");
    if (path === "/aggregates") {
      await expect(
        page.getByRole("link", { name: original.name, exact: true }),
      ).toBeVisible();
      let searches = 0;
      const watch = (request: import("@playwright/test").Request) => {
        if (
          request.method() === "GET" &&
          /\/api\/v1\/aggregates\?/.test(request.url())
        )
          searches++;
      };
      page.on("request", watch);
      await page.getByRole("button", { name: "Фильтры", exact: true }).click();
      await page.getByLabel("Поиск агрегатов по имени").fill("Не применять");
      await page
        .getByRole("dialog")
        .getByRole("button", { name: "Отмена", exact: true })
        .click();
      expect(searches).toBe(0);
      await page.getByRole("button", { name: "Фильтры", exact: true }).click();
      await page
        .getByLabel("Поиск агрегатов по имени")
        .fill("Расширенный агрегат");
      await inspectAggregateScreen(page, "list-filters");
      await page
        .getByRole("dialog")
        .getByRole("button", { name: "Применить фильтры", exact: true })
        .click();
      await expect(page.getByRole("dialog")).toHaveCount(0);
      await expect(
        page.getByRole("link", { name: original.name, exact: true }),
      ).toBeVisible();
      expect(searches).toBe(1);
      page.off("request", watch);
    }
    await inspectAggregateScreen(
      page,
      path.includes("copy=")
        ? "copy"
        : path === "/aggregates"
          ? "list"
          : "card-reload",
    );
  }
  if (new URL(page.url()).protocol === "https:")
    for (const path of [
      "/docs",
      "/openapi.json",
      "/.env",
      "/unknown-task39-path",
      "/assets/missing-task39.js",
    ])
      expect((await page.request.get(path)).status()).toBe(404);
});
