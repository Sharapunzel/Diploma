import { expect, test, type Page } from "@playwright/test";
import { readFileSync, writeFileSync } from "node:fs";
import { parse, stringify } from "lossless-json";

const configPath = process.env.AGGREGATE_BROWSER_CONFIG;
type Config = {
  username: string;
  password: string;
  sources: string[];
  anchor: string;
  result: string;
};
export const config: Config | null = configPath
  ? JSON.parse(readFileSync(configPath, "utf8"))
  : null;
test.skip(
  !config,
  "Run through end-to-end --aggregate-only; fixtures are owned by the profile runner.",
);
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
export async function fillBase(page: Page, name: string, sourceIndex = 0) {
  await page.getByLabel("Имя агрегата", { exact: true }).fill(name);
  await page
    .getByRole("combobox", { name: "Собственный источник", exact: true })
    .click();
  const response = await page.request.get("/api/v1/sources?limit=50&offset=0");
  const source = (await response.json()).items.find(
    (s: { id: string }) => s.id === config!.sources[sourceIndex],
  );
  await page.getByTitle(source.name, { exact: true }).click();
  await page.getByLabel("Длительность, секунды", { exact: true }).fill("1");
  await page
    .getByLabel("Исходная граница", { exact: true })
    .fill(config!.anchor);
  await page
    .getByLabel("Начало preview-окна", { exact: true })
    .fill(config!.anchor);
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
}
test("real UI creates on two sources, previews, preserves exact values and original identity through copy/lifecycle/history", async ({
  page,
}) => {
  test.setTimeout(120_000);
  const owned: string[] = [];
  await login(page);
  for (let index = 0; index < 2; index++) {
    await page
      .getByRole("button", { name: "Создать агрегат", exact: true })
      .click();
    await fillBase(page, `UI ${index}`, index);
    // First source is created through visual controls, including a derived expression.
    if (!index) {
      await page
        .getByRole("button", { name: "Добавить признак", exact: true })
        .click();
      await page
        .getByRole("combobox", { name: "features[1].op", exact: true })
        .focus();
      await page
        .getByRole("combobox", { name: "features[1].op", exact: true })
        .press("ArrowDown");
      await page.getByText("derive", { exact: true }).click();
      await page
        .getByLabel("features[1].expr: константа", { exact: true })
        .fill("9007199254740993");
    }
    await page.getByText("JSON", { exact: true }).click();
    if (index)
      await page
        .getByLabel("aggregate_rule", { exact: true })
        .fill(
          '{"where":{"op":"in","field":"http.request.method","value":["GET","POST"]},"features":[{"key":"events","name":"События","op":"count"},{"key":"bytes","name":"Байты","op":"sum","field":"network.bytes"},{"key":"ratio","name":"Отношение","op":"derive","expr":{"op":"ratio","args":[{"ref":"bytes"},{"ref":"events"}]}},{"key":"precise","name":"Точное","op":"derive","expr":{"value":9007199254740993}}]}',
        );
    else {
      const rule = parse(
        await page.getByLabel("aggregate_rule", { exact: true }).inputValue(),
      ) as { where?: unknown };
      rule.where = { op: "eq", field: "event.outcome", value: "failure" };
      await page
        .getByLabel("aggregate_rule", { exact: true })
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
    const original = await response.json();
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
    await expect
      .poll(
        async () =>
          (
            await (
              await page.request.get(`/api/v1/aggregates/${original.id}/points`)
            ).json()
          ).total,
      )
      .toBeGreaterThan(0);
    await page
      .getByRole("button", { name: "Обновить точки", exact: true })
      .click();
    await expect(
      page.getByRole("region", { name: "Сохранённые точки" }),
    ).toContainText("9007199254740993");
    await page.screenshot({
      path: test.info().outputPath(`card-${index}.png`),
      fullPage: true,
    });
    await page
      .getByLabel("Начало исторического окна", { exact: true })
      .fill(config!.anchor);
    await page
      .getByRole("button", {
        name: "Рассчитать историческое окно",
        exact: true,
      })
      .click();
    await expect(
      page.getByText(
        "Историческое окно сохранено. Рабочий курсор не изменён.",
        { exact: true },
      ),
    ).toBeVisible();
    await page.getByRole("button", { name: "Копировать", exact: true }).click();
    await expect(page).toHaveURL(/\/aggregates\/new\?copy=/);
    await page.getByLabel("Имя агрегата", { exact: true }).fill("Копия");
    await page
      .getByLabel("Начало preview-окна", { exact: true })
      .fill(config!.anchor);
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
    await page
      .getByRole("button", { name: "Возобновить", exact: true })
      .click();
    await expect(
      page
        .getByTestId("workspace-content")
        .getByRole("button", { name: "Архивировать", exact: true }),
    ).toBeVisible();
    await page
      .getByRole("button", { name: "Удалить агрегат", exact: true })
      .click();
    await page
      .getByRole("dialog")
      .getByRole("button", { name: "Удалить", exact: true })
      .click();
    await expect(
      page.getByText(
        "Агрегат удалён. Новые расчёты окончательно остановлены, сохранённая история доступна.",
        { exact: true },
      ),
    ).toBeVisible();
    await page.goto("/aggregates");
  }
});
