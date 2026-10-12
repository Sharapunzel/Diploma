import { expect, test } from "@playwright/test";

const rule = {
  format_version: 1,
  variants: [
    {
      key: "event",
      priority: 10,
      when: { kind: "contains", value: "demo" },
      blocks: [
        {
          key: "parser",
          kind: "columns",
          input: "log",
          candidates: [
            { delimiter: " ", columns: ["first", "second", "bad-name"] },
          ],
        },
      ],
    },
  ],
};
const response = (body: unknown, status = 200) => ({
  status,
  contentType: "application/json",
  body: JSON.stringify(body),
});
const session = {
  authenticated: true,
  user: { id: "user", username: "demo", email: null, display_name: "Demo" },
  role: "demo",
  permissions: ["normalizers.read", "normalizers.write", "events.read"],
  authentication_method: "local",
  csrf_token: "csrf",
};

test("create and preview errors point to the exact draft field without losing data", async ({
  page,
}) => {
  let previewCalls = 0;
  await page.route("**/api/v1/**", async (route) => {
    const path = new URL(route.request().url()).pathname.replace("/api/v1", "");
    if (path === "/auth/session") return route.fulfill(response(session));
    if (path === "/normalizers/block-types") return route.fulfill(response([]));
    if (path === "/normalizers" && route.request().method() === "POST")
      return route.fulfill(
        response(
          {
            code: "validation_error",
            message: "secret traceback",
            request_id: "req-create",
            details: {
              errors: [
                {
                  loc: [
                    "body",
                    "rule",
                    "variants",
                    0,
                    "blocks",
                    0,
                    "columns",
                    "candidates",
                    0,
                    "columns",
                    2,
                  ],
                  type: "string_pattern_mismatch",
                  msg: "secret",
                },
                {
                  loc: ["body", "name"],
                  type: "string_too_short",
                  msg: "secret",
                },
              ],
            },
          },
          422,
        ),
      );
    if (path === "/normalizers/preview") {
      previewCalls++;
      return route.fulfill(
        response(
          {
            code: "normalizer_rule_invalid",
            message: "secret traceback",
            request_id: "req-preview",
            details: {
              variant: "event",
              block: "parser",
              parameter: "candidates.0.columns",
            },
          },
          422,
        ),
      );
    }
    return route.fulfill(
      response({ items: [], total: 0, limit: 50, offset: 0 }),
    );
  });
  await page.goto("/normalizers/new");
  await page.getByRole("button", { name: "JSON", exact: true }).click();
  await page
    .getByRole("textbox", { name: "Полный JSON RuleV1" })
    .fill(JSON.stringify(rule));
  await page
    .getByRole("button", { name: "Блоки", exact: true, exact: true })
    .click();
  await page
    .getByRole("textbox", { name: "Имя", exact: true })
    .fill("Draft name");
  await page
    .getByRole("textbox", { name: "log — одна сырая строка" })
    .fill("demo raw event");
  await page.getByRole("button", { name: "Создать" }).click();
  await expect(page.getByText("Ошибки правила: 2")).toBeVisible();
  const issue = page.getByRole("button", {
    name: /rule\.variants\[0\]\.blocks\[0\]\.candidates\[0\]\.columns\[2\]/,
  });
  await expect(issue).toContainText("3-е имя колонки");
  await expect(page.getByText("secret traceback")).toHaveCount(0);
  await issue.click();
  const dialog = page.getByRole("dialog", {
    name: /Настройки блока columns parser/,
  });
  await expect(
    dialog.getByRole("textbox", { name: "Колонки по порядку (через запятую)" }),
  ).toBeFocused();
  await expect(dialog.getByText(/3-е имя колонки/)).toBeVisible();
  await dialog.getByRole("button", { name: "Отмена" }).click();
  await expect(
    page.getByRole("textbox", { name: "Имя", exact: true }),
  ).toHaveValue("Draft name");
  await expect(
    page.getByRole("textbox", { name: "log — одна сырая строка" }),
  ).toHaveValue("demo raw event");
  await page.getByRole("button", { name: "Проверить черновик" }).click();
  await expect(page.getByText("Ошибки правила: 1")).toBeVisible();
  await expect(page.getByText(/Проверьте имена колонок/).first()).toBeVisible();
  expect(previewCalls).toBe(1);
  await page.getByRole("button", { name: "JSON", exact: true }).click();
  expect(
    JSON.parse(
      await page
        .getByRole("textbox", { name: "Полный JSON RuleV1" })
        .inputValue(),
    ),
  ).toEqual(rule);
});

test("condition spaces, contextual help and preview causes stay explicit", async ({
  page,
}) => {
  await page.addInitScript(() => localStorage.setItem("theme", "light"));
  let checks = 0;
  await page.route("**/api/v1/**", async (route) => {
    const path = new URL(route.request().url()).pathname.replace("/api/v1", "");
    if (path === "/auth/session") return route.fulfill(response(session));
    if (path === "/normalizers/block-types") return route.fulfill(response([]));
    if (path === "/ecs/fields")
      return route.fulfill(
        response({ items: [], total: 0, limit: 50, offset: 0 }),
      );
    if (path === "/normalizers/preview") {
      checks++;
      return route.fulfill(
        response(
          checks === 1
            ? {
                status: "partial",
                variant_key: "event",
                ecs_data: {},
                fluent_bit_collected_at: null,
                diagnostics: [
                  {
                    code: "event_time_fallback",
                    variant: "event",
                    block: null,
                    field: "@timestamp",
                  },
                  {
                    code: "optional_mapping_missing",
                    variant: "event",
                    block: "message",
                    field: "message",
                  },
                ],
                trace: [],
              }
            : {
                status: "failed",
                variant_key: null,
                ecs_data: null,
                fluent_bit_collected_at: null,
                diagnostics: [
                  {
                    code: "no_variant_matched",
                    variant: null,
                    block: null,
                    field: null,
                  },
                ],
                trace: [],
              },
        ),
      );
    }
    return route.fulfill(response(null));
  });
  await page.goto("/normalizers/new");
  await page
    .getByRole("button", { name: "Описание редактора нормализатора" })
    .click();
  await expect(page.getByText(/Правило — весь набор логики/)).toBeVisible();
  expect(
    (await page.locator(".ant-popover:visible").boundingBox())!.width,
  ).toBeLessThanOrEqual(360);
  await page.getByRole("button", { name: "Настройки варианта event" }).click();
  const dialog = page.getByRole("dialog");
  await expect(dialog.getByText(/Условие пустое/)).toBeVisible();
  await dialog.getByRole("button", { name: "Применить" }).click();
  await expect(dialog.getByText(/Задайте непустое условие/)).toBeVisible();
  expect(
    await dialog
      .locator(".ant-alert")
      .first()
      .evaluate((element) => getComputedStyle(element).backgroundColor),
  ).toBe("rgb(251, 253, 252)");
  await dialog.getByRole("button", { name: "Справка: Условие" }).focus();
  await page.keyboard.press("Enter");
  await expect(page.getByText(/prefix: строка начинается/)).toBeVisible();
  await dialog.getByRole("button", { name: "Справка: Условие" }).click();
  await dialog
    .getByRole("textbox", { name: "Значение условия" })
    .fill(" ^[0-9]{4} ");
  await dialog.locator(".ant-select-selector").first().click();
  await page
    .locator(".ant-select-dropdown:visible .ant-select-item-option")
    .filter({ hasText: "regex" })
    .click();
  await expect(
    dialog.getByText(/В условии есть пробелы по краям/),
  ).toBeVisible();
  await dialog.getByRole("button", { name: "Применить" }).click();
  await expect(dialog.getByText(/Выберите, убрать ли пробелы/)).toBeVisible();
  await dialog.getByRole("button", { name: "Убрать пробелы по краям" }).click();
  await dialog.getByRole("button", { name: "Применить" }).click();
  await expect(page.getByText("Условие regex: ^[0-9]{4}")).toBeVisible();
  await page.getByRole("button", { name: "Справка: Типы блоков" }).click();
  await expect(page.getByText(/columns разбивает строку/)).toBeVisible();
  await page.getByRole("button", { name: "Справка: Типы блоков" }).click();
  await page
    .getByRole("button", { name: "Настройки блока map_ecs message" })
    .click();
  await page
    .getByRole("dialog")
    .getByRole("button", { name: "Справка: Источник значения" })
    .click();
  await expect(page.getByText(/refs \+ join объединяет/)).toBeVisible();
  await page
    .getByRole("dialog")
    .getByRole("button", { name: "Отмена" })
    .click();
  await page
    .getByRole("textbox", { name: "log — одна сырая строка" })
    .fill("demo");
  await page.getByRole("button", { name: "Проверить черновик" }).click();
  await expect(
    page.getByText(/Не получено @timestamp из правила/),
  ).toBeVisible();
  await expect(
    page.getByText(/Не получено необязательное поле ECS/),
  ).toBeVisible();
  await page
    .getByRole("textbox", { name: "log — одна сырая строка" })
    .fill("other");
  await expect(page.getByText("Результат устарел")).toBeVisible();
  await page.getByRole("button", { name: "Проверить черновик" }).click();
  await expect(page.getByText(/Ни одно условие не подошло/)).toBeVisible();
  expect(checks).toBe(2);
});

test("PATCH reports every field error and keeps the edited JSON", async ({
  page,
}) => {
  const id = "00000000-0000-4000-8000-000000000091";
  await page.route("**/api/v1/**", async (route) => {
    const path = new URL(route.request().url()).pathname.replace("/api/v1", "");
    if (path === "/auth/session") return route.fulfill(response(session));
    if (path === "/normalizers/block-types") return route.fulfill(response([]));
    if (path === `/normalizers/${id}` && route.request().method() === "GET")
      return route.fulfill(
        response({
          id,
          name: "Original",
          description: null,
          rule,
          version: 1,
          rule_status: "ready",
          created_by_user_id: null,
          updated_by_user_id: null,
          created_at: "2026-01-01T00:00:00Z",
          updated_at: "2026-01-01T00:00:00Z",
        }),
      );
    if (path === `/normalizers/${id}` && route.request().method() === "PATCH")
      return route.fulfill(
        response(
          {
            code: "validation_error",
            message: "secret traceback",
            request_id: "req-patch",
            details: {
              errors: [
                { loc: ["body", "name"], type: "string_too_short" },
                {
                  loc: [
                    "body",
                    "rule",
                    "variants",
                    0,
                    "blocks",
                    0,
                    "columns",
                    "candidates",
                    0,
                    "columns",
                    2,
                  ],
                  type: "string_pattern_mismatch",
                },
              ],
            },
          },
          422,
        ),
      );
    return route.fulfill(response(null));
  });
  await page.goto(`/normalizers/${id}/edit`);
  await page.getByRole("textbox", { name: "Имя", exact: true }).fill("Edited");
  await page.getByRole("button", { name: "JSON", exact: true }).click();
  const editor = page.getByRole("textbox", { name: "Полный JSON RuleV1" });
  const draft = await editor.inputValue();
  const patch = page.waitForResponse(
    (response) =>
      response.url().includes(`/normalizers/${id}`) &&
      response.request().method() === "PATCH",
  );
  await page.getByRole("button", { name: "Сохранить" }).click();
  expect((await patch).status()).toBe(422);
  await expect(page.getByText("Ошибки правила: 2")).toBeVisible();
  await expect(
    page.getByRole("button", {
      name: /rule\.variants\[0\]\.blocks\[0\].*columns\[2\]/,
    }),
  ).toContainText("3-е имя колонки");
  await expect(page.getByText("secret traceback")).toHaveCount(0);
  await expect(editor).toHaveValue(draft);
  await expect(
    page.getByRole("textbox", { name: "Имя", exact: true }),
  ).toHaveValue("Edited");
});

test("editing a normalizer used by an active source warns before saving without another confirmation", async ({
  page,
}) => {
  const id = "00000000-0000-4000-8000-000000000099";
  let patches = 0;
  const validRule = {
    format_version: 1,
    variants: [
      {
        key: "event",
        priority: 10,
        when: { kind: "contains", value: "demo" },
        blocks: [
          {
            key: "message",
            kind: "map_ecs",
            target: "message",
            source: { ref: "log" },
            required: true,
          },
        ],
      },
    ],
  };
  const item = {
    id,
    name: "Active rule",
    description: null,
    rule: validRule,
    version: 1,
    rule_status: "ready",
    created_by_user_id: null,
    updated_by_user_id: null,
    created_at: "2026-01-01T00:00:00Z",
    updated_at: "2026-01-01T00:00:00Z",
  };
  await page.route("**/api/v1/**", async (route) => {
    const path = new URL(route.request().url()).pathname.replace("/api/v1", "");
    if (path === "/auth/session")
      return route.fulfill(
        response({
          ...session,
          permissions: [...session.permissions, "sources.read"],
        }),
      );
    if (path === "/normalizers/block-types") return route.fulfill(response([]));
    if (path === `/normalizers/${id}` && route.request().method() === "GET")
      return route.fulfill(response(item));
    if (path === "/sources")
      return route.fulfill(
        response({
          items: [
            {
              id: "source",
              name: "Live source",
              is_enabled: true,
              normalizer_id: id,
            },
          ],
          total: 1,
          limit: 50,
          offset: 0,
        }),
      );
    if (path === `/normalizers/${id}` && route.request().method() === "PATCH") {
      patches++;
      return route.fulfill(
        response({ ...item, ...route.request().postDataJSON(), version: 2 }),
      );
    }
    return route.fulfill(response(null));
  });
  await page.goto(`/normalizers/${id}/edit`);
  await expect(
    page.getByText(/назначен работающему Kafka-источнику/),
  ).toBeVisible();
  await expect(page.getByText(/только будущие сообщения/)).toBeVisible();
  await page
    .getByRole("textbox", { name: "Имя", exact: true })
    .fill("Updated rule");
  await page.getByRole("button", { name: "Сохранить" }).click();
  await expect.poll(() => patches).toBe(1);
  await expect(page.getByRole("dialog")).toHaveCount(0);
});
