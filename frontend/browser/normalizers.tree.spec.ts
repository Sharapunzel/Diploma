import { expect, test } from "@playwright/test";

const id = "00000000-0000-4000-8000-000000000090";
const rule = {
  format_version: 1,
  variants: [
    {
      key: "create",
      priority: 10,
      when: { kind: "contains", value: "create" },
      blocks: [
        {
          key: "parser",
          kind: "regex",
          input: "log",
          candidates: [
            { pattern: "(?P<user>.+)", set: { action: "create" } },
            { pattern: "(?P<actor>.+)" },
          ],
        },
        {
          key: "other",
          kind: "columns",
          input: "log",
          candidates: [{ delimiter: " ", columns: ["date", "time"] }],
        },
        {
          key: "nested",
          kind: "json",
          input: "parser.user",
          candidates: [{ requires: [], extract: { name: "name" } }],
        },
        {
          key: "ecs",
          kind: "map_ecs",
          target: "user.name",
          source: { refs: ["nested.name", "parser.actor"], join: " " },
          required: true,
        },
        {
          key: "constant",
          kind: "map_ecs",
          target: "event.action",
          source: { literal: "create" },
          required: false,
        },
      ],
    },
    {
      key: "delete",
      priority: 20,
      when: { kind: "contains", value: "delete" },
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

function session(write: boolean) {
  return {
    authenticated: true,
    user: { id: "user", username: "demo", email: null, display_name: "Demo" },
    role: "demo",
    permissions: [
      "normalizers.read",
      ...(write ? ["normalizers.write", "events.read"] : []),
    ],
    authentication_method: "local",
    csrf_token: "csrf",
  };
}

async function mockEditor(
  page: import("@playwright/test").Page,
  write: boolean,
  previews: unknown[],
) {
  await page.route("**/api/v1/**", async (route) => {
    const path = new URL(route.request().url()).pathname.replace("/api/v1", "");
    let body: unknown = null;
    if (path === "/auth/session") body = session(write);
    else if (path === `/normalizers/${id}`)
      body = {
        id,
        name: "Tree rule",
        description: null,
        rule,
        version: 1,
        rule_status: "ready",
        created_by_user_id: null,
        updated_by_user_id: null,
        created_at: "2026-01-01T00:00:00Z",
        updated_at: "2026-01-01T00:00:00Z",
      };
    else if (path === "/normalizers/block-types")
      body = ["regex", "template", "columns", "json", "map_ecs"].map(
        (kind) => ({
          kind,
          description: kind,
          parameters: [],
          input: "log",
          output: "fields",
          limits: {},
        }),
      );
    else if (path === "/ecs/fields")
      body = { items: [], total: 0, limit: 50, offset: 0 };
    else if (path === "/normalizers/preview") {
      previews.push(route.request().postDataJSON());
      body = {
        status: "partial",
        variant_key: "create",
        ecs_data: {},
        fluent_bit_collected_at: null,
        diagnostics: [],
        trace: [],
      };
    }
    await route.fulfill({
      contentType: "application/json",
      body: JSON.stringify(body),
    });
  });
}

test("tree follows references and modal edits preserve the rule draft", async ({
  page,
}) => {
  const previews: unknown[] = [];
  await mockEditor(page, true, previews);
  await page.goto(`/normalizers/${id}/edit`);
  await expect(
    page.getByRole("button", { name: "Настройки варианта create" }),
  ).toBeVisible();
  await expect(
    page.getByRole("button", { name: "Настройки варианта delete" }),
  ).toBeVisible();
  await expect(page.getByText("Приоритет 10")).toBeVisible();
  await expect(page.getByText("Приоритет 20")).toBeVisible();
  await expect(
    page.getByText("Входы: nested.name, parser.actor"),
  ).toBeVisible();
  await expect(page.getByText("Источник: константа")).toBeVisible();
  await expect(
    page.getByText(
      /Возможные выходы: parser.action, parser.user, parser.match, parser.actor/,
    ),
  ).toBeVisible();
  const parent = await page
    .getByRole("button", { name: "Настройки блока json nested" })
    .evaluate((button) => {
      const own = button.closest("li");
      return own?.parentElement?.closest("li")?.querySelector("strong")
        ?.textContent;
    });
  expect(parent).toContain("regex · parser");
  await page.getByRole("button", { name: "Свернуть вариант create" }).click();
  await expect(
    page.getByRole("button", { name: "Настройки блока regex parser" }),
  ).toHaveCount(0);
  await page.getByRole("button", { name: "Развернуть вариант create" }).click();
  await page.getByRole("button", { name: "Настройки варианта create" }).click();
  const variantDialog = page.getByRole("dialog", {
    name: /Настройки варианта create/,
  });
  await variantDialog
    .getByRole("button", { name: "Удалить", exact: true })
    .click();
  const variantDelete = page.getByRole("dialog", {
    name: "Удалить вариант из черновика?",
  });
  expect(
    await variantDelete.evaluate((element) =>
      Number(getComputedStyle(element.closest(".ant-modal-wrap")!).zIndex),
    ),
  ).toBeGreaterThan(
    await variantDialog.evaluate((element) =>
      Number(getComputedStyle(element.closest(".ant-modal-wrap")!).zIndex),
    ),
  );
  await variantDelete.getByRole("button", { name: "Отмена" }).click();
  await variantDialog
    .getByRole("textbox", { name: "Значение условия" })
    .fill("temporary");
  await variantDialog.getByRole("button", { name: "Отмена" }).click();
  await expect(page.getByText("Условие contains: create")).toBeVisible();
  await page.getByRole("button", { name: "Настройки варианта create" }).click();
  await variantDialog
    .getByRole("textbox", { name: "Значение условия" })
    .fill("create-user");
  await variantDialog.getByRole("button", { name: "Применить" }).click();
  await expect(page.getByText("Условие contains: create-user")).toBeVisible();
  await page
    .getByRole("button", { name: "Настройки блока map_ecs ecs" })
    .click();
  const mapDialog = page.getByRole("dialog");
  await mapDialog.getByRole("textbox", { name: "Разделитель" }).fill("-");
  await mapDialog.getByRole("button", { name: "Применить" }).click();
  await page
    .getByRole("button", { name: "Настройки блока columns other" })
    .click();
  await page
    .getByRole("dialog", { name: /Настройки блока columns other/ })
    .getByRole("button", { name: "Удалить", exact: true })
    .click();
  const deleteDialog = page.getByRole("dialog", {
    name: "Удалить блок из черновика?",
  });
  expect(
    await deleteDialog.evaluate((element) =>
      Number(getComputedStyle(element.closest(".ant-modal-wrap")!).zIndex),
    ),
  ).toBeGreaterThan(
    await page
      .getByRole("dialog", { name: /Настройки блока columns other/ })
      .evaluate((element) =>
        Number(getComputedStyle(element.closest(".ant-modal-wrap")!).zIndex),
      ),
  );
  await deleteDialog.getByRole("button", { name: "Отмена" }).click();
  await expect(
    page.getByRole("button", { name: "Настройки блока columns other" }),
  ).toBeVisible();
  await page
    .getByRole("dialog", { name: /Настройки блока columns other/ })
    .getByRole("button", { name: "Отмена" })
    .click();
  const trigger = page.getByRole("button", {
    name: "Настройки блока regex parser",
  });
  await trigger.focus();
  await page.keyboard.press("Enter");
  const dialog = page.getByRole("dialog");
  await expect(
    dialog.getByRole("textbox", { name: "Шаблон RE2" }).first(),
  ).toBeVisible();
  await dialog.getByRole("textbox", { name: "Ключ блока" }).fill("changed");
  await page.keyboard.press("Escape");
  await expect(dialog).toHaveCount(0);
  await expect(trigger).toBeFocused();
  await trigger.click();
  await dialog.getByRole("textbox", { name: "Ключ блока" }).fill("changed");
  await dialog
    .getByRole("textbox", { name: "Шаблон RE2" })
    .first()
    .fill("(?P<account>.+)");
  await dialog
    .getByRole("textbox", { name: "Константы set (JSON-объект)" })
    .first()
    .fill('{"action":"registered"}');
  await dialog.getByRole("button", { name: "Применить" }).click();
  await expect(
    page.getByRole("button", { name: "Настройки блока regex changed" }),
  ).toBeVisible();
  await expect(page.getByText(/Ссылка parser.user не указывает/)).toBeVisible();
  await page.getByRole("button", { name: "JSON", exact: true }).click();
  const json = JSON.parse(
    await page
      .getByRole("textbox", { name: "Полный JSON RuleV1" })
      .inputValue(),
  );
  expect(json.variants[0].blocks[0].key).toBe("changed");
  expect(json.variants[0].when.value).toBe("create-user");
  expect(json.variants[0].blocks[0].candidates[0].pattern).toBe(
    "(?P<account>.+)",
  );
  expect(json.variants[0].blocks[0].candidates[0].set.action).toBe(
    "registered",
  );
  expect(json.variants[0].blocks[3].source.join).toBe("-");
  await page.getByRole("button", { name: "Блоки", exact: true }).click();
  await page
    .getByRole("textbox", { name: "log — одна сырая строка" })
    .fill("create demo");
  await page.getByRole("button", { name: "Проверить черновик" }).click();
  await expect(page.getByText("Частично обработано (partial)")).toBeVisible();
  expect(
    (previews[0] as { rule: typeof rule }).rule.variants[0].blocks[0].key,
  ).toBe("changed");
});

test("read-only mobile tree exposes settings without mutations or document overflow", async ({
  page,
}) => {
  const previews: unknown[] = [];
  const mutationPaths: string[] = [];
  await page.setViewportSize({ width: 320, height: 800 });
  await mockEditor(page, false, previews);
  page.on("request", (request) => {
    if (request.method() !== "GET") mutationPaths.push(request.url());
  });
  await page.goto(`/normalizers/${id}`);
  await expect(
    page.getByRole("button", { name: "Настройки блока regex parser" }),
  ).toBeVisible();
  await page
    .getByRole("button", { name: "Настройки блока regex parser" })
    .click();
  const dialog = page.getByRole("dialog");
  await expect(
    dialog.getByRole("textbox", { name: "Ключ блока" }),
  ).toBeDisabled();
  await expect(dialog.getByRole("button", { name: "Применить" })).toHaveCount(
    0,
  );
  await dialog.getByRole("button", { name: "Закрыть" }).click();
  expect(mutationPaths).toEqual([]);
  expect(previews).toEqual([]);
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
  ).toBe(true);
});

test("tree connectors stop at the last child on desktop and mobile", async ({
  page,
}) => {
  await mockEditor(page, true, []);
  for (const [width, theme] of [
    [1280, "light"],
    [320, "dark"],
  ] as const) {
    await page.setViewportSize({ width, height: 800 });
    await page.addInitScript(
      (choice) => localStorage.setItem("theme", choice),
      theme,
    );
    await page.goto(`/normalizers/${id}/edit`);
    await expect(page.locator("html")).toHaveAttribute("data-theme", theme);
    const geometry = await page
      .locator('ul[class*="treeChildren"]')
      .first()
      .evaluate((list) => {
        const last = list.lastElementChild!;
        const line = getComputedStyle(last, "::after");
        const horizontal = getComputedStyle(last, "::before");
        const parent = list.firstElementChild!;
        const parentCard = parent.firstElementChild!;
        const childList = parent.querySelector("ul");
        const childCard = childList?.firstElementChild?.firstElementChild;
        return {
          trunk: getComputedStyle(list).borderLeftWidth,

          height: parseFloat(line.height),
          corner: parseFloat(horizontal.top),
          overflow: document.documentElement.scrollWidth > innerWidth,
          nestedGap: childCard
            ? childCard.getBoundingClientRect().top -
              parentCard.getBoundingClientRect().bottom
            : null,
        };
      });
    expect(geometry.trunk).toBe("0px");

    expect(geometry.height).toBe(geometry.corner + 10);
    expect(geometry.overflow).toBe(false);
    expect(geometry.nestedGap).not.toBeNull();
    expect(geometry.nestedGap!).toBeGreaterThanOrEqual(10);
    await page.getByRole("button", { name: "Свернуть вариант create" }).click();
    await expect(
      page.getByRole("button", { name: "Настройки блока regex parser" }),
    ).toHaveCount(0);
  }
});
