import { expect, test } from "@playwright/test";

const id = "00000000-0000-4000-8000-000000000090";
const session = (permissions: string[]) => ({
  authenticated: true,
  user: {
    id: "00000000-0000-4000-8000-000000000001",
    username: "demo",
    email: null,
    display_name: "Demo",
  },
  role: "demo",
  permissions,
  authentication_method: "local",
  csrf_token: "csrf",
});
const rule = {
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
const record = (version = 1) => ({
  id,
  name: "Demo rule",
  description: "Synthetic",
  rule,
  version,
  rule_status: "ready",
  created_by_user_id: null,
  updated_by_user_id: null,
  created_at: "2026-01-01T00:00:00Z",
  updated_at: "2026-01-01T00:00:00Z",
});
const pageData = (items: unknown[], offset = 0) => ({
  items,
  total: items.length,
  limit: 50,
  offset,
});
const blockTypes = ["regex", "template", "columns", "json", "map_ecs"].map(
  (kind) => ({
    kind,
    description: kind,
    parameters: [],
    input: "log",
    output: "fields",
    limits: {},
  }),
);

test("admin edits a draft, previews only log and confirms deletion", async ({
  page,
}) => {
  let saved = record();
  const mutations: { path: string; body: unknown }[] = [];
  await page.route("**/api/v1/**", async (route) => {
    const url = new URL(route.request().url());
    const path = url.pathname.replace("/api/v1", "");
    const method = route.request().method();
    let body: unknown = null;
    let status = 200;
    if (path === "/auth/session")
      body = session([
        "normalizers.read",
        "normalizers.write",
        "events.read",
        "sources.read",
      ]);
    else if (path === "/normalizers" && method === "GET")
      body = pageData([saved]);
    else if (path === "/normalizers/block-types") body = blockTypes;
    else if (path === `/normalizers/${id}` && method === "GET") body = saved;
    else if (path === "/sources") body = pageData([]);
    else if (path === "/ecs/fields")
      body = pageData([
        {
          name: "message",
          type: "match_only_text",
          description: "Message",
          is_array: false,
          mappable: true,
        },
      ]);
    else if (path === "/normalizers/preview") {
      const payload = route.request().postDataJSON();
      mutations.push({ path, body: payload });
      body = {
        status: "partial",
        variant_key: "event",
        ecs_data: {
          event: { original: payload.sample.log },
          message: payload.sample.log,
        },
        fluent_bit_collected_at: payload.sample.timestamp,
        diagnostics: [
          {
            code: "event_time_fallback",
            variant: "event",
            block: null,
            field: null,
          },
        ],
        trace: [
          {
            variant: "event",
            block: "message",
            candidate: null,
            result: "mapped",
          },
        ],
      };
    } else if (path === `/normalizers/${id}` && method === "PATCH") {
      const payload = route.request().postDataJSON();
      mutations.push({ path, body: payload });
      saved = { ...saved, ...payload, version: 2 };
      body = saved;
    } else if (path === `/normalizers/${id}` && method === "DELETE") {
      mutations.push({ path, body: null });
      status = 204;
    }
    await route.fulfill({
      status,
      contentType: "application/json",
      body: status === 204 ? "" : JSON.stringify(body),
    });
  });
  await page.goto("/normalizers");
  await expect(page.getByText("Demo rule")).toBeVisible();
  await page.getByRole("button", { name: "Изменить" }).click();
  await expect(
    page.getByRole("heading", { name: "Редактор нормализатора" }),
  ).toBeVisible();
  await expect(
    page.getByRole("menuitem", { name: "Нормализаторы" }),
  ).toHaveClass(/ant-menu-item-selected/);
  const layout = await page.getByLabel("Отладка").evaluate((right) => {
    const left = right.previousElementSibling as HTMLElement;
    const leftRect = left.getBoundingClientRect();
    const rightRect = right.getBoundingClientRect();
    return {
      top: leftRect.top,
      headerBottom: document
        .querySelector('[data-testid="workspace-header"]')!
        .getBoundingClientRect().bottom,
      leftBottom: leftRect.bottom,
      rightBottom: rightRect.bottom,
      gap: rightRect.left - leftRect.right,
      overflow: getComputedStyle(left).overflowY,
      scrolls: left.scrollHeight > left.clientHeight,
      viewport: window.innerHeight,
    };
  });
  expect(layout.leftBottom).toBeLessThanOrEqual(layout.viewport);
  expect(layout.rightBottom).toBeLessThanOrEqual(layout.viewport);
  expect(layout.top - layout.headerBottom).toBeLessThanOrEqual(24);
  expect(layout.gap).toBeGreaterThanOrEqual(16);
  expect(layout.overflow).toBe("auto");
  expect(layout.scrolls).toBe(true);
  await page.getByRole("button", { name: "JSON", exact: true }).click();
  const editor = page.getByRole("textbox", { name: "Полный JSON RuleV1" });
  expect((await editor.boundingBox())!.height).toBeGreaterThan(400);
  await editor.fill('{"format_version":1,');
  await page.getByRole("button", { name: "JSON", exact: true }).click();
  await expect(editor).toHaveValue('{"format_version":1,');
  await page.getByRole("button", { name: "Блоки", exact: true }).click();
  await expect(editor).toHaveValue('{"format_version":1,');
  await expect(page.getByText(/Синтаксис JSON:/)).toBeVisible();
  const changedRule = structuredClone(rule);
  changedRule.variants[0].key = "corrected";
  await editor.fill(JSON.stringify(changedRule));
  await page.getByRole("button", { name: "JSON", exact: true }).click();
  await expect(editor).toHaveValue(JSON.stringify(changedRule));
  await page.getByRole("button", { name: "Блоки", exact: true }).click();
  await expect(
    page.getByRole("button", { name: "Настройки варианта corrected" }),
  ).toBeVisible();
  await page.getByRole("button", { name: "JSON", exact: true }).click();
  expect(JSON.parse(await editor.inputValue())).toEqual(changedRule);
  await page.getByRole("button", { name: "Блоки", exact: true }).click();
  await expect(page.getByRole("textbox", { name: "timestamp" })).toHaveCount(0);
  await page
    .getByRole("textbox", { name: "log — одна сырая строка" })
    .fill("demo synthetic");
  const previewStart = Date.now();
  await page.getByRole("button", { name: "Проверить черновик" }).dblclick();
  await expect(page.getByText("Частично обработано (partial)")).toBeVisible();
  expect(
    mutations.filter((item) => item.path === "/normalizers/preview"),
  ).toHaveLength(1);
  expect((mutations[0].body as { sample: { log: string } }).sample.log).toBe(
    "demo synthetic",
  );
  const sentTime = (mutations[0].body as { sample: { timestamp: string } })
    .sample.timestamp;
  expect(Date.parse(sentTime)).toBeGreaterThanOrEqual(previewStart);
  expect(Date.parse(sentTime)).toBeLessThanOrEqual(Date.now());
  await expect(
    page.getByText(`Служебное время ручной проверки: ${sentTime}`),
  ).toBeVisible();
  await expect(
    page.getByText("Поле сопоставлено (mapped)", { exact: false }),
  ).toBeVisible();
  const ecsWindow = page
    .getByText("ECS", { exact: true })
    .locator("xpath=following-sibling::pre[1]");
  await expect(ecsWindow).toHaveAttribute("class", /preview/);
  expect(
    await ecsWindow.evaluate(
      (element) => getComputedStyle(element).borderTopWidth,
    ),
  ).not.toBe("0px");
  await page.getByRole("button", { name: "Сохранить" }).click();
  await expect(
    page.getByText("Нормализатор сохранён, версия 2."),
  ).toBeVisible();
  expect(
    await page
      .getByText("Нормализатор сохранён, версия 2.")
      .evaluate(
        (element) =>
          getComputedStyle(element.closest(".ant-alert")!).backgroundColor,
      ),
  ).not.toBe("rgba(0, 0, 0, 0)");
  expect(
    await page
      .getByText("Нормализатор сохранён, версия 2.")
      .evaluate(
        (element) =>
          getComputedStyle(element.closest(".ant-alert")!.parentElement!)
            .position,
      ),
  ).toBe("fixed");
  expect((mutations[1].body as { version: number }).version).toBe(1);
  await page.getByRole("link", { name: "← К списку" }).click();
  await page.getByRole("button", { name: "Удалить" }).first().click();
  await expect(
    page.getByRole("dialog").getByText(/источники выключатся/),
  ).toBeVisible();
  expect(
    mutations.filter(
      (item) => item.path === `/normalizers/${id}` && item.body === null,
    ),
  ).toHaveLength(0);
  await page
    .getByRole("dialog")
    .getByRole("button", { name: "Отмена" })
    .click();
  await page.getByRole("button", { name: "Удалить" }).first().click();
  await page
    .getByRole("dialog")
    .getByRole("button", { name: "Удалить" })
    .click();
  await expect(page.getByText(/Нормализатор удалён\./)).toBeVisible();
});

test("read-only user cannot call mutation or preview APIs on mobile", async ({
  page,
}) => {
  const mutations: string[] = [];
  await page.setViewportSize({ width: 320, height: 800 });
  await page.route("**/api/v1/**", async (route) => {
    const path = new URL(route.request().url()).pathname.replace("/api/v1", "");
    if (route.request().method() !== "GET") mutations.push(path);
    const body =
      path === "/auth/session"
        ? session(["normalizers.read"])
        : path === "/normalizers"
          ? pageData([record()])
          : path === `/normalizers/${id}`
            ? record()
            : null;
    await route.fulfill({
      contentType: "application/json",
      body: JSON.stringify(body),
    });
  });
  await page.goto(`/normalizers/${id}`);
  await expect(
    page.getByRole("heading", { name: "Просмотр нормализатора" }),
  ).toBeVisible();
  const headerFits = await page
    .getByTestId("workspace-header")
    .evaluate((header) => {
      const title = header.querySelector('[data-testid="workspace-title"]')!;
      const back = header.querySelector('a[href="/normalizers"]')!;
      const titleRect = title.getBoundingClientRect();
      const backRect = back.getBoundingClientRect();
      return {
        titleAfterBack: titleRect.left >= backRect.right,
        titleInsideHeader:
          titleRect.right <= header.getBoundingClientRect().right,
        backLabelHidden:
          getComputedStyle(back.lastElementChild!).display === "none",
      };
    });
  expect(headerFits).toEqual({
    titleAfterBack: true,
    titleInsideHeader: true,
    backLabelHidden: true,
  });
  await expect(page.getByRole("button", { name: "Блоки" })).toHaveCount(0);
  await expect(
    page.getByRole("button", { name: "JSON", exact: true }),
  ).toHaveCount(0);
  await expect(
    page.getByRole("button", { name: "Копировать JSON" }),
  ).toHaveCount(0);
  await expect(page.getByRole("button", { name: "Скачать .json" })).toHaveCount(
    0,
  );
  await expect(page.getByRole("button", { name: "Промпт для ИИ" })).toHaveCount(
    0,
  );
  await expect(
    page.getByRole("button", { name: "Проверить черновик" }),
  ).toHaveCount(0);
  await expect(
    page.getByText("Серверная проверка доступна только"),
  ).toBeVisible();
  expect(mutations).toEqual([]);
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
  ).toBe(true);
});

test("invalid import and version conflict preserve the local draft", async ({
  page,
}) => {
  let updates = 0;
  await page.route("**/api/v1/**", async (route) => {
    const path = new URL(route.request().url()).pathname.replace("/api/v1", "");
    let body: unknown = null;
    let status = 200;
    if (path === "/auth/session")
      body = session(["normalizers.read", "normalizers.write", "events.read"]);
    else if (path === "/normalizers/block-types") body = blockTypes;
    else if (
      path === `/normalizers/${id}` &&
      route.request().method() === "GET"
    )
      body = record();
    else if (path === "/ecs/fields") body = pageData([]);
    else if (
      path === `/normalizers/${id}` &&
      route.request().method() === "PATCH"
    ) {
      updates++;
      status = 409;
      body = {
        code: "version_conflict",
        message: "Conflict",
        request_id: "test-id",
        details: null,
      };
    }
    await route.fulfill({
      status,
      contentType: "application/json",
      body: JSON.stringify(body),
    });
  });
  await page.goto(`/normalizers/${id}/edit`);
  await page.getByRole("button", { name: "JSON", exact: true }).click();
  const editor = page.getByRole("textbox", { name: "Полный JSON RuleV1" });
  const original = await editor.inputValue();
  await page.locator('input[type="file"]').setInputFiles({
    name: "broken.json",
    mimeType: "application/json",
    buffer: Buffer.from('{"format_version":2}'),
  });
  await expect(page.getByText(/Замечания к JSON: /)).toBeVisible();
  expect(await editor.inputValue()).toBe('{"format_version":2}');
  await editor.fill(original);
  await editor.blur();
  await page
    .getByRole("textbox", { name: "Имя", exact: true })
    .fill("My unsaved draft");
  await page.getByRole("button", { name: "Сохранить" }).click();
  await expect(page.getByText(/Черновик сохранён локально/)).toBeVisible();
  await expect(
    page.getByRole("textbox", { name: "Имя", exact: true }),
  ).toHaveValue("My unsaved draft");
  await expect(page.getByRole("button", { name: "Сохранить" })).toBeDisabled();
  expect(updates).toBe(1);
  await page.getByRole("link", { name: "← К списку" }).click();
  await expect(
    page.getByRole("dialog").getByText(/Несохранённые изменения/),
  ).toBeVisible();
  await page
    .getByRole("dialog")
    .getByRole("button", { name: "Отмена" })
    .click();
  await expect(
    page.getByRole("textbox", { name: "Имя", exact: true }),
  ).toHaveValue("My unsaved draft");
  await page.getByRole("link", { name: "Нормализаторы" }).first().click();
  await expect(
    page.getByRole("dialog").getByText(/Несохранённые изменения/),
  ).toBeVisible();
  await page
    .getByRole("dialog")
    .getByRole("button", { name: "Отмена" })
    .click();
  await expect(
    page.getByRole("textbox", { name: "Имя", exact: true }),
  ).toHaveValue("My unsaved draft");
  await page.goBack();
  await expect(
    page.getByRole("dialog").getByText(/Несохранённые изменения/),
  ).toBeVisible();
  await page
    .getByRole("dialog")
    .getByRole("button", { name: "Отмена" })
    .click();
  await expect(
    page.getByRole("textbox", { name: "Имя", exact: true }),
  ).toHaveValue("My unsaved draft");
});
