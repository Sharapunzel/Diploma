import { expect, test } from "@playwright/test";

test("mobile editor exposes all extractor blocks and paged ECS fields", async ({
  page,
}) => {
  await page.setViewportSize({ width: 320, height: 800 });
  const fields = Array.from({ length: 51 }, (_, index) => ({
    name: index === 50 ? "message" : `labels.field${index}`,
    type: "keyword",
    description: "Synthetic field",
    is_array: false,
    mappable: true,
  }));
  const offsets: number[] = [];
  const fieldQueries: string[] = [];
  await page.route("**/api/v1/**", async (route) => {
    const url = new URL(route.request().url());
    const path = url.pathname.replace("/api/v1", "");
    let body: unknown = null;
    if (path === "/auth/session")
      body = {
        authenticated: true,
        user: {
          id: "00000000-0000-4000-8000-000000000001",
          username: "demo",
          email: null,
          display_name: "Demo",
        },
        role: "demo",
        permissions: ["normalizers.read", "normalizers.write", "events.read"],
        authentication_method: "local",
        csrf_token: "csrf",
      };
    else if (path === "/ecs/fields") {
      const offset = Number(url.searchParams.get("offset") || 0);
      const query = url.searchParams.get("q") || "";
      offsets.push(offset);
      fieldQueries.push(query);
      const matches = query === "message" ? fields.slice(50) : fields;
      body = {
        items: matches.slice(offset, offset + 50),
        total: matches.length,
        limit: 50,
        offset,
      };
    } else if (path.startsWith("/ecs/fields/"))
      body =
        fields.find(
          (field) =>
            field.name ===
            decodeURIComponent(path.slice("/ecs/fields/".length)),
        ) || null;
    else if (path === "/normalizers")
      body = { items: [], total: 0, limit: 50, offset: 0 };
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
    await route.fulfill({
      contentType: "application/json",
      body: JSON.stringify(body),
    });
  });
  await page.goto("/normalizers/new");
  await expect(
    page.getByRole("heading", { name: "Новый нормализатор" }),
  ).toBeVisible();
  const mobileLayout = await page.getByLabel("Отладка").evaluate((right) => {
    const left = right.previousElementSibling as HTMLElement;
    const leftRect = left.getBoundingClientRect();
    const rightRect = right.getBoundingClientRect();
    return {
      gap: rightRect.top - leftRect.bottom,
      overflow: getComputedStyle(left).overflowY,
      scrolls: left.scrollHeight > left.clientHeight,
      height: left.clientHeight,
      viewport: window.innerHeight,
      documentScrolls:
        document.getElementById("main")!.scrollHeight >
        document.getElementById("main")!.clientHeight,
    };
  });
  expect(mobileLayout.gap).toBeGreaterThanOrEqual(16);
  expect(mobileLayout.overflow).toBe("visible");
  expect(mobileLayout.scrolls).toBe(false);
  expect(mobileLayout.height).toBeGreaterThan(mobileLayout.viewport);
  expect(mobileLayout.documentScrolls).toBe(true);
  await page
    .getByRole("button", { name: "Настройки блока map_ecs message" })
    .click();
  const fieldPicker = page
    .getByRole("dialog")
    .getByRole("combobox", { name: "Поле ECS" });
  await fieldPicker.focus();
  await page.keyboard.press("ArrowDown");
  await expect(
    page.getByText("Начните вводить название или описание поля"),
  ).toBeVisible();
  expect(offsets).toHaveLength(0);
  await fieldPicker.fill("Synthetic");
  await expect.poll(() => fieldQueries.includes("Synthetic")).toBe(true);
  await expect(
    page
      .locator(".ant-select-dropdown:visible .ant-select-item-option")
      .first(),
  ).toBeVisible();
  await page
    .locator(".ant-select-dropdown:visible .rc-virtual-list-holder")
    .evaluate((element) => {
      element.scrollTop = element.scrollHeight;
      element.dispatchEvent(new Event("scroll", { bubbles: true }));
    });
  await expect.poll(() => offsets.includes(50)).toBe(true);
  await fieldPicker.fill("message");
  await expect(
    page
      .locator(".ant-select-dropdown:visible .ant-select-item-option")
      .filter({ hasText: "message" }),
  ).toBeVisible();
  const ecsWidth = await page
    .locator(".ant-select-dropdown:visible")
    .evaluate((element) => {
      const dialog = document.querySelector<HTMLElement>(".ant-modal-content")!;
      return {
        dropdown: element.getBoundingClientRect().width,
        dialog: dialog.getBoundingClientRect().width,
      };
    });
  expect(ecsWidth.dropdown).toBeLessThanOrEqual(ecsWidth.dialog);
  expect(
    await page
      .getByRole("dialog")
      .locator("[class*='settingsBody']")
      .evaluate((element) => element.scrollWidth <= element.clientWidth),
  ).toBe(true);
  await page
    .locator(".ant-select-dropdown:visible .ant-select-item-option")
    .filter({ hasText: "message" })
    .first()
    .click();
  await expect(
    page.getByRole("dialog").getByText("Synthetic field", { exact: true }),
  ).toBeVisible();
  await expect(
    page
      .getByRole("dialog")
      .locator(".ant-select-selection-item")
      .filter({ hasText: "message" }),
  ).toBeVisible();
  await page
    .getByRole("dialog")
    .getByRole("button", { name: "Применить" })
    .click();
  for (const kind of ["regex", "template", "columns", "json"]) {
    await page.getByLabel("Тип нового блока варианта event").first().click();
    await page
      .locator(".ant-select-dropdown:visible .ant-select-item-option")
      .filter({ hasText: kind })
      .click();
    await page.getByRole("button", { name: "Добавить блок" }).click();
    await expect(
      page.getByRole("button", {
        name: `Настройки блока ${kind} block${["regex", "template", "columns", "json"].indexOf(kind) + 2}`,
      }),
    ).toBeVisible();
  }
  await page
    .getByRole("button", { name: "Настройки блока json block5" })
    .click();
  const jsonDialog = page.getByRole("dialog", {
    name: /Настройки блока json block5/,
  });
  const settingsWidths = await jsonDialog.evaluate((dialog) => {
    const key = [...dialog.querySelectorAll("input")].find((input) =>
      input.closest("label")?.textContent?.startsWith("Ключ блока"),
    )!;
    const source = [...dialog.querySelectorAll("input")].find((input) =>
      input.closest("label")?.textContent?.startsWith("Вход:"),
    )!;
    return {
      keyRight: key.getBoundingClientRect().right,
      sourceRight: source.getBoundingClientRect().right,
    };
  });
  expect(
    Math.abs(settingsWidths.keyRight - settingsWidths.sourceRight),
  ).toBeLessThanOrEqual(2);
  const extract = jsonDialog.getByRole("textbox", {
    name: "Имена извлечений и JSON-пути",
  });
  const set = jsonDialog
    .getByRole("textbox", { name: "Константы set (JSON-объект)" })
    .first();
  await set.fill("{bad");
  await set.blur();
  await extract.fill('{"probe":"probe"}');
  await extract.blur();
  await jsonDialog.getByRole("button", { name: "Применить" }).click();
  await expect(
    jsonDialog.getByText(/Поле set: требуется JSON-объект/i),
  ).toBeVisible();
  await set.fill('{"safe":"one"}');
  await set.blur();
  await extract.fill("{bad");
  await extract.blur();
  await jsonDialog.getByRole("button", { name: "Применить" }).click();
  await expect(
    jsonDialog.getByText(/Поле extract: требуется JSON-объект/i),
  ).toBeVisible();
  await extract.fill(
    '{"probe":"<img src=x onerror=globalThis.__codeRan=true>"}',
  );
  await extract.blur();
  await jsonDialog.getByRole("button", { name: "Добавить кандидат" }).click();
  const secondSet = jsonDialog
    .getByRole("textbox", { name: "Константы set (JSON-объект)" })
    .nth(1);
  await secondSet.fill("{bad");
  await secondSet.blur();
  await set.fill('{"safe":"one"}');
  await set.blur();
  await jsonDialog.getByRole("button", { name: "Применить" }).click();
  await expect(
    jsonDialog.getByText(/Поле set: требуется JSON-объект/i),
  ).toBeVisible();
  await secondSet.fill('{"safeTwo":"two"}');
  await secondSet.blur();
  expect(
    await page.evaluate(
      () => (window as Window & { __codeRan?: boolean }).__codeRan,
    ),
  ).toBeUndefined();
  await jsonDialog.getByRole("button", { name: "Применить" }).click();
  await page
    .getByRole("button", { name: "Настройки блока map_ecs message" })
    .click();
  const dialog = page.getByRole("dialog");
  for (const [option, field] of [
    ["Константа literal", "Константа (JSON-значение)"],
    ["Несколько refs + join", "Разделитель"],
    ["Несколько refs + as_array", "Ссылки через запятую"],
    ["Ссылка ref", "Ссылка (log или block.field)"],
  ] as const) {
    await dialog.getByLabel("Источник значения").first().click();
    await page
      .locator(".ant-select-dropdown:visible .ant-select-item-option")
      .filter({ hasText: option })
      .click();
    await expect(
      dialog.getByRole("textbox", { name: field, exact: true }),
    ).toBeVisible();
  }
  await dialog
    .getByRole("textbox", { name: "Формат даты (необязательно)" })
    .fill("%b %d %H:%M:%S");
  await dialog
    .getByRole("button", { name: "Справка: Формат даты (необязательно)" })
    .focus();
  await page.keyboard.press("Enter");
  await expect(
    page.getByText(/исходной строки даты и времени, извлечённой из log/),
  ).toBeVisible();
  await expect(page.getByText(/а не время сбора Fluent Bit/)).toBeVisible();
  await dialog
    .getByRole("button", { name: "Справка: Формат даты (необязательно)" })
    .click();
  await expect(dialog.getByText(/Выберите UTC явно/)).toBeVisible();
  await dialog.getByRole("button", { name: "Применить" }).click();
  await expect(dialog).toBeVisible();
  await dialog
    .getByRole("combobox", { name: "Зона исходного времени" })
    .click();
  await page
    .locator(".ant-select-dropdown:visible .ant-select-item-option")
    .filter({ hasText: "Считать время без зоны UTC" })
    .click();
  await dialog
    .getByRole("spinbutton", { name: "Явный год (если отсутствует в формате)" })
    .fill("2026");
  await dialog.getByRole("button", { name: "Применить" }).click();
  await page.getByRole("button", { name: "JSON", exact: true }).click();
  const text = await page
    .getByRole("textbox", { name: "Полный JSON RuleV1" })
    .inputValue();
  for (const kind of ["regex", "template", "columns", "json", "map_ecs"])
    expect(text).toContain(`"kind": "${kind}"`);
  expect(text).toContain('"year": 2026');
  expect(text).toContain('"safe": "one"');
  expect(text).toContain('"safeTwo": "two"');
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
  ).toBe(true);
});

test("existing normalizers are searched in the selection field", async ({
  page,
}) => {
  const searches: string[] = [];
  await page.route("**/api/v1/**", async (route) => {
    const url = new URL(route.request().url());
    const path = url.pathname.replace("/api/v1", "");
    let body: unknown = null;
    if (path === "/auth/session")
      body = {
        authenticated: true,
        user: {
          id: "user",
          username: "demo",
          email: null,
          display_name: "Demo",
        },
        role: "demo",
        permissions: ["normalizers.read", "normalizers.write"],
        authentication_method: "local",
        csrf_token: "csrf",
      };
    else if (path === "/normalizers") {
      const q = url.searchParams.get("q") || "";
      searches.push(q);
      const items = q
        ? [{ id: "special", name: "Special rule", version: 1 }]
        : Array.from({ length: 50 }, (_, index) => ({
            id: `rule-${index}`,
            name: `Rule ${index}`,
            version: 1,
          }));
      body = { items, total: q ? 1 : 51, limit: 50, offset: 0 };
    } else if (
      path === "/normalizers/special" ||
      path === "/normalizers/rule-0"
    )
      body = {
        id: "special",
        name: path === "/normalizers/special" ? "Special rule" : "Rule 0",
        description: "Special description",
        version: 1,
        rule_status: "ready",
        created_by_user_id: null,
        updated_by_user_id: null,
        created_at: "2026-01-01T00:00:00Z",
        updated_at: "2026-01-01T00:00:00Z",
        rule: {
          format_version: 1,
          variants: [
            {
              key: "special",
              priority: 10,
              when: { kind: "contains", value: "special" },
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
        },
      };
    else if (path === "/normalizers/block-types") body = [];
    await route.fulfill({
      contentType: "application/json",
      body: JSON.stringify(body),
    });
  });
  await page.goto("/normalizers/new");
  await page.getByRole("combobox", { name: "Основа правила" }).focus();
  await page.keyboard.press("ArrowDown");
  await page
    .locator(".ant-select-dropdown:visible .ant-select-item-option")
    .filter({ hasText: "Существующий нормализатор" })
    .click();
  const picker = page.getByRole("combobox", {
    name: "Выбрать существующий нормализатор",
  });
  await expect.poll(() => searches.includes("")).toBe(true);
  await picker.focus();
  await page.keyboard.press("ArrowDown");
  await expect(
    page
      .locator(".ant-select-dropdown:visible .ant-select-item-option")
      .filter({ hasText: "Rule 0" }),
  ).toBeVisible({ timeout: 10000 });
  await picker.fill("Special");
  await expect.poll(() => searches.includes("Special")).toBe(true);
  await expect(
    page
      .locator(".ant-select-dropdown:visible .ant-select-item-option")
      .filter({ hasText: "Special rule" }),
  ).toBeVisible();
  await expect(
    page.getByRole("textbox", { name: "Поиск существующего нормализатора" }),
  ).toHaveCount(0);
  await page
    .locator(".ant-select-dropdown:visible .ant-select-item-option")
    .filter({ hasText: "Special rule" })
    .click();
  await expect(
    page.getByRole("dialog", { name: "Заменить черновик?" }),
  ).toBeVisible();
  await page
    .getByRole("dialog", { name: "Заменить черновик?" })
    .getByRole("button", { name: "Отмена" })
    .click();
  await expect(page.getByRole("textbox", { name: "Имя" })).toHaveValue("");
  await picker.focus();
  await picker.fill("Special");
  await page
    .locator(".ant-select-dropdown:visible .ant-select-item-option")
    .filter({ hasText: "Special rule" })
    .click();
  await page
    .getByRole("dialog", { name: "Заменить черновик?" })
    .getByRole("button", { name: "Продолжить" })
    .click();
  await expect(page.getByRole("textbox", { name: "Имя" })).toHaveValue(
    "Special rule (копия)",
  );
});

test("imported date policy stays intact and UTC requires an explicit choice", async ({
  page,
}) => {
  const imported = {
    format_version: 1,
    variants: [
      {
        key: "event",
        priority: 10,
        when: { kind: "contains", value: "2026" },
        blocks: [
          {
            key: "parsed",
            kind: "regex",
            input: "log",
            candidates: [{ pattern: "^(?P<time>.+)$" }],
          },
          {
            key: "time",
            kind: "map_ecs",
            target: "@timestamp",
            source: { ref: "parsed.time" },
            required: true,
            transform: { date_format: "%Y-%m-%d %H:%M:%S %z" },
          },
        ],
      },
    ],
  };
  const previews: unknown[] = [];
  await page.route("**/api/v1/**", async (route) => {
    const path = new URL(route.request().url()).pathname.replace("/api/v1", "");
    let body: unknown = null;
    if (path === "/auth/session")
      body = {
        authenticated: true,
        user: {
          id: "user",
          username: "demo",
          display_name: "Demo",
          email: null,
        },
        role: "demo",
        permissions: ["normalizers.read", "normalizers.write", "events.read"],
        authentication_method: "local",
        csrf_token: "csrf",
      };
    else if (path === "/normalizers/block-types") body = [];
    else if (path === "/normalizers/preview") {
      previews.push(route.request().postDataJSON());
      body = {
        status: "complete",
        variant_key: "event",
        ecs_data: { "@timestamp": "2026-10-12T14:30:05Z" },
        fluent_bit_collected_at: "2026-10-12T14:30:05Z",
        diagnostics: [],
        trace: [],
      };
    }
    await route.fulfill({
      contentType: "application/json",
      body: JSON.stringify(body),
    });
  });
  await page.goto("/normalizers/new");
  await page.locator('input[type="file"]').setInputFiles({
    name: "date.json",
    mimeType: "application/json",
    buffer: Buffer.from(JSON.stringify(imported)),
  });
  await expect(
    page.getByRole("button", { name: "Настройки блока map_ecs time" }),
  ).toBeVisible();
  await page
    .getByRole("button", { name: "Настройки блока map_ecs time" })
    .click();
  const dialog = page.getByRole("dialog");
  await expect(
    dialog
      .locator(".ant-select-selection-item")
      .filter({ hasText: "Зона указана в логе" }),
  ).toBeVisible();
  await dialog.getByRole("button", { name: "Применить" }).click();
  await page.getByRole("button", { name: "JSON", exact: true }).click();
  const text = page.getByRole("textbox", { name: "Полный JSON RuleV1" });
  expect(
    JSON.parse(await text.inputValue()).variants[0].blocks[1].transform,
  ).toEqual({ date_format: "%Y-%m-%d %H:%M:%S %z" });
  await page
    .getByRole("textbox", { name: /log — одна сырая строка/ })
    .fill("2026-10-12 14:30:05 +0000");
  await page.getByRole("button", { name: "Проверить черновик" }).click();
  await expect.poll(() => previews.length).toBe(1);
  await page.getByRole("button", { name: "Блоки", exact: true }).click();
  await page
    .getByRole("button", { name: "Настройки блока map_ecs time" })
    .click();
  await dialog
    .getByRole("textbox", { name: "Формат даты (необязательно)" })
    .fill("%Y-%m-%d %H:%M:%S");
  await expect(dialog.getByText(/Выберите UTC явно/)).toBeVisible();
  await dialog
    .getByRole("combobox", { name: "Зона исходного времени" })
    .click();
  await page
    .locator(".ant-select-dropdown:visible .ant-select-item-option")
    .filter({ hasText: "Считать время без зоны UTC" })
    .click();
  await dialog.getByRole("button", { name: "Применить" }).click();
  await page.getByRole("button", { name: "Проверить черновик" }).click();
  await expect.poll(() => previews.length).toBe(2);
  expect(
    (previews[0] as { rule: typeof imported }).rule.variants[0].blocks[1]
      .transform,
  ).toEqual({ date_format: "%Y-%m-%d %H:%M:%S %z" });
  expect(
    (previews[1] as { rule: typeof imported }).rule.variants[0].blocks[1]
      .transform,
  ).toEqual({ date_format: "%Y-%m-%d %H:%M:%S", timezone: "UTC" });
  const withoutPolicy = structuredClone(imported);
  withoutPolicy.variants[0].blocks[1].transform.date_format =
    "%Y-%m-%d %H:%M:%S";
  await page.locator('input[type="file"]').setInputFiles({
    name: "without-zone.json",
    mimeType: "application/json",
    buffer: Buffer.from(JSON.stringify(withoutPolicy)),
  });
  await page
    .getByRole("dialog", { name: "Заменить черновик?" })
    .getByRole("button", { name: "Продолжить" })
    .click();
  await page
    .getByRole("button", { name: "Настройки блока map_ecs time" })
    .click();
  await expect(dialog.getByText(/Выберите UTC явно/)).toBeVisible();
  await dialog
    .getByRole("textbox", { name: "Формат даты (необязательно)" })
    .fill("%Y-%m-%dT%H:%M:%S");
  await dialog.getByRole("button", { name: "Применить" }).click();
  await expect(dialog).toBeVisible();
  await dialog.getByRole("button", { name: "Отмена" }).click();
  await page.getByRole("button", { name: "JSON", exact: true }).click();
  expect(
    JSON.parse(await text.inputValue()).variants[0].blocks[1].transform,
  ).toEqual({ date_format: "%Y-%m-%d %H:%M:%S" });
});
