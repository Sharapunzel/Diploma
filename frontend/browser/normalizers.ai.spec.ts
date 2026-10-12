import { expect, test } from "@playwright/test";

const session = {
  authenticated: true,
  user: { id: "user", username: "demo", email: null, display_name: "Demo" },
  role: "demo",
  permissions: ["normalizers.read", "normalizers.write", "events.read"],
  authentication_method: "local",
  csrf_token: "csrf",
};
const reply = (body: unknown, status = 200) => ({
  status,
  contentType: "application/json",
  body: JSON.stringify(body),
});
const generated = {
  format_version: 1,
  variants: [
    {
      key: "nginx",
      priority: 10,
      when: { kind: "contains", value: "GET" },
      blocks: [
        {
          key: "access",
          kind: "extract",
          extractor: {
            kind: "regex",
            input: "log",
            candidates: [{ pattern: "(?P<request>.+)" }],
          },
        },
        {
          key: "other",
          kind: "extract",
          extractor: {
            kind: "regex",
            input: "log",
            candidates: [{ pattern: "(?P<date>.+)" }],
          },
        },
        { key: "time", kind: "date", target: "@timestamp", date_format: "%Y" },
        {
          key: "method",
          kind: "map_ecs",
          target: "http.request.method",
          source: { ref: "access.request" },
          required: true,
        },
        {
          key: "path",
          kind: "map_ecs",
          target: "url.path",
          source: { ref: "access.request" },
          required: false,
        },
        {
          key: "columns",
          kind: "columns",
          input: "log",
          candidates: [
            { delimiter: "PRIVATE_DELIMITER_624", columns: ["field"] },
          ],
        },
      ],
    },
  ],
};

test("invalid AI JSON keeps source and copies all safe corrections", async ({
  page,
}) => {
  const clipboard: string[] = [];
  await page.addInitScript(() => {
    Object.defineProperty(navigator, "clipboard", {
      configurable: true,
      value: {
        writeText: async (text: string) => {
          (window as Window & { copied?: string[] }).copied?.push(text);
        },
      },
    });
    (window as Window & { copied?: string[] }).copied = [];
    localStorage.setItem("theme", "dark");
  });
  await page.route("**/api/v1/**", async (route) => {
    const path = new URL(route.request().url()).pathname.replace("/api/v1", "");
    if (path === "/auth/session") return route.fulfill(reply(session));
    if (path === "/normalizers/block-types") return route.fulfill(reply([]));
    if (path === "/normalizers/preview")
      return route.fulfill(
        reply(
          {
            code: "normalizer_rule_invalid",
            message: "secret traceback",
            request_id: "req-ai",
            details: { variant: "nginx", block: "method", parameter: "target" },
          },
          422,
        ),
      );
    return route.fulfill(reply(null));
  });
  await page.setViewportSize({ width: 320, height: 800 });
  await page.goto("/normalizers/new");
  await expect(page.locator("html")).toHaveAttribute("data-theme", "dark");
  await page.getByRole("button", { name: "JSON", exact: true }).click();
  const editor = page.getByRole("textbox", { name: "Полный JSON RuleV1" });
  const secret = "SYNTHETIC_SECRET_PROPERTY_987";
  const source = JSON.stringify({
    ...generated,
    [secret]: true,
    variants: [{ ...generated.variants[0], [secret]: true }],
  });
  await editor.fill(source);
  await expect(page.getByText(/Замечания к JSON: /)).toBeVisible();
  await expect(
    page.getByRole("button", { name: secret, exact: true }),
  ).toBeVisible();
  for (const path of [
    "variants[0].blocks[0].kind",
    "variants[0].blocks[1].kind",
    "variants[0].blocks[2].kind",
    "variants[0].blocks[4].source",
    "variants[0].blocks[5].candidates[0].delimiter",
  ])
    await expect(
      page.getByRole("button", { name: path, exact: true }),
    ).toBeVisible();
  await page.getByRole("button", { name: "Блоки", exact: true }).click();
  await expect(editor).toHaveValue(source);
  await page
    .getByRole("textbox", { name: "log — одна сырая строка" })
    .fill("PRIVATE_LOG_624");
  await page
    .getByRole("button", { name: "Скопировать замечания для ИИ" })
    .focus();
  await page.keyboard.press("Enter");
  clipboard.push(
    ...(await page.evaluate(
      () => (window as Window & { copied?: string[] }).copied || [],
    )),
  );
  expect(clipboard).toHaveLength(1);
  expect(clipboard[0]).toContain("variants[0].blocks[4].source");
  expect(clipboard[0]).toContain(
    "variants[0].blocks[5].candidates[0].delimiter",
  );
  expect(clipboard[0]).toContain("пробел, запятую, ;, | или табуляцию");
  expect(clipboard[0]).not.toContain(source);
  expect(clipboard[0]).not.toContain(secret);
  expect(clipboard[0]).not.toContain("PRIVATE_DELIMITER_624");
  expect(clipboard[0]).not.toContain("PRIVATE_LOG_624");
  expect(clipboard[0]).toContain("Неподдерживаемое свойство");
  expect(clipboard[0]).not.toContain("secret traceback");
  await expect(page.getByText(/Замечания для ИИ скопированы/)).toBeVisible();
  await page.evaluate(() =>
    Object.defineProperty(navigator, "clipboard", {
      configurable: true,
      value: {
        writeText: async () => {
          throw new Error("denied");
        },
      },
    }),
  );
  await page
    .getByRole("button", { name: "Скопировать замечания для ИИ" })
    .click();
  await expect(
    page.getByText(/Не удалось скопировать замечания/),
  ).toBeVisible();
  const fixed = structuredClone(generated) as unknown as {
    variants: { blocks: unknown[] }[];
  };
  fixed.variants[0].blocks = [
    {
      key: "message",
      kind: "map_ecs",
      target: "message",
      source: { ref: "log" },
      required: true,
    },
  ];
  await editor.fill(JSON.stringify(fixed));
  await editor.blur();
  await page.getByRole("button", { name: "Блоки", exact: true }).click();
  await expect(
    page.getByRole("button", { name: "Настройки блока map_ecs message" }),
  ).toBeVisible();
  await page
    .getByRole("textbox", { name: "log — одна сырая строка" })
    .fill("GET /demo");
  await page.getByRole("button", { name: "Проверить черновик" }).click();
  await expect(page.getByText(/Проверьте ECS target/).first()).toBeVisible();
  await expect(page.getByText("secret traceback")).toHaveCount(0);
});

test("import retains invalid JSON and prompt omits draft and log", async ({
  page,
}) => {
  await page.addInitScript(() => {
    (window as Window & { copied?: string[] }).copied = [];
    Object.defineProperty(navigator, "clipboard", {
      configurable: true,
      value: {
        writeText: async (text: string) => {
          (window as Window & { copied?: string[] }).copied?.push(text);
        },
      },
    });
    localStorage.setItem("theme", "light");
  });
  await page.route("**/api/v1/**", async (route) => {
    const path = new URL(route.request().url()).pathname.replace("/api/v1", "");
    if (path === "/auth/session") return route.fulfill(reply(session));
    if (path === "/normalizers/block-types") return route.fulfill(reply([]));
    return route.fulfill(reply(null));
  });
  await page.goto("/normalizers/new");
  await expect(page.locator("html")).toHaveAttribute("data-theme", "light");
  const raw = JSON.stringify(generated);
  await page.locator('input[type="file"]').setInputFiles({
    name: "generated.json",
    mimeType: "application/json",
    buffer: Buffer.from(raw),
  });
  await page.getByRole("button", { name: "JSON", exact: true }).click();
  await expect(
    page.getByRole("textbox", { name: "Полный JSON RuleV1" }),
  ).toHaveValue(raw);
  await expect(page.getByText(/Замечания к JSON: /)).toBeVisible();
  await page
    .getByRole("textbox", { name: "log — одна сырая строка" })
    .fill("PRIVATE_LOG_123");
  await page.getByRole("button", { name: "Промпт для ИИ" }).click();
  const copied = await page.evaluate(
    () => (window as Window & { copied?: string[] }).copied || [],
  );
  expect(copied).toHaveLength(1);
  expect(copied[0]).toContain("версия промпта 3");
  expect(copied[0]).toContain("Сначала выясните недостающие сведения");
  expect(copied[0]).toContain("Режим разбора ошибки");
  expect(copied[0]).not.toContain(raw);
  expect(copied[0]).not.toContain("PRIVATE_LOG_123");
  await expect(page.getByText(/Промпт скопирован/)).toBeVisible();
});
