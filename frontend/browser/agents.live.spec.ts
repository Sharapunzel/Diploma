/** Real isolated TASK-21 smoke. Run only through scripts/agent_test_profiles.py. */
import { writeFileSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { expect, test } from "@playwright/test";

const run = process.env.TASK21_RUN_DIR;
const password = process.env.TASK21_ADMIN_PASSWORD;
const topic = process.env.TASK21_TOPIC;
test.skip(!run || !password || !topic, "requires the isolated TASK-21 profile");
test.setTimeout(120_000);
const setupTest = process.env.TASK21_PHASE === "setup" ? test : test.skip;
const verifyTest = process.env.TASK21_PHASE === "verify" ? test : test.skip;
const configureTest =
  process.env.TASK21_PHASE === "configure" ? test : test.skip;

setupTest(
  "registers an agent and explicitly claims its one-time configuration",
  async ({ page }) => {
    const consoleMessages: string[] = [];
    page.on("console", (message) => consoleMessages.push(message.text()));
    await page.setViewportSize({ width: 390, height: 844 });
    const login = await page.request.post("/api/v1/auth/local/login", {
      data: { username: "task21admin", password },
    });
    expect(login.ok()).toBeTruthy();
    await page.goto("/connections");
    await page.getByRole("button", { name: "Добавить подключение" }).click();
    const connectionForm = page.getByRole("dialog", {
      name: "Новое подключение",
    });
    await connectionForm.getByLabel("Имя").fill("Task 21 Kafka");
    await connectionForm
      .getByLabel("Адреса брокеров, по одному на строку")
      .fill("kafka:9092");
    await connectionForm.getByRole("button", { name: "Сохранить" }).click();
    await expect(connectionForm).toBeHidden();
    const connectionRow = page.getByRole("row").filter({
      hasText: "Task 21 Kafka",
    });
    await expect(connectionRow).toBeVisible();
    await connectionRow.getByRole("button", { name: "Проверить" }).click();
    await expect(page.getByText(/проверка успешна/)).toBeVisible();
    const connections = await page.request.get("/api/v1/kafka-connections");
    expect(connections.ok()).toBeTruthy();
    const connectionId = (await connections.json()).items.find(
      (item: { name: string }) => item.name === "Task 21 Kafka",
    ).id as string;
    await page.goto("/agents");
    await page.getByRole("button", { name: "Зарегистрировать агента" }).click();
    const form = page.getByRole("dialog");
    await form.getByLabel("Название").fill("Task 21 agent");
    const chooseConnection = async (index: number) => {
      const select = form.getByLabel("Подключение Kafka").nth(index);
      await select
        .locator("xpath=ancestor::div[contains(@class,'ant-select-selector')]")
        .click();
      await select.press("ArrowDown");
      await select.press("Enter");
      await expect(form.getByTitle("Task 21 Kafka").nth(index)).toBeVisible();
    };
    await chooseConnection(0);
    await form.getByLabel("Имя нового топика").fill(topic!);
    await form.getByRole("button", { name: "Ещё топик" }).click();
    await chooseConnection(1);
    await form.getByLabel("Имя нового топика").nth(1).fill(`${topic}-other`);
    await form.getByRole("button", { name: "Отправить" }).click();
    try {
      await expect(page).toHaveURL(/\/agents\/[0-9a-f-]+$/, {
        timeout: 60_000,
      });
    } catch {
      const validation = await form
        .locator(".ant-form-item-explain-error")
        .allTextContents();
      const alerts = await page.locator("[role=alert]").allTextContents();
      throw new Error(
        `Agent registration did not navigate: ${JSON.stringify({ validation, alerts })}`,
      );
    }
    try {
      await expect(
        page.getByRole("button", { name: "Получить конфигурацию" }),
      ).toBeEnabled({ timeout: 60_000 });
    } catch {
      const agentId = new URL(page.url()).pathname.split("/").at(-1)!;
      const agent = await page.request.get(`/api/v1/agents/${agentId}`);
      const card = await page.locator("main").innerText();
      throw new Error(
        `Agent did not become claimable: ${JSON.stringify({ status: agent.status(), body: await agent.json(), card })}`,
      );
    }
    await page.getByRole("button", { name: "Получить конфигурацию" }).click();
    const config = page
      .getByRole("dialog")
      .getByText("[SERVICE]", { exact: false });
    await expect(config).toBeVisible();
    const content = await page.getByRole("dialog").locator("pre").textContent();
    expect(content).toContain(`Topics ${topic}`);
    expect(content).toContain(`Topics ${topic}-other`);
    expect(content?.match(/\[OUTPUT\]/g)).toHaveLength(2);
    expect(content).toContain("rdkafka.sasl.password ");
    const [download] = await Promise.all([
      page.waitForEvent("download"),
      page.getByRole("button", { name: "Скачать конфиг" }).click(),
    ]);
    const downloaded = join(run!, "downloaded-fluent-bit.conf");
    await download.saveAs(downloaded);
    expect(readFileSync(downloaded, "utf8")).toBe(content);
    writeFileSync(join(run!, "fluent-bit.conf"), content!, {
      encoding: "utf8",
      mode: 0o600,
    });
    const agentId = new URL(page.url()).pathname.split("/").at(-1)!;
    const agent = await page.request.get(`/api/v1/agents/${agentId}`);
    expect(agent.ok()).toBeTruthy();
    const data = await agent.json();
    expect(data.credentials_claimable).toBe(false);
    writeFileSync(
      join(run!, "agent.json"),
      JSON.stringify({
        agentId,
        sourceId: data.topics[0].source_id,
        topic,
        connectionId,
      }),
      { encoding: "utf8", mode: 0o600 },
    );
    expect(page.url()).not.toContain("sasl.password");
    expect(
      await page.evaluate(() => JSON.stringify(localStorage)),
    ).not.toContain("rdkafka.sasl.password");
    expect(
      await page.evaluate(() => JSON.stringify(sessionStorage)),
    ).not.toContain("rdkafka.sasl.password");
    const secret = content?.match(/rdkafka\.sasl\.password (\S+)/)?.[1];
    expect(secret).toBeTruthy();
    expect(consoleMessages.join("\n")).not.toContain(secret!);
    await page.evaluate(() => history.back());
    await expect(
      page.getByRole("dialog", { name: "Покинуть страницу" }),
    ).toBeVisible();
    await page
      .getByRole("dialog", { name: "Покинуть страницу" })
      .getByRole("button", { name: "Отмена" })
      .click();
    await expect(page.getByRole("dialog").locator("pre")).toContainText(
      "rdkafka.sasl.password",
    );
    await page
      .getByRole("dialog", { name: "Однократная конфигурация Fluent Bit" })
      .locator(".ant-modal-close")
      .click();
    await expect(
      page.getByRole("dialog", { name: "Закрыть конфигурацию" }),
    ).toBeVisible();
    await page.getByRole("button", { name: "Отмена" }).click();
    await expect(page.getByRole("dialog").locator("pre")).toContainText(
      "rdkafka.sasl.password",
    );
    await page
      .getByRole("dialog", { name: "Однократная конфигурация Fluent Bit" })
      .locator(".ant-modal-close")
      .click();
    await page
      .getByRole("dialog", { name: "Закрыть конфигурацию" })
      .getByRole("button", { name: "Закрыть" })
      .click();
    await expect(
      page.getByRole("dialog", { name: "Однократная конфигурация Fluent Bit" }),
    ).not.toBeVisible();
    expect(
      await page.evaluate(() => JSON.stringify(sessionStorage)),
    ).not.toContain(secret!);
  },
);

configureTest(
  "previews a normalizer and enables the agent source through the UI",
  async ({ page }) => {
    const agent = JSON.parse(readFileSync(join(run!, "agent.json"), "utf8"));
    const login = await page.request.post("/api/v1/auth/local/login", {
      data: { username: "task21admin", password },
    });
    expect(login.ok()).toBeTruthy();
    const rule = {
      format_version: 1,
      variants: [
        {
          key: "plain",
          priority: 1,
          when: { kind: "contains", value: "task21" },
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
    await page.goto("/normalizers/new");
    await page.getByRole("textbox", { name: "Имя" }).fill("Task 21 normalizer");
    await page.getByRole("button", { name: "JSON", exact: true }).click();
    await page
      .getByRole("textbox", { name: "Полный JSON RuleV1" })
      .fill(JSON.stringify(rule));
    await page
      .getByRole("textbox", { name: "log — одна сырая строка" })
      .fill("task21 smoke event");
    await page.getByRole("button", { name: "Проверить черновик" }).click();
    await expect(page.getByText("task21 smoke event").last()).toBeVisible();
    await page.getByRole("button", { name: "Создать", exact: true }).click();
    await expect(page).toHaveURL(/\/normalizers\/[0-9a-f-]+\/edit$/);
    agent.normalizerId = new URL(page.url()).pathname.split("/").at(-2);
    writeFileSync(join(run!, "agent.json"), JSON.stringify(agent), {
      encoding: "utf8",
      mode: 0o600,
    });

    await page.goto("/sources");
    const row = page.locator(`tr[data-row-key="${agent.sourceId}"]`);
    await expect(row).toContainText("Выключен");
    await row.getByRole("button", { name: "Нормализатор" }).click();
    const dialog = page.getByRole("dialog", { name: "Назначить нормализатор" });
    await dialog.locator(".ant-select-selector").click();
    await page
      .locator(".ant-select-dropdown:visible")
      .getByText(/Task 21 normalizer/)
      .click();
    await dialog.getByRole("button", { name: "Назначить" }).click();
    await expect(dialog).toBeHidden();
    await expect(row).toContainText("Нормализатор: Task 21 normalizer");
    await row.getByRole("button", { name: "Включить" }).click();
    await expect(row).toContainText("Включён");
  },
);

verifyTest("shows the stored event in the Events page", async ({ page }) => {
  const { sourceId } = JSON.parse(
    readFileSync(join(run!, "agent.json"), "utf8"),
  );
  const login = await page.request.post("/api/v1/auth/local/login", {
    data: { username: "task21admin", password },
  });
  expect(login.ok()).toBeTruthy();
  const source = await page.request.get(`/api/v1/sources/${sourceId}`);
  expect(source.ok()).toBeTruthy();
  const sourceName = (await source.json()).name as string;
  await page.goto("/events");
  await page.getByRole("combobox", { name: "Источник" }).click();
  const sourceOption = page.getByTitle(`${sourceName} · Kafka`, {
    exact: true,
  });
  await expect(sourceOption).toBeVisible({ timeout: 10_000 });
  await sourceOption.click();
  await expect(
    page.getByRole("table").getByText("task21 smoke event"),
  ).toBeVisible({
    timeout: 30_000,
  });
});

verifyTest(
  "shows the real source topology through the proxy",
  async ({ page }) => {
    const { agentId, sourceId, topic } = JSON.parse(
      readFileSync(join(run!, "agent.json"), "utf8"),
    );
    const login = await page.request.post("/api/v1/auth/local/login", {
      data: { username: "task21admin", password },
    });
    expect(login.ok()).toBeTruthy();
    const topology = await page.request.get(
      `/api/v1/diagnostics/sources/${sourceId}/topology`,
    );
    expect(topology.ok()).toBeTruthy();
    expect((await topology.json()).agent_id).toBe(agentId);
    await page.setViewportSize({ width: 390, height: 844 });
    await page.goto(`/diagnostics?source=${sourceId}`);
    const eventPath = page.getByRole("list", { name: /Путь событий/ });
    await expect(eventPath).toBeVisible();
    await expect(eventPath).toContainText(topic);
    const originalTheme = await page.locator("html").getAttribute("data-theme");
    for (const theme of ["light", "dark"] as const) {
      await page.locator("html").evaluate((element, value) => {
        element.setAttribute("data-theme", value);
      }, theme);
      for (const viewport of [
        { width: 1280, height: 900 },
        { width: 320, height: 800 },
      ]) {
        await page.setViewportSize(viewport);
        const palette = await page
          .getByRole("list", { name: "Путь событий" })
          .evaluate((list) => {
            const firstEdge = list.querySelector("li[data-edge]");
            if (!firstEdge) throw new Error("No topology edge found");
            firstEdge.setAttribute("data-edge", "broken");
            const label = firstEdge.querySelector<HTMLElement>(
              ":scope > span:last-child",
            );
            if (!label) throw new Error("No topology edge label found");
            const probe = document.createElement("span");
            probe.style.backgroundColor = "var(--color-surface-solid)";
            probe.style.color = "var(--color-warning)";
            document.body.append(probe);
            const expected = getComputedStyle(probe);
            const actual = getComputedStyle(label);
            const result = {
              background: actual.backgroundColor,
              expectedBackground: expected.backgroundColor,
              color: actual.color,
              expectedColor: expected.color,
              scrollFits: document.documentElement.scrollWidth <= innerWidth,
            };
            probe.remove();
            return result;
          });
        expect(palette.background).toBe(palette.expectedBackground);
        expect(palette.color).toBe(palette.expectedColor);
        expect(palette.scrollFits).toBe(true);
      }
    }
    if (originalTheme)
      await page.locator("html").evaluate((element, value) => {
        element.setAttribute("data-theme", value);
      }, originalTheme);
    else
      await page
        .locator("html")
        .evaluate((element) => element.removeAttribute("data-theme"));
    await page.setViewportSize({ width: 390, height: 844 });
    if (process.env.TASK23_DIAGNOSTIC_FIXTURES === "1") {
      await expect(
        page.getByText("Декодирование", { exact: false }).last(),
      ).toBeVisible();
      await expect(
        page.getByText("Потеря диапазона Kafka", { exact: false }).last(),
      ).toBeVisible();
      await page
        .getByRole("button", { name: /Декодирование/ })
        .last()
        .click();
      const errorDialog = page.getByRole("dialog", {
        name: "Сохранённая диагностическая запись",
      });
      await errorDialog
        .getByRole("button", { name: "Показать payload" })
        .click();
      await expect(errorDialog.locator("pre")).toContainText(
        "task23-invalid-envelope",
      );
      await page.keyboard.press("Escape");
      await expect(errorDialog).toBeHidden();
      await page
        .getByRole("button", { name: /Потеря диапазона Kafka/ })
        .last()
        .click();
      await expect(page.getByRole("dialog")).toContainText(
        "не равен точному числу потерянных сообщений",
      );
      await page.getByRole("dialog").locator(".ant-modal-close").click();
      await expect(page.getByRole("dialog")).toBeHidden();
    }
    await page.getByRole("button", { name: /1\. Агент и сбор/ }).click();
    await expect(
      page.getByRole("link", { name: "Перейти к агенту" }),
    ).toHaveAttribute("href", `/agents/${agentId}`);
    await page.getByRole("button", { name: /2\. Kafka/ }).click();
    const connectionLink = page.getByRole("link", {
      name: "Перейти к Kafka-подключению",
    });
    const { connectionId } = JSON.parse(
      readFileSync(join(run!, "agent.json"), "utf8"),
    );
    await expect(connectionLink).toHaveAttribute(
      "href",
      `/connections?id=${connectionId}`,
    );
    await connectionLink.click();
    await expect(page).toHaveURL(/\/connections\?id=[0-9a-f-]+$/);
    await expect(
      page.getByRole("dialog", { name: /Наборы данных для подключения/ }),
    ).toBeVisible();
    await page.getByRole("dialog").locator(".ant-modal-close").click();
    await page.goto(`/diagnostics?source=${sourceId}`);
    await page.getByRole("button", { name: /3\. Источник/ }).click();
    const sourceLink = page.getByRole("link", { name: "Перейти к источнику" });
    await expect(sourceLink).toHaveAttribute(
      "href",
      `/sources?kind=kafka&id=${sourceId}`,
    );
    await sourceLink.click();
    await expect(page).toHaveURL(
      new RegExp(`/sources\\?kind=kafka&id=${sourceId}`),
    );
    await page.goto(`/diagnostics?source=${sourceId}`);
    await page.getByRole("button", { name: /4\. .*Нормализатор/ }).click();
    const normalizerLink = page.getByRole("link", {
      name: "Перейти к нормализатору",
    });
    await expect(normalizerLink).toHaveAttribute(
      "href",
      /\/normalizers\/[0-9a-f-]+$/,
    );
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth,
      ),
    ).toBe(true);
    await normalizerLink.click();
    await expect(page).toHaveURL(/\/normalizers\/[0-9a-f-]+$/);
    await expect(page.getByText("Task 21 normalizer")).toBeVisible();
  },
);
