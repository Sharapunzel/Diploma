/** Real isolated TASK-21 smoke. Run only through scripts/agent_test_profiles.py. */
import { writeFileSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { expect, test } from "@playwright/test";

const run = process.env.TASK21_RUN_DIR;
const password = process.env.TASK21_ADMIN_PASSWORD;
const topic = process.env.TASK21_TOPIC;
test.skip(!run || !password || !topic, "requires the isolated TASK-21 profile");
test.setTimeout(120_000);
const setupTest = process.env.TASK21_PHASE === "verify" ? test.skip : test;
const verifyTest = process.env.TASK21_PHASE === "verify" ? test : test.skip;

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
    const csrf = (await login.json()).csrf_token as string;
    const connection = await page.request.post("/api/v1/kafka-connections", {
      headers: { "X-CSRF-Token": csrf },
      data: {
        name: "Task 21 Kafka",
        bootstrap_servers: ["kafka:9092"],
      },
    });
    expect(connection.status()).toBe(201);
    await page.goto("/agents");
    await page.getByRole("button", { name: "Зарегистрировать агента" }).click();
    const form = page.getByRole("dialog");
    await form.getByLabel("Название").fill("Task 21 agent");
    await form.getByLabel("Подключение Kafka").click();
    await page.getByText("Task 21 Kafka", { exact: true }).last().click();
    await form.getByLabel("Имя нового топика").fill(topic!);
    await form.getByRole("button", { name: "Ещё топик" }).click();
    await form.getByLabel("Подключение Kafka").nth(1).click();
    await page.getByText("Task 21 Kafka", { exact: true }).last().click();
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
    await expect(
      page.getByRole("button", { name: "Получить конфигурацию" }),
    ).toBeEnabled({ timeout: 60_000 });
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
      JSON.stringify({ agentId, sourceId: data.topics[0].source_id, topic }),
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
