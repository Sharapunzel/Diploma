import { expect, test } from "@playwright/test";

const password = process.env.TASK29_ADMIN_PASSWORD;
const readerPassword = process.env.TASK29_READER_PASSWORD;
test.skip(
  !password || !readerPassword,
  "requires the isolated external-end-to-end profile",
);
test.setTimeout(120_000);

test("searches and filters live Wazuh-shaped events through production HTTPS", async ({
  page,
}) => {
  await page.goto("/login");
  await page
    .getByRole("textbox", { name: "Имя пользователя" })
    .fill("task29admin");
  await page.getByLabel("Пароль").fill(password!);
  await page.getByRole("button", { name: "Войти", exact: true }).click();
  await expect(page).toHaveURL(/\/$/);

  await page.goto("/connections");
  await page.getByRole("tab", { name: "Внешние подключения" }).click();
  await page.getByRole("button", { name: "Добавить подключение" }).click();
  const connectionDialog = page.getByRole("dialog", {
    name: "Новое подключение",
  });
  await connectionDialog.getByLabel("Имя").fill("Task 29 UI indexer");
  await connectionDialog
    .getByLabel("HTTPS URL индексера")
    .fill("https://indexer:9200");
  await connectionDialog
    .getByLabel("Пользователь")
    .fill("diploma_task12_reader");
  await connectionDialog.getByLabel("Пароль").fill(readerPassword!);
  await connectionDialog.getByRole("button", { name: "Сохранить" }).click();
  await expect(connectionDialog).toBeHidden();
  const connection = page.getByRole("row").filter({
    hasText: "Task 29 UI indexer",
  });
  await expect(connection).toBeVisible();
  await connection.getByRole("button", { name: "CA", exact: true }).click();
  const caDialog = page.getByRole("dialog", {
    name: "CA для Task 29 UI indexer",
  });
  await caDialog
    .locator('input[type="file"]')
    .setInputFiles("/tmp/indexer-ca.pem");
  await caDialog.getByRole("button", { name: "Загрузить CA" }).click();
  await expect(caDialog).toBeHidden();
  await connection.getByRole("button", { name: "Проверить" }).click();
  await expect(page.getByText(/проверка успешна/)).toBeVisible();

  await page.goto("/sources");
  await page.getByRole("tab", { name: "Внешние источники" }).click();
  await page.getByRole("button", { name: "Добавить источник" }).click();
  const sourceDialog = page.getByRole("dialog", { name: "Новый источник" });
  await sourceDialog.locator("#name").fill("Task 29 UI findings");
  await sourceDialog.getByRole("combobox", { name: "Подключение" }).click();
  await page.getByText("Task 29 UI indexer", { exact: true }).last().click();
  await expect(sourceDialog.getByTitle("Task 29 UI indexer")).toBeVisible();
  const discovered = page.waitForResponse(
    (response) =>
      response.url().includes("/data-streams") &&
      response.request().method() === "GET",
  );
  await sourceDialog
    .locator("#target_type")
    .locator("xpath=ancestor::div[contains(@class,'ant-select-selector')]")
    .click();
  await page.getByText("Поток данных", { exact: true }).last().click();
  const response = await discovered;
  expect(response.ok()).toBeTruthy();
  expect((await response.json()).items).toContain("wazuh-findings-v5-security");
  const target = sourceDialog.getByRole("combobox", {
    name: "Обнаруженное имя",
  });
  await expect(target).toBeEnabled();
  await target
    .locator("xpath=ancestor::div[contains(@class,'ant-select-selector')]")
    .click();
  await expect(target).toHaveAttribute("aria-expanded", "true");
  await target.fill("wazuh-findings-v5-security");
  await target.press("ArrowDown");
  await expect(target).toHaveAttribute("aria-expanded", "true");
  await target.press("Enter");
  await expect(
    sourceDialog.getByTitle("wazuh-findings-v5-security"),
  ).toBeVisible();
  await sourceDialog.getByRole("button", { name: "Сохранить" }).click();
  await expect(sourceDialog).toBeHidden();
  const source = page
    .getByRole("row")
    .filter({ hasText: "Task 29 UI findings" });
  await expect(source).toBeVisible();
  await source.getByRole("button", { name: "Включить" }).click();
  await expect(source).toContainText("Включён");

  await page.goto("/events");
  await page.getByRole("combobox", { name: "Источник" }).click();
  await page.getByText("Task 29 UI findings · OpenSearch").click();
  await page.getByRole("button", { name: "Фильтры и поля" }).click();
  const dialog = page.getByRole("dialog", { name: "Фильтры и поля" });
  await dialog.getByLabel(/От \(время события/).fill("2026-06-15T11:59");
  await dialog.getByLabel(/До \(исключительно/).fill("2026-06-15T12:05");
  await dialog.getByRole("button", { name: "Добавить условие" }).click();
  await dialog
    .getByRole("combobox", { name: "Поле условия" })
    .fill("wazuh.agent.name");
  await page.getByText("wazuh.agent.name", { exact: true }).last().click();
  await dialog
    .getByRole("combobox", { name: "Оператор условия" })
    .locator("xpath=ancestor::div[contains(@class,'ant-select-selector')]")
    .click();
  await page.getByText("Равно", { exact: true }).last().click();
  await dialog.getByPlaceholder("Значение").fill("agent-0");
  await dialog.getByRole("button", { name: "Искать" }).click();
  await expect(dialog).toBeHidden();
  await expect(
    page.getByText(/wazuh\.agent\.name Равно agent-0/),
  ).toBeVisible();
  const table = page.getByRole("table");
  await expect(
    table.getByRole("button", { name: "Открыть" }).first(),
  ).toBeVisible();
  await expect(table.getByRole("button", { name: "Удалить" })).toHaveCount(0);
  await table.getByRole("button", { name: "Открыть" }).first().click();
  await expect(page.getByRole("dialog", { name: "Событие" })).toContainText(
    "agent-0",
  );
  await page
    .getByRole("dialog", { name: "Событие" })
    .locator(".ant-modal-close")
    .click();

  await page
    .getByRole("combobox", { name: "Источник" })
    .locator("xpath=ancestor::div[contains(@class,'ant-select-selector')]")
    .click();
  await page.getByText(/Task 29 local event · Kafka/).click();
  await expect(
    page
      .getByRole("combobox", { name: "Источник" })
      .locator("xpath=ancestor::div[contains(@class,'ant-select-selector')]"),
  ).toContainText("Task 29 local event");
  await page.getByRole("button", { name: "Фильтры и поля" }).click();
  const localFilters = page.getByRole("dialog", { name: "Фильтры и поля" });
  await localFilters.getByLabel(/От \(время события/).fill("2026-06-15T11:59");
  await localFilters.getByLabel(/До \(исключительно/).fill("2026-06-15T12:05");
  await localFilters.getByRole("button", { name: "Добавить условие" }).click();
  await localFilters
    .getByRole("combobox", { name: "Поле условия" })
    .fill("message");
  await page.getByText("message", { exact: true }).last().click();
  await localFilters
    .getByRole("combobox", { name: "Оператор условия" })
    .click();
  await page.getByText("Содержит", { exact: true }).last().click();
  await localFilters.getByPlaceholder("Значение").fill("local smoke");
  await localFilters.getByRole("button", { name: "Искать" }).click();
  await expect(localFilters).toBeHidden();
  await expect(
    page.getByRole("table").getByRole("button", { name: "Открыть" }).first(),
  ).toBeVisible();
  await page
    .getByRole("table")
    .getByRole("button", { name: "Открыть" })
    .first()
    .click();
  const localCard = page.getByRole("dialog", { name: "Событие" });
  await expect(localCard).toContainText("task29 local smoke event");
  await expect(localCard).toContainText("Исходная запись");
  await localCard.locator(".ant-modal-close").click();
  await expect(
    page.getByRole("button", { name: "Удаление событий" }),
  ).toBeVisible();
});
