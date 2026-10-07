import { expect, test } from "@playwright/test";

const session = {
  authenticated: true,
  user: {
    id: "user-a",
    username: "fixture",
    email: null,
    display_name: "Fixture",
  },
  role: "fixture",
  permissions: [
    "connections.read",
    "connections.write",
    "sources.read",
    "sources.write",
  ],
  authentication_method: "local",
  csrf_token: "csrf",
};
const source = {
  id: "00000000-0000-4000-8000-000000000021",
  source_type: "external",
  name: "Delayed source",
  external_connection_id: "00000000-0000-4000-8000-000000000012",
  target_type: "index",
  index_name: "smoke-index",
  index_pattern: null,
  data_stream_name: null,
  data_stream_pattern: null,
  normalizer_id: null,
  is_enabled: true,
  is_archived: false,
  created_at: "2026-10-01T00:00:00Z",
  updated_at: "2026-10-01T00:00:00Z",
};
const externalConnection = {
  id: source.external_connection_id,
  name: "Fixture indexer",
  base_url: "https://indexer.example.test",
  username: "reader",
  has_password: true,
  has_ca: true,
  created_at: "2026-10-01T00:00:00Z",
  updated_at: "2026-10-01T00:00:00Z",
};

for (const failure of [false, true]) {
  test(`source disable confirmation waits for API ${failure ? "error" : "success"}`, async ({
    page,
  }) => {
    let release!: (status: number, body: object) => void;
    let started!: () => void;
    const pendingResponse = new Promise<{ status: number; body: object }>(
      (resolve) => (release = (status, body) => resolve({ status, body })),
    );
    const requestStarted = new Promise<void>((resolve) => (started = resolve));
    let mutations = 0;
    await page.route("**/api/v1/**", async (route) => {
      const path = new URL(route.request().url()).pathname.replace(
        "/api/v1",
        "",
      );
      if (path === "/auth/session") return route.fulfill({ json: session });
      if (path === "/kafka-sources" || path === "/external-sources")
        return route.fulfill({
          json: { items: [source], total: 1, limit: 50, offset: 0 },
        });
      if (path === `/external-sources/${source.id}/disable`) {
        mutations++;
        started();
        const response = await pendingResponse;
        return route.fulfill({ status: response.status, json: response.body });
      }
      return route.fulfill({
        json: { items: [], total: 0, limit: 50, offset: 0 },
      });
    });
    await page.goto("/sources");
    await page.getByRole("tab").nth(1).click();
    await page.getByRole("button", { name: "Выключить" }).click();
    const dialog = page.getByRole("dialog", { name: "Выключить источник?" });
    await dialog.getByRole("button", { name: "Выключить" }).click();
    await requestStarted;
    await expect(dialog).toBeVisible();
    await expect(
      dialog.getByRole("button", { name: "Выключить" }),
    ).toBeDisabled();
    await expect.poll(() => mutations).toBe(1);
    release(
      failure ? 409 : 200,
      failure
        ? {
            code: "source_configuration_changed",
            message: "Conflict",
            request_id: "request-conflict",
            details: null,
          }
        : {},
    );
    if (failure) {
      await expect(dialog).toBeVisible();
      await expect(
        dialog.getByText(
          "Настройки подключения изменились. Обновите состояние перед повтором.",
        ),
      ).toBeVisible();
      await expect(page.getByText("Источник выключен.")).toHaveCount(0);
    } else {
      await expect(dialog).toBeHidden();
      await expect(page.getByText("Источник выключен.")).toBeVisible();
    }
    expect(mutations).toBe(1);
  });
}

test("cancelling a source confirmation with Escape restores focus and sends no request", async ({
  page,
}) => {
  let mutations = 0;
  await page.route("**/api/v1/**", async (route) => {
    const path = new URL(route.request().url()).pathname.replace("/api/v1", "");
    if (path === "/auth/session") return route.fulfill({ json: session });
    if (path === "/external-sources")
      return route.fulfill({
        json: { items: [source], total: 1, limit: 50, offset: 0 },
      });
    if (path === "/sources")
      return route.fulfill({
        json: { items: [], total: 0, limit: 50, offset: 0 },
      });
    if (path === "/external-connections")
      return route.fulfill({
        json: { items: [externalConnection], total: 1, limit: 50, offset: 0 },
      });
    if (path === `/external-connections/${externalConnection.id}`)
      return route.fulfill({ json: externalConnection });
    if (path === "/kafka-connections")
      return route.fulfill({
        json: { items: [], total: 0, limit: 50, offset: 0 },
      });
    if (route.request().method() !== "GET") mutations++;
    return route.fulfill({ json: {} });
  });

  await page.goto("/sources");
  await page.getByRole("tab").nth(1).click();
  await expect(page.getByText("Delayed source")).toBeVisible();
  const disable = page
    .getByTestId("workspace-content")
    .getByRole("button", { name: "Выключить" });
  await disable.click();
  const dialog = page.getByRole("dialog", { name: "Выключить источник?" });
  await expect(dialog).toBeVisible();
  await page.keyboard.press("Escape");
  await expect(dialog).toBeHidden();
  await expect(disable).toBeFocused();
  expect(mutations).toBe(0);
});

test("unknown source action outcome requires refresh before manual retry", async ({
  page,
}) => {
  let mutations = 0;
  let reads = 0;
  await page.route("**/api/v1/**", async (route) => {
    const path = new URL(route.request().url()).pathname.replace("/api/v1", "");
    if (path === "/auth/session") return route.fulfill({ json: session });
    if (path === "/external-sources") {
      reads++;
      return route.fulfill({
        json: { items: [source], total: 1, limit: 50, offset: 0 },
      });
    }
    if (path === "/sources")
      return route.fulfill({
        json: { items: [], total: 0, limit: 50, offset: 0 },
      });
    if (path === "/external-connections")
      return route.fulfill({
        json: { items: [externalConnection], total: 1, limit: 50, offset: 0 },
      });
    if (path === `/external-connections/${externalConnection.id}`)
      return route.fulfill({ json: externalConnection });
    if (path === "/kafka-connections")
      return route.fulfill({
        json: { items: [], total: 0, limit: 50, offset: 0 },
      });
    if (path === `/external-sources/${source.id}/disable`) {
      mutations++;
      return route.abort("failed");
    }
    return route.fulfill({ json: {} });
  });

  await page.goto("/sources");
  await page.getByRole("tab").nth(1).click();
  await expect(page.getByText("Delayed source")).toBeVisible();
  await page.getByRole("button", { name: "Выключить" }).click();
  const dialog = page.getByRole("dialog", { name: "Выключить источник?" });
  const confirm = dialog.getByRole("button", { name: "Выключить" });
  await confirm.click();
  await expect(
    dialog.getByText(
      "Неизвестно, применилось ли действие. Сначала обновите список; повтор отправьте вручную после сверки.",
    ),
  ).toBeVisible();
  await expect(confirm).toBeDisabled();
  expect(mutations).toBe(1);

  await dialog.getByRole("button", { name: "Обновить состояние" }).click();
  await expect(confirm).toBeEnabled();
  expect(reads).toBeGreaterThan(1);
  expect(mutations).toBe(1);
});

test("losing the session during external connection precheck sends no mutation", async ({
  page,
}) => {
  const connection = externalConnection;
  let mutations = 0;
  await page.route("**/api/v1/**", async (route) => {
    const url = new URL(route.request().url());
    const path = url.pathname.replace("/api/v1", "");
    if (path === "/auth/session") return route.fulfill({ json: session });
    if (path === "/kafka-connections" || path === "/sources")
      return route.fulfill({
        json: { items: [], total: 0, limit: 50, offset: 0 },
      });
    if (path === "/external-connections")
      return route.fulfill({
        json: { items: [connection], total: 1, limit: 50, offset: 0 },
      });
    if (path === `/external-connections/${connection.id}`)
      return route.fulfill({ json: connection });
    if (path === "/external-sources")
      return route.fulfill({ status: 401, json: {} });
    if (route.request().method() !== "GET") mutations++;
    return route.fulfill({ json: {} });
  });

  await page.goto("/connections");
  await page.getByRole("tab").nth(1).click();
  await page.getByRole("button", { name: "Изменить" }).last().click();
  const dialog = page.getByRole("dialog", {
    name: "Изменить подключение",
  });
  await dialog
    .getByRole("textbox", { name: /Имя/ })
    .fill("Changed while checking");
  await dialog.getByRole("button", { name: "Сохранить" }).click();

  await expect(page).toHaveURL(/\/login$/);
  await expect(
    page.getByRole("dialog", { name: "Изменить внешнее подключение?" }),
  ).toHaveCount(0);
  expect(mutations).toBe(0);
});

test("source delete waits for response, ignores double confirm, and keeps failure visible", async ({
  page,
}) => {
  let release!: (status: number) => void;
  let started!: () => void;
  const pendingResponse = new Promise<number>((resolve) => (release = resolve));
  const requestStarted = new Promise<void>((resolve) => (started = resolve));
  let deletes = 0;
  await page.route("**/api/v1/**", async (route) => {
    const path = new URL(route.request().url()).pathname.replace("/api/v1", "");
    if (path === "/auth/session") return route.fulfill({ json: session });
    if (path === "/external-sources")
      return route.fulfill({
        json: { items: [source], total: 1, limit: 50, offset: 0 },
      });
    if (path === "/sources")
      return route.fulfill({
        json: { items: [], total: 0, limit: 50, offset: 0 },
      });
    if (path === "/external-connections")
      return route.fulfill({
        json: { items: [externalConnection], total: 1, limit: 50, offset: 0 },
      });
    if (path === `/external-connections/${externalConnection.id}`)
      return route.fulfill({ json: externalConnection });
    if (
      path === `/external-sources/${source.id}` &&
      route.request().method() === "DELETE"
    ) {
      deletes++;
      started();
      await pendingResponse;
      return route.fulfill({ status: await pendingResponse, body: "" });
    }
    return route.fulfill({ json: {} });
  });

  await page.goto("/sources");
  await page.getByRole("tab").nth(1).click();
  await expect(page.getByText("Delayed source")).toBeVisible();
  await page.getByRole("button", { name: "Удалить" }).last().click();
  const dialog = page.getByRole("dialog", { name: "Удалить источник?" });
  const confirm = dialog.getByRole("button", { name: "Удалить" });
  await confirm.click();
  await requestStarted;
  await confirm.click({ force: true });
  await expect(dialog).toBeVisible();
  await expect(confirm).toBeDisabled();
  expect(deletes).toBe(1);

  release(409);
  await expect(dialog).toBeVisible();
  await expect(dialog.getByRole("alert")).toBeVisible();
  await expect(page.getByText("Источник удалён.")).toHaveCount(0);
  expect(deletes).toBe(1);
});

test("expired identity during CA precheck closes the old form without mutation", async ({
  page,
}) => {
  const caConnection = { ...externalConnection, has_ca: false };
  let release!: () => void;
  let started!: () => void;
  const waiting = new Promise<void>((resolve) => (release = resolve));
  const requestStarted = new Promise<void>((resolve) => (started = resolve));
  let mutationCount = 0;
  await page.route("**/api/v1/**", async (route) => {
    const path = new URL(route.request().url()).pathname.replace("/api/v1", "");
    if (path === "/auth/session") return route.fulfill({ json: session });
    if (path === "/external-connections")
      return route.fulfill({
        json: {
          items: [caConnection],
          total: 1,
          limit: 50,
          offset: 0,
        },
      });
    if (path === "/external-sources") {
      started();
      await waiting;
      return route.fulfill({ status: 401, json: {} });
    }
    if (path === "/kafka-connections" || path === "/sources")
      return route.fulfill({
        json: { items: [], total: 0, limit: 50, offset: 0 },
      });
    if (route.request().method() !== "GET") mutationCount++;
    return route.fulfill({ json: {} });
  });

  await page.goto("/connections");
  await page.getByRole("tab").nth(1).click();
  await page.getByRole("button", { name: "CA" }).first().click();
  const dialog = page.getByRole("dialog");
  await dialog.locator('input[type="file"]').setInputFiles({
    name: "ca.pem",
    mimeType: "application/x-pem-file",
    buffer: Buffer.from(
      "-----BEGIN CERTIFICATE-----\\nsynthetic\\n-----END CERTIFICATE-----",
    ),
  });
  await dialog.getByRole("button").last().click();
  await requestStarted;

  release();
  await expect(page.getByRole("dialog")).toHaveCount(0);
  expect(mutationCount).toBe(0);
});
