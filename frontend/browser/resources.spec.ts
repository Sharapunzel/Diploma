import { expect, test } from "@playwright/test";

const session = (permissions: string[]) => ({
  authenticated: true,
  user: {
    id: "00000000-0000-4000-8000-000000000001",
    username: "fixture",
    email: null,
    display_name: "Fixture",
  },
  role: "fixture",
  permissions,
  authentication_method: "local",
  csrf_token: "fixture-csrf",
});
const kafka = {
  id: "00000000-0000-4000-8000-000000000011",
  name: "Тестовый Kafka",
  bootstrap_servers: ["kafka:9092"],
  security_protocol: "PLAINTEXT",
  created_at: "2026-10-01T00:00:00Z",
  updated_at: "2026-10-01T00:00:00Z",
};
const external = {
  id: "00000000-0000-4000-8000-000000000012",
  name: "Тестовый индексер",
  base_url: "https://indexer.example.test",
  username: "analyst",
  has_password: true,
  has_ca: false,
  created_at: "2026-10-01T00:00:00Z",
  updated_at: "2026-10-01T00:00:00Z",
};
const page = <T>(items: T[], offset = 0) => ({
  items,
  total: items.length,
  limit: 50,
  offset,
});

test("admin sees both categories, discovers without creating a source, and confirms deletion", async ({
  page: browser,
}) => {
  const mutations: string[] = [];
  await browser.route("**/api/v1/**", async (route) => {
    const url = new URL(route.request().url());
    const path = url.pathname.replace("/api/v1", "");
    const method = route.request().method();
    if (method !== "GET") mutations.push(`${method} ${path}`);
    let body: unknown = null;
    let status = 200;
    if (path === "/auth/session")
      body = session([
        "connections.read",
        "connections.write",
        "sources.read",
        "sources.write",
        "normalizers.read",
      ]);
    else if (path === "/kafka-connections") body = page([kafka]);
    else if (path === "/external-connections") body = page([external]);
    else if (path === `/kafka-connections/${kafka.id}/topics`) {
      const includeInternal =
        url.searchParams.get("include_internal") === "true";
      body = {
        items: includeInternal
          ? [
              {
                name: "synthetic-topic",
                partition_count: 3,
                is_registered: false,
              },
              {
                name: "__consumer_offsets",
                partition_count: 50,
                is_registered: false,
              },
            ]
          : [
              {
                name: "synthetic-topic",
                partition_count: 3,
                is_registered: false,
              },
            ],
        total: includeInternal ? 2 : 1,
      };
    } else if (path === "/sources") body = page([]);
    else if (path === "/external-sources") body = page([]);
    else if (path === `/external-connections/${external.id}/indices`)
      body = page(["synthetic-index"]);
    else if (path === `/external-connections/${external.id}/data-streams`)
      body = page(["synthetic-stream"]);
    else if (method === "DELETE") status = 204;
    await route.fulfill({
      status,
      contentType: "application/json",
      body: status === 204 ? "" : JSON.stringify(body),
    });
  });
  await browser.goto("/connections");
  await expect(browser.getByRole("tab")).toHaveCount(2);
  await browser.getByRole("button", { name: "Наборы" }).first().click();
  await expect(browser.getByText("synthetic-topic")).toBeVisible();
  await expect(
    browser.getByText("Не зарегистрирован в текущем поколении"),
  ).toBeVisible();
  const datasetDialog = browser.getByRole("dialog");
  await datasetDialog.getByRole("switch").click();
  await expect(datasetDialog.getByText("__consumer_offsets")).toBeVisible();
  await expect(
    datasetDialog.getByText("synthetic-topic", { exact: true }),
  ).toHaveCount(0);
  await expect(datasetDialog.getByText("Найдено: 1")).toBeVisible();
  await datasetDialog.getByRole("switch").click();
  await expect(datasetDialog.getByText("synthetic-topic")).toBeVisible();
  await expect(
    datasetDialog.getByText("__consumer_offsets", { exact: true }),
  ).toHaveCount(0);
  expect(mutations).toEqual([]);
  await browser
    .getByRole("dialog")
    .getByRole("button", { name: "Close" })
    .click();
  await browser.getByRole("button", { name: "Удалить" }).first().click();
  await expect(
    browser.getByText("все связанные источники будут удалены"),
  ).toBeVisible();
  expect(mutations).toEqual([]);
  await browser
    .getByRole("dialog")
    .getByRole("button", { name: "Отмена" })
    .click();
  expect(mutations).toEqual([]);
});

test("connection verification notice identifies the tested connection", async ({
  page: browser,
}) => {
  await browser.route("**/api/v1/**", async (route) => {
    const url = new URL(route.request().url());
    const path = url.pathname.replace("/api/v1", "");
    let body: unknown = null;
    if (path === "/auth/session")
      body = session(["connections.read", "connections.write"]);
    else if (path === "/kafka-connections") body = page([kafka]);
    else if (path === "/external-connections") body = page([]);
    else if (path === `/kafka-connections/${kafka.id}/test`)
      body = { status: "ok", broker_count: 1, topic_count: 4, latency_ms: 12 };
    await route.fulfill({
      contentType: "application/json",
      body: JSON.stringify(body),
    });
  });

  await browser.goto("/connections");
  await browser.getByRole("button", { name: "Проверить" }).click();
  await expect(
    browser
      .getByRole("alert")
      .getByText(
        `Подключение «${kafka.name}»: проверка успешна — 1 брокеров, 4 топиков, 12 мс.`,
      ),
  ).toBeVisible();
});

test("dataset discovery shows a modal spinner alongside its loading message", async ({
  page: browser,
}) => {
  let releaseDiscovery!: () => void;
  const discoveryGate = new Promise<void>((resolve) => {
    releaseDiscovery = resolve;
  });
  let discoveryPending = false;
  await browser.route("**/api/v1/**", async (route) => {
    const url = new URL(route.request().url());
    const path = url.pathname.replace("/api/v1", "");
    let body: unknown = page([]);
    if (path === "/auth/session")
      body = session(["connections.read", "connections.write"]);
    else if (path === "/kafka-connections") body = page([kafka]);
    else if (path === `/kafka-connections/${kafka.id}/topics`) {
      discoveryPending = true;
      await discoveryGate;
      body = {
        items: [
          { name: "delayed-topic", partition_count: 1, is_registered: false },
        ],
        total: 1,
      };
    }
    await route.fulfill({
      contentType: "application/json",
      body: JSON.stringify(body),
    });
  });

  await browser.goto("/connections");
  await browser.getByRole("button", { name: "Наборы" }).click();
  const loadingStatus = browser.getByRole("dialog").getByRole("status");
  await expect.poll(() => discoveryPending).toBe(true);
  await expect(loadingStatus).toContainText("Загрузка наборов…");
  await expect(loadingStatus.locator(".ant-spin-spinning")).toBeVisible();
  releaseDiscovery();
  await expect(
    browser.getByRole("dialog").getByText("delayed-topic"),
  ).toBeVisible();
});

test("resource page loading stays inside each table", async ({
  page: browser,
}) => {
  let releaseConnections!: () => void;
  let releaseSources!: () => void;
  const connectionsGate = new Promise<void>((resolve) => {
    releaseConnections = resolve;
  });
  const sourcesGate = new Promise<void>((resolve) => {
    releaseSources = resolve;
  });
  let connectionRequests = 0;
  let sourceRequests = 0;

  await browser.route("**/api/v1/**", async (route) => {
    const path = new URL(route.request().url()).pathname.replace("/api/v1", "");
    let body: unknown = page([]);
    if (path === "/auth/session")
      body = session(["connections.read", "sources.read"]);
    else if (
      path === "/kafka-connections" ||
      path === "/external-connections"
    ) {
      connectionRequests += 1;
      await connectionsGate;
    } else if (path === "/sources" || path === "/external-sources") {
      sourceRequests += 1;
      await sourcesGate;
    }
    await route.fulfill({
      contentType: "application/json",
      body: JSON.stringify(body),
    });
  });

  await browser.goto("/connections");
  await expect.poll(() => connectionRequests).toBe(2);
  await expect(browser.getByTestId("loading-overlay")).toHaveCount(0);
  await expect(
    browser.locator(".ant-table-wrapper .ant-spin-spinning").first(),
  ).toBeVisible();
  releaseConnections();
  await expect(
    browser.locator(".ant-table-wrapper .ant-spin-spinning").first(),
  ).toHaveCount(0);

  await browser.goto("/sources");
  await expect.poll(() => sourceRequests).toBe(2);
  await expect(browser.getByTestId("loading-overlay")).toHaveCount(0);
  await expect(
    browser.locator(".ant-table-wrapper .ant-spin-spinning").first(),
  ).toBeVisible();
  releaseSources();
  await expect(
    browser.locator(".ant-table-wrapper .ant-spin-spinning").first(),
  ).toHaveCount(0);
});

test("resource tables size from content and long dataset names stay inside the modal", async ({
  page: browser,
}) => {
  const connectionName = `Kafka-${"connection-name-".repeat(12)}`;
  const topicName = `topic-${"unbroken-name-".repeat(12)}`;
  const longKafka = { ...kafka, name: connectionName };
  await browser.route("**/api/v1/**", async (route) => {
    const path = new URL(route.request().url()).pathname.replace("/api/v1", "");
    const body =
      path === "/auth/session"
        ? session(["connections.read", "connections.write", "sources.read"])
        : path === "/kafka-connections"
          ? page([longKafka])
          : path === `/kafka-connections/${kafka.id}/topics`
            ? {
                items: [
                  {
                    name: topicName,
                    partition_count: 1,
                    is_registered: false,
                  },
                ],
                total: 1,
              }
            : page([]);
    await route.fulfill({
      contentType: "application/json",
      body: JSON.stringify(body),
    });
  });

  await browser.goto("/connections");
  const connectionTable = browser.locator("table").first();
  await expect(connectionTable).toHaveCSS("table-layout", "auto");
  const divider = await connectionTable
    .locator("thead th")
    .first()
    .evaluate((cell) => {
      const style = getComputedStyle(cell, "::after");
      return {
        top: style.top,
        bottom: style.bottom,
        width: style.borderRightWidth,
      };
    });
  expect(divider).toEqual({ top: "8px", bottom: "8px", width: "1px" });

  await browser.getByRole("button", { name: "Наборы" }).click();
  const dialog = browser.getByRole("dialog");
  const viewportHeight = await browser.evaluate(() => window.innerHeight);
  await expect(dialog).toHaveCSS("top", `${viewportHeight * 0.09}px`);
  const title = dialog.locator(".ant-modal-title");
  await expect(title).toHaveCSS("overflow-wrap", "anywhere");
  const dataset = dialog.locator('[class*="listItem"]');
  await expect(dataset).toContainText(topicName);
  await expect(dataset).toHaveCSS("overflow-wrap", "anywhere");

  await browser.goto("/sources");
  const sourceTable = browser.locator("table").first();
  await expect(sourceTable).toHaveCSS("table-layout", "auto");
});

test("guest sees saved details without live external discovery or write actions", async ({
  page: browser,
}) => {
  const requests: string[] = [];
  await browser.route("**/api/v1/**", async (route) => {
    const path = new URL(route.request().url()).pathname.replace("/api/v1", "");
    requests.push(path);
    const body =
      path === "/auth/session"
        ? session(["connections.read", "sources.read"])
        : path === "/kafka-connections"
          ? page([kafka])
          : path === "/external-connections"
            ? page([external])
            : page([]);
    await route.fulfill({
      contentType: "application/json",
      body: JSON.stringify(body),
    });
  });
  await browser.goto("/connections");
  await browser.getByRole("tab").nth(1).click();
  await expect(browser.getByText("Тестовый индексер")).toBeVisible();
  await expect(
    browser.getByRole("button", { name: "Добавить подключение" }),
  ).toHaveCount(0);
  await browser.getByRole("button", { name: "Наборы" }).last().click();
  await expect(
    browser.getByText(
      "Для просмотра наборов требуются права изменения подключений.",
    ),
  ).toBeVisible();
  expect(
    requests.some(
      (path) =>
        path.includes("/indices") ||
        path.includes("/data-streams") ||
        path.endsWith("/test"),
    ),
  ).toBe(false);
});

test("creates Kafka and external mask sources with exact payloads", async ({
  page: browser,
}) => {
  const payloads: unknown[] = [];
  let releaseKafkaCreate!: () => void;
  const kafkaCreateGate = new Promise<void>((resolve) => {
    releaseKafkaCreate = resolve;
  });
  let kafkaCreatePending = false;
  await browser.route("**/api/v1/**", async (route) => {
    const url = new URL(route.request().url());
    const path = url.pathname.replace("/api/v1", "");
    const method = route.request().method();
    let body: unknown = page([]);
    if (path === "/auth/session")
      body = session([
        "connections.read",
        "connections.write",
        "sources.read",
        "sources.write",
      ]);
    else if (path === "/kafka-connections") body = page([kafka]);
    else if (path === "/external-connections") body = page([external]);
    else if (path === `/kafka-connections/${kafka.id}/topics`)
      body = {
        items: [
          { name: "synthetic-topic", partition_count: 1, is_registered: false },
        ],
        total: 1,
      };
    else if (path === `/external-connections/${external.id}/indices`)
      body = page(["synthetic-index"]);
    else if (method === "POST") {
      payloads.push({ path, body: route.request().postDataJSON() });
      if (path === "/sources") {
        kafkaCreatePending = true;
        await kafkaCreateGate;
      }
      body = { id: "new" };
    }
    await route.fulfill({
      status: method === "POST" ? 201 : 200,
      contentType: "application/json",
      body: JSON.stringify(body),
    });
  });
  await browser.goto("/sources");
  await browser
    .getByRole("button", { name: "Добавить источник" })
    .first()
    .click();
  await browser
    .getByRole("dialog")
    .getByLabel("Имя")
    .fill("Синтетический Kafka");
  await browser.getByRole("dialog").getByLabel("Подключение").click();
  await browser.getByText(kafka.name).last().click();
  await browser.getByRole("dialog").getByLabel("Топик").click();
  await browser.getByText("synthetic-topic").last().click();
  const saveButton = browser
    .getByRole("dialog")
    .getByRole("button", { name: "Сохранить" });
  await saveButton.click();
  await expect.poll(() => kafkaCreatePending).toBe(true);
  await expect(saveButton).toHaveClass(/ant-btn-loading/);
  await expect(saveButton).toBeDisabled();
  releaseKafkaCreate();
  await expect.poll(() => payloads.length).toBe(1);
  expect(payloads[0]).toEqual({
    path: "/sources",
    body: {
      name: "Синтетический Kafka",
      connection_id: kafka.id,
      topic_name: "synthetic-topic",
    },
  });
  await browser.getByRole("tab").nth(1).click();
  await browser
    .getByRole("button", { name: "Добавить источник" })
    .last()
    .click();
  await browser
    .getByRole("dialog")
    .getByRole("textbox", { name: "* Имя" })
    .fill("Синтетическая маска");
  await browser.getByRole("dialog").getByLabel("Подключение").click();
  await browser.getByText(external.name).last().click();
  await browser
    .getByRole("dialog")
    .getByText("Индекс", { exact: true })
    .click();
  await browser.getByText("Маска индексов").last().click();
  await browser.getByRole("dialog").getByLabel("Маска").fill("synthetic-*");
  await browser
    .getByRole("dialog")
    .getByRole("button", { name: "Сохранить" })
    .click();
  await expect.poll(() => payloads.length).toBe(2);
  expect(payloads[1]).toEqual({
    path: "/external-sources",
    body: {
      name: "Синтетическая маска",
      external_connection_id: external.id,
      target_type: "index_pattern",
      index_pattern: "synthetic-*",
    },
  });
});

test("external rename keeps the saved password and waits for confirmation", async ({
  page: browser,
}) => {
  const patches: unknown[] = [];
  await browser.route("**/api/v1/**", async (route) => {
    const path = new URL(route.request().url()).pathname.replace("/api/v1", "");
    const method = route.request().method();
    let body: unknown = page([]);
    if (path === "/auth/session")
      body = session(["connections.read", "connections.write", "sources.read"]);
    else if (path === "/kafka-connections") body = page([kafka]);
    else if (path === "/external-connections") body = page([external]);
    else if (path === "/external-sources")
      body = page([{ external_connection_id: external.id, is_enabled: true }]);
    else if (method === "PATCH") {
      patches.push(route.request().postDataJSON());
      body = external;
    }
    await route.fulfill({
      contentType: "application/json",
      body: JSON.stringify(body),
    });
  });
  await browser.goto("/connections");
  await browser.getByRole("tab").nth(1).click();
  await browser.getByRole("button", { name: "Изменить" }).last().click();
  await browser.getByRole("dialog").getByLabel("Имя").fill("Новое имя");
  await browser
    .getByRole("dialog")
    .getByRole("button", { name: "Сохранить" })
    .click();
  expect(patches).toEqual([]);
  await browser
    .getByRole("dialog", { name: "Изменить внешнее подключение?" })
    .getByRole("button", { name: "Отмена" })
    .click();
  expect(patches).toEqual([]);
  await browser
    .getByRole("dialog", { name: "Изменить подключение" })
    .getByRole("button", { name: "Сохранить" })
    .click();
  await browser
    .getByRole("dialog", { name: "Изменить внешнее подключение?" })
    .getByRole("button", { name: "Сохранить" })
    .click();
  await expect.poll(() => patches.length).toBe(1);
  expect(patches[0]).toEqual({ name: "Новое имя" });
});

test("resource pages keep wide tables inside their panels at target widths", async ({
  page: browser,
}) => {
  await browser.route("**/api/v1/**", async (route) => {
    const path = new URL(route.request().url()).pathname.replace("/api/v1", "");
    const body =
      path === "/auth/session"
        ? session([
            "connections.read",
            "connections.write",
            "sources.read",
            "sources.write",
          ])
        : path === "/kafka-connections"
          ? page([kafka])
          : path === "/external-connections"
            ? page([external])
            : page([]);
    await route.fulfill({
      contentType: "application/json",
      body: JSON.stringify(body),
    });
  });
  for (const width of [320, 375, 768, 1440, 2560]) {
    await browser.setViewportSize({ width, height: 900 });
    await browser.goto("/connections");
    await expect(browser.getByRole("tab").nth(0)).toBeVisible();
    expect(
      await browser.evaluate(
        () => document.documentElement.scrollWidth <= window.innerWidth,
      ),
    ).toBe(true);
    await browser.goto("/sources");
    await expect(browser.getByRole("tab").nth(0)).toBeVisible();
    expect(
      await browser.evaluate(
        () => document.documentElement.scrollWidth <= window.innerWidth,
      ),
    ).toBe(true);
  }
});

test("external relations continue beyond 100 sources before showing a complete result", async ({
  page: browser,
}) => {
  const sources = Array.from({ length: 120 }, (_, index) => ({
    id: `00000000-0000-4000-8000-${String(index + 1000).padStart(12, "0")}`,
    source_type: "external",
    name: `Источник ${index}`,
    external_connection_id: external.id,
    target_type: index === 115 ? "index_pattern" : "index",
    index_name: index === 115 ? null : `other-${index}`,
    index_pattern: index === 115 ? "synthetic-*" : null,
    data_stream_name: null,
    data_stream_pattern: null,
    is_enabled: false,
  }));
  await browser.route("**/api/v1/**", async (route) => {
    const url = new URL(route.request().url());
    const path = url.pathname.replace("/api/v1", "");
    const offset = Number(url.searchParams.get("offset") || 0);
    const body =
      path === "/auth/session"
        ? session(["connections.read", "connections.write", "sources.read"])
        : path === "/kafka-connections"
          ? page([])
          : path === "/external-connections"
            ? page([external])
            : path === `/external-connections/${external.id}/indices`
              ? page(["synthetic-index"])
              : path === "/external-sources"
                ? {
                    items: sources.slice(offset, offset + 50),
                    total: sources.length,
                    limit: 50,
                    offset,
                  }
                : page([]);
    await route.fulfill({
      contentType: "application/json",
      body: JSON.stringify(body),
    });
  });
  await browser.goto("/connections");
  await browser.getByRole("tab").nth(1).click();
  await browser.getByRole("button", { name: "Наборы" }).click();
  await expect(
    browser.getByRole("link", { name: "Источник 115" }),
  ).toBeVisible();
  await expect(browser.getByText("Связей нет")).toHaveCount(0);
});

test("CA upload sends raw PEM only after confirming active source shutdown", async ({
  page: browser,
}) => {
  const uploads: { contentType: string | undefined; body: string | null }[] =
    [];
  await browser.route("**/api/v1/**", async (route) => {
    const path = new URL(route.request().url()).pathname.replace("/api/v1", "");
    if (route.request().method() === "PUT") {
      uploads.push({
        contentType: route.request().headers()["content-type"],
        body: route.request().postData(),
      });
      await route.fulfill({ status: 204 });
      return;
    }
    const body =
      path === "/auth/session"
        ? session(["connections.read", "connections.write", "sources.read"])
        : path === "/kafka-connections"
          ? page([])
          : path === "/external-connections"
            ? page([external])
            : path === "/external-sources"
              ? page([
                  { external_connection_id: external.id, is_enabled: true },
                ])
              : page([]);
    await route.fulfill({
      contentType: "application/json",
      body: JSON.stringify(body),
    });
  });
  await browser.goto("/connections");
  await browser.getByRole("tab").nth(1).click();
  await browser.getByRole("button", { name: "CA" }).click();
  const pem =
    "-----BEGIN CERTIFICATE-----\nsynthetic\n-----END CERTIFICATE-----\n";
  await browser
    .getByRole("dialog")
    .locator('input[type="file"]')
    .setInputFiles({
      name: "test-ca.pem",
      mimeType: "application/x-pem-file",
      buffer: Buffer.from(pem),
    });
  await browser
    .getByRole("dialog")
    .getByRole("button", { name: "Загрузить CA" })
    .click();
  await expect(
    browser.getByRole("dialog", { name: "Загрузить CA?" }),
  ).toBeVisible();
  expect(uploads).toEqual([]);
  await browser
    .getByRole("dialog", { name: "Загрузить CA?" })
    .getByRole("button", { name: "Отмена" })
    .click();
  expect(uploads).toEqual([]);
  await browser
    .getByRole("dialog", { name: /CA для/ })
    .getByRole("button", { name: "Загрузить CA" })
    .click();
  await browser
    .getByRole("dialog", { name: "Загрузить CA?" })
    .getByRole("button", { name: "Загрузить" })
    .click();
  await expect.poll(() => uploads.length).toBe(1);
  expect(uploads[0]).toEqual({
    contentType: "application/x-pem-file",
    body: pem,
  });
});

test("changing external target type clears the previous exact name", async ({
  page: browser,
}) => {
  const payloads: unknown[] = [];
  await browser.route("**/api/v1/**", async (route) => {
    const path = new URL(route.request().url()).pathname.replace("/api/v1", "");
    if (route.request().method() === "POST") {
      payloads.push(route.request().postDataJSON());
      await route.fulfill({
        status: 201,
        contentType: "application/json",
        body: JSON.stringify({ id: "new" }),
      });
      return;
    }
    const body =
      path === "/auth/session"
        ? session([
            "connections.read",
            "connections.write",
            "sources.read",
            "sources.write",
          ])
        : path === "/kafka-connections"
          ? page([])
          : path === "/external-connections"
            ? page([external])
            : path === `/external-connections/${external.id}/indices`
              ? page(["synthetic-index"])
              : path === `/external-connections/${external.id}/data-streams`
                ? page(["synthetic-stream"])
                : page([]);
    await route.fulfill({
      contentType: "application/json",
      body: JSON.stringify(body),
    });
  });
  await browser.goto("/sources");
  await browser.getByRole("tab").nth(1).click();
  await browser
    .getByRole("button", { name: "Добавить источник" })
    .last()
    .click();
  const dialog = browser.getByRole("dialog");
  await dialog.getByRole("textbox", { name: "* Имя" }).fill("Новая цель");
  await dialog.getByLabel("Подключение").click();
  await browser.getByText(external.name).last().click();
  await dialog.getByLabel("Обнаруженное имя").click();
  await browser.getByText("synthetic-index").last().click();
  await dialog.getByText("Индекс", { exact: true }).click();
  await browser.getByText("Поток данных", { exact: true }).last().click();
  await dialog.getByRole("button", { name: "Сохранить" }).click();
  expect(payloads).toEqual([]);
  await dialog.getByLabel("Обнаруженное имя").click();
  await browser.getByText("synthetic-stream").last().click();
  await dialog.getByRole("button", { name: "Сохранить" }).click();
  await expect.poll(() => payloads.length).toBe(1);
  expect(payloads[0]).toEqual({
    name: "Новая цель",
    external_connection_id: external.id,
    target_type: "data_stream",
    data_stream_name: "synthetic-stream",
  });
});

test("late index page cannot contaminate current data streams", async ({
  page: browser,
}) => {
  let release!: () => void;
  let started!: () => void;
  const waiting = new Promise<void>((resolve) => (release = resolve));
  const requested = new Promise<void>((resolve) => (started = resolve));
  await browser.route("**/api/v1/**", async (route) => {
    const url = new URL(route.request().url());
    const path = url.pathname.replace("/api/v1", "");
    let body: unknown = page([]);
    if (path === "/auth/session")
      body = session(["connections.read", "connections.write", "sources.read"]);
    else if (path === "/external-connections") body = page([external]);
    else if (path === `/external-connections/${external.id}/indices`) {
      if (url.searchParams.get("offset") === "50") {
        started();
        await waiting;
        body = { items: ["OLD-INDEX-PAGE"], total: 51, limit: 50, offset: 50 };
      } else
        body = {
          items: Array.from({ length: 50 }, (_, index) => `index-${index}`),
          total: 51,
          limit: 50,
          offset: 0,
        };
    } else if (path === `/external-connections/${external.id}/data-streams`)
      body = page(["CURRENT-STREAM"]);
    await route.fulfill({
      contentType: "application/json",
      body: JSON.stringify(body),
    });
  });

  await browser.goto("/connections");
  await browser.getByRole("tab").nth(1).click();
  await browser.getByRole("button", { name: "Наборы" }).last().click();
  await browser.getByRole("button", { name: "Загрузить ещё" }).click();
  await requested;
  await browser.getByRole("button", { name: "Потоки данных" }).click();
  await expect(
    browser.getByText("CURRENT-STREAM", { exact: true }),
  ).toBeVisible();
  release();
  await browser.waitForTimeout(300);
  await expect(
    browser.getByText("OLD-INDEX-PAGE", { exact: true }),
  ).toHaveCount(0);
});

test("late Kafka option page cannot contaminate an external source form", async ({
  page: browser,
}) => {
  let release!: () => void;
  let started!: () => void;
  const waiting = new Promise<void>((resolve) => (release = resolve));
  const requested = new Promise<void>((resolve) => (started = resolve));
  await browser.route("**/api/v1/**", async (route) => {
    const url = new URL(route.request().url());
    const path = url.pathname.replace("/api/v1", "");
    let body: unknown = page([]);
    if (path === "/auth/session")
      body = session([
        "connections.read",
        "connections.write",
        "sources.read",
        "sources.write",
      ]);
    else if (path === "/kafka-connections") {
      if (url.searchParams.get("offset") === "50") {
        started();
        await waiting;
        body = {
          items: [{ ...kafka, id: "old-kafka", name: "OLD-KAFKA-OPTION" }],
          total: 51,
          limit: 50,
          offset: 50,
        };
      } else
        body = {
          items: Array.from({ length: 50 }, (_, index) => ({
            ...kafka,
            id: `kafka-${index}`,
            name: `Kafka ${index}`,
          })),
          total: 51,
          limit: 50,
          offset: 0,
        };
    } else if (path === "/external-connections") body = page([external]);
    await route.fulfill({
      contentType: "application/json",
      body: JSON.stringify(body),
    });
  });

  await browser.goto("/sources");
  await browser
    .getByRole("button", { name: "Добавить источник" })
    .first()
    .click();
  await browser
    .getByRole("button", { name: "Загрузить ещё подключения" })
    .click();
  await requested;
  await browser
    .getByRole("dialog")
    .getByRole("button", { name: "Отмена", exact: true })
    .click();
  await browser.getByRole("tab").nth(1).click();
  await browser
    .getByRole("button", { name: "Добавить источник" })
    .last()
    .click();
  await browser.getByRole("dialog").getByLabel("Подключение").click();
  await expect(browser.getByText(external.name, { exact: true })).toBeVisible();
  release();
  await browser.waitForTimeout(300);
  await expect(
    browser.getByText("OLD-KAFKA-OPTION", { exact: true }),
  ).toHaveCount(0);
});

test("selected normalizer beyond the first page remains named in the modal", async ({
  page: browser,
}) => {
  const selectedId = "00000000-0000-4000-8000-000000000099";
  const source = {
    id: "00000000-0000-4000-8000-000000000021",
    source_type: "kafka",
    name: "Source with paged normalizer",
    connection_id: kafka.id,
    topic_name: "synthetic-topic",
    normalizer_id: selectedId,
    is_enabled: false,
    is_archived: false,
    created_at: "2026-10-01T00:00:00Z",
    updated_at: "2026-10-01T00:00:00Z",
  };
  const selected = {
    id: selectedId,
    name: "Selected from page three",
    version: 7,
    rule_status: "ready",
  };
  await browser.route("**/api/v1/**", async (route) => {
    const url = new URL(route.request().url());
    const path = url.pathname.replace("/api/v1", "");
    let body: unknown = page([]);
    if (path === "/auth/session")
      body = session([
        "connections.read",
        "sources.read",
        "sources.write",
        "normalizers.read",
      ]);
    else if (path === "/sources") body = page([source]);
    else if (path === "/kafka-connections") body = page([kafka]);
    else if (path === `/normalizers/${selectedId}`) body = selected;
    else if (path === "/normalizers")
      body = {
        items: Array.from({ length: 50 }, (_, index) => ({
          id: `normalizer-${index}`,
          name: `Normalizer ${index}`,
          version: 1,
          rule_status: "ready",
        })),
        total: 101,
        limit: 50,
        offset: 0,
      };
    await route.fulfill({
      contentType: "application/json",
      body: JSON.stringify(body),
    });
  });

  await browser.goto("/sources");
  await expect(browser.getByText("Selected from page three")).toBeVisible();
  await browser.getByRole("button", { name: "Нормализатор" }).click();
  await expect(
    browser.getByText("Selected from page three · v7 · готов"),
  ).toBeVisible();
  await expect(
    browser.getByRole("button", { name: "Загрузить ещё" }),
  ).toBeVisible();
});
