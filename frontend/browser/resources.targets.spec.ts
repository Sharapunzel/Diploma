import { expect, test } from "@playwright/test";

import { targetLabel } from "../src/app/api/resources";

const connection = {
  id: "00000000-0000-4000-8000-000000000112",
  name: "Target fixture indexer",
  base_url: "https://indexer.example.test",
  username: "reader",
  has_password: true,
  has_ca: true,
  created_at: "2026-10-01T00:00:00Z",
  updated_at: "2026-10-01T00:00:00Z",
};
const targets = [
  { type: "index", field: "index_name", value: "task15-index" },
  { type: "index_pattern", field: "index_pattern", value: "task15-index-*" },
  { type: "data_stream", field: "data_stream_name", value: "task15-stream" },
  {
    type: "data_stream_pattern",
    field: "data_stream_pattern",
    value: "task15-stream-*",
  },
] as const;
const session = {
  authenticated: true,
  user: {
    id: "00000000-0000-4000-8000-000000000201",
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
    "normalizers.read",
  ],
  authentication_method: "local",
  csrf_token: "csrf",
};
const pageData = (items: unknown[]) => ({
  items,
  total: items.length,
  limit: 50,
  offset: 0,
});
test("external target form creates all four target contracts", async ({
  page,
}) => {
  const created: unknown[] = [];
  const sources: Record<string, unknown>[] = [];
  await page.route("**/api/v1/**", async (route) => {
    const url = new URL(route.request().url());
    const path = url.pathname.replace("/api/v1", "");
    const method = route.request().method();
    let status = 200;
    let body: unknown = pageData([]);
    if (path === "/auth/session") body = session;
    else if (path === "/external-connections") body = pageData([connection]);
    else if (path === "/external-sources" && method === "GET")
      body = pageData(sources);
    else if (path === `/external-connections/${connection.id}/indices`)
      body = pageData(["task15-index"]);
    else if (path === `/external-connections/${connection.id}/data-streams`)
      body = pageData(["task15-stream"]);
    else if (method === "POST" && path === "/external-sources") {
      const payload = route.request().postDataJSON() as Record<string, unknown>;
      created.push(payload);
      const target = targets.find((item) => item.type === payload.target_type)!;
      const source = {
        id: `source-${sources.length}`,
        source_type: "external",
        name: payload.name,
        external_connection_id: connection.id,
        target_type: target.type,
        index_name: null,
        index_pattern: null,
        data_stream_name: null,
        data_stream_pattern: null,
        [target.field]: payload[target.field],
        is_enabled: false,
        created_at: "2026-10-01T00:00:00Z",
        updated_at: "2026-10-01T00:00:00Z",
      };
      sources.push(source);
      status = 201;
      body = source;
    }
    await route.fulfill({
      status,
      contentType: "application/json",
      body: JSON.stringify(body),
    });
  });

  await page.goto("/sources");
  await page.getByRole("tab").nth(1).click();
  for (const [index, target] of targets.entries()) {
    await page.locator("section").last().getByRole("button").first().click();
    const dialog = page.getByRole("dialog");
    await dialog.getByRole("textbox").first().fill(`target-${target.type}`);
    await dialog.locator(".ant-select-selector").nth(0).click({ force: true });
    await page.getByText(connection.name).last().click();
    if (index > 0) {
      await dialog
        .getByText(targetLabel.index, { exact: true })
        .click({ force: true });
      await page
        .getByText(targetLabel[target.type], { exact: true })
        .last()
        .click({ force: true });
    }
    if (target.type.endsWith("pattern"))
      await dialog.locator(".ant-input").last().fill(target.value);
    else {
      await dialog
        .locator(".ant-select-selector")
        .nth(2)
        .click({ force: true });
      await page.getByText(target.value, { exact: true }).last().click();
    }
    await dialog.getByRole("button").last().click();
    await expect.poll(() => created.length).toBe(index + 1);
    expect(created[index]).toEqual({
      name: `target-${target.type}`,
      external_connection_id: connection.id,
      target_type: target.type,
      [target.field]: target.value,
    });
    await expect(dialog).toBeHidden();
  }
});

test("duplicate external target conflict stays in the form and does not show success", async ({
  page,
}) => {
  let creates = 0;
  await page.route("**/api/v1/**", async (route) => {
    const path = new URL(route.request().url()).pathname.replace("/api/v1", "");
    if (route.request().method() === "POST" && path === "/external-sources") {
      creates++;
      return route.fulfill({
        status: 409,
        json: {
          code: "external_source_conflict",
          message: "Conflict",
          request_id: "target-conflict",
        },
      });
    }
    const body =
      path === "/auth/session"
        ? session
        : path === "/external-connections"
          ? pageData([connection])
          : path === `/external-connections/${connection.id}/indices`
            ? pageData(["task15-index"])
            : pageData([]);
    return route.fulfill({ json: body });
  });
  await page.goto("/sources");
  await page.getByRole("tab").nth(1).click();
  await page.locator("section").last().getByRole("button").first().click();
  const dialog = page.getByRole("dialog");
  await dialog.getByRole("textbox").first().fill("Duplicate target");
  await dialog.locator(".ant-select-selector").nth(0).click({ force: true });
  await page.getByText(connection.name).last().click();
  await dialog.locator(".ant-select-selector").nth(2).click({ force: true });
  await page.getByText("task15-index", { exact: true }).last().click();
  await dialog.getByRole("button").last().click();
  await expect.poll(() => creates).toBe(1);
  await expect(dialog).toBeVisible();
  await expect(page.getByRole("alert")).toBeVisible();
});

test("Kafka source can receive and later clear a normalizer through the UI", async ({
  page,
}) => {
  const normalizer = {
    id: "00000000-0000-4000-8000-000000000131",
    name: "Ready smoke rule",
    version: 1,
    rule_status: "ready",
  };
  const source: Record<string, unknown> = {
    id: "00000000-0000-4000-8000-000000000132",
    source_type: "kafka",
    name: "Normalizer source",
    connection_id: "00000000-0000-4000-8000-000000000133",
    topic_name: "task15-topic",
    normalizer_id: null,
    is_enabled: false,
    is_archived: false,
    created_at: "2026-10-01T00:00:00Z",
    updated_at: "2026-10-01T00:00:00Z",
  };
  const assignments: unknown[] = [];
  await page.route("**/api/v1/**", async (route) => {
    const path = new URL(route.request().url()).pathname.replace("/api/v1", "");
    const method = route.request().method();
    if (method === "PUT" && path.endsWith("/normalizer")) {
      const body = route.request().postDataJSON() as {
        normalizer_id: string | null;
      };
      assignments.push(body);
      source.normalizer_id = body.normalizer_id;
      return route.fulfill({ json: source });
    }
    const body =
      path === "/auth/session"
        ? session
        : path === "/sources"
          ? pageData([source])
          : path === "/kafka-connections"
            ? pageData([
                {
                  id: source.connection_id,
                  name: "Kafka fixture",
                  bootstrap_servers: ["kafka:9092"],
                  security_protocol: "PLAINTEXT",
                },
              ])
            : path === "/normalizers"
              ? pageData([normalizer])
              : path === `/normalizers/${normalizer.id}`
                ? normalizer
                : pageData([]);
    return route.fulfill({ json: body });
  });
  await page.goto("/sources");
  const row = page.getByRole("row").filter({ hasText: "Normalizer source" });
  await row.getByRole("button").nth(1).click();
  const dialog = page.getByRole("dialog");
  await dialog.getByRole("combobox").click();
  await page
    .locator(".ant-select-dropdown:visible .ant-select-item-option")
    .nth(1)
    .click();
  await dialog.getByRole("button").last().click();
  await expect.poll(() => assignments.length).toBe(1);
  expect(assignments[0]).toEqual({ normalizer_id: normalizer.id });
  await expect(dialog).toBeHidden();

  await row.getByRole("button").nth(1).click();
  const clearDialog = page.getByRole("dialog");
  await clearDialog.locator(".ant-select-selector").click({ force: true });
  await page
    .locator(".ant-select-dropdown:visible .ant-select-item-option")
    .first()
    .click();
  await clearDialog.getByRole("button").last().click();
  await expect.poll(() => assignments.length).toBe(2);
  expect(assignments[1]).toEqual({ normalizer_id: null });
});
