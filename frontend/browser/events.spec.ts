import { expect, test } from "@playwright/test";

const localId = "00000000-0000-4000-8000-000000000018";
const externalId = "00000000-0000-4000-8000-000000000019";
const source = (id: string, type: "kafka" | "external", enabled: boolean) => ({
  id,
  source_type: type,
  name: type === "kafka" ? "Synthetic Kafka" : "Synthetic OpenSearch",
  is_enabled: enabled,
});
const session = (admin: boolean) => ({
  authenticated: true,
  user: {
    id: "synthetic",
    username: "synthetic",
    email: null,
    display_name: "Synthetic",
  },
  role: admin ? "admin" : "guest",
  permissions: admin
    ? ["events.read", "events.delete", "sources.read"]
    : ["events.read", "sources.read"],
  authentication_method: "local",
  csrf_token: "synthetic-csrf",
});
const row = (type: "kafka" | "external") => ({
  id: "opaque-event",
  source_id: type === "kafka" ? localId : externalId,
  source_type: type,
  event_timestamp: "2026-10-12T10:00:00Z",
  fields: { "@timestamp": "2026-10-12T10:00:00Z", message: "synthetic event" },
  ...(type === "kafka" ? { normalization_status: "partial" } : {}),
});

for (const viewport of [
  { width: 320, height: 540 },
  { width: 1440, height: 900 },
  { width: 390, height: 844 },
  { width: 768, height: 650 },
  { width: 2560, height: 900 },
]) {
  test(`events page works at ${viewport.width}px`, async ({ page }) => {
    await page.setViewportSize(viewport);
    await page.emulateMedia({
      colorScheme: viewport.width < 500 ? "dark" : "light",
    });
    let deletions = 0;
    let searchCalls = 0;
    const bulkScopes: unknown[] = [];
    await page.route("**/api/v1/**", async (route) => {
      const url = new URL(route.request().url());
      const path = url.pathname.replace("/api/v1", "");
      const method = route.request().method();
      let body: unknown = null;
      let status = 200;
      if (path === "/auth/session") body = session(true);
      else if (path === "/sources")
        body = {
          items: [source(localId, "kafka", false)],
          total: 1,
          limit: 50,
          offset: 0,
        };
      else if (path === "/external-sources")
        body = {
          items: [source(externalId, "external", true)],
          total: 1,
          limit: 50,
          offset: 0,
        };
      else if (path.endsWith("/fields"))
        body = {
          items: [
            {
              name: "message",
              type: "text",
              operators: ["eq", "contains", "exists"],
            },
          ],
          total: 1,
          limit: 100,
          offset: 0,
        };
      else if (path.endsWith("/search")) {
        searchCalls++;
        const cursor = route.request().postDataJSON()?.cursor;
        const isLocal = path.includes(localId);
        body = {
          items: [
            {
              ...row(isLocal ? "kafka" : "external"),
              id: cursor === "next" ? "opaque-event-page-two" : "opaque-event",
              fields: {
                message:
                  cursor === "next"
                    ? "snapshot second page"
                    : "synthetic event",
              },
            },
          ],
          has_more: isLocal && cursor !== "next",
          next_cursor: isLocal && cursor !== "next" ? "next" : null,
        };
      } else if (path.includes("/events/") && method === "GET")
        body = {
          ...row("kafka"),
          details: {
            raw: "synthetic raw",
            metadata: {
              normalization_status: "partial",
              normalization_diagnostics: [{ code: "event_time_fallback" }],
            },
          },
        };
      else if (method === "DELETE") {
        deletions++;
        status = 204;
      } else if (path === "/parsed-logs/actions/delete" && method === "POST") {
        bulkScopes.push(route.request().postDataJSON());
        body = { deleted_count: 2 };
      }
      await route.fulfill({
        status,
        contentType: "application/json",
        body: status === 204 ? "" : JSON.stringify(body),
      });
    });
    await page.goto("/events");
    await page.getByRole("combobox", { name: "Источник" }).click();
    await page.getByText(/Synthetic Kafka/).click();
    await expect(
      page.getByRole("table").getByText("synthetic event"),
    ).toBeVisible();
    await expect(page.getByRole("table").getByText("partial")).toBeVisible();
    await page.keyboard.press("Escape");
    await page.getByRole("button", { name: "Фильтры и поля" }).click();
    await expect(
      page.getByRole("dialog", { name: "Фильтры и поля" }),
    ).toBeVisible();
    await page.waitForTimeout(350);
    await page.keyboard.press("Tab");
    expect(
      await page.evaluate(
        () => !!document.activeElement?.closest('[role="dialog"]'),
      ),
    ).toBe(true);
    await page
      .getByRole("dialog", { name: "Фильтры и поля" })
      .getByRole("button", { name: "Close" })
      .click();
    await expect(
      page.getByRole("dialog", { name: "Фильтры и поля" }),
    ).toBeHidden();
    if (viewport.width > 500) {
      await page.getByRole("button", { name: "Вперёд" }).click();
      await expect(
        page.getByRole("table").getByText("snapshot second page"),
      ).toBeVisible();
      await page.getByRole("button", { name: "Назад" }).click();
      await expect(
        page.getByRole("table").getByText("synthetic event"),
      ).toBeVisible();
      expect(searchCalls).toBe(2);
      await page.getByRole("button", { name: "Фильтры и поля" }).click();
      const filterDialog = page.getByRole("dialog", { name: "Фильтры и поля" });
      await filterDialog.getByText("Новые сначала").click();
      await page.getByText("Старые сначала", { exact: true }).click();
      await filterDialog.getByRole("button", { name: "Искать" }).click();
      await expect(filterDialog).toBeHidden();
      await expect(page.getByText("Порядок: Старые сначала")).toBeVisible();
      expect(searchCalls).toBe(3);
      await page.getByRole("button", { name: "Фильтры и поля" }).click();
      await filterDialog.getByText("Старые сначала").click();
      await page.getByText("Новые сначала", { exact: true }).click();
      await filterDialog.getByRole("button", { name: "Close" }).click();
      await expect(page.getByText("Порядок: Старые сначала")).toBeVisible();
    }
    await page
      .getByRole("table")
      .getByRole("button", { name: "Открыть" })
      .click();
    await expect(page.getByText("synthetic raw")).toBeVisible();
    await expect(
      page.getByText(/Время события взято из времени сбора/),
    ).toBeVisible();
    await page
      .getByRole("dialog", { name: "Событие" })
      .getByRole("button", { name: "Close" })
      .click();
    await expect(page.getByRole("dialog", { name: "Событие" })).toBeHidden();
    await page
      .getByRole("table")
      .getByRole("button", { name: "Удалить" })
      .click();
    await expect(page.getByRole("dialog")).toBeVisible();
    expect(deletions).toBe(0);
    await page.keyboard.press("Escape");
    await expect(page.getByRole("dialog")).toBeHidden();
    expect(deletions).toBe(0);
    if (viewport.width > 500) {
      await page
        .getByRole("table")
        .getByRole("button", { name: "Удалить" })
        .click();
      await page
        .getByRole("dialog")
        .getByRole("button", { name: "Удалить" })
        .click();
      await expect.poll(() => deletions).toBe(1);
      await expect(page.getByRole("dialog")).toBeHidden();
      await expect(
        page.getByRole("combobox", { name: "Источник" }),
      ).toBeEnabled();
      await page.getByRole("button", { name: "Удаление событий" }).click();
      await page
        .getByRole("button", { name: "Удалить события источника" })
        .click();
      await expect(
        page.getByRole("dialog", { name: "Удалить источник целиком" }),
      ).toContainText("независимо от времени");
      await page
        .getByRole("dialog", { name: "Удалить источник целиком" })
        .getByRole("button", { name: "Удалить" })
        .click();
      await expect.poll(() => bulkScopes.length).toBe(1);
      expect(bulkScopes[0]).toEqual({ source_id: localId });
      await page.getByRole("button", { name: "Удаление событий" }).click();
      await page
        .getByRole("button", { name: "Взять период применённого поиска" })
        .click();
      await page
        .getByRole("button", { name: "Удалить источник за период" })
        .click();
      await page
        .getByRole("dialog", { name: "Удалить события за период" })
        .getByRole("button", { name: "Удалить" })
        .click();
      await expect.poll(() => bulkScopes.length).toBe(2);
      expect(bulkScopes[1]).toMatchObject({
        source_id: localId,
        timestamp_from: expect.any(String),
        timestamp_to: expect.any(String),
      });
      await page.getByRole("button", { name: "Удаление событий" }).click();
      await page
        .getByRole("button", { name: "Удалить все локальные за период" })
        .click();
      await page
        .getByRole("dialog", { name: "Удалить события за период" })
        .getByRole("button", { name: "Удалить" })
        .click();
      await expect.poll(() => bulkScopes.length).toBe(3);
      expect(bulkScopes[2]).toEqual({
        timestamp_from: expect.any(String),
        timestamp_to: expect.any(String),
      });
      await page
        .locator("label")
        .filter({ hasText: "Источник" })
        .locator(".ant-select-selector")
        .click();
      await page.getByText(/Synthetic OpenSearch/).click();
      await expect(
        page.getByRole("table").getByText("synthetic event"),
      ).toBeVisible();
      await expect(
        page.getByRole("table").getByRole("button", { name: "Удалить" }),
      ).toHaveCount(0);
    }
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= window.innerWidth + 1,
      ),
    ).toBe(true);
  });
}

for (const viewport of [
  { width: 320, height: 540 },
  { width: 1440, height: 900 },
]) {
  test(`keeps failed search diagnostics available at ${viewport.width}px`, async ({
    page,
  }) => {
    await page.setViewportSize(viewport);
    let searchCalls = 0;
    await page.route("**/api/v1/**", async (route) => {
      const path = new URL(route.request().url()).pathname.replace(
        "/api/v1",
        "",
      );
      let status = 200;
      let body: unknown;
      if (path === "/auth/session") body = session(false);
      else if (path === "/sources")
        body = {
          items: [source(localId, "kafka", false)],
          total: 1,
          limit: 50,
          offset: 0,
        };
      else if (path === "/external-sources")
        body = { items: [], total: 0, limit: 50, offset: 0 };
      else if (path.endsWith("/fields"))
        body = {
          items: [{ name: "message", type: "text", operators: ["contains"] }],
          total: 1,
          limit: 100,
          offset: 0,
        };
      else if (path.endsWith("/search")) {
        searchCalls++;
        if (searchCalls === 1)
          body = {
            items: [row("kafka")],
            has_more: false,
            next_cursor: null,
          };
        else {
          status = 422;
          body = {
            code: "event_filter_invalid",
            message: "unsafe server detail must not be displayed",
            details: { index: 0 },
            request_id: "synthetic-request",
          };
        }
      }
      await route.fulfill({
        status,
        contentType: "application/json",
        body: JSON.stringify(body),
      });
    });
    await page.goto("/events");
    await page.getByRole("combobox", { name: "Источник" }).click();
    await page.getByText(/Synthetic Kafka/).click();
    await expect(
      page.getByRole("table").getByText("synthetic event"),
    ).toBeVisible();
    await page.getByRole("button", { name: "Фильтры и поля" }).click();
    const dialog = page.getByRole("dialog", { name: "Фильтры и поля" });
    await dialog.getByRole("button", { name: "Добавить условие" }).click();
    await dialog.getByRole("combobox", { name: "Поле условия" }).click();
    await page.getByText("message", { exact: true }).last().click();
    await dialog.getByRole("combobox", { name: "Оператор условия" }).click();
    await page.getByText("Содержит", { exact: true }).last().click();
    await dialog.getByPlaceholder("Значение").fill("short");
    await dialog.getByRole("button", { name: "Искать" }).click();
    await expect(
      dialog.getByText(/Источник отклонил это условие/),
    ).toBeVisible();
    await expect(dialog.getByPlaceholder("Значение")).toHaveValue("short");
    await expect(page.getByText("Условий: 0")).toBeVisible();
    await expect(
      page.getByRole("table").getByText("synthetic event"),
    ).toBeVisible();
    await expect(page.getByText(/unsafe server detail/)).toHaveCount(0);
    await expect
      .poll(() =>
        page.evaluate(
          () => document.documentElement.scrollWidth <= window.innerWidth + 1,
        ),
      )
      .toBe(true);
  });
}

test("guest can read events without deletion controls", async ({ page }) => {
  await page.route("**/api/v1/**", async (route) => {
    const path = new URL(route.request().url()).pathname.replace("/api/v1", "");
    const body =
      path === "/auth/session"
        ? session(false)
        : path === "/sources"
          ? {
              items: [source(localId, "kafka", false)],
              total: 1,
              limit: 50,
              offset: 0,
            }
          : path === "/external-sources"
            ? { items: [], total: 0, limit: 50, offset: 0 }
            : path.endsWith("/fields")
              ? { items: [], total: 0, limit: 100, offset: 0 }
              : path.endsWith("/search")
                ? { items: [row("kafka")], has_more: false, next_cursor: null }
                : null;
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify(body),
    });
  });
  await page.goto("/events");
  await page.getByRole("combobox", { name: "Источник" }).click();
  await page.getByText(/Synthetic Kafka/).click();
  await expect(
    page.getByRole("table").getByRole("button", { name: "Открыть" }),
  ).toBeVisible();
  await expect(page.getByRole("button", { name: /Удалить/ })).toHaveCount(0);
});
