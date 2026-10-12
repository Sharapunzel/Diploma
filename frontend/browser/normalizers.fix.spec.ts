import { expect, test } from "@playwright/test";

const id = "00000000-0000-4000-8000-000000000090";
const another = "00000000-0000-4000-8000-000000000091";
const session = {
  authenticated: true,
  user: { id: "user", username: "demo", email: null, display_name: "Demo" },
  role: "demo",
  permissions: ["normalizers.read", "normalizers.write", "sources.read"],
  authentication_method: "local",
  csrf_token: "csrf",
};
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
const record = (key: string, name: string) => ({
  id: key,
  name,
  description: "Test",
  rule,
  version: 1,
  rule_status: "ready",
  created_by_user_id: null,
  updated_by_user_id: null,
  created_at: "2026-01-01T00:00:00Z",
  updated_at: "2026-01-01T00:00:00Z",
});
const data = (items: unknown[], total = items.length, offset = 0) => ({
  items,
  total,
  limit: 50,
  offset,
});
const respond = (body: unknown, status = 200) => ({
  status,
  contentType: "application/json",
  body: status === 204 ? "" : JSON.stringify(body),
});

test("confirmed deletion survives list failure and retry", async ({ page }) => {
  let deleted = false;
  let failRead = true;
  let deletes = 0;
  await page.route("**/api/v1/**", async (route) => {
    const url = new URL(route.request().url());
    const path = url.pathname.replace("/api/v1", "");
    if (path === "/auth/session") return route.fulfill(respond(session));
    if (path === "/normalizers" && deleted && failRead)
      return route.fulfill(
        respond({ code: "server_error", message: "read failed" }, 503),
      );
    if (path === "/normalizers")
      return route.fulfill(
        respond(data(deleted ? [] : [record(id, "Delete me")])),
      );
    if (
      path === `/normalizers/${id}` &&
      route.request().method() === "DELETE"
    ) {
      deletes++;
      deleted = true;
      return route.fulfill(respond(null, 204));
    }
    return route.fulfill(respond(null));
  });
  await page.goto("/normalizers");
  await expect(page.getByText("Delete me")).toBeVisible();
  await page.getByRole("button", { name: "Удалить" }).first().click();
  await page
    .getByRole("dialog")
    .getByRole("button", { name: "Удалить" })
    .click();
  await expect(page.getByText(/Нормализатор удалён\./)).toBeVisible();
  await expect(page.getByText(/Список ещё не сверен с сервером/)).toBeVisible();
  await page
    .getByRole("button", { name: "Закрыть сообщение об ошибке" })
    .click();
  await expect(page.getByText(/Список ещё не сверен с сервером/)).toBeVisible();
  await expect(page.getByRole("table").getByText("Delete me")).toHaveCount(0);
  expect(deletes).toBe(1);
  failRead = false;
  await page.getByRole("button", { name: "Повторить чтение списка" }).click();
  await expect(page.getByText(/Список ещё не сверен с сервером/)).toHaveCount(
    0,
  );
  expect(deletes).toBe(1);
});

test("unknown deletion result handles failed verification before a successful check", async ({
  page,
}) => {
  let checks = 0;
  let deletes = 0;
  await page.route("**/api/v1/**", async (route) => {
    const path = new URL(route.request().url()).pathname.replace("/api/v1", "");
    if (path === "/auth/session") return route.fulfill(respond(session));
    if (path === "/normalizers")
      return route.fulfill(
        respond(data(checks >= 2 ? [] : [record(id, "Uncertain")])),
      );
    if (
      path === `/normalizers/${id}` &&
      route.request().method() === "DELETE"
    ) {
      deletes++;
      return route.abort("failed");
    }
    if (path === `/normalizers/${id}` && route.request().method() === "GET") {
      checks++;
      return route.fulfill(
        checks === 1
          ? respond({ code: "server_error" }, 503)
          : respond({ code: "not_found" }, 404),
      );
    }
    return route.fulfill(respond(null));
  });
  await page.goto("/normalizers");
  await page.getByRole("button", { name: "Удалить" }).first().click();
  const dialog = page.getByRole("dialog");
  await dialog.getByRole("button", { name: "Удалить" }).click();
  await expect(dialog.getByText(/Результат запроса неизвестен/)).toBeVisible();
  await expect(dialog.getByRole("button", { name: "Удалить" })).toBeDisabled();
  await dialog.getByRole("button", { name: "Обновить состояние" }).click();
  await expect(dialog.getByText(/Результат запроса неизвестен/)).toBeVisible();
  await dialog.getByRole("button", { name: "Обновить состояние" }).click();
  await expect(dialog).toHaveCount(0);
  await expect(page.getByText(/Нормализатор удалён/)).toBeVisible();
  expect(deletes).toBe(1);
});

test("deleting the sole row on a later page returns to the previous page", async ({
  page,
}) => {
  const offsets: number[] = [];
  let deleted = false;
  await page.route("**/api/v1/**", async (route) => {
    const url = new URL(route.request().url());
    const path = url.pathname.replace("/api/v1", "");
    if (path === "/auth/session") return route.fulfill(respond(session));
    if (path === "/normalizers") {
      const offset = Number(url.searchParams.get("offset") || 0);
      offsets.push(offset);
      return route.fulfill(
        respond(
          data(
            offset === 50 && !deleted
              ? [record(id, "Last row")]
              : offset === 0
                ? [record(another, "First page")]
                : [],
            deleted ? 50 : 51,
            offset,
          ),
        ),
      );
    }
    if (
      path === `/normalizers/${id}` &&
      route.request().method() === "DELETE"
    ) {
      deleted = true;
      return route.fulfill(respond(null, 204));
    }
    return route.fulfill(respond(null));
  });
  await page.goto("/normalizers");
  await page.getByRole("listitem", { name: "2" }).last().click();
  await expect(page.getByText("Last row")).toBeVisible();
  await page.getByRole("button", { name: "Удалить" }).first().click();
  await page
    .getByRole("dialog")
    .getByRole("button", { name: "Удалить" })
    .click();
  await expect(page.getByText("First page")).toBeVisible();
  expect(offsets.at(-1)).toBe(0);
});

test("latest sources page wins when responses arrive out of order", async ({
  page,
}) => {
  let releaseOlder: (() => void) | undefined;
  let secondPageReads = 0;
  await page.route("**/api/v1/**", async (route) => {
    const url = new URL(route.request().url());
    const path = url.pathname.replace("/api/v1", "");
    if (path === "/auth/session") return route.fulfill(respond(session));
    if (path === "/normalizers")
      return route.fulfill(respond(data([record(id, "With sources")])));
    if (path === "/sources") {
      const offset = Number(url.searchParams.get("offset") || 0);
      if (offset === 0) {
        return route.fulfill(
          respond(
            data(
              [
                {
                  id: "current",
                  name: "Current source",
                  is_enabled: false,
                },
              ],
              51,
              0,
            ),
          ),
        );
      }
      secondPageReads++;
      const requestNumber = secondPageReads;
      if (requestNumber === 1)
        await new Promise<void>((resolve) => {
          releaseOlder = resolve;
        });
      return route.fulfill(
        respond(
          data(
            [
              {
                id: "source",
                name: requestNumber === 1 ? "Old source" : "New source",
                is_enabled: true,
              },
            ],
            51,
            50,
          ),
        ),
      );
    }
    return route.fulfill(respond(null));
  });
  await page.goto("/normalizers");
  await page.locator(".ant-table-row-expand-icon").first().click();
  await expect(page.getByText("Current source")).toBeVisible();
  await page.getByRole("listitem", { name: "2" }).first().click();
  await page.getByRole("listitem", { name: "2" }).first().click();
  await expect(page.getByText("New source")).toBeVisible();
  releaseOlder?.();
  await expect(page.getByText("Old source")).toHaveCount(0);
});
