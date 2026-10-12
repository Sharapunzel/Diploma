import { expect, test } from "@playwright/test";

const id = "00000000-0000-4000-8000-000000000090";
const sourceId = "00000000-0000-4000-8000-000000000091";
const rule = {
  format_version: 1,
  variants: [
    {
      key: "event",
      priority: 1,
      when: { kind: "contains", value: "hello" },
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

for (const [width, theme] of [
  [320, "light"],
  [1280, "dark"],
  [2560, "light"],
] as const) {
  test(`source debug at ${width}px in ${theme} theme`, async ({ page }) => {
    const pageErrors: string[] = [];
    page.on("pageerror", (error) => pageErrors.push(error.message));
    await page.setViewportSize({ width, height: 900 });
    await page.addInitScript(
      (choice) => localStorage.setItem("theme", choice),
      theme,
    );
    let previewSample: unknown = null;
    await page.route("**/api/v1/**", async (route) => {
      const path = new URL(route.request().url()).pathname.replace(
        "/api/v1",
        "",
      );
      let body: unknown = {};
      if (path === "/auth/session")
        body = {
          authenticated: true,
          user: {
            id: "u",
            username: "demo",
            email: null,
            display_name: "Demo",
          },
          role: "demo",
          permissions: [
            "normalizers.read",
            "normalizers.write",
            "sources.read",
          ],
          authentication_method: "local",
          csrf_token: "csrf",
        };
      else if (path === `/normalizers/${id}`)
        body = {
          id,
          name: "Synthetic rule",
          description: null,
          rule,
          version: 1,
          rule_status: "ready",
          created_by_user_id: null,
          updated_by_user_id: null,
          created_at: "2026-01-01T00:00:00Z",
          updated_at: "2026-01-01T00:00:00Z",
        };
      else if (path === "/normalizers/block-types") body = [];
      else if (path === "/sources")
        body = {
          items: [
            {
              id: sourceId,
              source_type: "kafka",
              name: "Synthetic Kafka",
              is_archived: false,
              is_enabled: false,
            },
          ],
          total: 1,
          limit: 50,
          offset: 0,
        };
      else if (path === `/sources/${sourceId}/debug-snapshot`)
        body = {
          records: [0, 1].map((position) => ({
            position,
            timestamp: "2024-01-01T00:00:00Z",
            log: position === 0 ? "hello secret-value" : "second log marker",
            issue: null,
          })),
          skipped: 0,
          truncated: false,
        };
      else if (path === "/normalizers/preview") {
        previewSample = route.request().postDataJSON().sample;
        body = {
          status: "partial",
          variant_key: "event",
          ecs_data: { message: "hello secret-value" },
          fluent_bit_collected_at: "2024-01-01T00:00:00Z",
          diagnostics: [
            {
              code: "event_time_fallback",
              variant: "event",
              block: null,
              field: null,
            },
          ],
          trace: [
            {
              variant: "event",
              block: "message",
              candidate: null,
              result: "mapped",
            },
          ],
        };
      }
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify(body),
      });
    });
    await page.goto(`/normalizers/${id}/edit`);
    await page.getByRole("button", { name: "Лог из источника" }).click();
    await page.getByRole("combobox", { name: "Kafka-источник" }).click();
    await page.getByText("Synthetic Kafka").last().click();
    await expect(page.getByText(/Доступно записей: 2/)).toBeVisible();
    await page
      .locator('[aria-label="Запись снимка"] .ant-select-selector')
      .click();
    await page.getByText(/Запись 2: second log marker/).click();
    await expect(
      page.getByText("second log marker", { exact: true }),
    ).toBeVisible();
    await page.getByRole("button", { name: "Тест", exact: true }).focus();
    await page.keyboard.press("Enter");
    await expect(
      page.getByText("second log marker", { exact: true }),
    ).toBeVisible();
    expect(previewSample).toEqual({
      timestamp: "2024-01-01T00:00:00Z",
      log: "second log marker",
    });
    const download = page.waitForEvent("download");
    await page
      .getByRole("button", { name: "Скачать безопасный отчёт" })
      .click();
    expect((await download).suggestedFilename()).toBe(
      "normalizer-debug-report.json",
    );
    await page.getByLabel("Имя", { exact: true }).fill("Unsaved draft");
    await page.evaluate(() =>
      window.dispatchEvent(new PopStateEvent("popstate")),
    );
    await expect(
      page.getByRole("dialog", { name: "Покинуть редактор?" }),
    ).toBeVisible();
    await expect(page.getByRole("dialog")).toHaveCount(1);
    const beforeUnloadBlocked = await page.evaluate(() => {
      const event = new Event("beforeunload", { cancelable: true });
      return !window.dispatchEvent(event);
    });
    expect(beforeUnloadBlocked).toBe(true);
    await page
      .getByRole("dialog", { name: "Покинуть редактор?" })
      .getByRole("button", { name: "Отмена" })
      .click();
    await expect(page.getByText("Проверено: 1")).toBeVisible();
    const dimensions = await page.evaluate(() => ({
      document: document.documentElement.scrollWidth,
      viewport: document.documentElement.clientWidth,
    }));
    expect(dimensions.document).toBeLessThanOrEqual(dimensions.viewport);
    await expect(page.locator("html")).toHaveAttribute("data-theme", theme);
    expect(pageErrors).toEqual([]);
  });
}
