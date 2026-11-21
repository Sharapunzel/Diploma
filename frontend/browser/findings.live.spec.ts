import { expect, test, type Page } from "@playwright/test";
import { config, loginFindings } from "./findings.fixtures";

const fixture = config.findings;
if (!fixture) throw new Error("Missing saved findings fixture");
test.setTimeout(90_000);

function eventSearch(page: Page, source: string) {
  return page.waitForResponse(
    (response) =>
      new URL(response.url()).pathname ===
      `/api/v1/events/sources/${source}/search`,
  );
}

for (const role of ["admin", "guest"] as const) {
  test(`${role} reads a cleared finding and opens the exact event range across reload`, async ({
    page,
  }) => {
    const capture = role === "admin";
    if (capture) {
      await page.emulateMedia({ colorScheme: "light" });
      await page.setViewportSize({ width: 1540, height: 1120 });
    }
    const borrowedApiCalls: string[] = [];
    page.on("request", (request) => {
      if (/\/api\/v1\/(aggregates|detectors)(\/|\?|$)/.test(request.url()))
        borrowedApiCalls.push(request.url());
    });
    await loginFindings(
      page,
      role === "admin" ? config.username : fixture.guest,
    );
    expect((await page.reload())?.ok()).toBeTruthy();
    const nav = page.getByRole("navigation", { name: "Основное меню" });
    await expect(
      nav.getByRole("link", { name: "Находки", exact: true }),
    ).toBeVisible();
    for (const name of ["Агрегаты", "Детекторы"])
      await expect(nav.getByRole("link", { name, exact: true })).toHaveCount(
        role === "admin" ? 1 : 0,
      );
    const primary = page
      .getByRole("row")
      .filter({ hasText: "Запоздалая оценка" });
    await expect(primary).toBeVisible();
    await expect(
      primary.getByText("Использован ускоренный прогрев", { exact: false }),
    ).toBeVisible();
    if (capture) {
      await expect(
        page.getByRole("list", { name: "Детекторы и основание времени" }),
      ).toBeVisible();
      await page.screenshot({
        path: test.info().outputPath("findings-page.png"),
        animations: "disabled",
        fullPage: true,
      });
    }

    await page.getByRole("button", { name: "Фильтры", exact: true }).click();
    const dialog = page.getByRole("dialog", { name: "Фильтры находок" });
    await dialog
      .getByRole("combobox", { name: "Детектор", exact: true })
      .click();
    await page
      .locator(".ant-select-dropdown")
      .getByText(`${fixture.detector_name} · ${fixture.detectors[0]}`, {
        exact: true,
      })
      .click();
    await dialog
      .getByRole("spinbutton", { name: "Степень аномальности, от" })
      .fill("0.8");
    await dialog
      .getByRole("spinbutton", { name: "Степень аномальности, до" })
      .fill("0.8");
    await dialog
      .getByRole("spinbutton", { name: "Уверенность оценки, от" })
      .fill("0.9");
    const filtered = page.waitForResponse((response) => {
      const url = new URL(response.url());
      return (
        url.pathname === "/api/v1/findings" &&
        url.searchParams.has("confidence_min")
      );
    });
    await dialog.getByRole("button", { name: "Применить" }).click();
    expect(await (await filtered).json()).toMatchObject({
      total: 1,
      items: [{ id: fixture.findings.primary }],
    });
    const column = page.locator(`#finding-window-${fixture.findings.primary}`);
    await column.hover();
    const hint = page.getByRole("tooltip");
    await expect(hint).toContainText("Начало окна");
    await expect(hint).toContainText("Конец окна");
    await expect(hint).toContainText("0.8");
    await expect(hint).toContainText("0.9");
    await column.click();
    await expect(primary).toHaveAttribute("data-highlighted", "true");
    await expect(primary).toBeFocused();
    await primary.getByRole("button", { name: "На графике" }).click();
    await expect(column).toHaveAttribute("data-highlighted", "true");
    await expect(column).toBeFocused();
    const trigger = primary.getByRole("button", {
      name: fixture.detector_name,
      exact: true,
    });
    await trigger.click();
    const details = page.getByRole("dialog", { name: "Подробности находки" });
    await expect(
      details.getByText("1152921504606846977", { exact: true }),
    ).toBeVisible();
    await expect(
      details.getByText("1152921504606846976.75", { exact: true }),
    ).toBeVisible();
    await expect(
      details.getByRole("list", { name: "Доли вкладов признаков" }),
    ).toContainText("75");
    await expect(
      details.getByRole("heading", { name: /^Как получена находка/ }),
    ).toBeAttached();
    await expect
      .poll(() =>
        details.evaluate((dialog) => dialog.contains(document.activeElement)),
      )
      .toBe(true);
    if (capture) {
      await page.setViewportSize({ width: 1540, height: 1570 });
      const card = details.locator(".ant-modal-content");
      await expect
        .poll(async () => (await card.boundingBox())?.width ?? 0)
        .toBeGreaterThan(1000);
      await page.screenshot({
        path: test.info().outputPath("finding-card.png"),
        animations: "disabled",
        clip: (await card.boundingBox())!,
      });
      const help = details.getByRole("button", {
        name: "Справка: Фактическое",
      });
      await help.focus();
      await page.keyboard.press("Enter");
      await expect(
        page.getByText(
          "Точное значение признака, которое детектор получил из агрегата.",
        ),
      ).toBeVisible();
      await page.keyboard.press("Escape");
      await expect(help).toHaveAttribute("aria-expanded", "false");
      await expect(details).toBeVisible();

      for (const frame of [
        { width: 320, height: 560, zoom: 1 },
        { width: 800, height: 600, zoom: 2 },
      ]) {
        await page.emulateMedia({ colorScheme: "dark" });
        await page.setViewportSize({
          width: frame.width,
          height: frame.height,
        });
        await page.evaluate((zoom) => {
          document.documentElement.style.zoom = String(zoom);
        }, frame.zoom);
        await expect(page.locator("html")).toHaveAttribute(
          "data-theme",
          "dark",
        );
        expect(
          await details
            .locator(".ant-modal-body")
            .evaluate(
              (area) =>
                area.scrollWidth <= area.clientWidth + 1 &&
                area.scrollHeight > area.clientHeight,
            ),
        ).toBe(true);
        const bounds = await details.boundingBox();
        expect(bounds!.x + bounds!.width).toBeLessThanOrEqual(frame.width + 1);
        expect(bounds!.y + bounds!.height).toBeLessThanOrEqual(
          frame.height + 1,
        );
      }
      await page.evaluate(() => {
        document.documentElement.style.zoom = "";
      });
      await page.setViewportSize({ width: 1540, height: 1120 });
    }
    await expect
      .poll(() =>
        details.evaluate((dialog) => dialog.contains(document.activeElement)),
      )
      .toBe(true);
    await page.keyboard.press("Escape");
    await expect(details).toBeHidden();
    await expect(trigger).toBeFocused();
    expect(borrowedApiCalls).toEqual([]);

    const sources = await page.request.get("/api/v1/sources?limit=50&offset=0");
    expect(sources.ok()).toBeTruthy();
    const catalog = (await sources.json()) as { items: { id: string }[] };
    expect(catalog.items).toHaveLength(50);
    expect(catalog.items.map(({ id }) => id)).not.toContain(fixture.source);
    await trigger.click();
    for (const navigate of [
      () => details.getByRole("link", { name: "Перейти к событиям" }).click(),
      async () => expect((await page.reload())?.ok()).toBeTruthy(),
    ]) {
      const searched = eventSearch(page, fixture.source);
      await navigate();
      const response = await searched;
      expect(response.ok()).toBeTruthy();
      expect(response.request().postDataJSON()).toMatchObject({
        timestamp_from: fixture.from,
        timestamp_to: fixture.to,
      });
      const body = (await response.json()) as {
        items: { fields: { message: string } }[];
      };
      expect(body.items.map(({ fields }) => fields.message).sort()).toEqual([
        "findings-late",
        "findings-max",
        "findings-min",
      ]);
    }
    await expect(
      page.getByText(fixture.source_name, { exact: true }),
    ).toBeVisible();
    await page.getByRole("link", { name: "Обычный поиск" }).click();
    await expect(
      page.getByText("Выберите источник для просмотра событий."),
    ).toBeVisible();
  });
}
