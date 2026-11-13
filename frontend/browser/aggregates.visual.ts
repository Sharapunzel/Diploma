import { expect, test, type Page } from "@playwright/test";

/** Chromium canonicalizes a zero-second datetime-local value to minute precision. */
export function calendarInputValue(iso: string) {
  const wall = iso.slice(0, 19);
  return wall.length === 19 && wall.endsWith(":00") ? wall.slice(0, 16) : wall;
}

/** Representative nonempty screens; functional scenarios are not multiplied by viewport. */
export async function inspectAggregateScreen(page: Page, name: string) {
  for (const theme of ["light", "dark"] as const) {
    await page.emulateMedia({ colorScheme: theme });
    await expect(page.locator("html")).toHaveAttribute("data-theme", theme);
    for (const width of [320, 2560]) {
      await page.setViewportSize({ width, height: 720 });
      await page.mouse.move(0, 0);
      const dialog = page.getByRole("dialog");
      if (name === "primary-modal" || name === "derived-modal") {
        await expect
          .poll(async () => {
            const box = await dialog.boundingBox();
            const footer = await dialog
              .getByRole("button", { name: "Отмена", exact: true })
              .boundingBox();
            return (
              !!box &&
              !!footer &&
              box.y >= 12 &&
              box.y + box.height <= 708 &&
              footer.y >= 12 &&
              footer.y + footer.height <= 708
            );
          })
          .toBe(true);
        for (const field of await dialog
          .getByRole("combobox", { name: "Поле ECS", exact: true })
          .all()) {
          const widths = await field.evaluate((input) => ({
            select: input.closest(".ant-select")!.getBoundingClientRect().width,
            field: input
              .closest(".ant-select")!
              .parentElement!.getBoundingClientRect().width,
          }));
          expect(widths.select).toBeGreaterThan(widths.field * 0.9);
        }
      }
      if (name === "diagnostics")
        await page
          .getByRole("heading", { name: "Предварительный расчёт", exact: true })
          .evaluate((el) => el.scrollIntoView({ block: "start" }));
      if (
        (await dialog.count()) &&
        (await dialog
          .getByRole("heading", { name: "1. Настройки признака", exact: true })
          .count())
      )
        await dialog
          .getByRole("heading", { name: "1. Настройки признака", exact: true })
          .scrollIntoViewIfNeeded();
      else if (
        name !== "diagnostics" &&
        (await page
          .getByRole("link", { name: "← К агрегатам", exact: true })
          .count())
      )
        await page
          .getByRole("link", { name: "← К агрегатам", exact: true })
          .scrollIntoViewIfNeeded();
      await expect
        .poll(() =>
          page.evaluate(
            () => document.documentElement.scrollWidth <= innerWidth,
          ),
        )
        .toBe(true);
      await page.screenshot({
        path: test.info().outputPath(`${name}-${theme}-${width}.png`),
        fullPage: false,
      });
      if (
        (await dialog.count()) &&
        (await dialog
          .getByRole("heading", { name: "4. Итоговая формула", exact: true })
          .count())
      ) {
        await dialog
          .getByRole("heading", { name: "4. Итоговая формула", exact: true })
          .scrollIntoViewIfNeeded();
        await page.screenshot({
          path: test.info().outputPath(`${name}-${theme}-${width}-formula.png`),
          fullPage: false,
        });
      }
      if (name === "diagnostics") {
        await page
          .getByRole("region", { name: "Значения признаков", exact: true })
          .evaluate((el) => el.scrollIntoView({ block: "start" }));
        await page.screenshot({
          path: test.info().outputPath(`${name}-${theme}-${width}-values.png`),
          fullPage: false,
        });
      }
      if (["create", "copy", "saved-card"].includes(name)) {
        await page
          .getByRole("region", { name: "Признаки", exact: true })
          .scrollIntoViewIfNeeded();
        await page.screenshot({
          path: test
            .info()
            .outputPath(`${name}-${theme}-${width}-features.png`),
          fullPage: false,
        });
      }
    }
  }
  await page.setViewportSize({ width: 800, height: 600 });
  await page.evaluate(() => {
    document.documentElement.style.zoom = "2";
  });
  try {
    const dialog = page.getByRole("dialog");
    if (await dialog.count()) {
      await expect
        .poll(async () => {
          const box = await dialog.boundingBox();
          return !!box && box.x >= 0 && box.x + box.width <= 801;
        })
        .toBe(true);
      if (name === "primary-modal" || name === "derived-modal") {
        await expect
          .poll(async () => {
            const box = await dialog.boundingBox();
            const footer = await dialog
              .getByRole("button", { name: "Отмена", exact: true })
              .boundingBox();
            return (
              !!box &&
              !!footer &&
              box.y >= 12 &&
              box.y + box.height <= 588 &&
              footer.y + footer.height <= 588
            );
          })
          .toBe(true);
      }
    }
    await expect
      .poll(() =>
        page.evaluate(() => document.documentElement.scrollWidth <= innerWidth),
      )
      .toBe(true);
    await page.screenshot({
      path: test.info().outputPath(`${name}-zoom-200.png`),
      fullPage: false,
    });
  } finally {
    await page.evaluate(() => {
      document.documentElement.style.zoom = "";
    });
    await page.setViewportSize({ width: 1280, height: 800 });
  }
}
