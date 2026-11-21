import { expect, test } from "@playwright/test";
import { config, loginFindings } from "./findings.fixtures";

test.setTimeout(90_000);

test("findings dialog uses the available viewport, both themes and keyboard focus", async ({
  page,
}) => {
  const start = "2026-11-12T20:00:00.123456Z";
  const end = "2026-11-12T20:01:00.123456Z";
  const id = "00000000-0000-4000-8000-000000000042";
  const feature = "Признак с длинным названием ".repeat(6);
  const row = {
    id,
    detector_id: id,
    detector_name: "Детектор с длинным названием ".repeat(6),
    source_name: "Источник с длинным названием ".repeat(6),
    window_start: start,
    window_end: end,
    time_basis: "event_time",
    confidence: 0.6789,
    grade: 0.279,
    late: true,
    accelerated_used: true,
  };
  const card = {
    ...row,
    source_id: id,
    aggregate_name: "Агрегат с длинным названием ".repeat(6),
    event_range: null,
    event_range_reason: "empty_window",
    aggregation_started_at: start,
    aggregation_completed_at: end,
    evaluation_started_at: end,
    evaluation_completed_at: end,
    expected_origin: "preceding_median",
    features: [
      {
        key: "a",
        name: feature,
        actual: "12345678901234567890.00001234",
        expected: "12345678901234567889.123456",
        attribution: 3,
      },
      { key: "b", name: "Второй", actual: "1", expected: "2", attribution: 1 },
      {
        key: "c",
        name: "Нулевой вклад",
        actual: "0",
        expected: null,
        attribution: 0,
      },
    ],
  };
  await page.route(/\/api\/v1\/findings(?:[/?]|$)/, (route) => {
    const slug = new URL(route.request().url()).pathname.split("/").at(-1);
    const json =
      slug === "timeline"
        ? { items: [row], total: 1, offset: 0, limit: 100 }
        : slug === "detectors"
          ? [row]
          : slug === id
            ? card
            : { items: [row], total: 1, offset: 0, limit: 25 };
    return route.fulfill({ json });
  });
  await loginFindings(page, config.username);
  for (const frame of [
    { theme: "light", width: 2560, height: 900, zoom: 1 },
    { theme: "dark", width: 320, height: 560, zoom: 1 },
    { theme: "dark", width: 1280, height: 360, zoom: 1 },
    { theme: "light", width: 800, height: 600, zoom: 2 },
  ] as const) {
    await page.emulateMedia({ colorScheme: frame.theme });
    await expect(page.locator("html")).toHaveAttribute(
      "data-theme",
      frame.theme,
    );
    await page.setViewportSize({ width: frame.width, height: frame.height });
    await page.evaluate((zoom) => {
      document.documentElement.style.zoom = String(zoom);
    }, frame.zoom);
    const trigger = page
      .getByRole("row")
      .filter({ hasText: "Запоздалая оценка" })
      .getByRole("button", { name: row.detector_name, exact: true });
    await trigger.focus();
    await trigger.press("Enter");
    const modal = page.getByRole("dialog", { name: "Подробности находки" });
    await expect(
      modal.getByRole("heading", { name: "Степень аномальности" }),
    ).toBeVisible();
    await expect(
      modal.getByText("12345678901234567890.00001234"),
    ).toBeVisible();
    const legend = modal.getByRole("list", { name: "Доли вкладов признаков" });
    await expect(legend.getByText(/Нулевой вклад.*0/)).toBeAttached();
    await legend.focus();
    await expect(legend).toBeFocused();
    await expect(
      modal.getByText("В сохранённом окне не было событий."),
    ).toBeAttached();
    const body = modal.locator(".ant-modal-body");
    expect(
      await body.evaluate((area) => area.scrollWidth <= area.clientWidth + 1),
    ).toBe(true);
    if (frame.height <= 600)
      expect(
        await body.evaluate((area) => area.scrollHeight > area.clientHeight),
      ).toBe(true);
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth,
      ),
    ).toBe(true);
    const bounds = await modal.boundingBox();
    expect(bounds!.x).toBeGreaterThanOrEqual(0);
    expect(bounds!.x + bounds!.width).toBeLessThanOrEqual(frame.width + 1);
    expect(bounds!.y + bounds!.height).toBeLessThanOrEqual(frame.height + 1);
    if (frame.width > 1200)
      await expect
        .poll(async () => (await modal.boundingBox())?.width ?? 0)
        .toBeGreaterThanOrEqual(1020);
    await page.screenshot({
      path: test
        .info()
        .outputPath(`findings-${frame.theme}-${frame.width}.png`),
      animations: "disabled",
    });
    await page.keyboard.press("Escape");
    await expect(modal).toBeHidden();
    await expect(trigger).toBeFocused();
  }
});
