/** Isolated Home smoke through the project's end-to-end profile. */
import { expect, test } from "@playwright/test";

const password = process.env.TASK21_ADMIN_PASSWORD;
test.skip(!password, "requires the isolated end-to-end profile");

test("shows local and external inventory through the proxy", async ({
  page,
  browser,
}) => {
  const login = await page.request.post("/api/v1/auth/local/login", {
    data: { username: "task21admin", password },
  });
  expect(login.ok()).toBeTruthy();
  const csrf = (await login.json()).csrf_token as string;
  const headers = { "X-CSRF-Token": csrf };
  const overview = await page.request.get("/api/v1/home/overview");
  expect(overview.ok()).toBeTruthy();
  const snapshot = await overview.json();
  expect(snapshot.sources.external_disabled).toBeGreaterThanOrEqual(1);
  expect(snapshot.sources.own_disabled).toBeGreaterThanOrEqual(1);
  expect(snapshot.connections.external).toBeGreaterThanOrEqual(1);

  await page.goto("/");
  await expect(
    page.getByRole("heading", { name: "Обработка данных" }),
  ).toBeVisible();
  await expect(
    page.getByRole("heading", { name: "Внешние источники" }),
  ).toBeVisible();
  for (const width of [320, 1280, 2560]) {
    await page.setViewportSize({ width, height: 800 });
    expect(
      await page.evaluate(() => document.documentElement.scrollWidth),
    ).toBeLessThanOrEqual(width);
    await expect(
      page.getByRole("heading", { name: "Собственные источники" }),
    ).toBeVisible();
  }
  let previousSurface: string | null = null;
  for (const theme of ["light", "dark"] as const) {
    await page.evaluate(
      (choice) => localStorage.setItem("theme", choice),
      theme,
    );
    await page.reload();
    await expect(page.locator("html")).toHaveAttribute("data-theme", theme);
    await expect(
      page.getByRole("heading", { name: "Анализ данных" }),
    ).toBeVisible();
    const surface = await page
      .getByRole("region", { name: "Обработка данных" })
      .evaluate((element) => getComputedStyle(element).backgroundColor);
    expect(surface).not.toBe("");
    if (previousSurface !== null) expect(surface).not.toBe(previousSurface);
    previousSurface = surface;
  }
  const refresh = page.getByRole("button", { name: "Обновить показатели" });
  await refresh.focus();
  await page.keyboard.press("Space");
  await expect(page.getByText("Снимок:")).toBeVisible();
  const sourcesLink = page
    .getByRole("region", { name: "Обработка данных" })
    .getByRole("link", { name: /Источники/ });
  await sourcesLink.focus();
  await page.keyboard.press("Enter");
  await expect(page).toHaveURL(/\/sources$/);
  await page.goto("/");
  await page
    .getByRole("region", { name: "Обработка данных" })
    .getByRole("link", { name: /Диагностика/ })
    .click();
  await expect(page).toHaveURL(/\/diagnostics$/);
  await page.goto("/");
  await page
    .getByRole("region", { name: "Анализ данных" })
    .getByRole("link", { name: /События/ })
    .click();
  await expect(page).toHaveURL(/\/events$/);

  const roles = await page.request.get("/api/v1/roles");
  expect(roles.ok()).toBeTruthy();
  const guestRole = (await roles.json()).find(
    (role: { name: string }) => role.name === "Guest",
  );
  expect(guestRole).toBeDefined();
  const guestPassword = "synthetic-guest-password";
  const guest = await page.request.post("/api/v1/users", {
    headers,
    data: {
      username: "homeguest",
      password: guestPassword,
      display_name: "Home guest",
      role_id: guestRole.id,
    },
  });
  expect(guest.status()).toBe(201);
  const guestContext = await browser.newContext({
    baseURL: new URL(page.url()).origin,
  });
  try {
    const guestPage = await guestContext.newPage();
    const guestLogin = await guestPage.request.post(
      "/api/v1/auth/local/login",
      {
        data: { username: "homeguest", password: guestPassword },
      },
    );
    expect(guestLogin.ok()).toBeTruthy();
    const guestOverview = await guestPage.request.get("/api/v1/home/overview");
    expect(guestOverview.status()).toBe(200);
    expect(Object.keys(await guestOverview.json())).toEqual(["captured_at"]);
    await guestPage.goto("/");
    await expect(
      guestPage.getByRole("heading", { name: "Анализ данных" }),
    ).toBeVisible();
    await expect(
      guestPage.getByText(/Инвентарные показатели недоступны/),
    ).toBeVisible();
    await expect(guestPage.locator("#main").getByRole("link")).toHaveCount(0);
    await expect(
      guestPage
        .locator("#main")
        .getByRole("link", { name: "Администрирование" }),
    ).toHaveCount(0);
  } finally {
    await guestContext.close();
  }
});
