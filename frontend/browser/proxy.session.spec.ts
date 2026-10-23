import { expect, test } from "@playwright/test";

const password = process.env.TASK21_ADMIN_PASSWORD;
const secureCookie = process.env.TASK21_SECURE_COOKIE === "true";
test.skip(!password, "requires the isolated production profile");

test("local session survives reload and ends through the proxy", async ({
  page,
}) => {
  await page.goto("/login");
  await expect(page.getByTestId("login-page")).toBeVisible();
  await page
    .getByRole("textbox", { name: "Имя пользователя" })
    .fill("task21admin");
  await page.getByLabel("Пароль").fill(password!);
  await page.getByRole("button", { name: "Войти", exact: true }).click();
  await expect(page).toHaveURL(/\/$/);
  const sessionCookie = (await page.context().cookies()).find(
    (cookie) => cookie.name === "diploma_session",
  );
  expect(sessionCookie).toBeDefined();
  expect(sessionCookie?.secure).toBe(secureCookie);
  expect(sessionCookie?.httpOnly).toBe(true);
  expect(sessionCookie?.sameSite).toBe("Lax");
  await page.goto("/administration");
  await expect(page).toHaveURL(/\/administration$/);
  await expect(page.getByTestId("login-page")).toHaveCount(0);
  await page.reload();
  await expect(page).toHaveURL(/\/administration$/);
  await expect(page.getByTestId("login-page")).toHaveCount(0);
  await page.getByRole("button", { name: "Аккаунт: task21admin" }).click();
  await page.getByRole("button", { name: "Выйти" }).click();
  const logoutRequestPromise = page.waitForRequest((request) =>
    request.url().endsWith("/api/v1/auth/logout"),
  );
  await page
    .getByRole("dialog", { name: "Выйти из аккаунта?" })
    .getByRole("button", { name: "Выйти" })
    .click();
  const logoutRequest = await logoutRequestPromise;
  expect(new URL(logoutRequest.url()).origin).toBe(
    new URL(process.env.PLAYWRIGHT_BASE_URL!).origin,
  );
  expect((await logoutRequest.allHeaders())["x-csrf-token"]).toBeTruthy();
  await expect(page.getByTestId("login-page")).toBeVisible();
});
