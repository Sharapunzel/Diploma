import { expect, test } from "@playwright/test";

const password = process.env.TASK27_ADMIN_PASSWORD;
test.skip(!password, "requires an isolated OIDC smoke administrator");

test("OIDC trial preserves local admin and active login creates an OIDC session", async ({
  page,
}) => {
  const failures: string[] = [];
  page.on("requestfailed", (request) => {
    const url = new URL(request.url());
    failures.push(
      `${url.origin}${url.pathname}: ${request.failure()?.errorText}`,
    );
  });
  await page.goto("/login");
  await page
    .getByRole("textbox", { name: "Имя пользователя" })
    .fill("task27admin");
  await page.getByLabel("Пароль").fill(password!);
  await page.getByRole("button", { name: "Войти", exact: true }).click();
  await expect(page).toHaveURL(/\/$/);
  await page.goto("/administration");
  const trial = page.getByRole("button", { name: "Пробный вход" });
  await expect(trial).toBeEnabled();
  await trial.click();
  try {
    await expect(
      page.getByText("Пробный вход прошёл без создания сессии приложения"),
    ).toBeVisible({ timeout: 20_000 });
  } catch (error) {
    const url = new URL(page.url());
    console.log(`OIDC browser route: ${url.origin}${url.pathname}`);
    console.log(`OIDC browser request failures: ${failures.join("; ")}`);
    throw error;
  }

  const body = await page.evaluate(async () => {
    const response = await fetch("/api/v1/auth/session");
    if (!response.ok) throw new Error("session request failed");
    return response.json();
  });
  expect(body.user.username).toBe("task27admin");
  expect(body.authentication_method).toBe("local");

  await page.getByRole("button", { name: "Активировать", exact: true }).click();
  await page
    .getByRole("dialog")
    .getByRole("button", { name: "Активировать", exact: true })
    .click();
  await expect
    .poll(async () =>
      page.evaluate(async () => {
        const response = await fetch("/api/v1/oidc-provider");
        if (!response.ok) throw new Error("provider status request failed");
        return (await response.json()).mode;
      }),
    )
    .toBe("managed");

  const csrf = await page.evaluate(async () => {
    const response = await fetch("/api/v1/auth/session");
    if (!response.ok) throw new Error("session request failed");
    return (await response.json()).csrf_token;
  });
  const logout = await page.evaluate(
    async (csrfToken) =>
      fetch("/api/v1/auth/logout", {
        method: "POST",
        headers: { "X-CSRF-Token": csrfToken },
      }).then((response) => response.status),
    csrf,
  );
  expect(logout).toBe(200);

  await page.goto("/login");
  await page.getByRole("link", { name: "Войти через провайдера" }).click();
  await expect(page).toHaveURL(/\/$/);
  const oidcSession = await page.evaluate(async () => {
    const response = await fetch("/api/v1/auth/session");
    if (!response.ok) throw new Error("OIDC session request failed");
    return response.json();
  });
  expect(oidcSession.authenticated).toBe(true);
  expect(oidcSession.authentication_method).toBe("oidc");
  expect(oidcSession.user.display_name).toBe("Тест OIDC");
});
