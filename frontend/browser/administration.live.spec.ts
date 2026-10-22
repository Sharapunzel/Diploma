/** TASK-25 administration smoke on the project's isolated end-to-end profile. */
import { expect, test } from "@playwright/test";

const password = process.env.TASK21_ADMIN_PASSWORD;
test.skip(!password, "requires the isolated end-to-end profile");

test("admin manages the page while Guest cannot read it or its APIs", async ({
  page,
  browser,
}) => {
  test.setTimeout(120_000);
  const login = await page.request.post("/api/v1/auth/local/login", {
    data: { username: "task21admin", password },
  });
  expect(login.ok()).toBeTruthy();
  const csrf = (await login.json()).csrf_token as string;
  const headers = { "X-CSRF-Token": csrf };
  const rolesResponse = await page.request.get("/api/v1/roles");
  expect(rolesResponse.ok()).toBeTruthy();
  const roles = (await rolesResponse.json()) as { id: string; name: string }[];
  const guestRole = roles.find((role) => role.name === "Guest");
  expect(guestRole).toBeDefined();
  const suffix = Date.now().toString(36);
  const username = `admin-smoke-${suffix}`;
  const guestPassword = "synthetic-guest-password";
  let userId: string | null = null;
  let mappingId: string | null = null;
  try {
    await page.goto("/administration");
    await expect(
      page.getByRole("heading", { name: "Пользователи" }),
    ).toBeVisible();
    await expect(
      page.getByRole("heading", { name: "Роли и доступ" }),
    ).toBeVisible();
    const users = page.getByRole("region", { name: "Пользователи" });
    const access = page.getByRole("region", { name: "Роли и доступ" });
    const rolesTab = access.getByRole("tab", { name: "Роли" });
    const mappingsTab = access.getByRole("tab", { name: "Сопоставления OIDC" });
    const rolesPanel = page.locator("#administration-panel-roles");
    const mappingsPanel = page.locator("#administration-panel-mappings");
    expect(await rolesTab.getAttribute("aria-controls")).toBe(
      "administration-panel-roles",
    );
    expect(await mappingsTab.getAttribute("aria-controls")).toBe(
      "administration-panel-mappings",
    );
    await expect(rolesPanel).toHaveAttribute(
      "aria-labelledby",
      (await rolesTab.getAttribute("id")) ?? "",
    );
    await expect(mappingsPanel).toHaveAttribute(
      "aria-labelledby",
      (await mappingsTab.getAttribute("id")) ?? "",
    );
    await expect(mappingsPanel).toHaveAttribute("aria-hidden", "true");
    await expect(mappingsPanel).toBeHidden();
    await expect(rolesPanel.getByText("Administrator")).toBeVisible();
    for (const viewportSize of [
      { width: 320, height: 480 },
      { width: 1280, height: 900 },
      { width: 2560, height: 650 },
    ]) {
      await page.setViewportSize(viewportSize);
      expect(
        await page.evaluate(() => document.documentElement.scrollWidth),
      ).toBeLessThanOrEqual(viewportSize.width);
      const userBox = await users.boundingBox();
      const accessBox = await access.boundingBox();
      expect(userBox && accessBox && accessBox.y).toBeGreaterThanOrEqual(
        (userBox?.y ?? 0) + (userBox?.height ?? 0),
      );
    }
    for (const theme of ["light", "dark"] as const) {
      await page.evaluate(
        (choice) => localStorage.setItem("theme", choice),
        theme,
      );
      await page.reload();
      await expect(page.locator("html")).toHaveAttribute("data-theme", theme);
      await expect(users).toBeVisible();
    }

    await users
      .getByRole("button", { name: "Создать локального пользователя" })
      .click();
    const form = page.getByRole("dialog", {
      name: "Создать локального пользователя",
    });
    await form.getByRole("textbox", { name: "Имя входа" }).fill(username);
    await form.getByLabel("Пароль").fill(guestPassword);
    await form
      .getByRole("textbox", { name: "Отображаемое имя" })
      .fill(`Smoke ${suffix}`);
    await form.getByRole("combobox", { name: "Роль" }).click();
    await page
      .locator(".ant-select-dropdown:visible .ant-select-item-option-content")
      .getByText("Guest", { exact: true })
      .click();
    await form.getByRole("button", { name: "Сохранить" }).click();
    await expect(form).toBeHidden();
    await users
      .getByRole("searchbox", { name: "Поиск пользователей" })
      .fill(username);
    await users
      .getByRole("searchbox", { name: "Поиск пользователей" })
      .press("Enter");
    await expect(users.getByText(`Smoke ${suffix}`)).toBeVisible();
    const list = await page.request.get(
      `/api/v1/users?q=${encodeURIComponent(username)}`,
    );
    expect(list.ok()).toBeTruthy();
    userId = (await list.json()).items[0].id as string;

    await mappingsTab.focus();
    await page.keyboard.press("Enter");
    await expect(mappingsTab).toHaveAttribute("aria-selected", "true");
    const mappingPanel = mappingsPanel;
    await expect(mappingPanel).toHaveAttribute("aria-hidden", "false");
    await expect(rolesPanel).toHaveAttribute("aria-hidden", "true");
    await page.emulateMedia({ reducedMotion: "reduce" });
    await expect
      .poll(() =>
        page
          .getByTestId("administration-panels-viewport")
          .evaluate((element) => getComputedStyle(element).transitionDuration),
      )
      .toBe("0s");
    await page.emulateMedia({ reducedMotion: "no-preference" });
    await access.getByRole("button", { name: "Добавить" }).click();
    const mappingForm = page.getByRole("dialog", {
      name: "Новое сопоставление OIDC",
    });
    await mappingForm
      .getByRole("textbox", { name: "Issuer URL" })
      .fill("https://idp.example.test/task25");
    await mappingForm
      .getByRole("textbox", { name: "Имя claim" })
      .fill("groups");
    await mappingForm
      .getByRole("textbox", { name: "Значение claim" })
      .fill(`smoke-${suffix}`);
    await mappingForm.getByRole("combobox", { name: "Системная роль" }).click();
    await page
      .locator(".ant-select-dropdown:visible .ant-select-item-option-content")
      .getByText("Guest", { exact: true })
      .click();
    await mappingForm.getByRole("button", { name: "Сохранить" }).click();
    await expect(mappingForm).toBeHidden();
    await expect(access.getByText(`smoke-${suffix}`)).toBeVisible();
    const panelsViewport = page.getByTestId("administration-panels-viewport");
    await expect(mappingPanel.getByText(`smoke-${suffix}`)).toBeVisible();
    await expect(mappingPanel.getByRole("row")).toHaveCount(2);
    for (const viewportSize of [
      { width: 320, height: 480 },
      { width: 2560, height: 650 },
    ]) {
      await page.setViewportSize(viewportSize);
      expect(
        await page.evaluate(() => document.documentElement.scrollWidth),
      ).toBeLessThanOrEqual(viewportSize.width);
      await expect(mappingPanel.getByText(`smoke-${suffix}`)).toBeVisible();
    }
    await page.emulateMedia({ reducedMotion: "reduce" });
    await mappingsTab.focus();
    await page.keyboard.press("ArrowLeft");
    await expect(rolesTab).toHaveAttribute("aria-selected", "true");
    await expect(rolesPanel.getByText("Administrator")).toBeVisible();
    await rolesTab.focus();
    await page.keyboard.press("ArrowRight");
    await expect(mappingsTab).toHaveAttribute("aria-selected", "true");
    await expect(mappingPanel.getByText(`smoke-${suffix}`)).toBeVisible();
    await page.emulateMedia({ reducedMotion: "no-preference" });
    const panelHeights = await panelsViewport.evaluate((element) =>
      Array.from(
        element.children,
        (panel) => (panel as HTMLElement).scrollHeight,
      ),
    );
    expect(
      Math.abs(panelHeights[0] - panelHeights[1]),
      `Expected loaded tab panels to have different heights, received ${panelHeights.join(" and ")}`,
    ).toBeGreaterThan(1);
    await expect
      .poll(() =>
        panelsViewport.evaluate(
          (element) => getComputedStyle(element).transitionDuration,
        ),
      )
      .toBe("0.18s");
    await panelsViewport.evaluate((element) => {
      element.setAttribute("data-height-transition", "pending");
      const events: string[] = [];
      for (const eventName of [
        "transitionrun",
        "transitionstart",
        "transitioncancel",
        "transitionend",
      ]) {
        element.addEventListener(eventName, (event) => {
          const transition = event as TransitionEvent;
          if (transition.propertyName === "height") {
            events.push(eventName);
            element.setAttribute("data-height-transition", events.join(","));
          }
        });
      }
    });
    await mappingsTab.focus();
    await page.keyboard.press("ArrowLeft");
    await expect(rolesTab).toHaveAttribute("aria-selected", "true");
    await page.waitForTimeout(250);
    const transitionDiagnostics = await panelsViewport.evaluate((element) => ({
      events: element.getAttribute("data-height-transition"),
      inlineHeight: (element as HTMLElement).style.height,
      renderedHeight: element.getBoundingClientRect().height,
      panelHeights: Array.from(
        element.children,
        (panel) => (panel as HTMLElement).scrollHeight,
      ),
      transition: getComputedStyle(element).transition,
    }));
    expect(
      transitionDiagnostics.events,
      JSON.stringify(transitionDiagnostics),
    ).toContain("transitionrun");
    await expect(rolesPanel.getByText("Administrator")).toBeVisible();
    await expect(mappingPanel).toHaveAttribute("aria-hidden", "true");
    await rolesTab.focus();
    await page.keyboard.press("ArrowRight");
    await expect(mappingsTab).toHaveAttribute("aria-selected", "true");
    await expect(mappingPanel.getByText(`smoke-${suffix}`)).toBeVisible();
    await expect(rolesPanel).toHaveAttribute("aria-hidden", "true");
    const mappings = await page.request.get("/api/v1/oidc-role-mappings");
    mappingId =
      (await mappings.json()).items.find(
        (item: { claim_value: string }) =>
          item.claim_value === `smoke-${suffix}`,
      )?.id ?? null;
    expect(mappingId).not.toBeNull();

    const guestContext = await browser.newContext({
      baseURL: new URL(page.url()).origin,
    });
    try {
      const guestPage = await guestContext.newPage();
      const guestLogin = await guestPage.request.post(
        "/api/v1/auth/local/login",
        { data: { username, password: guestPassword } },
      );
      expect(guestLogin.ok()).toBeTruthy();
      const guestSession = (await guestLogin.json()) as {
        permissions: string[];
      };
      expect(guestSession.permissions).not.toContain("users.read");
      expect(
        guestSession.permissions.includes("events.read") ||
          guestSession.permissions.includes("settings.read"),
      ).toBe(true);
      await guestPage.goto("/");
      await expect(
        guestPage.getByRole("link", { name: "Администрирование" }),
      ).toHaveCount(0);
      await guestPage.goto("/administration");
      await expect(
        guestPage.getByText("Недостаточно прав для просмотра раздела"),
      ).toBeVisible();
      for (const path of [
        "/users",
        `/users/${userId}`,
        "/roles",
        `/roles/${guestRole!.id}`,
        "/oidc-role-mappings",
        `/oidc-role-mappings/${mappingId}`,
      ]) {
        expect((await guestPage.request.get(`/api/v1${path}`)).status()).toBe(
          403,
        );
      }
    } finally {
      await guestContext.close();
    }
  } finally {
    if (mappingId)
      await page.request.delete(`/api/v1/oidc-role-mappings/${mappingId}`, {
        headers,
      });
    if (userId)
      await page.request.delete(`/api/v1/users/${userId}`, { headers });
  }
});
