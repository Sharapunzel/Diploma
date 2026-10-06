import { readFile, writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { expect, test, type Locator, type Page } from "@playwright/test";

const username = process.env.PLAYWRIGHT_USERNAME;
const password = process.env.PLAYWRIGHT_PASSWORD;
const visualUsername = "smoke14-long-username-visual-check-0123456789";
if (!username || !password || process.env.PLAYWRIGHT_OIDC !== "1") {
  throw new Error(
    "Browser acceptance requires PLAYWRIGHT_USERNAME, PLAYWRIGHT_PASSWORD and PLAYWRIGHT_OIDC=1",
  );
}

async function tabTo(page: Page, target: Locator) {
  for (let step = 0; step < 40; step++) {
    if (await target.evaluate((element) => element === document.activeElement))
      return;
    await page.keyboard.press("Tab");
  }
  throw new Error("Target is not reachable with Tab");
}

async function localLogin(page: Page, path = "/events") {
  await page.goto(path);
  await expect(page).toHaveURL(/\/login$/);
  const userField = page.getByRole("textbox", { name: "Имя пользователя" });
  await expect(userField).toBeVisible();
  await userField.fill(username!);
  await page.getByLabel("Пароль").fill(password!);
  await page.getByRole("button", { name: "Войти" }).click();
  await expect(page).toHaveURL(new RegExp(`${path}$`));
}

async function verifyThemePalette(page: Page) {
  await page.evaluate(
    () =>
      new Promise<void>((resolve) => requestAnimationFrame(() => resolve())),
  );
  await expect
    .poll(() =>
      page
        .locator(".ant-btn-primary")
        .evaluate((element) => getComputedStyle(element).backgroundColor),
    )
    .toBe("rgb(100, 248, 106)");
  const palette = await page.evaluate(() => ({
    accent: getComputedStyle(document.documentElement)
      .getPropertyValue("--color-accent")
      .trim(),
    accentHover: getComputedStyle(document.documentElement)
      .getPropertyValue("--color-accent-hover")
      .trim(),
    button: getComputedStyle(document.querySelector(".ant-btn-primary")!)
      .backgroundColor,
    text: getComputedStyle(document.querySelector(".ant-btn-primary")!).color,
  }));
  expect(palette.accent).toBe("#64f86a");
  expect(palette.button).toBe("rgb(100, 248, 106)");
  const [red, green, blue] = palette.text.match(/\d+/g)!.map(Number);
  expect(red * 0.2126 + green * 0.7152 + blue * 0.0722).toBeLessThan(80);
  await page.locator(".ant-btn-primary").hover();
  const [hoverRed, hoverGreen, hoverBlue] = palette.accentHover
    .match(/[a-f\d]{2}/gi)!
    .map((channel) => Number.parseInt(channel, 16));
  await expect
    .poll(() =>
      page
        .locator(".ant-btn-primary")
        .evaluate((element) => getComputedStyle(element).backgroundColor),
    )
    .toBe(`rgb(${hoverRed}, ${hoverGreen}, ${hoverBlue})`);
  await page.getByTestId("theme-toggle").hover();
}

async function capture(page: Page, name: string) {
  await page.screenshot({
    path: `test-results/visual-review/${name}.png`,
    fullPage: true,
  });
}

test("theme toggle cycles system/light/dark, updates Ant tokens and local fonts", async ({
  page,
}) => {
  await page.addInitScript(() => localStorage.removeItem("theme"));
  const externalFonts: string[] = [];
  page.on("request", (request) => {
    if (
      request.resourceType() === "font" &&
      new URL(request.url()).origin !== new URL(page.url()).origin
    )
      externalFonts.push(request.url());
  });
  await page.emulateMedia({ colorScheme: "dark" });
  await page.goto("/login");
  await expect(page.getByTestId("login-page")).toBeVisible();
  await expect(
    page.getByRole("textbox", { name: "Имя пользователя" }),
  ).toHaveValue("");
  await expect(page.getByLabel("Пароль")).toHaveValue("");
  await page.evaluate(() => document.fonts.ready);
  const theme = page.getByTestId("theme-toggle");
  await page.keyboard.press("Tab");
  await expect(theme).toBeFocused();
  await capture(page, "keyboard-focus-theme");
  await expect(page.locator("html")).toHaveAttribute("data-theme", "dark");
  await verifyThemePalette(page);
  await theme.click();
  await expect(page.locator("html")).toHaveAttribute("data-theme", "light");
  await verifyThemePalette(page);
  const inputs = page.locator('[data-testid="login-page"] input');
  await expect
    .poll(() =>
      inputs
        .first()
        .evaluate((element) => getComputedStyle(element).backgroundColor),
    )
    .toBe("rgb(251, 253, 252)");
  await inputs.first().fill("synthetic-review-user");
  await page.getByLabel("Пароль").fill("synthetic-review-password");
  await tabTo(page, page.getByRole("button", { name: "Войти" }));
  await capture(page, "keyboard-focus-login");
  await capture(page, "login-light-filled");
  await theme.press("Enter");
  await expect(page.locator("html")).toHaveAttribute("data-theme", "dark");
  await verifyThemePalette(page);
  await capture(page, "login-dark-filled");
  await theme.press("Space");
  await expect(theme).toHaveAttribute(
    "aria-label",
    "Тема: системная. Переключить на светлую",
  );
  await expect(page.locator("html")).toHaveAttribute("data-theme", "dark");
  await page.emulateMedia({ colorScheme: "light" });
  await expect(page.locator("html")).toHaveAttribute("data-theme", "light");
  await verifyThemePalette(page);
  await theme.hover();
  await expect(page.locator(".ant-tooltip")).toHaveCount(0);
  await theme.press("Enter");
  await expect(theme).toHaveAttribute(
    "aria-label",
    "Тема: светлая. Переключить на тёмную",
  );
  await theme.press("Enter");
  await expect(page.locator("html")).toHaveAttribute("data-theme", "dark");
  await theme.hover();
  await expect(page.locator(".ant-tooltip")).toHaveCount(0);
  await theme.press("Space");
  await expect(theme).toHaveAttribute(
    "aria-label",
    "Тема: системная. Переключить на светлую",
  );
  await expect(page.locator("html")).toHaveAttribute("data-theme", "light");
  await verifyThemePalette(page);
  await theme.press("Enter");
  await expect(page.locator("html")).toHaveAttribute("data-theme", "light");
  await expect
    .poll(() => page.evaluate(() => localStorage.getItem("theme")))
    .toBe("light");
  await page.reload();
  await expect(page.locator("html")).toHaveAttribute("data-theme", "light");
  await expect(page.getByTestId("login-page")).toBeVisible();
  await page.evaluate(() => document.fonts.ready);
  const fontState = await page.evaluate(() => ({
    loaded: document.fonts.check("700 16px Play", "Анализ событий"),
    bodyFont: getComputedStyle(document.body).fontFamily,
    overflow:
      document.documentElement.scrollWidth >
      document.documentElement.clientWidth,
  }));
  expect(fontState.loaded).toBe(true);
  expect(fontState.bodyFont).toContain("Play");
  expect(fontState.overflow).toBe(false);
  expect(externalFonts).toEqual([]);
});

test("login confines overflow to the internal scroll area on a short viewport", async ({
  page,
}) => {
  await page.setViewportSize({ width: 320, height: 480 });
  await page.goto("/login");
  await expect(page.getByTestId("login-page")).toBeVisible();
  await expect(
    page.getByRole("button", { name: "Войти", exact: true }),
  ).toBeVisible();
  const dimensions = await page.evaluate(() => {
    const page = document.querySelector('[data-testid="login-page"]')!;
    const scrollArea = page.querySelector(":scope > div")!;
    return {
      documentOverflow:
        document.documentElement.scrollHeight >
        document.documentElement.clientHeight,
      contentOverflow: scrollArea.scrollHeight > scrollArea.clientHeight,
      pageHeight: page.getBoundingClientRect().height,
    };
  });
  expect(dimensions.documentOverflow).toBe(false);
  expect(dimensions.contentOverflow).toBe(true);
  expect(dimensions.pageHeight).toBeLessThanOrEqual(480);
});

test("session loading and connection errors use compact centered status screens", async ({
  page,
}) => {
  await page.route("**/api/v1/auth/session", async (route) => {
    await new Promise((resolve) => setTimeout(resolve, 2500));
    await route.fulfill({
      status: 503,
      contentType: "application/json",
      body: JSON.stringify({ code: "service_unavailable" }),
    });
  });
  await page.goto("/");
  const statusPage = page.getByTestId("session-status-page");
  await expect(
    statusPage.getByTestId("session-status-card").locator(".ant-spin"),
  ).toBeVisible();
  await expect(page.getByTestId("status-version")).toHaveText("v0.1.0");
  await capture(page, "session-loading");
  await expect
    .poll(() =>
      statusPage.evaluate(
        (element) => getComputedStyle(element, "::before").pointerEvents,
      ),
    )
    .toBe("none");
  await statusPage.getByTestId("theme-toggle").click();
  await expect(
    statusPage.getByRole("button", { name: /Повторить/ }),
  ).toBeVisible({ timeout: 5000 });
  await expect(
    statusPage.getByRole("heading", {
      name: "Не удалось загрузить приложение",
    }),
  ).toBeVisible();
  await capture(page, "session-error");
  const card = statusPage.getByTestId("session-status-card");
  await expect(card).toBeVisible();
  const center = await card.evaluate((element) => {
    const rect = element.getBoundingClientRect();
    return { x: (rect.left + rect.right) / 2 };
  });
  const viewportCenter = await page.evaluate(() => innerWidth / 2);
  expect(Math.abs(center.x - viewportCenter)).toBeLessThanOrEqual(1);
});

test("local login, server session, protected route and confirmed keyboard logout", async ({
  page,
}) => {
  await page.goto("/login");
  await page.evaluate(() => localStorage.setItem("theme", "light"));
  await localLogin(page);
  await expect(page.getByTestId("workspace-title")).toHaveText("События");
  await capture(page, "workspace-light");
  const firstMenuLink = page
    .locator(".ant-layout-sider .ant-menu-item a")
    .first();
  await tabTo(page, firstMenuLink);
  const menuFocus = await firstMenuLink.evaluate((element) => ({
    linkOutline: getComputedStyle(element).outlineStyle,
    rowOutline: getComputedStyle(element.closest(".ant-menu-item")!)
      .outlineStyle,
    indicator: getComputedStyle(element.closest(".ant-menu-item")!, "::after")
      .display,
  }));
  expect(menuFocus.linkOutline).toBe("none");
  expect(menuFocus.rowOutline).toBe("solid");
  expect(menuFocus.indicator).toBe("none");
  await page.reload();
  await expect(page.getByTestId("workspace-title")).toHaveText("События");

  await tabTo(
    page,
    page.getByRole("link", { name: "К основному содержимому" }),
  );
  await page.keyboard.press("Enter");
  await expect(page.locator("#main")).toBeFocused();
  const collapse = page.getByRole("button", { name: "Свернуть меню" });
  await tabTo(page, collapse);
  await page.keyboard.press("Space");
  await expect(
    page.getByRole("button", { name: "Развернуть меню" }),
  ).toHaveAttribute("aria-expanded", "false");
  await tabTo(page, page.getByRole("link", { name: "Главная", exact: true }));
  await page.keyboard.press("Enter");
  await expect(page).toHaveURL(/\/$/);
  await expect(page.locator("#main")).toBeFocused();
  await tabTo(page, page.getByRole("link", { name: "События" }));
  await page.keyboard.press("Enter");
  await expect(page).toHaveURL(/\/events$/);
  await expect(page.locator("#main")).toBeFocused();

  const account = page.getByRole("button", {
    name: `Аккаунт: ${visualUsername}`,
  });
  await account.click();
  const panel = page.getByRole("region", { name: "Аккаунт" });
  await expect(panel).toBeVisible();
  await expect(panel.locator("h2")).toHaveCSS("text-align", "right");
  await expect(panel.locator("dd").first()).toHaveCSS("text-align", "right");
  await expect(panel.locator("dl > div").first()).toHaveCSS(
    "border-bottom-style",
    "solid",
  );
  const accountLayout = await panel.evaluate((element) => {
    const label = element.querySelector("dt")!;
    const value = element.querySelector("dd")!;
    const themeButton = element.querySelector('[data-testid="theme-toggle"]')!;
    return {
      width: element.getBoundingClientRect().width,
      labelWeight: Number(getComputedStyle(label).fontWeight),
      valueWeight: Number(getComputedStyle(value).fontWeight),
      themeRightGap:
        element.getBoundingClientRect().right -
        themeButton.getBoundingClientRect().right,
      headingSize: Number.parseFloat(
        getComputedStyle(element.querySelector("h2")!).fontSize,
      ),
      headingGap:
        label.getBoundingClientRect().top -
        element.querySelector("h2")!.getBoundingClientRect().bottom,
      themeCenterOffset: (() => {
        const row = themeButton.closest("div")!;
        const bounds = row.getBoundingClientRect();
        const button = themeButton.getBoundingClientRect();
        return Math.abs(
          (bounds.top + bounds.bottom - button.top - button.bottom) / 2,
        );
      })(),
    };
  });
  expect(accountLayout.width).toBeLessThanOrEqual(248);
  expect(accountLayout.labelWeight).toBeGreaterThan(accountLayout.valueWeight);
  expect(accountLayout.themeRightGap).toBeLessThanOrEqual(14);
  expect(accountLayout.headingSize).toBeGreaterThanOrEqual(16);
  expect(accountLayout.headingGap).toBeGreaterThanOrEqual(8);
  expect(accountLayout.themeCenterOffset).toBeLessThanOrEqual(1);
  await capture(page, "account-panel");
  await tabTo(page, panel.getByRole("button", { name: "Выйти" }));
  await capture(page, "keyboard-focus-account-action");
  await panel
    .getByRole("button", { name: "Тема: светлая. Переключить на тёмную" })
    .click();
  await expect(page.locator("html")).toHaveAttribute("data-theme", "dark");
  await expect(panel.getByText(visualUsername, { exact: true })).toBeVisible();
  await expect(
    panel.getByText("Алиса Очень Длинное Отображаемое Имя Для Проверки Экрана"),
  ).toBeHidden();
  await capture(page, "workspace-dark");
  await expect
    .poll(() => page.evaluate(() => localStorage.getItem("theme")))
    .toBe("dark");
  await page.reload();
  await expect(page.locator("html")).toHaveAttribute("data-theme", "dark");

  await page
    .getByRole("button", {
      name: `Аккаунт: ${visualUsername}`,
    })
    .click();
  await page
    .getByRole("region", { name: "Аккаунт" })
    .getByRole("button", { name: "Выйти" })
    .click();
  const dialog = page.getByRole("dialog", { name: "Выйти из аккаунта?" });
  await expect(dialog).toBeVisible();
  await expect(dialog.getByRole("button", { name: "Отмена" })).toBeFocused();
  await page.waitForTimeout(400);
  await capture(page, "logout-confirmation");
  const beforeCancel = await page.request.get("/api/v1/auth/session");
  expect((await beforeCancel.json()).authenticated).toBe(true);
  await page.keyboard.press("Escape");
  await expect(dialog).not.toBeVisible();
  const afterCancel = await page.request.get("/api/v1/auth/session");
  expect((await afterCancel.json()).authenticated).toBe(true);

  await expect(panel).toBeVisible();
  await panel.getByRole("button", { name: "Выйти" }).click();
  const logoutRequest = page.waitForRequest(
    (request) =>
      request.url().endsWith("/api/v1/auth/logout") &&
      request.method() === "POST",
  );
  await page.getByRole("dialog").getByRole("button", { name: "Выйти" }).click();
  await logoutRequest;
  await expect(page).toHaveURL(/\/login$/);
});

test("mobile navigation, account layout and viewport bounds", async ({
  page,
}) => {
  await localLogin(page, "/");
  const desktopNav = page.getByRole("navigation", { name: "Основное меню" });
  await expect(desktopNav.locator(".ant-menu-item-group-title")).toHaveCount(3);
  const desktopGroupHeadings = await desktopNav
    .locator(".ant-menu-item-group-title")
    .evaluateAll((titles) =>
      titles
        .filter((title) => getComputedStyle(title).display !== "none")
        .map((title) => title.textContent?.trim()),
    );
  expect(desktopGroupHeadings).toEqual([
    "Обработка данных",
    "Анализ данных",
    "Управление",
  ]);
  const headingAndItemFontSizes = await desktopNav.evaluate((element) => ({
    heading: getComputedStyle(
      Array.from(element.querySelectorAll(".ant-menu-item-group-title")).find(
        (title) => getComputedStyle(title).display !== "none",
      )!,
    ).fontSize,
    item: getComputedStyle(element.querySelector(".ant-menu-item a")!).fontSize,
  }));
  expect(headingAndItemFontSizes.heading).toBe(headingAndItemFontSizes.item);
  await expect(desktopNav.getByRole("link")).toHaveCount(8);
  await desktopNav.getByRole("link", { name: "Настройки приложения" }).click();
  await expect(page).toHaveURL(/\/settings$/);
  await expect(page.getByTestId("workspace-title")).toHaveText(
    "Настройки приложения",
  );
  await expect(desktopNav.locator(".ant-menu-item-selected a")).toHaveAttribute(
    "href",
    "/settings",
  );
  await page.goto("/");
  await expect(page.getByTestId("workspace-brand")).toHaveJSProperty(
    "tagName",
    "DIV",
  );
  await expect(page.getByTestId("app-version")).toHaveText("v0.1.0");
  await expect(
    page.getByTestId("workspace-brand").locator("img"),
  ).toHaveAttribute("src", "/pictures/logoGreen.svg");
  await expect(page.getByTestId("placeholder-panel")).toBeVisible();
  await page.getByRole("button", { name: "Показать загрузку" }).click();
  const overlay = page.getByTestId("loading-overlay");
  await expect(overlay).toBeVisible();
  await expect(overlay).toHaveCSS("pointer-events", "none");
  const loadingLayout = await page.evaluate(() => {
    const rect = (selector: string) =>
      document.querySelector(selector)!.getBoundingClientRect();
    const dim = rect('[data-testid="loading-overlay"]');
    const sidebar = rect(".ant-layout-sider");
    const spinner = rect('[data-testid="loading-overlay"] .ant-spin');
    return {
      top: dim.top,
      left: dim.left,
      right: dim.right,
      bottom: dim.bottom,
      sidebarRight: sidebar.right,
      spinnerWidth: spinner.width,
      bodyFont: Number.parseFloat(getComputedStyle(document.body).fontSize),
      viewportWidth: innerWidth,
      viewportHeight: innerHeight,
    };
  });
  expect(loadingLayout.top).toBe(0);
  expect(loadingLayout.left).toBe(loadingLayout.sidebarRight);
  expect(loadingLayout.right).toBe(loadingLayout.viewportWidth);
  expect(loadingLayout.bottom).toBe(loadingLayout.viewportHeight);
  expect(loadingLayout.spinnerWidth).toBe(36);
  expect(loadingLayout.bodyFont).toBe(15);
  await page.locator('[aria-label^="Аккаунт:"]').click();
  await expect(page.locator("#account-panel")).toBeVisible();
  await page.locator('[aria-label^="Аккаунт:"]').click();
  await capture(page, "home-loading-overlay");
  await page.getByRole("button", { name: "Скрыть загрузку" }).click();
  await expect(overlay).toHaveCount(0);
  await page.goto("/missing-review-route");
  await expect(
    page
      .getByTestId("placeholder-panel")
      .getByRole("heading", { name: "Страница не найдена" }),
  ).toBeVisible();
  await expect(page.getByRole("link", { name: "На главную" })).toBeVisible();
  await capture(page, "not-found-page");
  await page.getByRole("link", { name: "На главную" }).click();
  await expect(page).toHaveURL(/\/$/);
  for (const choice of ["light", "dark"] as const) {
    await page.evaluate(
      (value) => localStorage.setItem("theme", value),
      choice,
    );
    await page.reload();
    await expect(page.locator("html")).toHaveAttribute("data-theme", choice);
    await expect(page.getByTestId("workspace-title")).toBeVisible();
    for (const width of [320, 375, 768, 1440, 2560]) {
      await page.setViewportSize({ width, height: 900 });
      const layout = await page.evaluate(() => {
        const shell = document.querySelector(
          '[data-testid="workspace-shell"]',
        )!;
        const title = document.querySelector(
          '[data-testid="workspace-title"]',
        )!;
        const account = document.querySelector('[aria-label^="Аккаунт:"]')!;
        const titleRect = title.getBoundingClientRect();
        const accountRect = account.getBoundingClientRect();
        const header = document.querySelector(
          '[data-testid="workspace-header"]',
        )!;
        const headerRect = header.getBoundingClientRect();
        const intersects =
          titleRect.left < accountRect.right &&
          titleRect.right > accountRect.left;
        return {
          width: document.documentElement.scrollWidth,
          viewport: document.documentElement.clientWidth,
          shellLeft: shell.getBoundingClientRect().left,
          shellWidth: shell.getBoundingClientRect().width,
          titleCenter: (titleRect.left + titleRect.right) / 2,
          workspaceCenter: (headerRect.left + headerRect.right) / 2,
          titleMiddle: (titleRect.top + titleRect.bottom) / 2,
          headerMiddle: (headerRect.top + headerRect.bottom) / 2,
          titleIntersectsAccount: intersects,
          titleText: title.textContent?.trim(),
          titleClientWidth: title.clientWidth,
          titleScrollWidth: title.scrollWidth,
        };
      });
      expect(
        layout.width,
        `${choice} document overflow at ${width}px`,
      ).toBeLessThanOrEqual(layout.viewport);
      expect(layout.shellLeft).toBe(0);
      expect(layout.shellWidth).toBe(width);
      expect(
        Math.abs(layout.titleCenter - layout.workspaceCenter),
      ).toBeLessThanOrEqual(1);
      expect(
        Math.abs(layout.titleMiddle - layout.headerMiddle),
      ).toBeLessThanOrEqual(1);
      expect(
        layout.titleIntersectsAccount,
        `${choice} title/account overlap at ${width}px`,
      ).toBe(false);
      expect(layout.titleText).toBeTruthy();
      expect(layout.titleClientWidth).toBeGreaterThanOrEqual(
        Math.min(layout.titleScrollWidth, 120),
      );
    }
    if (choice === "light") {
      await page.setViewportSize({ width: 2560, height: 900 });
      const expandedFirstY = await page
        .getByRole("navigation", { name: "Основное меню" })
        .locator(".ant-menu-item .anticon")
        .first()
        .evaluate((element) => {
          const rect = element.getBoundingClientRect();
          return (rect.top + rect.bottom) / 2;
        });
      const centeredBefore = await page
        .getByTestId("workspace-title")
        .evaluate((element) => {
          const rect = element.getBoundingClientRect();
          return (rect.left + rect.right) / 2;
        });
      await page.getByRole("button", { name: "Свернуть меню" }).click();
      await expect
        .poll(() =>
          page.getByTestId("workspace-title").evaluate((element) => {
            const rect = element.getBoundingClientRect();
            return (rect.left + rect.right) / 2;
          }),
        )
        .toBeLessThan(centeredBefore - 1);
      const sidebar = page.locator(".ant-layout-sider");
      await expect(sidebar).toHaveCSS("width", "76px");
      const collapsedLayout = await sidebar.evaluate((element) => {
        const center = (target: Element) => {
          const rect = target.getBoundingClientRect();
          return (rect.left + rect.right) / 2;
        };
        const toggle = element.querySelector("button .anticon")!;
        const firstItem = element.querySelector(
          '[data-testid="compact-navigation"] a .anticon',
        )!;
        const logo = element.querySelector(
          '[data-testid="workspace-brand"] img',
        )!;
        element.scrollLeft = 100;
        return {
          sidebarCenter: center(element),
          toggleCenter: center(toggle),
          itemCenter: center(firstItem),
          logoCenter: center(logo),
          itemY: (() => {
            const rect = firstItem.getBoundingClientRect();
            return (rect.top + rect.bottom) / 2;
          })(),
          scrollLeft: element.scrollLeft,
          scrollWidth: element.scrollWidth,
          clientWidth: element.clientWidth,
        };
      });
      expect(
        Math.abs(collapsedLayout.toggleCenter - collapsedLayout.sidebarCenter),
      ).toBeLessThanOrEqual(1);
      expect(
        Math.abs(collapsedLayout.itemCenter - collapsedLayout.sidebarCenter),
      ).toBeLessThanOrEqual(1);
      expect(
        Math.abs(collapsedLayout.logoCenter - collapsedLayout.sidebarCenter),
      ).toBeLessThanOrEqual(1);
      expect(
        Math.abs(collapsedLayout.itemY - expandedFirstY),
      ).toBeLessThanOrEqual(2);
      expect(collapsedLayout.scrollLeft).toBe(0);
      expect(collapsedLayout.scrollWidth).toBeLessThanOrEqual(
        collapsedLayout.clientWidth,
      );
      await expect(
        page.getByTestId("compact-navigation").getByRole("separator"),
      ).toHaveCount(3);
      await page.getByRole("button", { name: "Развернуть меню" }).click();
      await expect(
        page.getByTestId("workspace-brand").locator("span"),
      ).toHaveCount(0);
      await expect(sidebar).toHaveCSS("width", "248px");
      await expect(
        page.getByTestId("workspace-brand").locator("span"),
      ).toBeVisible();
      await expect(
        page.getByTestId("workspace-brand").locator("span"),
      ).toHaveCSS("opacity", "1");
      await capture(page, "menu-expanded");
    }
    await page.setViewportSize({ width: 640, height: 360 });
    const zoomLayout = await page.evaluate(() => ({
      documentOverflow:
        document.documentElement.scrollWidth >
        document.documentElement.clientWidth,
      titleCenter: (() => {
        const rect = document
          .querySelector('[data-testid="workspace-title"]')!
          .getBoundingClientRect();
        return (rect.left + rect.right) / 2;
      })(),
      workspaceCenter: (() => {
        const rect = document
          .querySelector('[data-testid="workspace-header"]')!
          .getBoundingClientRect();
        return (rect.left + rect.right) / 2;
      })(),
    }));
    expect(zoomLayout.documentOverflow).toBe(false);
    expect(
      Math.abs(zoomLayout.titleCenter - zoomLayout.workspaceCenter),
    ).toBeLessThanOrEqual(1);
    await page.setViewportSize({ width: 320, height: 900 });
    const mobile = page.getByRole("button", { name: "Открыть меню" });
    await mobile.click();
    const nav = page.getByRole("navigation", { name: "Мобильное меню" });
    await expect(nav).toBeVisible();
    await expect(nav.locator(".ant-menu-item-group-title")).toHaveCount(3);
    const mobileGroupHeadings = await nav
      .locator(".ant-menu-item-group-title")
      .evaluateAll((titles) =>
        titles
          .filter((title) => getComputedStyle(title).display !== "none")
          .map((title) => title.textContent?.trim()),
      );
    expect(mobileGroupHeadings).toEqual([
      "Обработка данных",
      "Анализ данных",
      "Управление",
    ]);
    expect(
      await nav
        .locator(".ant-menu-item-group-title")
        .evaluateAll((titles) =>
          titles.every((title) => (title as HTMLElement).tabIndex === -1),
        ),
    ).toBe(true);
    if (choice === "light") {
      const drawerBackground = await nav.evaluate(
        (element) =>
          getComputedStyle(element.closest(".ant-drawer-body")!)
            .backgroundColor,
      );
      expect(drawerBackground).toBe("rgb(251, 253, 252)");
    }
    await page.keyboard.press("Escape");
    await expect(mobile).toBeFocused();
    await expect(nav).not.toBeVisible();

    const account = page.getByRole("button", {
      name: `Аккаунт: ${visualUsername}`,
    });
    await account.click();
    const panel = page.getByRole("region", { name: "Аккаунт" });
    await expect(panel).toBeVisible();
    await expect(
      panel.getByText(visualUsername, { exact: true }),
    ).toBeVisible();
    await expect(
      panel.getByText(
        "Алиса Очень Длинное Отображаемое Имя Для Проверки Экрана",
      ),
    ).toHaveCount(0);
    const panelOverflow = await panel.evaluate(
      (element) => element.scrollWidth > element.clientWidth,
    );
    expect(panelOverflow, `${choice} account panel overflow`).toBe(false);
    await page.keyboard.press("Escape");
    await expect(account).toBeFocused();
    if (choice === "light") {
      await mobile.click();
      await nav.getByRole("link", { name: "Настройки приложения" }).click();
      await expect(page).toHaveURL(/\/settings$/);
      await expect(nav).not.toBeVisible();
      await expect(page.getByTestId("workspace-title")).toHaveText(
        "Настройки приложения",
      );
      await page.goto("/");
    }
  }
});

test("OIDC signed token, callback and server cookie through proxy", async ({
  page,
}) => {
  await page.goto("/login");
  await page.getByRole("link", { name: "Войти через провайдера" }).click();
  await expect(page).toHaveURL(/\/$/);
  await expect(page.getByTestId("workspace-title")).toHaveText("Главная");
  const response = await page.request.get("/api/v1/auth/session");
  expect((await response.json()).authentication_method).toBe("oidc");
});

test("CSS changes reach the browser through HMR on the external proxy port", async ({
  page,
}) => {
  await page.addInitScript(() => localStorage.setItem("theme", "light"));
  await page.goto("/login");
  const cssPath = fileURLToPath(
    new URL("../src/app/styles.css", import.meta.url),
  );
  const original = await readFile(cssPath, "utf8");
  const current = "--color-page: #eef5f2;";
  const changed = "--color-page: #e4f7e4;";
  if (!original.includes(current))
    throw new Error("HMR test marker is missing");
  try {
    await writeFile(cssPath, original.replace(current, changed), "utf8");
    await expect
      .poll(() =>
        page.evaluate(() =>
          getComputedStyle(document.documentElement)
            .getPropertyValue("--color-page")
            .trim(),
        ),
      )
      .toBe("#e4f7e4");
  } finally {
    await writeFile(cssPath, original, "utf8");
  }
  await expect
    .poll(() =>
      page.evaluate(() =>
        getComputedStyle(document.documentElement)
          .getPropertyValue("--color-page")
          .trim(),
      ),
    )
    .toBe("#eef5f2");
});
