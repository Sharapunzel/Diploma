import { readFileSync } from "node:fs";
import { expect, type Page } from "@playwright/test";

type SavedFindings = {
  guest: string;
  source: string;
  source_name: string;
  detector_name: string;
  detectors: [string, string];
  findings: { primary: string; single: string; legacy: string };
  start: string;
  stop: string;
  from: string;
  to: string;
};

const path = process.env.DETECTOR_BROWSER_CONFIG;
if (!path)
  throw new Error("Findings specs require the isolated browser profile");
export const config = JSON.parse(readFileSync(path, "utf8")) as {
  username: string;
  password: string;
  findings?: SavedFindings;
};

export async function loginFindings(page: Page, username: string) {
  expect((await page.goto("/findings"))?.ok()).toBeTruthy();
  await page.getByRole("textbox", { name: "Имя пользователя" }).fill(username);
  await page.getByLabel("Пароль", { exact: true }).fill(config.password);
  await page.getByRole("button", { name: "Войти", exact: true }).click();
  await expect(
    page.getByRole("heading", { name: "Находки", exact: true }),
  ).toBeVisible();
}
