import { defineConfig } from "@playwright/test";
export default defineConfig({
  testDir: "./browser",
  use: {
    baseURL: process.env.PLAYWRIGHT_BASE_URL || "http://localhost:8080",
    browserName: "chromium",
    launchOptions: process.env.PLAYWRIGHT_CHROME_PATH
      ? { executablePath: process.env.PLAYWRIGHT_CHROME_PATH }
      : undefined,
  },
  reporter: "list",
});
