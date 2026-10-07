import { expect, test } from "@playwright/test";

const connection = {
  id: "00000000-0000-4000-8000-000000000031",
  name: "Policy Kafka",
  bootstrap_servers: ["kafka:9092"],
  security_protocol: "PLAINTEXT",
  created_at: "2026-10-01T00:00:00Z",
  updated_at: "2026-10-01T00:00:00Z",
};
const archived = {
  id: "00000000-0000-4000-8000-000000000032",
  source_type: "kafka",
  name: "Archived Kafka source",
  connection_id: connection.id,
  topic_name: "archived-topic",
  normalizer_id: null,
  is_enabled: false,
  is_archived: true,
  created_at: "2026-10-01T00:00:00Z",
  updated_at: "2026-10-01T00:00:00Z",
};
const legacy = {
  ...archived,
  id: "00000000-0000-4000-8000-000000000033",
  name: "Legacy Kafka source",
  topic_name: "legacy-topic",
  normalizer_id: "00000000-0000-4000-8000-000000000034",
  is_archived: false,
};

test("archived sources stay read-only and a legacy normalizer cannot enable Kafka", async ({
  page,
}) => {
  await page.route("**/api/v1/**", async (route) => {
    const path = new URL(route.request().url()).pathname.replace("/api/v1", "");
    const body =
      path === "/auth/session"
        ? {
            authenticated: true,
            user: {
              id: "00000000-0000-4000-8000-000000000001",
              username: "policy-fixture",
              email: null,
              display_name: "Policy Fixture",
            },
            role: "fixture",
            permissions: [
              "connections.read",
              "sources.read",
              "sources.write",
              "normalizers.read",
            ],
            authentication_method: "local",
            csrf_token: "csrf",
          }
        : path === "/sources"
          ? { items: [archived, legacy], total: 2, limit: 50, offset: 0 }
          : path === "/kafka-connections"
            ? { items: [connection], total: 1, limit: 50, offset: 0 }
            : path === `/kafka-connections/${connection.id}`
              ? connection
              : path === `/normalizers/${legacy.normalizer_id}`
                ? {
                    id: legacy.normalizer_id,
                    name: "Legacy rule",
                    version: 0,
                    rule_status: "legacy_incompatible",
                  }
                : { items: [], total: 0, limit: 50, offset: 0 };
    await route.fulfill({
      contentType: "application/json",
      body: JSON.stringify(body),
    });
  });

  await page.goto("/sources");
  const archivedRow = page.getByRole("row", { name: /Archived Kafka source/ });
  await expect(archivedRow).toBeVisible();
  await expect(
    archivedRow.getByRole("button", { name: "Изменить" }),
  ).toHaveCount(0);
  await expect(
    archivedRow.getByRole("button", { name: "Нормализатор" }),
  ).toHaveCount(0);
  await expect(
    archivedRow.getByRole("button", { name: "Включить" }),
  ).toHaveCount(0);

  const legacyRow = page.getByRole("row", { name: /Legacy Kafka source/ });
  await expect(legacyRow).toBeVisible();
  const enable = legacyRow.getByRole("button", { name: "Включить" });
  await expect(enable).toBeDisabled();
  await expect(legacyRow.getByText("Legacy rule")).toBeVisible();
});
