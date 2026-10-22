import { describe, expect, it, vi } from "vitest";
import { AdministrationApi } from "./administration";
import type { ApiClient } from "./client";

describe("administration API", () => {
  it("passes user filters and paging to the server", async () => {
    const request = vi.fn(async (_path: string, _options?: unknown) => {
      void _path;
      void _options;
      return { items: [], total: 0, limit: 20, offset: 20 };
    });
    const api = new AdministrationApi({ request } as unknown as ApiClient);
    await api.users({
      q: "alice",
      role_id: "role",
      is_active: false,
      authentication_method: "oidc",
      limit: 20,
      offset: 20,
    });
    const path = request.mock.calls[0][0];
    const params = new URLSearchParams(path.split("?")[1]);
    expect(path.startsWith("/users?")).toBe(true);
    expect(Object.fromEntries(params)).toEqual({
      q: "alice",
      role_id: "role",
      is_active: "false",
      authentication_method: "oidc",
      limit: "20",
      offset: "20",
    });
  });

  it("sends distinct write operations with their supported payloads", async () => {
    const request = vi.fn(async (_path: string, _options?: unknown) => {
      void _path;
      void _options;
      return {};
    });
    const api = new AdministrationApi({ request } as unknown as ApiClient);
    await api.role("user", "role", false);
    expect(request).toHaveBeenCalledWith(
      "/users/user/role",
      expect.objectContaining({
        method: "PUT",
        body: JSON.stringify({ role_id: "role", role_managed_by_oidc: false }),
      }),
    );
    await api.password("user", "new-password-123");
    expect(request).toHaveBeenCalledWith(
      "/users/user/password",
      expect.objectContaining({
        method: "PUT",
        body: JSON.stringify({ password: "new-password-123" }),
      }),
    );
    await api.deleteMapping("mapping");
    expect(request).toHaveBeenCalledWith("/oidc-role-mappings/mapping", {
      method: "DELETE",
    });
  });
});
