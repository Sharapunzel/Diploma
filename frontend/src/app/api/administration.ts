import { ApiClient } from "./client";

export type ManagedUser = {
  id: string;
  username: string | null;
  oidc_issuer: string | null;
  oidc_subject: string | null;
  email: string | null;
  display_name: string;
  role_id: string | null;
  role_managed_by_oidc: boolean;
  is_active: boolean;
  authentication_method: "local" | "oidc";
  created_at: string;
  updated_at: string;
  last_login_at: string | null;
};
export type Role = {
  id: string;
  name: string;
  permissions: string[];
  priority: number;
};
export type Mapping = {
  id: string;
  issuer: string;
  claim_name: string;
  claim_value: string;
  role_id: string | null;
};
export type Page<T> = {
  items: T[];
  total: number;
  limit: number;
  offset: number;
};
export type UserQuery = {
  q?: string;
  role_id?: string;
  is_active?: boolean;
  authentication_method?: "local" | "oidc";
  limit: number;
  offset: number;
};

const json = (body: object) => ({
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify(body),
});
const query = (
  values: Record<string, string | number | boolean | undefined>,
) => {
  const params = new URLSearchParams();
  Object.entries(values).forEach(([key, value]) => {
    if (value !== undefined && value !== "") params.set(key, String(value));
  });
  return `?${params}`;
};

export class AdministrationApi {
  constructor(private api: ApiClient) {}
  users(filters: UserQuery) {
    return this.api.request<Page<ManagedUser>>(`/users${query(filters)}`);
  }
  user(id: string) {
    return this.api.request<ManagedUser>(`/users/${id}`);
  }
  createUser(body: object) {
    return this.api.request<ManagedUser>("/users", {
      method: "POST",
      ...json(body),
    });
  }
  updateUser(id: string, body: object) {
    return this.api.request<ManagedUser>(`/users/${id}`, {
      method: "PATCH",
      ...json(body),
    });
  }
  deleteUser(id: string) {
    return this.api.request<void>(`/users/${id}`, { method: "DELETE" });
  }
  active(id: string, value: boolean) {
    return this.api.request<ManagedUser>(
      `/users/${id}/${value ? "activate" : "deactivate"}`,
      { method: "POST" },
    );
  }
  role(id: string, role_id: string, role_managed_by_oidc: boolean) {
    return this.api.request<ManagedUser>(`/users/${id}/role`, {
      method: "PUT",
      ...json({ role_id, role_managed_by_oidc }),
    });
  }
  password(id: string, password: string) {
    return this.api.request<ManagedUser>(`/users/${id}/password`, {
      method: "PUT",
      ...json({ password }),
    });
  }
  roles() {
    return this.api.request<Role[]>("/roles");
  }
  renameRole(id: string, name: string) {
    return this.api.request<Role>(`/roles/${id}`, {
      method: "PATCH",
      ...json({ name }),
    });
  }
  mappings(limit: number, offset: number) {
    return this.api.request<Page<Mapping>>(
      `/oidc-role-mappings${query({ limit, offset })}`,
    );
  }
  createMapping(body: object) {
    return this.api.request<Mapping>("/oidc-role-mappings", {
      method: "POST",
      ...json(body),
    });
  }
  updateMapping(id: string, body: object) {
    return this.api.request<Mapping>(`/oidc-role-mappings/${id}`, {
      method: "PATCH",
      ...json(body),
    });
  }
  deleteMapping(id: string) {
    return this.api.request<void>(`/oidc-role-mappings/${id}`, {
      method: "DELETE",
    });
  }
  methods() {
    return this.api.methods();
  }
}
