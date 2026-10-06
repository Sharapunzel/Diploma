export interface User {
  id: string;
  username: string | null;
  email: string | null;
  display_name: string;
}

export interface Session {
  authenticated: boolean;
  user: User | null;
  role: string | null;
  permissions: string[];
  authentication_method: string | null;
  csrf_token: string | null;
}

export interface Methods {
  local_enabled: boolean;
  oidc_enabled: boolean;
}

export interface ApiErrorBody {
  code: string;
  message: string;
  request_id: string;
  details: unknown;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function isMethods(value: unknown): value is Methods {
  return (
    isRecord(value) &&
    typeof value.local_enabled === "boolean" &&
    typeof value.oidc_enabled === "boolean"
  );
}

export function isSession(value: unknown): value is Session {
  if (!isRecord(value) || typeof value.authenticated !== "boolean")
    return false;
  if (
    !Array.isArray(value.permissions) ||
    !value.permissions.every((permission) => typeof permission === "string") ||
    !(typeof value.role === "string" || value.role === null) ||
    !(
      typeof value.authentication_method === "string" ||
      value.authentication_method === null
    ) ||
    !(typeof value.csrf_token === "string" || value.csrf_token === null)
  )
    return false;
  if (!value.authenticated) return value.user === null;
  const user = value.user;
  return (
    isRecord(user) &&
    typeof user.id === "string" &&
    (typeof user.username === "string" || user.username === null) &&
    (typeof user.email === "string" || user.email === null) &&
    typeof user.display_name === "string" &&
    typeof value.role === "string" &&
    typeof value.authentication_method === "string"
  );
}

export function isErrorBody(value: unknown): value is ApiErrorBody {
  return (
    isRecord(value) &&
    typeof value.code === "string" &&
    typeof value.message === "string" &&
    typeof value.request_id === "string" &&
    "details" in value
  );
}
