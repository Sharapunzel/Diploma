import { afterEach, expect, it, vi } from "vitest";
import {
  cleanup,
  render,
  screen,
  waitFor,
  within,
} from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { StoresProvider } from "../../app/providers/StoresProvider";
import { AppStores } from "../../app/stores/AppStores";
import { ApiError } from "../../app/api/client";
import { OidcMethodsBlock } from "./OidcMethodsBlock";

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

const initial = {
  mode: "env" as const,
  env_enabled: false,
  version: 0,
  active_version: null,
  active: null,
  draft: null,
  checked: false,
  trial_succeeded: false,
};

function mount(
  write = true,
  provider: object = initial,
  mutation?: (path: string) => Promise<unknown>,
) {
  const stores = new AppStores();
  stores.session.accept(
    {
      authenticated: true,
      user: {
        id: "admin",
        username: "admin",
        email: null,
        display_name: "Admin",
      },
      role: "Administrator",
      permissions: write
        ? ["settings.read", "settings.write"]
        : ["settings.read"],
      authentication_method: "local",
      csrf_token: "csrf",
    },
    stores.session.version,
  );
  const request = vi
    .spyOn(stores.api, "request")
    .mockImplementation(async (path) => {
      if (path === "/oidc-provider") return provider as never;
      if (mutation) return (await mutation(path)) as never;
      throw new ApiError(0, "network_error");
    });
  render(
    <StoresProvider stores={stores}>
      <OidcMethodsBlock />
    </StoresProvider>,
  );
  return request;
}

it("confirms replacement before activating and updates after the server reply", async () => {
  const config = {
    issuer_url: "https://identity.example.test",
    client_id: "client",
    redirect_uri: "https://app.example.test/api/v1/auth/oidc/callback",
    scopes: ["openid"],
    has_secret: true,
    has_ca: false,
  };
  const ready = {
    ...initial,
    version: 1,
    draft: config,
    checked: true,
    trial_succeeded: true,
  };
  const request = mount(true, ready, async () => ({
    ...ready,
    mode: "managed",
    active_version: 1,
    active: config,
    trial_succeeded: false,
  }));
  await screen.findByText(/Пробный вход прошёл/);
  await userEvent.click(screen.getByRole("button", { name: "Активировать" }));
  await userEvent.click(
    within(screen.getByRole("dialog")).getByRole("button", { name: "Отмена" }),
  );
  expect(
    request.mock.calls.filter(([path]) => path === "/oidc-provider/activate"),
  ).toHaveLength(0);
  await userEvent.click(screen.getByRole("button", { name: "Активировать" }));
  await userEvent.click(
    within(screen.getByRole("dialog")).getByRole("button", {
      name: "Активировать",
    }),
  );
  await waitFor(() =>
    expect(
      request.mock.calls.filter(([path]) => path === "/oidc-provider/activate"),
    ).toHaveLength(1),
  );
  expect(await screen.findByText(/Источник: интерфейс/)).toBeDefined();
});

it("keeps local fallback visible and hides mutations without settings.write", async () => {
  const request = mount(false);
  expect(
    await screen.findByText(/Локальный вход всегда доступен/),
  ).toBeDefined();
  expect(
    screen.queryByRole("button", { name: "Сохранить черновик" }),
  ).toBeNull();
  expect(request.mock.calls.map(([path]) => path)).toEqual(["/oidc-provider"]);
});

it("does not replay a draft mutation after an unknown result", async () => {
  const request = mount();
  await screen.findByText(/Источник: настройки хоста/);
  await userEvent.type(
    screen.getByRole("textbox", { name: "Issuer URL" }),
    "https://id.example.test",
  );
  await userEvent.type(
    screen.getByRole("textbox", { name: "Client ID" }),
    "client",
  );
  await userEvent.type(
    screen.getByRole("textbox", { name: "Callback URL" }),
    "https://app.example.test/api/v1/auth/oidc/callback",
  );
  await userEvent.click(
    screen.getByRole("button", { name: "Сохранить черновик" }),
  );
  await waitFor(() =>
    expect(
      request.mock.calls.filter(([path]) => path === "/oidc-provider/draft"),
    ).toHaveLength(1),
  );
  expect(await screen.findByText(/Исход изменения неизвестен/)).toBeDefined();
  await userEvent.click(screen.getByRole("button", { name: "Обновить" }));
  expect(
    request.mock.calls.filter(([path]) => path === "/oidc-provider/draft"),
  ).toHaveLength(1);
});

it("deletes only the saved secret and keeps unsaved form edits local", async () => {
  const saved = {
    issuer_url: "https://identity.example.test",
    client_id: "saved-client",
    redirect_uri: "http://localhost:8080/api/v1/auth/oidc/callback",
    scopes: ["openid", "email"],
    has_secret: true,
    has_ca: false,
  };
  const ready = { ...initial, version: 7, draft: saved };
  const request = mount(true, ready, async (path) => {
    if (path.startsWith("/oidc-provider/draft/secret?"))
      return { ...ready, version: 8, draft: { ...saved, has_secret: false } };
    throw new ApiError(0, "network_error");
  });
  await screen.findByText(/Секрет сохранён/);
  const issuer = screen.getByRole("textbox", { name: "Issuer URL" });
  const clientId = screen.getByRole("textbox", { name: "Client ID" });
  const callback = screen.getByRole("textbox", { name: "Callback URL" });
  const scopes = screen.getByRole("textbox", { name: "Scopes через пробел" });
  await userEvent.clear(issuer);
  await userEvent.type(issuer, "https://unsaved.example.test");
  await userEvent.clear(clientId);
  await userEvent.type(clientId, "unsaved-client");
  await userEvent.clear(callback);
  await userEvent.type(callback, "http://localhost:8080/unsaved");
  await userEvent.clear(scopes);
  await userEvent.type(scopes, "openid profile");

  await userEvent.click(
    screen.getByRole("button", { name: "Удалить секрет из черновика" }),
  );

  await waitFor(() =>
    expect(
      request.mock.calls.some(([path]) =>
        String(path).startsWith("/oidc-provider/draft/secret?version=7"),
      ),
    ).toBe(true),
  );
  const deletion = request.mock.calls.find(([path]) =>
    String(path).startsWith("/oidc-provider/draft/secret?version=7"),
  );
  expect(deletion?.[1]).toMatchObject({ method: "DELETE" });
  expect(deletion?.[1]).not.toHaveProperty("body");
  expect((issuer as HTMLInputElement).value).toBe(
    "https://unsaved.example.test",
  );
  expect((clientId as HTMLInputElement).value).toBe("unsaved-client");
  expect((callback as HTMLInputElement).value).toBe(
    "http://localhost:8080/unsaved",
  );
  expect((scopes as HTMLInputElement).value).toBe("openid profile");
});

it.each(["issuer_url", "redirect_uri"] as const)(
  "attaches HTTPS validation errors to the %s field",
  async (field) => {
    const request = mount(true, initial, async () => {
      throw new ApiError(422, "oidc_https_required", null, null, { field });
    });
    await screen.findByText(/Источник: настройки хоста/);
    await userEvent.type(
      screen.getByRole("textbox", { name: "Issuer URL" }),
      "http://identity.example.test",
    );
    await userEvent.type(
      screen.getByRole("textbox", { name: "Client ID" }),
      "client",
    );
    await userEvent.type(
      screen.getByRole("textbox", { name: "Callback URL" }),
      "http://other.example/api/v1/auth/oidc/callback",
    );
    await userEvent.click(
      screen.getByRole("button", { name: "Сохранить черновик" }),
    );
    await waitFor(() => expect(request).toHaveBeenCalledTimes(2));
    const target = screen.getByRole("textbox", {
      name: field === "issuer_url" ? "Issuer URL" : "Callback URL",
    });
    expect(
      target
        .closest(".ant-form-item")
        ?.classList.contains("ant-form-item-has-error"),
    ).toBe(true);
    const other = screen.getByRole("textbox", {
      name: field === "issuer_url" ? "Callback URL" : "Issuer URL",
    });
    expect(
      other
        .closest(".ant-form-item")
        ?.classList.contains("ant-form-item-has-error"),
    ).toBe(false);
  },
);
