import { afterEach, describe, expect, it, vi } from "vitest";
import {
  cleanup,
  render,
  screen,
  waitFor,
  within,
} from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { StoresProvider } from "../../app/providers/StoresProvider";
import { AppStores } from "../../app/stores/AppStores";
import type { Session } from "../../app/api/dto";
import type { Agent } from "../../app/api/agents";
import { ApiError } from "../../app/api/client";
import { AgentsPage } from "./AgentsPage";

const agent: Agent = {
  id: "a",
  name: "edge",
  description: "server logs",
  state: "ready",
  access_revision: 1,
  credentials_claimable: true,
  safe_error: null,
  operation_id: null,
  topics: [
    {
      id: "t",
      connection_id: "c",
      source_id: "s",
      topic_identity: "i",
      topic_name: "logs",
      action: "create",
      state: "ready",
    },
  ],
};
function setup(
  write: boolean,
  currentAgent: Agent = agent,
  failInitialGet = false,
  failOperationReadOnce = false,
) {
  const stores = new AppStores();
  const session: Session = {
    authenticated: true,
    user: { id: "u", username: "reader", email: null, display_name: "Reader" },
    role: "renamed role",
    permissions: write ? ["agents.read", "agents.write"] : ["agents.read"],
    authentication_method: "local",
    csrf_token: "csrf",
  };
  stores.session.accept(session, stores.session.version);
  let failedOperationRead = false;
  const request = vi
    .spyOn(stores.api, "request")
    .mockImplementation(async (path) => {
      if (path === "/agents/a") {
        if (failInitialGet) throw new ApiError(0, "network_error");
        return currentAgent as never;
      }
      if (path === "/agents/a/revoke")
        return {
          id: "op",
          agent_id: "a",
          state: "pending",
        } as never;
      if (path === "/agents/a/topics")
        return {
          id: "op",
          agent_id: "a",
          state: "pending",
        } as never;
      if (path === "/agents/a/operations/op") {
        if (failOperationReadOnce && !failedOperationRead) {
          failedOperationRead = true;
          throw new ApiError(0, "network_error");
        }
        return { id: "op", agent_id: "a", state: "succeeded" } as never;
      }
      if (path === "/agents/transport")
        return {
          bootstrap_server: "127.0.0.1:9092",
          security_protocol: "SASL_PLAINTEXT",
          ca_pem: null,
        } as never;
      if (path.startsWith("/kafka-connections?"))
        return { items: [{ id: "c", name: "Kafka" }], total: 1 } as never;
      if (path === "/kafka-connections/c/topics?include_internal=false")
        return {
          items: [{ name: "new-topic", is_registered: false }],
          total: 1,
        } as never;
      if (path === "/agents/a/credentials/claim")
        return {
          agent_id: "a",
          revision: 1,
          username: "user",
          password: "secret",
        } as never;
      return { items: [agent], total: 1, limit: 20, offset: 0 } as never;
    });
  render(
    <StoresProvider stores={stores}>
      <MemoryRouter initialEntries={["/agents/a"]}>
        <Routes>
          <Route path="/agents/:id" element={<AgentsPage />} />
        </Routes>
      </MemoryRouter>
    </StoresProvider>,
  );
  return request;
}
afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
  sessionStorage.clear();
});

function setupList(
  operationState: "succeeded" | "failed" = "succeeded",
  loseCreateResponse = false,
  failOperationReads = 0,
) {
  const stores = new AppStores();
  stores.session.accept(
    {
      authenticated: true,
      user: { id: "u", username: "admin", email: null, display_name: "Admin" },
      role: "custom",
      permissions: ["agents.read", "agents.write"],
      authentication_method: "local",
      csrf_token: "csrf",
    },
    stores.session.version,
  );
  let failedOperationReads = 0;
  const request = vi
    .spyOn(stores.api, "request")
    .mockImplementation(async (path) => {
      if (path.startsWith("/kafka-connections?"))
        return { items: [{ id: "c", name: "Kafka" }], total: 1 } as never;
      if (path.startsWith("/kafka-connections/c/topics"))
        return {
          items: [
            { name: "existing", partition_count: 1, is_registered: true },
          ],
          total: 1,
        } as never;
      if (path === "/agents?limit=20&offset=0")
        return { items: [], total: 0, limit: 20, offset: 0 } as never;
      if (path === "/agents" && loseCreateResponse)
        throw new ApiError(0, "network_error");
      if (path === "/agents" || path === "/agents/a")
        return {
          ...agent,
          id: "a",
          state: operationState === "failed" ? "error" : "ready",
          operation_id: "op",
        } as never;
      if (
        path === "/agents/a/operations/op" &&
        failedOperationReads < failOperationReads
      ) {
        failedOperationReads += 1;
        throw new ApiError(0, "network_error");
      }
      if (path === "/agents/a/operations/op")
        return {
          id: "op",
          agent_id: "a",
          kind: "register",
          state: operationState,
          attempts: 1,
          safe_error:
            operationState === "failed" ? "Не удалось настроить Kafka" : null,
        } as never;
      throw new Error(`Unexpected request: ${path}`);
    });
  render(
    <StoresProvider stores={stores}>
      <MemoryRouter initialEntries={["/agents"]}>
        <Routes>
          <Route path="/agents" element={<AgentsPage />} />
          <Route path="/agents/:id" element={<AgentsPage />} />
        </Routes>
      </MemoryRouter>
    </StoresProvider>,
  );
  return request;
}

describe("AgentsPage", () => {
  it("shows safe metadata to a guest without mutation or claim controls", async () => {
    const request = setup(false);
    expect(await screen.findByDisplayValue("edge")).toBeDefined();
    expect(
      screen.queryByRole("button", { name: "Получить конфигурацию" }),
    ).toBeNull();
    expect(
      screen.queryByRole("button", { name: "Отозвать доступ" }),
    ).toBeNull();
    expect(request).not.toHaveBeenCalledWith(
      "/agents/a/credentials/claim",
      expect.anything(),
    );
  });
  it("explains an ordinary read failure without offering mutation replay", async () => {
    setup(true, agent, true);
    expect(await screen.findByText(/Нет соединения с сервером/)).toBeDefined();
    expect(
      screen.queryByRole("button", {
        name: "Повторить запрос с прежним ключом",
      }),
    ).toBeNull();
    expect(
      screen.queryByRole("button", {
        name: "Повторить чтение состояния",
      }),
    ).toBeNull();
  });
  it("offers to reread a confirmed operation after its status request fails", async () => {
    const request = setup(true, agent, false, true);
    await userEvent.click(
      await screen.findByRole("button", { name: "Отозвать доступ" }),
    );
    const confirmation = await screen.findByRole("dialog");
    await userEvent.click(
      within(confirmation).getByRole("button", { name: "Отозвать" }),
    );
    expect(
      await screen.findByText(
        /Запрос принят сервером, но состояние операции пока не удалось прочитать/,
      ),
    ).toBeDefined();
    expect(
      screen.getByRole("button", { name: "Повторить чтение состояния" }),
    ).toBeDefined();
    expect(
      screen.queryByRole("button", {
        name: "Повторить запрос с прежним ключом",
      }),
    ).toBeNull();
    const retryRead = await screen.findByRole("button", {
      name: /Повторить чтение состояния/,
    });
    await waitFor(() => expect(retryRead.hasAttribute("disabled")).toBe(false));
    await userEvent.click(retryRead);
    expect(await screen.findByText(/Операция: Завершена/)).toBeDefined();
    expect(
      request.mock.calls.filter(([path]) => path === "/agents/a/revoke"),
    ).toHaveLength(1);
  });
  it("recovers an accepted registration from its server card without posting twice", async () => {
    const request = setupList("succeeded", false, 2);
    await userEvent.click(
      await screen.findByRole("button", { name: "Зарегистрировать агента" }),
    );
    await userEvent.type(
      screen.getByRole("textbox", { name: "Название" }),
      "New agent",
    );
    await userEvent.click(
      screen.getByRole("combobox", { name: "Подключение Kafka" }),
    );
    await userEvent.click(await screen.findByText("Kafka"));
    await userEvent.type(
      screen.getByRole("textbox", { name: "Имя нового топика" }),
      "new-topic",
    );
    await userEvent.click(screen.getByRole("button", { name: "Отправить" }));
    expect(
      await screen.findByText(
        /Запрос принят сервером, но состояние операции пока не удалось прочитать/,
      ),
    ).toBeDefined();
    expect(
      screen.queryByRole("dialog", { name: "Регистрация агента" }),
    ).toBeNull();
    const retryRead = await screen.findByRole("button", {
      name: /Повторить чтение состояния/,
    });
    await waitFor(() => expect(retryRead.hasAttribute("disabled")).toBe(false));
    await userEvent.click(retryRead);
    expect(await screen.findByText(/Операция: Завершена/)).toBeDefined();
    expect(
      request.mock.calls.filter(([path]) => path === "/agents"),
    ).toHaveLength(1);
    expect(
      request.mock.calls.filter(([path]) => path === "/agents/a/operations/op"),
    ).toHaveLength(3);
  }, 60_000);
  it("closes add-topic form after accepted mutation and retries only its observation", async () => {
    const request = setup(true, agent, false, true);
    await userEvent.click(
      await screen.findByRole("button", { name: "Добавить топик" }),
    );
    await userEvent.click(
      await screen.findByRole("combobox", { name: "Подключение Kafka" }),
    );
    await userEvent.click(await screen.findByText("Kafka"));
    await userEvent.type(
      screen.getByRole("textbox", { name: "Имя нового топика" }),
      "new-topic",
    );
    await userEvent.click(screen.getByRole("button", { name: "Отправить" }));
    expect(
      await screen.findByText(
        /Запрос принят сервером, но состояние операции пока не удалось прочитать/,
      ),
    ).toBeDefined();
    expect(screen.queryByRole("dialog", { name: "Добавить топик" })).toBeNull();
    await userEvent.click(
      screen.getByRole("button", { name: "Повторить чтение состояния" }),
    );
    expect(await screen.findByText(/Операция: Завершена/)).toBeDefined();
    expect(
      request.mock.calls.filter(([path]) => path === "/agents/a/topics"),
    ).toHaveLength(1);
  }, 60_000);
  it("claims only after a separate explicit action", async () => {
    const request = setup(true);
    const button = await screen.findByRole("button", {
      name: "Получить конфигурацию",
    });
    expect(
      request.mock.calls.some(
        ([path]) => path === "/agents/a/credentials/claim",
      ),
    ).toBe(false);
    await userEvent.click(button);
    expect(
      await screen.findByText(/Однократная конфигурация Fluent Bit/),
    ).toBeDefined();
    expect(
      request.mock.calls.filter(
        ([path]) => path === "/agents/a/credentials/claim",
      ),
    ).toHaveLength(1);
    expect(screen.getByText(/rdkafka.sasl.password secret/)).toBeDefined();
  });
  it("previews a password-free template repeatedly without claiming credentials", async () => {
    const request = setup(true);
    const preview = await screen.findByRole("button", {
      name: "Просмотреть шаблон",
    });
    await userEvent.click(preview);
    expect(
      await screen.findByText(/Шаблон конфигурации Fluent Bit/),
    ).toBeDefined();
    expect(screen.getByText(/__REPLACE_WITH_AGENT_PASSWORD__/)).toBeDefined();
    expect(screen.queryByText(/rdkafka.sasl.password secret/)).toBeNull();
    expect(
      request.mock.calls.some(
        ([path]) => path === "/agents/a/credentials/claim",
      ),
    ).toBe(false);
    expect(sessionStorage.length).toBe(0);
    const dialog = screen.getByRole("dialog", {
      name: "Шаблон конфигурации Fluent Bit",
    });
    await userEvent.click(
      within(dialog).getByRole("button", { name: "Close" }),
    );
    await userEvent.click(preview);
    expect(
      await screen.findByText(/__REPLACE_WITH_AGENT_PASSWORD__/),
    ).toBeDefined();
    expect(
      request.mock.calls.filter(
        ([path]) => path === "/agents/a/credentials/claim",
      ),
    ).toHaveLength(0);
  });
  it("keeps input choices per topic and claims once after selecting an input", async () => {
    const multiTopicAgent = {
      ...agent,
      topics: [
        agent.topics[0],
        { ...agent.topics[0], id: "t2", topic_name: "audit" },
      ],
    };
    const request = setup(true, multiTopicAgent);
    await userEvent.click(
      await screen.findByRole("combobox", {
        name: "Вход Fluent Bit для топика logs",
      }),
    );
    await userEvent.click(
      await screen.findByText("Журнал Windows (winevtlog)", { exact: true }),
    );
    expect(
      within(screen.getByRole("row", { name: /audit/ })).getAllByText(
        "Файл (tail)",
      ).length,
    ).toBeGreaterThan(0);
    expect(
      within(screen.getByRole("row", { name: /logs/ })).getAllByText(
        "Журнал Windows (winevtlog)",
      ).length,
    ).toBeGreaterThan(0);
    await userEvent.click(
      await screen.findByRole("button", { name: "Получить конфигурацию" }),
    );
    const configDialog = await screen.findByRole("dialog", {
      name: "Однократная конфигурация Fluent Bit",
    });
    await userEvent.click(
      within(configDialog).getByRole("combobox", {
        name: "Вход Fluent Bit для топика logs",
      }),
    );
    const systemdOptions = await screen.findAllByText(
      "Linux journald (systemd)",
      { exact: true },
    );
    await userEvent.click(
      systemdOptions
        .filter((element) =>
          element.classList.contains("ant-select-item-option-content"),
        )
        .at(-1)!,
    );
    expect(
      within(configDialog).getByText(/Systemd_Filter _SYSTEMD_UNIT/),
    ).toBeDefined();
    expect(
      within(configDialog).getByText(/Call diploma_systemd_envelope/),
    ).toBeDefined();
    expect(within(configDialog).getByText(/journal_time=/)).toBeDefined();
    expect(
      screen.getByText(/Исходное время события остаётся в строке log/),
    ).toBeDefined();
    expect(screen.getByText(/время обработки агентом/)).toBeDefined();
    expect(
      screen.getByText(/выберите unit и сохраните DB cursor/),
    ).toBeDefined();
    expect(
      within(configDialog).getByText(/rdkafka.sasl.password secret/),
    ).toBeDefined();
    expect(
      screen.getByText(/Path \/REPLACE\/WITH\/SERVICE_2.log/),
    ).toBeDefined();
    expect(
      request.mock.calls.filter(
        ([path]) => path === "/agents/a/credentials/claim",
      ),
    ).toHaveLength(1);
  }, 30_000);
  it("does not consume credentials when the configuration preflight fails", async () => {
    const request = setup(true, {
      ...agent,
      topics: [{ ...agent.topics[0], state: "error" }],
    });
    await userEvent.click(
      await screen.findByRole("button", { name: "Получить конфигурацию" }),
    );
    expect(await screen.findByText(/Пароль не запрашивался/)).toBeDefined();
    expect(
      request.mock.calls.some(
        ([path]) => path === "/agents/a/credentials/claim",
      ),
    ).toBe(false);
  });
  it("rejects a mixed Windows and Linux input choice before claiming", async () => {
    const multiTopicAgent = {
      ...agent,
      topics: [
        agent.topics[0],
        { ...agent.topics[0], id: "t2", topic_name: "audit" },
      ],
    };
    const request = setup(true, multiTopicAgent);
    await userEvent.click(
      await screen.findByRole("combobox", {
        name: "Вход Fluent Bit для топика logs",
      }),
    );
    await userEvent.click(
      await screen.findByText("Журнал Windows (winevtlog)", { exact: true }),
    );
    await userEvent.click(
      screen.getByRole("combobox", {
        name: "Вход Fluent Bit для топика audit",
      }),
    );
    const systemdOptions = await screen.findAllByText(
      "Linux journald (systemd)",
      { exact: true },
    );
    await userEvent.click(
      systemdOptions
        .filter((element) =>
          element.classList.contains("ant-select-item-option-content"),
        )
        .at(-1)!,
    );
    expect(await screen.findByText(/одной ОС/)).toBeDefined();
    expect(
      request.mock.calls.some(
        ([path]) => path === "/agents/a/credentials/claim",
      ),
    ).toBe(false);
    expect(
      within(screen.getByRole("row", { name: /audit/ })).getAllByText(
        "Файл (tail)",
      ).length,
    ).toBeGreaterThan(0);
  });
  it("previews a Windows tail-only config before the one-time claim", async () => {
    const request = setup(true);
    await userEvent.click(
      await screen.findByRole("combobox", { name: "ОС хоста агента" }),
    );
    await userEvent.click(await screen.findByText("Windows", { exact: true }));
    await userEvent.click(
      await screen.findByRole("button", { name: "Просмотреть шаблон" }),
    );
    const dialog = await screen.findByRole("dialog", {
      name: "Шаблон конфигурации Fluent Bit",
    });
    expect(
      within(dialog).getByText(
        /DB C:\/ProgramData\/FluentBit\/state\/logs-t.db/,
      ),
    ).toBeDefined();
    expect(
      request.mock.calls.some(
        ([path]) => path === "/agents/a/credentials/claim",
      ),
    ).toBe(false);
  });
  it("does not revoke access when the confirmation is cancelled", async () => {
    const request = setup(true);
    await userEvent.click(
      await screen.findByRole("button", { name: "Отозвать доступ" }),
    );
    expect(
      await screen.findByText(/Топики, источники и события останутся/),
    ).toBeDefined();
    expect(
      request.mock.calls.some(([path]) => path === "/agents/a/revoke"),
    ).toBe(false);
    await userEvent.click(screen.getByRole("button", { name: "Отмена" }));
    expect(
      request.mock.calls.some(([path]) => path === "/agents/a/revoke"),
    ).toBe(false);
  });
  it("allows attaching a topic whose source is registered but has no agent", async () => {
    const request = setupList();
    await userEvent.click(
      await screen.findByRole("button", { name: "Зарегистрировать агента" }),
    );
    await userEvent.type(
      screen.getByRole("textbox", { name: "Название" }),
      "New agent",
    );
    await userEvent.click(
      screen.getByRole("combobox", { name: "Подключение Kafka" }),
    );
    await userEvent.click(await screen.findByText("Kafka"));
    await userEvent.click(screen.getByRole("radio", { name: "Закрепить" }));
    await userEvent.click(
      screen.getByRole("combobox", { name: "Свободный топик" }),
    );
    await userEvent.click(
      await screen.findByText(
        /existing — источник уже зарегистрирован, топик свободен/,
      ),
    );
    await userEvent.click(screen.getByRole("button", { name: "Отправить" }));
    expect(await screen.findByText("Карточка агента")).toBeDefined();
    const creation = request.mock.calls.find(([path]) => path === "/agents");
    expect(JSON.parse(String(creation?.[1]?.body)).topics).toEqual([
      { connection_id: "c", topic_name: "existing", action: "attach" },
    ]);
  }, 60_000);
  it("keeps the entered registration when provisioning fails", async () => {
    setupList("failed");
    await userEvent.click(
      await screen.findByRole("button", { name: "Зарегистрировать агента" }),
    );
    await userEvent.type(
      screen.getByRole("textbox", { name: "Название" }),
      "Retain this name",
    );
    await userEvent.click(
      screen.getByRole("combobox", { name: "Подключение Kafka" }),
    );
    await userEvent.click(await screen.findByText("Kafka"));
    await userEvent.type(
      screen.getByRole("textbox", { name: "Имя нового топика" }),
      "new-topic",
    );
    await userEvent.click(screen.getByRole("button", { name: "Отправить" }));
    expect(await screen.findByText(/Не удалось настроить Kafka/)).toBeDefined();
    expect(screen.getByRole("textbox", { name: "Название" })).toHaveProperty(
      "value",
      "Retain this name",
    );
    expect(
      screen.getByRole("dialog", { name: "Регистрация агента" }),
    ).toBeDefined();
  }, 60_000);
  it("replays an uncertain registration after reload with the same key", async () => {
    const first = setupList("succeeded", true);
    await userEvent.click(
      await screen.findByRole("button", { name: "Зарегистрировать агента" }),
    );
    await userEvent.type(
      screen.getByRole("textbox", { name: "Название" }),
      "Recovered agent",
    );
    await userEvent.click(
      screen.getByRole("combobox", { name: "Подключение Kafka" }),
    );
    await userEvent.click(await screen.findByText("Kafka"));
    await userEvent.type(
      screen.getByRole("textbox", { name: "Имя нового топика" }),
      "recover-topic",
    );
    await userEvent.click(screen.getByRole("button", { name: "Отправить" }));
    expect(
      await screen.findByText("Результат регистрации ещё не подтверждён"),
    ).toBeDefined();
    const firstKey = (
      first.mock.calls.find(([path]) => path === "/agents")?.[1]
        ?.headers as Record<string, string>
    )["Idempotency-Key"];
    cleanup();
    const second = setupList();
    await userEvent.click(
      await screen.findByRole("button", { name: "Повторить с прежним ключом" }),
    );
    expect(await screen.findByText("Карточка агента")).toBeDefined();
    const secondKey = (
      second.mock.calls.find(([path]) => path === "/agents")?.[1]
        ?.headers as Record<string, string>
    )["Idempotency-Key"];
    expect(secondKey).toBe(firstKey);
    expect(sessionStorage.getItem("diploma.agent.pending-create")).toBeNull();
  }, 90_000);
});
