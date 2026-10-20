import { afterEach, expect, it, vi } from "vitest";
import {
  act,
  cleanup,
  render,
  screen,
  waitFor,
  within,
} from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { StoresProvider } from "../../app/providers/StoresProvider";
import { AppStores } from "../../app/stores/AppStores";
import type { SourceState, SourceTopology } from "../../app/api/diagnostics";
import { ApiError } from "../../app/api/client";
import { DiagnosticsPage, explainDiagnosticReason } from "./DiagnosticsPage";

afterEach(() => {
  cleanup();
  vi.useRealTimers();
});

const initialTime = new Date("2026-10-20T12:00:00Z");
const source = (
  id = "source-1",
  overrides: Partial<SourceState> = {},
): SourceState => ({
  source_id: id,
  source_name: `Source ${id}`,
  connection_id: "connection-1",
  connection_name: "Kafka test",
  topic: "topic-test",
  topic_id: "topic-id",
  is_enabled: true,
  is_archived: false,
  worker_status: "running",
  reason_code: null,
  reason_description: null,
  captured_at: new Date().toISOString(),
  partitions: [],
  ...overrides,
});
const topology = (overrides: Partial<SourceTopology> = {}): SourceTopology => ({
  source_id: "source-1",
  agent_id: "agent-1",
  agent_name: "Agent test",
  agent_state: "ready",
  assignment_state: "ready",
  normalizer_id: "normalizer-1",
  normalizer_name: "Normalizer test",
  ...overrides,
});
const errorRecord = {
  id: "error-1",
  source_id: "source-1",
  connection_id: "connection-1",
  normalizer_id: "normalizer-1",
  stage: "normalization" as const,
  diagnostics: [{ code: "no_variant_matched" }],
  source_name: "Source source-1",
  connection_name: "Kafka test",
  normalizer_name: "Normalizer test",
  normalizer_version: 2,
  kafka_topic: "topic-test",
  kafka_partition: 0,
  kafka_offset: 10,
  fluent_bit_collected_at: null,
  backend_received_at: initialTime.toISOString(),
  backend_processed_at: initialTime.toISOString(),
};

function renderPage({
  currentSource = source(),
  currentTopology = topology(),
  permissions = [
    "events.read",
    "agents.read",
    "connections.read",
    "sources.read",
    "normalizers.read",
    "processing_errors.raw.read",
  ],
  initialEntry = "/diagnostics?source=source-1",
  failErrors = false,
  historyTotal = 1,
  missingNormalizer = false,
  normalizerFailureTypes = [],
}: {
  currentSource?: SourceState;
  currentTopology?: SourceTopology | null;
  permissions?: string[];
  initialEntry?: string;
  failErrors?: boolean;
  historyTotal?: number;
  missingNormalizer?: boolean;
  normalizerFailureTypes?: Array<"network" | "server">;
} = {}) {
  const stores = new AppStores();
  stores.session.data = {
    authenticated: true,
    user: { id: "user", username: "user", email: null, display_name: "User" },
    role: "admin",
    permissions,
    authentication_method: "local",
    csrf_token: "csrf",
  };
  stores.session.state = "authenticated";
  stores.api.request = vi.fn(async (path: string) => {
    if (
      path.startsWith("/diagnostics/sources?") ||
      path.startsWith("/diagnostics/sources&")
    )
      return { items: [currentSource], total: 1, limit: 20, offset: 0 };
    if (path.endsWith("/topology")) return currentTopology;
    if (path.startsWith("/diagnostics/sources/")) return currentSource;
    if (path.startsWith("/diagnostics/processing-errors?")) {
      if (failErrors) throw new Error("request failed");
      return {
        items: [errorRecord],
        total: historyTotal,
        limit: 20,
        offset: 0,
      };
    }
    if (path.startsWith("/diagnostics/kafka-events?"))
      return { items: [], total: historyTotal, limit: 20, offset: 0 };
    if (path === "/normalizers/normalizer-1") {
      if (missingNormalizer) throw new ApiError(404, "not_found");
      if (normalizerFailureTypes.length > 0) {
        const failureType = normalizerFailureTypes.shift();
        if (failureType === "network") throw new Error("Network unavailable");
        throw new ApiError(503, "unavailable");
      }
      return { id: "normalizer-1", name: "Normalizer test" };
    }
    if (path.endsWith("/raw"))
      return {
        data: btoa("synthetic raw"),
        original_size: 13,
        returned_size: 13,
        truncated: false,
      };
    throw new Error(`Unexpected API request ${path}`);
  }) as typeof stores.api.request;
  render(
    <StoresProvider stores={stores}>
      <MemoryRouter initialEntries={[initialEntry]}>
        <DiagnosticsPage />
      </MemoryRouter>
    </StoresProvider>,
  );
  return { stores };
}

it("ages a running snapshot to unknown after 120 seconds without another API request", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(initialTime);
  const { stores } = renderPage({
    currentSource: source("source-1", {
      captured_at: new Date(initialTime.getTime() - 119_000).toISOString(),
    }),
  });
  await act(async () => {
    for (let i = 0; i < 12; i++) await Promise.resolve();
  });
  expect(screen.getByText("Обработчик: Работает")).not.toBeNull();
  const requestCount = vi.mocked(stores.api.request).mock.calls.length;
  await act(async () => vi.advanceTimersByTimeAsync(2_001));
  expect(screen.getByText("Обработчик: Неизвестно")).not.toBeNull();
  expect(
    screen.getByText(/Снимок устарел; обновите его вручную/),
  ).not.toBeNull();
  expect(stores.api.request).toHaveBeenCalledTimes(requestCount);
});

it("explains common backend and Kafka reason codes safely", () => {
  expect(explainDiagnosticReason("supervisor_unavailable")).toContain(
    "Супервизор Kafka недоступен",
  );
  expect(explainDiagnosticReason("topic_not_found")).toContain(
    "Топик не найден",
  );
  expect(explainDiagnosticReason("kafka_poll_authentication_failed")).toContain(
    "Kafka отклонила учётные данные",
  );
  expect(explainDiagnosticReason("untrusted_backend_detail")).toBe(
    "Неизвестная причина; подробное объяснение недоступно.",
  );
});

it("shows disabled and archived sources as not running and marks missing assignments as broken links", async () => {
  renderPage({
    currentSource: source("source-1", {
      is_enabled: false,
      reason_code: "supervisor_unavailable",
    }),
    currentTopology: topology({
      agent_id: null,
      agent_name: null,
      normalizer_id: null,
      normalizer_name: null,
    }),
  });
  expect((await screen.findByRole("alert")).textContent).toContain(
    "Обработчик: Не запущен",
  );
  expect(screen.getByRole("alert").textContent).toContain(
    "Супервизор Kafka недоступен",
  );
  expect(
    screen.getByRole("list", { name: /Связь с агентом отсутствует/ })
      .textContent,
  ).toContain("Разрыв связи");
  expect(screen.getAllByText("Разрыв связи")).toHaveLength(3);
  expect(
    screen.getByRole("list", {
      name: /Связь с нормализатором и хранением отсутствует/,
    }),
  ).toBeTruthy();
  expect(
    screen.getByRole("list", {
      name: /Связь с нормализатором и хранением отсутствует/,
    }).textContent,
  ).toContain("Не назначен");
});

it("does not report missing topology assignments as broken while topology is unavailable", async () => {
  renderPage({ currentTopology: null });
  await screen.findByRole("alert");
  await waitFor(() => expect(document.querySelector("ol")).not.toBeNull());
  const flow = document.querySelector("ol");
  expect(flow?.querySelector('[data-edge="broken"]')).toBeNull();
  expect(flow?.textContent).not.toContain("Разрыв связи");
});

it("keeps archived and explicitly unknown workers out of the running state", async () => {
  renderPage({
    currentSource: source("source-1", { is_archived: true }),
  });
  expect((await screen.findByRole("alert")).textContent).toContain(
    "Архивный источник",
  );
  expect(screen.getByRole("alert").textContent).toContain(
    "Обработчик: Не запущен",
  );
  cleanup();
  renderPage({
    currentSource: source("source-1", { worker_status: "unknown" }),
  });
  expect((await screen.findByRole("alert")).textContent).toContain(
    "Обработчик: Неизвестно",
  );
});

it("keeps historical normalization errors visible while the worker is running and links without a block", async () => {
  const user = userEvent.setup();
  renderPage();
  await screen.findByText("Обработчик: Работает");
  expect(screen.getByRole("alert").textContent).toContain(
    "Обработчик: Работает",
  );
  await user.click(
    await screen.findByRole("button", { name: /topic-test\/0\/10/ }),
  );
  const dialog = await screen.findByRole("dialog");
  expect(
    within(dialog)
      .getByRole("link", { name: "Перейти к нормализатору" })
      .getAttribute("href"),
  ).toBe("/normalizers/normalizer-1");
  expect(
    within(dialog).queryByText("Конкретный блок в диагностике не указан."),
  ).not.toBeNull();
});

it("preserves separate history failures and applies filters and pagination", async () => {
  const { stores } = renderPage({ failErrors: true });
  await screen.findByText("Обработчик: Работает");
  await screen.findByText("Не удалось выполнить запрос.");
  expect(screen.getByText("История событий Kafka")).not.toBeNull();
  expect(stores.api.request).toHaveBeenCalledWith(
    expect.stringContaining("/diagnostics/kafka-events?"),
    expect.anything(),
  );
});

it("applies the stage filter and history pagination to independent queries", async () => {
  const user = userEvent.setup();
  const { stores } = renderPage({ historyTotal: 41 });
  await screen.findByText("Обработчик: Работает");
  await user.click(screen.getByRole("combobox", { name: "Этап ошибки" }));
  await user.click(
    await screen.findByText("Нормализация", {
      selector: ".ant-select-item-option-content",
    }),
  );
  await waitFor(() =>
    expect(stores.api.request).toHaveBeenCalledWith(
      expect.stringContaining("stage=normalization"),
      expect.anything(),
    ),
  );
  const nextButtons = screen.getAllByRole("button", { name: "right" });
  await user.click(nextButtons[1]);
  await waitFor(() =>
    expect(stores.api.request).toHaveBeenCalledWith(
      expect.stringContaining("offset=20"),
      expect.anything(),
    ),
  );
});

it("does not link a deleted historical normalizer", async () => {
  const user = userEvent.setup();
  renderPage({ missingNormalizer: true });
  await screen.findByText("Обработчик: Работает");
  await user.click(
    await screen.findByRole("button", { name: /topic-test\/0\/10/ }),
  );
  const dialog = await screen.findByRole("dialog");
  expect(
    await within(dialog).findByText(
      "Сохранённый нормализатор удалён или недоступен.",
    ),
  ).not.toBeNull();
  expect(
    within(dialog).queryByRole("link", { name: "Перейти к нормализатору" }),
  ).toBeNull();
});

it("keeps server and network failures unconfirmed until a retry succeeds", async () => {
  const user = userEvent.setup();
  const { stores } = renderPage({
    normalizerFailureTypes: ["server", "network"],
  });
  await screen.findByText("Обработчик: Работает");
  await user.click(
    await screen.findByRole("button", { name: /topic-test\/0\/10/ }),
  );
  const dialog = await screen.findByRole("dialog");
  expect(
    await within(dialog).findByText(
      "Не удалось проверить нормализатор; ссылка не подтверждена.",
    ),
  ).not.toBeNull();
  expect(
    within(dialog).queryByRole("link", { name: "Перейти к нормализатору" }),
  ).toBeNull();
  await user.click(
    within(dialog).getByRole("button", { name: "Повторить проверку" }),
  );
  expect(
    await within(dialog).findByText(
      "Не удалось проверить нормализатор; ссылка не подтверждена.",
    ),
  ).not.toBeNull();
  expect(
    within(dialog).queryByRole("link", { name: "Перейти к нормализатору" }),
  ).toBeNull();
  await user.click(
    within(dialog).getByRole("button", { name: "Повторить проверку" }),
  );
  expect(
    (
      await within(dialog).findByRole("link", {
        name: "Перейти к нормализатору",
      })
    ).getAttribute("href"),
  ).toBe("/normalizers/normalizer-1");
  expect(
    vi
      .mocked(stores.api.request)
      .mock.calls.filter(([path]) =>
        path.startsWith("/normalizers/normalizer-1"),
      ),
  ).toHaveLength(3);
});

it("gates raw and resource navigation by permissions and gives an addressed Kafka connection link", async () => {
  const user = userEvent.setup();
  const { stores } = renderPage({ permissions: ["events.read"] });
  await screen.findByRole("alert");
  await user.click(await screen.findByRole("button", { name: /2\. Kafka/ }));
  expect(screen.getByText("Раздел недоступен по правам")).not.toBeNull();
  await user.click(screen.getByRole("button", { name: /3\. Источник/ }));
  expect(
    screen.getAllByText("Раздел недоступен по правам").length,
  ).toBeGreaterThan(0);
  await user.click(
    await screen.findByRole("button", { name: /topic-test\/0\/10/ }),
  );
  expect(screen.queryByRole("button", { name: "Показать payload" })).toBeNull();
  expect(
    vi
      .mocked(stores.api.request)
      .mock.calls.some(([path]) => path.startsWith("/normalizers/")),
  ).toBe(false);
}, 60_000);
