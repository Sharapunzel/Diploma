import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import {
  Link,
  MemoryRouter,
  Route,
  Routes,
  useNavigate,
} from "react-router-dom";
import { AppStores } from "../../app/stores/AppStores";
import { StoresProvider } from "../../app/providers/StoresProvider";
import type {
  Aggregate,
  CleanupOperation,
  CleanupProtection,
  Point,
} from "../../app/api/aggregates";
import { parseAggregate, printAggregate } from "../../app/api/aggregateJson";
import {
  aggregateFixture,
  aggregateId,
  aggregateTime,
  cleanupFixture,
  cleanupId,
  pointFixture,
  protectionFixture,
  receiptFixture,
} from "../../app/api/aggregateTestData";
import { AggregateCardPage } from "./AggregateCardPage";

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
  vi.useRealTimers();
  Object.defineProperty(document, "hidden", {
    configurable: true,
    value: false,
  });
});
const otherId = "00000000-0000-4000-8000-000000000043";
function json(value: unknown, status = 200) {
  return new Response(printAggregate(value), {
    status,
    headers: { "X-Request-ID": "aggregate-ui" },
  });
}
function failure(code: string, status = 503) {
  return json(
    { code, message: "synthetic", request_id: "aggregate-ui", details: null },
    status,
  );
}
type Attempt = {
  path: string;
  key: string | null;
  body: string | undefined;
  method: string;
};
type Override = (
  path: string,
  options: RequestInit,
) => Promise<Response> | Response | undefined;
function setup() {
  const cards = new Map<string, Aggregate>([
    [aggregateId, aggregateFixture({ name: "Агрегат А" })],
    [otherId, aggregateFixture({ id: otherId, name: "Агрегат Б" })],
  ]);
  const attempts: Attempt[] = [],
    operationReads: string[] = [];
  let override: Override = () => undefined;
  let operation: CleanupOperation = cleanupFixture();
  let plan: CleanupProtection | null = null;
  let points: Point[] = [pointFixture()];
  let pointTotal = 10;
  const request = vi.fn(
    async (input: RequestInfo | URL, options: RequestInit = {}) => {
      const url = new URL(String(input), "https://example.test"),
        path = url.pathname.replace(/^\/api\/v1/, ""),
        method = options.method ?? "GET";
      if (method !== "GET")
        attempts.push({
          path,
          method,
          key: new Headers(options.headers).get("Idempotency-Key"),
          body: options.body as string | undefined,
        });
      const intercepted = override(path + url.search, options);
      if (intercepted) return await intercepted;
      if (path === "/aggregates/operators")
        return json([
          {
            name: "count",
            kind: "feature",
            inputs: [],
            parameters: [],
            parameter_types: {},
            compatible_fields: [],
            arity: null,
            weight: 1,
            weight_formula: "weight",
            result_nullable: false,
            result: "number",
          },
        ]);
      const match = /^\/aggregates\/([^/]+)(.*)$/.exec(path);
      if (!match) throw new Error("Unexpected request: " + path);
      const target = match[1],
        suffix = match[2],
        card = cards.get(target);
      if (!card) return failure("not_found", 404);
      if (!suffix && method === "GET") return json(card);
      if (suffix === "/points")
        return json({
          items: points.map((point) => ({ ...point, aggregate_id: target })),
          total: pointTotal,
        });
      if (suffix === "/points/cleanup-preview")
        return json(
          plan ??
            protectionFixture({
              aggregate_id: target,
              requested_keep: parseAggregate(
                url.searchParams.get("keep_last")!,
              ),
              planned_delete: 10,
            }),
        );
      if (suffix === "/pause" || suffix === "/unpause") {
        cards.set(target, {
          ...card,
          paused: suffix === "/pause",
          execution_generation: 1,
        });
        return json(
          receiptFixture({
            aggregate_id: target,
            action: suffix === "/pause" ? "pause" : "unpause",
          }),
        );
      }
      if (suffix === "/points/cleanup") {
        cards.set(target, { ...card, active_cleanup_id: cleanupId });
        return json(
          receiptFixture({
            aggregate_id: target,
            action: "cleanup",
            cleanup_operation_id: cleanupId,
          }),
          202,
        );
      }
      if (suffix === `/cleanup-operations/${cleanupId}`) {
        operationReads.push(cleanupId);
        return json({ ...operation, aggregate_id: target });
      }
      if (suffix === `/cleanup-operations/${cleanupId}/retry`)
        return json(
          receiptFixture({
            aggregate_id: target,
            action: "cleanup_retry",
            cleanup_operation_id: cleanupId,
          }),
          202,
        );
      throw new Error("Unexpected request: " + path);
    },
  );
  vi.stubGlobal("fetch", request);
  Object.defineProperty(document, "hidden", {
    configurable: true,
    value: false,
  });
  const stores = new AppStores();
  stores.session.accept(
    {
      authenticated: true,
      user: {
        id: "00000000-0000-4000-8000-000000000003",
        username: "admin",
        email: null,
        display_name: "Администратор",
      },
      role: "admin",
      permissions: ["aggregates.read", "aggregates.write"],
      authentication_method: "local",
      csrf_token: "csrf",
    },
    0,
  );
  function Navigation() {
    const navigate = useNavigate();
    return (
      <nav>
        <Link to={`/aggregates/${otherId}`}>Открыть Б</Link>
        <button onClick={() => navigate(-1)}>Назад</button>
        <Link to="/elsewhere">Уйти</Link>
      </nav>
    );
  }
  function mount() {
    return render(
      <StoresProvider stores={stores}>
        <MemoryRouter initialEntries={[`/aggregates/${aggregateId}`]}>
          <Navigation />
          <Routes>
            <Route path="/aggregates/:id" element={<AggregateCardPage />} />
            <Route
              path="/aggregates/new"
              element={<p>Новый черновик копии</p>}
            />
            <Route path="/elsewhere" element={<p>Другая страница</p>} />
          </Routes>
        </MemoryRouter>
      </StoresProvider>,
    );
  }
  return {
    cards,
    stores,
    attempts,
    operationReads,
    request,
    mount,
    intercept(value: Override) {
      override = value;
    },
    setOperation(value: CleanupOperation) {
      operation = value;
    },
    setPlan(value: CleanupProtection | null) {
      plan = value;
    },
    clearPoints() {
      points = [];
      pointTotal = 0;
    },
  };
}
function button(
  name: string,
  region: HTMLElement = document.body,
): HTMLButtonElement {
  const element = within(region)
    .getByText(name, { exact: true })
    .closest("button");
  if (!element) throw new Error(`No button for ${name}`);
  return element;
}
async function findButton(
  name: string,
  region: HTMLElement = document.body,
): Promise<HTMLButtonElement> {
  const element = (
    await within(region).findByText(name, { exact: true })
  ).closest("button");
  if (!element) throw new Error(`No button for ${name}`);
  await waitFor(() => expect(element.disabled).toBe(false));
  return element;
}
async function ready() {
  await screen.findByLabelText("Пауза");
  await screen.findByLabelText("Таблица точек");
}
async function openPause() {
  fireEvent.click(screen.getByLabelText("Пауза"));
  return await screen.findByRole("dialog", {
    name: "Поставить агрегат на паузу?",
  });
}
function cleanupPanel() {
  return screen.getByLabelText("Ручная очистка истории");
}
async function prepareCleanup() {
  fireEvent.click(button("Проверить защиту истории", cleanupPanel()));
  await screen.findByLabelText("План защиты истории");
}
async function confirmCleanup() {
  fireEvent.click(button("Очистить историю", cleanupPanel()));
  const dialog = await screen.findByRole("dialog", {
    name: "Очистить историю?",
  });
  fireEvent.click(within(dialog).getByRole("button", { name: "Очистить" }));
}

it("requires pause confirmation, cancellation sends nothing, loading blocks duplicates and resume is a separate explicit action", async () => {
  const context = setup();
  context.mount();
  await ready();
  let dialog = await openPause();
  expect(context.attempts).toHaveLength(0);
  expect(within(dialog).getByText(new RegExp(aggregateId))).toBeTruthy();
  fireEvent.click(within(dialog).getByRole("button", { name: "Отмена" }));
  await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
  expect(context.attempts).toHaveLength(0);
  let release!: (value: Response) => void;
  context.intercept((path) =>
    path.endsWith("/pause")
      ? new Promise<Response>((resolve) => {
          release = resolve;
        })
      : undefined,
  );
  dialog = await openPause();
  fireEvent.click(within(dialog).getByRole("button", { name: "Пауза" }));
  await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
  await waitFor(() => expect(context.attempts).toHaveLength(1));
  expect((screen.getByLabelText("Пауза") as HTMLButtonElement).disabled).toBe(
    true,
  );
  fireEvent.click(screen.getByLabelText("Пауза"));
  expect(context.attempts).toHaveLength(1);
  context.cards.set(
    aggregateId,
    aggregateFixture({ name: "Агрегат А", paused: true, next_index: 7 }),
  );
  await act(async () => release(json(receiptFixture())));
  await screen.findByLabelText("Продолжить");
  expect(screen.getByText("Следующий индекс: 7")).toBeTruthy();
  context.intercept(() => undefined);
  fireEvent.click(screen.getByLabelText("Продолжить"));
  await waitFor(() => expect(context.attempts).toHaveLength(2));
  expect(context.attempts.map((item) => item.path.split("/").at(-1))).toEqual([
    "pause",
    "unpause",
  ]);
  expect(context.attempts[0].key).toBeTruthy();
  expect(context.attempts[1].key).not.toBe(context.attempts[0].key);
});

it("retains the unknown pause body/key, never confirms it from an old receipt and retries GET only after accepted command", async () => {
  const context = setup();
  let posts = 0,
    cardFails = false;
  context.intercept((path, options) => {
    if (path.endsWith("/pause") && options.method === "POST") {
      posts++;
      if (posts === 1) throw new TypeError("network");
      cardFails = true;
      return json(receiptFixture());
    }
    if (path === `/aggregates/${aggregateId}` && cardFails)
      return failure("temporary_unavailable");
    return undefined;
  });
  context.mount();
  await ready();
  const dialog = await openPause();
  fireEvent.click(within(dialog).getByRole("button", { name: "Пауза" }));
  const retry = await findButton("Повторить команду с тем же ключом");
  expect(
    screen.queryByText(
      "Команда подтверждена. Текущее состояние читается отдельно.",
    ),
  ).toBeNull();
  expect((screen.getByLabelText("Пауза") as HTMLButtonElement).disabled).toBe(
    true,
  );
  fireEvent.click(retry);
  await screen.findByText(
    "Команда принята; чтение карточки не удалось. Повторите только чтение.",
  );
  expect(screen.getAllByText("aggregate-ui", { exact: true })).toHaveLength(1);
  expect(
    screen.queryByText(/Не удалось обновить состояние хранения/),
  ).toBeNull();
  expect(context.attempts).toHaveLength(2);
  expect(context.attempts[1]).toEqual(context.attempts[0]);
  cardFails = false;
  context.cards.set(aggregateId, aggregateFixture({ paused: true }));
  const refresh = screen.getByLabelText("Обновить состояние");
  await waitFor(() =>
    expect(refresh.querySelector('[aria-label="loading"]')).toBeNull(),
  );
  fireEvent.click(refresh);
  await screen.findByLabelText("Продолжить");
  expect(context.attempts).toHaveLength(2);
  // The old accepted receipt must not claim that this later independent
  // manual read is still the command's unresolved follow-up.
  cardFails = true;
  fireEvent.click(await findButton("Обновить состояние"));
  await screen.findByText("aggregate-ui", { exact: true });
  expect(screen.getAllByText("aggregate-ui", { exact: true })).toHaveLength(1);
  expect(
    screen.queryByText(
      "Команда принята; чтение карточки не удалось. Повторите только чтение.",
    ),
  ).toBeNull();
  expect(context.attempts).toHaveLength(2);
});

it("invalidates an A confirmation on navigation and restores an unknown A intent without assigning it to B", async () => {
  const context = setup();
  context.mount();
  await ready();
  await openPause();
  fireEvent.click(screen.getByRole("link", { name: "Открыть Б" }));
  await screen.findByText("Агрегат Б");
  await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
  expect(context.attempts).toHaveLength(0);
  fireEvent.click(screen.getByRole("button", { name: "Назад" }));
  await screen.findByText("Агрегат А");
  context.intercept((path) => {
    if (path.endsWith("/pause")) throw new TypeError("network");
    return undefined;
  });
  const dialog = await openPause();
  fireEvent.click(within(dialog).getByRole("button", { name: "Пауза" }));
  await findButton("Повторить команду с тем же ключом");
  const original = context.attempts[0];
  fireEvent.click(screen.getByRole("link", { name: "Открыть Б" }));
  await screen.findByText("Агрегат Б");
  expect(
    screen.queryByText("Повторить команду с тем же ключом", { exact: true }),
  ).toBeNull();
  fireEvent.click(screen.getByRole("button", { name: "Назад" }));
  await screen.findByText("Агрегат А");
  fireEvent.click(await findButton("Повторить команду с тем же ключом"));
  await waitFor(() => expect(context.attempts).toHaveLength(2));
  expect(context.attempts[1]).toEqual(original);
});

it("requires an exact valid N and fresh all-detector protection, displaying both blocked ranges without submitting", async () => {
  const context = setup();
  context.mount();
  await ready();
  const field = screen.getByLabelText("Оставить последние N точек");
  for (const value of ["", "-1", "1.5", "1e3", "true", "9223372036854775808"]) {
    fireEvent.change(field, { target: { value } });
    expect(
      (button("Проверить защиту истории", cleanupPanel()) as HTMLButtonElement)
        .disabled,
    ).toBe(true);
  }
  fireEvent.change(field, { target: { value: "1" } });
  context.setPlan(
    protectionFixture({
      requested_keep: 1,
      minimum_keep: 3,
      maximum_delete: 7,
      can_cleanup: false,
      reason: "detector_not_stopped",
      blockers: [
        {
          detector_id: aggregateId,
          name: "Модель А",
          reason: "detector_not_stopped",
          minimum_keep: 1,
          protected_start_index: null,
          protected_end_index: null,
        },
        {
          detector_id: otherId,
          name: "Модель Б",
          reason: "protected_history",
          minimum_keep: 3,
          protected_start_index: 7,
          protected_end_index: 9,
        },
      ],
    }),
  );
  await prepareCleanup();
  const plan = screen.getByLabelText("План защиты истории");
  expect(within(plan).getByText(/Модель А/)).toBeTruthy();
  expect(within(plan).getByText(/Модель Б/)).toBeTruthy();
  expect(
    (button("Очистить историю", cleanupPanel()) as HTMLButtonElement).disabled,
  ).toBe(true);
  expect(context.attempts).toHaveLength(0);
  fireEvent.change(field, { target: { value: "9223372036854775807" } });
  expect(screen.queryByLabelText("План защиты истории")).toBeNull();
  context.setPlan(null);
  await prepareCleanup();
  expect(
    context.request.mock.calls.some(([path]) =>
      String(path).includes("keep_last=9223372036854775807"),
    ),
  ).toBe(true);
});

it("cancels cleanup without POST and cannot submit a confirmation whose N or protection has changed", async () => {
  const context = setup();
  context.mount();
  await ready();
  await prepareCleanup();
  fireEvent.click(button("Очистить историю", cleanupPanel()));
  let dialog = await screen.findByRole("dialog", { name: "Очистить историю?" });
  expect(context.attempts).toHaveLength(0);
  fireEvent.click(within(dialog).getByRole("button", { name: "Отмена" }));
  expect(context.attempts).toHaveLength(0);
  fireEvent.click(button("Очистить историю", cleanupPanel()));
  dialog = await screen.findByRole("dialog", { name: "Очистить историю?" });
  fireEvent.change(screen.getByLabelText("Оставить последние N точек"), {
    target: { value: "1" },
  });
  await waitFor(() =>
    expect(
      screen.queryByRole("dialog", { name: "Очистить историю?" }),
    ).toBeNull(),
  );
  expect(context.attempts).toHaveLength(0);
  expect(screen.queryByLabelText("План защиты истории")).toBeNull();
});

it("treats 202 as acceptance, reads cleanup_operation_id, updates rows only at succeeded and resumes observation after navigation", async () => {
  const context = setup();
  context.mount();
  await ready();
  fireEvent.click(screen.getByLabelText(`Выбрать точку ${pointFixture().id}`));
  await screen.findByLabelText("Подробности выбранной точки");
  await prepareCleanup();
  await confirmCleanup();
  await screen.findByText(new RegExp("Ожидает выполнения.*" + cleanupId));
  expect(screen.queryByText(/Очистка завершена\. Список/)).toBeNull();
  expect(context.operationReads).toEqual([cleanupId]);
  expect(context.attempts[0].body).toBe('{"keep_last":0}');
  fireEvent.click(screen.getByRole("link", { name: "Уйти" }));
  await screen.findByText("Другая страница");
  fireEvent.click(screen.getByRole("button", { name: "Назад" }));
  await screen.findByText(new RegExp("Ожидает выполнения.*" + cleanupId));
  expect(context.attempts).toHaveLength(1);
  context.setOperation(
    cleanupFixture({
      status: "succeeded",
      deleted_points: 10,
      completed_at: aggregateTime,
    }),
  );
  context.clearPoints();
  context.cards.set(aggregateId, aggregateFixture({ name: "Агрегат А" }));
  fireEvent.click(button("Обновить состояние очистки", cleanupPanel()));
  await screen.findByText(/Очистка завершена\. Список/);
  await screen.findByText(
    "На загруженной странице пока нет сохранённых точек.",
  );
  expect(screen.queryByLabelText("Подробности выбранной точки")).toBeNull();
  expect(
    screen.queryByLabelText(`Выбрать точку ${pointFixture().id}`),
  ).toBeNull();
  fireEvent.click(screen.getByLabelText("Обновить точки"));
  await waitFor(() => expect(context.attempts).toHaveLength(1));
});

it("requires a new explicit failed cleanup retry key and preserves that retry after an unknown response", async () => {
  const context = setup();
  context.setOperation(
    cleanupFixture({ status: "failed", safe_error: "temporary_unavailable" }),
  );
  context.mount();
  await ready();
  await prepareCleanup();
  await confirmCleanup();
  const retry = await findButton("Продолжить очистку", cleanupPanel());
  const originalKey = context.attempts[0].key;
  let retries = 0;
  context.intercept((path) => {
    if (path.endsWith("/retry") && ++retries === 1)
      throw new TypeError("network");
    return undefined;
  });
  fireEvent.click(retry);
  const replay = await findButton("Повторить команду с тем же ключом");
  expect(context.attempts[1].key).not.toBe(originalKey);
  expect(context.attempts[1].path).toContain(cleanupId);
  fireEvent.click(replay);
  await waitFor(() => expect(context.attempts).toHaveLength(3));
  expect(context.attempts[2]).toEqual(context.attempts[1]);
});

it("reads retired results with their original basis and offers copy/cleanup without execution actions", async () => {
  const context = setup();
  context.cards.set(
    aggregateId,
    aggregateFixture({
      name: "Прежний агрегат",
      semantic_basis: "availability_v1",
      execution_mode: "retired",
      execution_reason: "aggregate_time_semantics_retired",
      online_start_index: null,
      bootstrap_at: null,
    }),
  );
  context.intercept((path) =>
    path.startsWith(`/aggregates/${aggregateId}/points?`)
      ? json({
          items: [
            pointFixture({
              origin: "legacy",
              time_basis: "availability_at",
              window_index: null,
            }),
          ],
          total: 1,
        })
      : undefined,
  );
  context.mount();
  await screen.findByText("Прежний агрегат");
  await screen.findByLabelText("Таблица точек");
  expect(screen.queryByLabelText("Пауза")).toBeNull();
  expect(screen.queryByLabelText("Продолжить")).toBeNull();
  expect(screen.queryByLabelText("Архивировать")).toBeNull();
  expect(screen.queryByLabelText("Возобновить")).toBeNull();
  expect(screen.queryByLabelText("Просмотреть сохранённое окно")).toBeNull();
  expect(button("Проверить защиту истории", cleanupPanel())).toBeTruthy();
  expect(
    screen.getAllByText(/прежнее время доступности|Прежнее время доступности/)
      .length,
  ).toBeGreaterThan(0);
  const copy = screen.getByRole("link", { name: "Копировать" });
  expect(copy.getAttribute("href")).toBe(`/aggregates/new?copy=${aggregateId}`);
  fireEvent.click(copy);
  await screen.findByText("Новый черновик копии");
  expect(context.attempts).toHaveLength(0);
});

function lifecycleText() {
  return screen.getByText("Состояние", { selector: "dt" }).nextElementSibling
    ?.textContent;
}

it("keeps archived lifecycle and its intentional skip separate after archiving a paused aggregate", async () => {
  const context = setup();
  context.cards.set(
    aggregateId,
    aggregateFixture({ name: "Агрегат А", paused: true }),
  );
  context.intercept((path, options) => {
    if (
      path === `/aggregates/${aggregateId}/archive` &&
      options.method === "POST"
    ) {
      const archived = aggregateFixture({
        name: "Агрегат А",
        state: "archived",
        paused: true,
        archived_at: aggregateTime,
      });
      context.cards.set(aggregateId, archived);
      return json(archived);
    }
    return undefined;
  });
  context.mount();
  await screen.findByLabelText("Продолжить");
  expect(lifecycleText()).toMatch(/Активен.*Пауза/);
  expect(
    screen.getByText(
      /После продолжения доступные накопившиеся окна восполняются/,
    ),
  ).toBeTruthy();
  fireEvent.click(screen.getByLabelText("Архивировать"));
  const dialog = await screen.findByRole("dialog", {
    name: "Архивировать агрегат?",
  });
  fireEvent.click(button("Архивировать", dialog));
  await screen.findByLabelText("Возобновить");
  expect(lifecycleText()).toMatch(/В архиве.*Пауза/);
  expect(screen.queryByLabelText("Продолжить")).toBeNull();
  expect(screen.queryByLabelText("Пауза")).toBeNull();
  expect(
    screen.queryByText(
      /После продолжения доступные накопившиеся окна восполняются/,
    ),
  ).toBeNull();
  expect(
    screen.getByText(/окна намеренно пропускаются и не восполняются/),
  ).toBeTruthy();
  expect(context.attempts.map(({ path }) => path)).toEqual([
    `/aggregates/${aggregateId}/archive`,
  ]);
});

it.each(["current", "retired"] as const)(
  "keeps deleted lifecycle visible with paused %s and only offers the supported retired copy",
  async (mode) => {
    const context = setup();
    context.cards.set(
      aggregateId,
      aggregateFixture({
        name: "Удалённый агрегат",
        state: "deleted",
        paused: true,
        deleted_at: aggregateTime,
        deleted_by: "00000000-0000-4000-8000-000000000003",
        ...(mode === "retired"
          ? {
              semantic_basis: "availability_v1",
              execution_mode: "retired",
              execution_reason: "aggregate_time_semantics_retired",
              bootstrap_at: null,
              online_start_index: null,
            }
          : {}),
      }),
    );
    context.mount();
    await screen.findByText("Удалённый агрегат");
    await screen.findByLabelText("Таблица точек");
    expect(lifecycleText()).toMatch(/Удалён.*Пауза/);
    if (mode === "retired")
      expect(lifecycleText()).toContain("Прежний расчёт остановлен");
    for (const name of [
      "Архивировать",
      "Возобновить",
      "Пауза",
      "Продолжить",
      "Удалить агрегат",
    ])
      expect(screen.queryByRole("button", { name })).toBeNull();
    expect(
      screen.queryByText(
        /После продолжения доступные накопившиеся окна восполняются/,
      ),
    ).toBeNull();
    const copy = screen.queryByRole("link", { name: "Копировать" });
    if (mode === "retired") {
      expect(copy?.getAttribute("href")).toBe(
        `/aggregates/new?copy=${aggregateId}`,
      );
      fireEvent.click(copy!);
      await screen.findByText("Новый черновик копии");
    } else expect(copy).toBeNull();
    expect(context.attempts).toHaveLength(0);
  },
);

it("removes the completed cleanup result while loading a different active cleanup", async () => {
  const context = setup();
  context.cards.set(
    aggregateId,
    aggregateFixture({ active_cleanup_id: cleanupId }),
  );
  context.setOperation(
    cleanupFixture({
      status: "succeeded",
      deleted_points: 10,
      completed_at: aggregateTime,
    }),
  );
  context.mount();
  await screen.findByText(/Очистка завершена\. Список/);
  await findButton("Обновить состояние");
  const secondId = "00000000-0000-4000-8000-000000000044";
  let resolve!: (value: Response) => void;
  context.intercept((path) =>
    path === `/aggregates/${aggregateId}/cleanup-operations/${secondId}`
      ? new Promise<Response>((done) => {
          resolve = done;
        })
      : undefined,
  );
  context.cards.set(
    aggregateId,
    aggregateFixture({ active_cleanup_id: secondId }),
  );
  fireEvent.click(screen.getByLabelText("Обновить состояние"));
  await waitFor(() => expect(resolve).toBeTypeOf("function"));
  const progress = screen.getByLabelText("Состояние очистки");
  expect(within(progress).queryByText(/Очистка завершена\. Список/)).toBeNull();
  expect(progress.textContent).not.toContain(cleanupId);
  resolve(json(cleanupFixture({ id: secondId, status: "running" })));
  await within(progress).findByText(new RegExp(`Выполняется.*${secondId}`));
  expect(context.attempts).toHaveLength(0);
});

it.each([
  [500, "internal_error"],
  [503, "aggregate_database_error"],
] as const)(
  "shows one failed manual read after an unknown %s %s deletion, keeping the mutation fenced",
  async (status, code) => {
    const context = setup();
    let cardReads = 0;
    let manualReads = 0;
    let manualResponse: Response | undefined;
    context.intercept((path, options) => {
      if (path !== `/aggregates/${aggregateId}`) return undefined;
      if (options.method === "DELETE")
        return json(
          {
            code,
            message: "private traceback",
            request_id: "mutation-request",
            details: null,
          },
          status,
        );
      cardReads++;
      if (!manualResponse) return undefined;
      manualReads++;
      const response = manualResponse;
      manualResponse = undefined;
      return response;
    });
    const view = context.mount();
    const page = within(view.container);
    await ready();
    // Exercise a genuine background card read before the manual sequence.
    // It must receive the normal card even if it becomes the second GET.
    const initialReads = cardReads;
    const realNow = Date.now.bind(Date);
    const shiftedClock = vi
      .spyOn(Date, "now")
      .mockImplementation(() => realNow() + 6000);
    try {
      fireEvent(document, new Event("visibilitychange"));
      await waitFor(() => expect(cardReads).toBeGreaterThan(initialReads));
    } finally {
      shiftedClock.mockRestore();
    }
    const remove = screen.getByRole("button", {
      name: "Удалить агрегат",
    }) as HTMLButtonElement;
    fireEvent.click(remove);
    const dialog = await screen.findByRole("dialog", {
      name: "Удалить агрегат?",
    });
    fireEvent.click(within(dialog).getByRole("button", { name: "Удалить" }));
    await page.findByText(/Результат изменения неизвестен/);
    await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
    const refresh = screen.getByLabelText("Обновить состояние");
    const readManually = async (response: Response) => {
      await waitFor(() =>
        expect((refresh as HTMLButtonElement).disabled).toBe(false),
      );
      const readsBefore = manualReads;
      manualResponse = response;
      try {
        fireEvent.click(refresh);
        expect(manualReads).toBe(readsBefore + 1);
      } finally {
        manualResponse = undefined;
      }
    };
    const failures = [
      [
        "read-request",
        json(
          {
            code: "aggregate_database_error",
            message: "private traceback",
            request_id: "read-request",
            details: null,
          },
          503,
        ),
      ],
      [
        "empty-read-3",
        new Response(null, {
          status: 200,
          headers: { "X-Request-ID": "empty-read-3" },
        }),
      ],
      [
        "empty-read-4",
        new Response(null, {
          status: 204,
          headers: { "X-Request-ID": "empty-read-4" },
        }),
      ],
    ] as const;
    for (const [id, response] of failures) {
      await readManually(response);
      await page.findByText(id, { exact: true });
      expect(page.getAllByText(id, { exact: true })).toHaveLength(1);
      expect(
        page.queryByText(/Не удалось обновить состояние хранения/),
      ).toBeNull();
      expect(page.getByText(/Результат изменения неизвестен/)).toBeTruthy();
      expect(remove.disabled).toBe(true);
      expect(context.attempts).toHaveLength(1);
    }
    expect(page.queryByText("private traceback")).toBeNull();
    await readManually(
      json({ ...context.cards.get(aggregateId)!, state: "deleted" }),
    );
    await waitFor(() =>
      expect(page.queryByText(/Результат изменения неизвестен/)).toBeNull(),
    );
    expect(
      screen.queryByRole("button", { name: "Удалить агрегат" }),
    ).toBeNull();
    expect(context.attempts).toHaveLength(1);
    expect(manualReads).toBe(4);
  },
);
