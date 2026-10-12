import { afterEach, expect, it, vi } from "vitest";
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import type { NormalizersApi, RuleV1 } from "../../app/api/normalizers";
import { ApiError } from "../../app/api/client";
import { SourceDebugPanel } from "./SourceDebugPanel";

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
  vi.useRealTimers();
});

it("checks a selected Kafka record using its original timestamp", async () => {
  const preview = vi.fn().mockResolvedValue({
    status: "complete",
    variant_key: "v",
    ecs_data: { message: "sample" },
    fluent_bit_collected_at: "2024-01-01T00:00:00Z",
    diagnostics: [],
    trace: [],
  });
  const service = {
    kafkaSources: vi.fn().mockResolvedValue({
      items: [
        {
          id: "source-id",
          name: "Synthetic Kafka",
          source_type: "kafka",
          is_archived: false,
        },
      ],
      total: 1,
    }),
    sourceSnapshot: vi.fn().mockResolvedValue({
      records: [
        ...[0, 1].map((position) => ({
          position,
          timestamp: "2024-01-01T00:00:00Z",
          log:
            position === 0 ? "first distinctive log" : "second distinctive log",
          issue: null,
        })),
        {
          position: 2,
          timestamp: null,
          log: null,
          issue: "invalid_envelope" as const,
        },
      ],
      skipped: 0,
      truncated: false,
    }),
    preview,
  } as unknown as NormalizersApi;
  const rule: RuleV1 = { format_version: 1, variants: [] };
  render(
    <SourceDebugPanel
      service={service}
      currentRule={() => rule}
      onSessionChange={() => {}}
    />,
  );
  fireEvent.mouseDown(screen.getByRole("combobox", { name: "Kafka-источник" }));
  fireEvent.click(await screen.findByText("Synthetic Kafka"));
  await screen.findByText(/Доступно записей: 2/);
  fireEvent.mouseDown(screen.getByRole("combobox", { name: "Запись снимка" }));
  const invalidOption = await screen.findByText("Запись 3: неверный формат");
  fireEvent.click(invalidOption);
  expect(screen.getByText("first distinctive log")).toBeTruthy();
  expect(preview).not.toHaveBeenCalled();
  fireEvent.mouseDown(screen.getByRole("combobox", { name: "Запись снимка" }));
  fireEvent.click(await screen.findByText(/Запись 2: second distinctive log/));
  expect(screen.getByText("second distinctive log")).toBeTruthy();
  expect(preview).not.toHaveBeenCalled();
  fireEvent.mouseDown(screen.getByRole("combobox", { name: "Запись снимка" }));
  fireEvent.click(await screen.findByText(/Запись 1: first distinctive log/));
  fireEvent.click(screen.getByRole("button", { name: "Тест" }));
  await waitFor(() => expect(preview).toHaveBeenCalledOnce());
  expect(preview).toHaveBeenCalledWith(rule, {
    timestamp: "2024-01-01T00:00:00Z",
    log: "first distinctive log",
  });
  await screen.findByText("Проверено: 1");
  fireEvent.click(screen.getByRole("button", { name: "Обновить снимок" }));
  expect(
    screen.getByRole("dialog", { name: "Сбросить результаты отладки?" }),
  ).toBeTruthy();
  fireEvent.click(screen.getByRole("button", { name: "Отмена" }));
  expect(service.sourceSnapshot).toHaveBeenCalledTimes(1);
  expect(screen.getByText("Проверено: 1")).toBeTruthy();
  fireEvent.click(screen.getByRole("button", { name: "Обновить снимок" }));
  fireEvent.click(screen.getByRole("button", { name: "Продолжить" }));
  await waitFor(() => expect(service.sourceSnapshot).toHaveBeenCalledTimes(2));
});

it("clears an old preview error only after confirming a new run", async () => {
  const preview = vi
    .fn()
    .mockRejectedValueOnce(new ApiError(503, "unavailable"))
    .mockResolvedValueOnce({
      status: "complete",
      variant_key: "v",
      ecs_data: { message: "ok" },
      fluent_bit_collected_at: null,
      diagnostics: [],
      trace: [],
    });
  const service = {
    kafkaSources: vi.fn().mockResolvedValue({
      items: [
        {
          id: "source-id",
          name: "Synthetic Kafka",
          source_type: "kafka",
          is_archived: false,
        },
      ],
      total: 1,
    }),
    sourceSnapshot: vi.fn().mockResolvedValue({
      records: [
        {
          position: 0,
          timestamp: "2024-01-01T00:00:00Z",
          log: "sample",
          issue: null,
        },
      ],
      skipped: 0,
      truncated: false,
    }),
    preview,
  } as unknown as NormalizersApi;
  render(
    <SourceDebugPanel
      service={service}
      currentRule={() => ({ format_version: 1, variants: [] })}
      onSessionChange={() => {}}
    />,
  );
  fireEvent.mouseDown(screen.getByRole("combobox", { name: "Kafka-источник" }));
  fireEvent.click(await screen.findByText("Synthetic Kafka"));
  await screen.findByText(/Доступно записей: 1/);
  fireEvent.click(screen.getByRole("button", { name: "Тест" }));
  await screen.findByText("Сервер временно недоступен.");
  fireEvent.click(screen.getByRole("button", { name: "Тест" }));
  fireEvent.click(screen.getByRole("button", { name: "Отмена" }));
  expect(screen.getByText("Сервер временно недоступен.")).toBeTruthy();
  expect(preview).toHaveBeenCalledTimes(1);
  fireEvent.click(screen.getByRole("button", { name: "Тест" }));
  fireEvent.click(screen.getByRole("button", { name: "Продолжить" }));
  await screen.findByText("Проверено: 1");
  await waitFor(() => expect(preview).toHaveBeenCalledTimes(2));
  expect(screen.queryByText("Сервер временно недоступен.")).toBeNull();
});

it("holds the draft and waits five seconds between automatic checks", async () => {
  const preview = vi.fn().mockResolvedValue({
    status: "complete",
    variant_key: "v",
    ecs_data: { message: "normalized" },
    fluent_bit_collected_at: null,
    diagnostics: [],
    trace: [],
  });
  const service = {
    kafkaSources: vi.fn().mockResolvedValue({
      items: [
        {
          id: "source-id",
          name: "Synthetic Kafka",
          source_type: "kafka",
          is_archived: false,
        },
      ],
      total: 1,
    }),
    sourceSnapshot: vi.fn().mockResolvedValue({
      records: [0, 1, 2, 3].map((position) => ({
        position,
        timestamp: "2024-01-01T00:00:00Z",
        log: `sample ${position}`,
        issue: null,
      })),
      skipped: 0,
      truncated: false,
    }),
    preview,
  } as unknown as NormalizersApi;
  let rule: RuleV1 = { format_version: 1, variants: [] };
  render(
    <SourceDebugPanel
      service={service}
      currentRule={() => rule}
      onSessionChange={() => {}}
    />,
  );
  fireEvent.mouseDown(screen.getByRole("combobox", { name: "Kafka-источник" }));
  fireEvent.click(await screen.findByText("Synthetic Kafka"));
  await screen.findByText(/Доступно записей: 4/);
  vi.useFakeTimers();
  fireEvent.click(screen.getByRole("button", { name: "Авто" }));
  await act(async () => {
    await Promise.resolve();
  });
  expect(preview).toHaveBeenCalledTimes(1);
  expect(screen.getByText("Проверено: 1")).toBeTruthy();
  fireEvent.click(screen.getByRole("button", { name: "Запись 1" }));
  expect(screen.getByText("ECS записи 1")).toBeTruthy();
  rule = {
    format_version: 1,
    variants: [
      {
        key: "later",
        priority: 1,
        when: { kind: "contains", value: "x" },
        blocks: [],
      },
    ],
  };
  await act(async () => {
    await vi.advanceTimersByTimeAsync(4999);
  });
  expect(preview).toHaveBeenCalledTimes(1);
  await act(async () => {
    await vi.advanceTimersByTimeAsync(1);
  });
  expect(preview).toHaveBeenCalledTimes(2);
  expect(preview.mock.calls[1][0]).toEqual({ format_version: 1, variants: [] });
  expect(screen.getByText("ECS записи 1")).toBeTruthy();
  fireEvent.click(screen.getByRole("button", { name: "Пауза" }));
  await act(async () => {
    await vi.advanceTimersByTimeAsync(10000);
  });
  expect(preview).toHaveBeenCalledTimes(2);
  expect(screen.getByText("Проверено: 2")).toBeTruthy();
  fireEvent.mouseDown(screen.getByRole("combobox", { name: "Запись снимка" }));
  fireEvent.click(screen.getByText("Запись 4: sample 3"));
  expect(screen.getByText("sample 1")).toBeTruthy();
  expect(screen.getByText("ECS записи 1")).toBeTruthy();
  fireEvent.click(screen.getByRole("button", { name: "Продолжить" }));
  await act(async () => {
    await vi.advanceTimersByTimeAsync(5000);
  });
  expect(preview).toHaveBeenCalledTimes(3);
  expect(screen.getByText("sample 2")).toBeTruthy();
  fireEvent.click(screen.getByRole("button", { name: "Остановить" }));
  expect(screen.getByText("Проверено: 3")).toBeTruthy();
});

it("finishes at the end of the snapshot and paginates accumulated failures", async () => {
  const preview = vi.fn().mockImplementation(async (_rule, sample) => ({
    status: Number(sample.log.slice(6)) % 2 === 0 ? "complete" : "failed",
    variant_key: null,
    ecs_data: null,
    fluent_bit_collected_at: null,
    diagnostics: [],
    trace: [],
  }));
  const service = {
    kafkaSources: vi.fn().mockResolvedValue({
      items: [
        {
          id: "source-id",
          name: "Synthetic Kafka",
          source_type: "kafka",
          is_archived: false,
        },
      ],
      total: 1,
    }),
    sourceSnapshot: vi.fn().mockResolvedValue({
      records: Array.from({ length: 12 }, (_, position) => ({
        position,
        timestamp: "2024-01-01T00:00:00Z",
        log: `event ${position}`,
        issue: null,
      })),
      skipped: 0,
      truncated: false,
    }),
    preview,
  } as unknown as NormalizersApi;
  const view = render(
    <SourceDebugPanel
      service={service}
      currentRule={() => ({ format_version: 1, variants: [] })}
      onSessionChange={() => {}}
    />,
  );
  fireEvent.mouseDown(screen.getByRole("combobox", { name: "Kafka-источник" }));
  fireEvent.click(await screen.findByText("Synthetic Kafka"));
  await screen.findByText(/Доступно записей: 12/);
  vi.useFakeTimers();
  fireEvent.click(screen.getByRole("button", { name: "Авто" }));
  for (let index = 1; index < 12; index++) {
    await act(async () => {
      await vi.advanceTimersByTimeAsync(5000);
    });
  }
  expect(preview).toHaveBeenCalledTimes(12);
  expect(screen.getByText("Проверено: 12")).toBeTruthy();
  expect(screen.queryByRole("button", { name: "Пауза" })).toBeNull();
  const nextPages = view.container.querySelectorAll<HTMLButtonElement>(
    ".ant-pagination-next button",
  );
  expect(nextPages).toHaveLength(2);
  expect(nextPages[0].disabled).toBe(false);
  expect(nextPages[1].disabled).toBe(false);
  fireEvent.click(nextPages[0]);
  expect(screen.getByRole("button", { name: "Запись 11" })).toBeTruthy();
  fireEvent.click(nextPages[1]);
  expect(screen.getByRole("button", { name: "Запись 12" })).toBeTruthy();
}, 15000);
