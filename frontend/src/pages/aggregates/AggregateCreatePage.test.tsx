import {
  render,
  screen,
  fireEvent,
  waitFor,
  cleanup,
} from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { MemoryRouter } from "react-router-dom";
import { AppStores } from "../../app/stores/AppStores";
import { StoresProvider } from "../../app/providers/StoresProvider";
import { ApiError } from "../../app/api/client";
import { AggregateCreatePage } from "./AggregateCreatePage";

afterEach(cleanup);
it("exposes an enabled explicit retry after an unknown create result without changing body or key", async () => {
  const stores = new AppStores();
  stores.session.data = {
    authenticated: true,
    user: { id: "u", username: "u", email: null, display_name: "U" },
    role: "admin",
    permissions: ["aggregates.read", "aggregates.write"],
    authentication_method: "local",
    csrf_token: "csrf",
  };
  stores.session.state = "authenticated";
  const hash = "a".repeat(64);
  const moment = "2026-01-01T00:00:00Z";
  const attempts: { body: unknown; key: unknown }[] = [];
  let releaseValidation!: () => void;
  const validationGate = new Promise<void>((resolve) => {
    releaseValidation = resolve;
  });
  const request = vi.fn(
    async (
      path: string,
      options?: { body?: unknown; headers?: Record<string, string> },
    ) => {
      if (path.startsWith("/sources?"))
        return {
          items: [
            {
              id: "s",
              name: "Source",
              source_type: "kafka",
              is_archived: false,
            },
          ],
          total: 1,
        };
      if (path.endsWith("/operators"))
        return [
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
            result_nullable: true,
            result: "number",
          },
        ];
      if (path.endsWith("/validate")) {
        await validationGate;
        return {
          rule_hash: hash,
          anchor_utc: moment,
          duration_seconds: 300,
          feature_keys: ["events"],
          budget: {},
        };
      }
      if (path.endsWith("/preview"))
        return {
          rule_hash: hash,
          start_utc: moment,
          end_utc: moment,
          scheduled_at: moment,
          observed_rows: 0,
          event_count: 0,
          event_time_min: null,
          event_time_max: null,
          features: [],
          completeness: "unverified",
          completeness_reason: "preview_snapshot_without_coverage_proof",
          time_basis: "availability_at",
          budget: {},
        };
      if (path === "/aggregates") {
        attempts.push({
          body: options?.body,
          key: options?.headers?.["Idempotency-Key"],
        });
        throw new ApiError(0, "network_error");
      }
      throw new Error(path);
    },
  );
  stores.api.request = request as typeof stores.api.request;
  render(
    <StoresProvider stores={stores}>
      <MemoryRouter initialEntries={["/aggregates/new"]}>
        <AggregateCreatePage />
      </MemoryRouter>
    </StoresProvider>,
  );
  fireEvent.change(screen.getByLabelText("Имя агрегата", { exact: true }), {
    target: { value: "Тест" },
  });
  fireEvent.mouseDown(
    screen.getByRole("combobox", { name: "Собственный источник" }),
  );
  fireEvent.click(await screen.findByText("Source"));
  fireEvent.change(screen.getByLabelText("Исходная граница", { exact: true }), {
    target: { value: moment },
  });
  fireEvent.change(
    screen.getByLabelText("Начало preview-окна", { exact: true }),
    { target: { value: moment } },
  );
  fireEvent.click(screen.getByRole("button", { name: "Проверить правило" }));
  await screen.findByRole("img", { name: "loading" });
  expect(
    (
      screen.getByRole("button", {
        name: "Проверить правило",
      }) as HTMLButtonElement
    ).disabled,
  ).toBe(true);
  releaseValidation();
  await screen.findByText("Правило и расписание проверены сервером.");
  fireEvent.click(
    screen.getByRole("button", { name: "Предварительный расчёт" }),
  );
  await screen.findByRole("region", { name: "Предварительный результат" });
  fireEvent.click(screen.getByRole("button", { name: "Создать агрегат" }));
  const retry = await screen.findByRole("button", {
    name: "Повторить создание с тем же ключом",
  });
  await waitFor(() =>
    expect((retry as HTMLButtonElement).disabled).toBe(false),
  );
  fireEvent.click(retry);
  await waitFor(() => expect(attempts).toHaveLength(2));
  expect(attempts[1]).toEqual(attempts[0]);
});
