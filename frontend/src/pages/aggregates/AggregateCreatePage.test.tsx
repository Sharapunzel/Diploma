import {
  render,
  screen,
  fireEvent,
  waitFor,
  cleanup,
  within,
} from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { runInAction } from "mobx";
import { MemoryRouter } from "react-router-dom";
import { AppStores } from "../../app/stores/AppStores";
import { StoresProvider } from "../../app/providers/StoresProvider";
import { ApiError } from "../../app/api/client";
import { AggregateCreatePage } from "./AggregateCreatePage";

afterEach(cleanup);
it("exposes an enabled explicit retry after an unknown create result without changing body or key", async () => {
  const stores = new AppStores();
  runInAction(() => {
    stores.session.data = {
      authenticated: true,
      user: { id: "u", username: "u", email: null, display_name: "U" },
      role: "admin",
      permissions: ["aggregates.read", "aggregates.write"],
      authentication_method: "local",
      csrf_token: "csrf",
    };
    stores.session.state = "authenticated";
  });
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
      if (path.includes("/operators?include_extended=true"))
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
          budget: { estimated_rows: 11 },
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
          time_basis: "event_time",
          origin: "preview",
          in_process: true,
          snapshot_reference_at: moment,
          close_not_before_at: moment,
          snapshot_metadata: { delivery_coverage: "not_attested" },
          budget: { estimated_rows: 22 },
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
  runInAction(() => {
    stores.api.request = request as typeof stores.api.request;
  });
  render(
    <StoresProvider stores={stores}>
      <MemoryRouter initialEntries={["/aggregates/new"]}>
        <AggregateCreatePage />
      </MemoryRouter>
    </StoresProvider>,
  );
  const process = within(screen.getByLabelText("Параметры процесса"));
  const diagnosticsBlock = screen.getByLabelText("Проверка и диагностика");
  const ruleBlock = screen.getByLabelText("Выходной вектор и полное правило");
  const diagnostics = within(diagnosticsBlock);
  expect((ruleBlock as HTMLFieldSetElement).disabled).toBe(true);
  expect((diagnosticsBlock as HTMLFieldSetElement).disabled).toBe(true);
  expect(ruleBlock.hasAttribute("inert")).toBe(true);
  expect(
    process
      .getByLabelText("Имя агрегата", { exact: true })
      .getAttribute("aria-required"),
  ).toBe("true");
  expect(
    process
      .getByLabelText("Начало истории и сетка окон", { exact: true })
      .getAttribute("aria-required"),
  ).toBe("true");
  const validate = diagnostics.getByRole("button", {
    name: "Проверить правило",
  });
  const preview = diagnostics.getByRole("button", {
    name: "Предварительный расчёт",
  });
  const create = diagnostics.getByRole("button", { name: "Создать агрегат" });
  fireEvent.change(process.getByLabelText("Имя агрегата", { exact: true }), {
    target: { value: "Тест" },
  });
  fireEvent.mouseDown(
    process.getByRole("combobox", { name: "Собственный источник" }),
  );
  fireEvent.click(await screen.findByText(/Source ·/));
  fireEvent.change(
    process.getByLabelText("Начало истории и сетка окон", { exact: true }),
    {
      target: { value: moment.slice(0, 19) },
    },
  );
  expect((ruleBlock as HTMLFieldSetElement).disabled).toBe(false);
  expect((diagnosticsBlock as HTMLFieldSetElement).disabled).toBe(false);
  fireEvent.click(within(ruleBlock).getByRole("radio", { name: "JSON" }));
  const ruleText = within(ruleBlock).getByLabelText(
    "Полное правило JSON",
  ) as HTMLTextAreaElement;
  const editedRule =
    '{"features":[{"key":"custom_events","name":"Количество","op":"count"}]}';
  fireEvent.change(ruleText, { target: { value: editedRule } });
  fireEvent.change(process.getByLabelText("Имя агрегата", { exact: true }), {
    target: { value: "" },
  });
  expect((ruleBlock as HTMLFieldSetElement).disabled).toBe(true);
  expect((diagnosticsBlock as HTMLFieldSetElement).disabled).toBe(true);
  expect(ruleText.value).toBe(editedRule);
  fireEvent.change(process.getByLabelText("Имя агрегата", { exact: true }), {
    target: { value: "Тест" },
  });
  expect((ruleBlock as HTMLFieldSetElement).disabled).toBe(false);
  expect(ruleText.value).toBe(editedRule);
  expect((preview as HTMLButtonElement).disabled).toBe(true);
  fireEvent.change(
    diagnostics.getByLabelText("Начало окна для расчёта", { exact: true }),
    { target: { value: moment.slice(0, 19) } },
  );
  fireEvent.click(validate);
  await within(validate).findByRole("img", { name: "loading" });
  expect((validate as HTMLButtonElement).disabled).toBe(true);
  releaseValidation();
  await diagnostics.findByText("Правило и расписание проверены сервером.");
  expect(
    diagnostics.getAllByRole("list", { name: "Ресурсы расчёта" }),
  ).toHaveLength(1);
  expect(diagnostics.getByRole("listitem").textContent).toBe(
    "Записей в окне: 11",
  );
  fireEvent.click(preview);
  await diagnostics.findByRole("region", { name: "Предварительный результат" });
  expect(
    diagnostics.getAllByRole("list", { name: "Ресурсы расчёта" }),
  ).toHaveLength(1);
  expect(diagnostics.getByRole("listitem").textContent).toBe(
    "Записей в окне: 22",
  );
  const zone = process.getByLabelText("Пояс отображения", { exact: true });
  fireEvent.change(zone, { target: { value: "Missing/Zone" } });
  expect((ruleBlock as HTMLFieldSetElement).disabled).toBe(true);
  expect((diagnosticsBlock as HTMLFieldSetElement).disabled).toBe(true);
  fireEvent.change(zone, { target: { value: "UTC" } });
  expect((diagnosticsBlock as HTMLFieldSetElement).disabled).toBe(false);
  expect(diagnostics.getByRole("listitem").textContent).toBe(
    "Записей в окне: 22",
  );
  expect(
    request.mock.calls.filter(([path]) => path.endsWith("/preview")),
  ).toHaveLength(1);
  fireEvent.click(create);
  const retry = await screen.findByRole("button", {
    name: "Повторить создание с тем же ключом",
  });
  await waitFor(() =>
    expect((retry as HTMLButtonElement).disabled).toBe(false),
  );
  fireEvent.change(process.getByLabelText("Имя агрегата", { exact: true }), {
    target: { value: "" },
  });
  expect((diagnosticsBlock as HTMLFieldSetElement).disabled).toBe(true);
  expect((retry as HTMLButtonElement).disabled).toBe(false);
  fireEvent.click(retry);
  await waitFor(() => expect(attempts).toHaveLength(2));
  expect(attempts[1]).toEqual(attempts[0]);
  expect(ruleText.value).toBe(editedRule);
});
