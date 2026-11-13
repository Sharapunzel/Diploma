import { afterEach, expect, it, vi } from "vitest";
import {
  cleanup,
  act,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import type { AggregatesApi, Operator } from "../../app/api/aggregates";
import { RuleExchange } from "./RuleExchange";

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  vi.useRealTimers();
});
const ops = [
  {
    name: "count",
    kind: "feature",
    parameters: [],
    inputs: [],
    arity: null,
    result: "number",
    parameter_types: {},
    compatible_fields: [],
    weight: 1,
    weight_formula: "weight",
    result_nullable: true,
  },
] as Operator[];
const props = {
  api: {} as AggregatesApi,
  source: "source",
  operators: ops,
  text: '{"features":[{"key":"n","name":"N","op":"count"}]}',
};
function file(text: string) {
  const f = new File([text], "aggregate.json", { type: "application/json" });
  Object.defineProperty(f, "arrayBuffer", {
    value: vi.fn().mockResolvedValue(new TextEncoder().encode(text).buffer),
  });
  return f;
}
it("copies a standalone instruction directly and exposes manual fallback without a dialog", async () => {
  const writeText = vi.fn().mockRejectedValue(new Error("denied"));
  vi.stubGlobal("navigator", { clipboard: { writeText } });
  render(<RuleExchange {...props} />);
  fireEvent.click(screen.getByRole("button", { name: "Инструкция для ИИ" }));
  await screen.findByLabelText("Текст для ручного копирования");
  expect(writeText).toHaveBeenCalledTimes(1);
  expect(writeText.mock.calls[0][0]).toContain("СНАЧАЛА уточни цель");
  expect(writeText.mock.calls[0][0]).not.toContain(props.text);
  expect(screen.queryByRole("dialog")).toBeNull();
  writeText.mockResolvedValue(undefined);
  fireEvent.click(
    screen.getByRole("button", { name: "Копировать подготовленный текст" }),
  );
  await screen.findByText("Текст скопирован.");
});
it("confirms replacement, keeps invalid input for correction and supports selecting the same file again", async () => {
  const onImport = vi.fn();
  render(<RuleExchange {...props} onImport={onImport} />);
  const input = screen.getByLabelText("Файл правила JSON");
  const chosen = file('{"features":[');
  fireEvent.change(input, { target: { files: [chosen] } });
  await screen.findByRole("dialog");
  fireEvent.click(screen.getByRole("button", { name: "Отмена" }));
  expect(onImport).not.toHaveBeenCalled();
  fireEvent.change(input, { target: { files: [chosen] } });
  await screen.findByRole("dialog");
  fireEvent.click(screen.getByRole("button", { name: "Заменить правило" }));
  expect(onImport).toHaveBeenCalledWith('{"features":[');
});
it("refuses transport oversize and unreadable files before replacing the old rule", async () => {
  const onImport = vi.fn();
  render(<RuleExchange {...props} onImport={onImport} />);
  const oversized = file(" ".repeat(1048577));
  fireEvent.change(screen.getByLabelText("Файл правила JSON"), {
    target: { files: [oversized] },
  });
  expect(oversized.arrayBuffer).not.toHaveBeenCalled();
  expect(onImport).not.toHaveBeenCalled();
  const unreadable = file("broken");
  vi.mocked(unreadable.arrayBuffer).mockRejectedValue(new Error("read failed"));
  fireEvent.change(screen.getByLabelText("Файл правила JSON"), {
    target: { files: [unreadable] },
  });
  await screen.findByText(/Не удалось прочитать UTF-8 файл/);
  expect(onImport).not.toHaveBeenCalled();
});
it("ignores a late file read after source transition and prevents changes during create", async () => {
  let resolve!: (buffer: ArrayBuffer) => void;
  const onImport = vi.fn(),
    pending = file("rule");
  vi.mocked(pending.arrayBuffer).mockReturnValue(
    new Promise((r) => {
      resolve = r;
    }),
  );
  const view = render(<RuleExchange {...props} onImport={onImport} />);
  fireEvent.change(screen.getByLabelText("Файл правила JSON"), {
    target: { files: [pending] },
  });
  view.rerender(
    <RuleExchange {...props} source="other" onImport={onImport} disabled />,
  );
  resolve(new TextEncoder().encode(props.text).buffer);
  await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
  expect(onImport).not.toHaveBeenCalled();
  expect(
    (screen.getByRole("button", { name: "Импорт .json" }) as HTMLButtonElement)
      .disabled,
  ).toBe(true);
});
it("does not let an old catalog request copy fields or lock the next source", async () => {
  let resolve!: (value: unknown) => void;
  const pending = new Promise((r) => {
    resolve = r;
  });
  const page = {
    items: [],
    total: 0,
    limit: 100,
    offset: 0,
    catalog_hash: "a".repeat(64),
  };
  const api = {
    fieldCatalog: vi.fn().mockReturnValueOnce(pending).mockResolvedValue(page),
  } as unknown as AggregatesApi;
  const writeText = vi.fn().mockResolvedValue(undefined);
  vi.stubGlobal("navigator", { clipboard: { writeText } });
  const view = render(<RuleExchange {...props} api={api} />);
  fireEvent.click(
    screen.getByRole("button", { name: "Скопировать поля источника" }),
  );
  view.rerender(<RuleExchange {...props} api={api} source="other" />);
  fireEvent.click(
    screen.getByRole("button", { name: "Скопировать поля источника" }),
  );
  await screen.findByText("Полный каталог полей скопирован.");
  resolve(page);
  await waitFor(() => expect(writeText).toHaveBeenCalledTimes(1));
  expect(vi.mocked(api.fieldCatalog).mock.calls.map((c) => c[0])).toEqual([
    "source",
    "other",
  ]);
});
it("exports the current lossless JSON and releases its object URL; malformed text remains editable", async () => {
  const create = vi.fn().mockReturnValue("blob:test"),
    revoke = vi.fn();
  Object.defineProperty(URL, "createObjectURL", {
    configurable: true,
    value: create,
  });
  Object.defineProperty(URL, "revokeObjectURL", {
    configurable: true,
    value: revoke,
  });
  vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(() => {});
  const text =
    '{"features":[{"key":"n","name":"N","op":"derive","expr":{"value":9223372036854775807}}]}';
  const view = render(<RuleExchange {...props} text={text} />);
  fireEvent.click(screen.getByRole("button", { name: "Скачать .json" }));
  expect(create).toHaveBeenCalledTimes(1);
  expect(revoke).toHaveBeenCalledWith("blob:test");
  view.rerender(<RuleExchange {...props} text="{" />);
  fireEvent.click(screen.getByRole("button", { name: "Скачать .json" }));
  await screen.findByText(/Текущий текст нельзя экспортировать/);
  expect(create).toHaveBeenCalledTimes(1);
});

it("matches normalizer action order and copies the current lossless rule without metadata", async () => {
  const writeText = vi.fn().mockResolvedValue(undefined);
  vi.stubGlobal("navigator", { clipboard: { writeText } });
  const text =
    '{"features":[{"key":"exact","name":"Exact","op":"derive","expr":{"op":"add","args":[{"value":9223372036854775807},{"value":1.0}]}}]}';
  render(<RuleExchange {...props} text={text} onImport={vi.fn()} />);
  const controls = screen.getByRole("group", { name: "Обмен правилом" });
  expect(
    Array.from(controls.querySelectorAll("button"))
      .slice(0, 5)
      .map((button) => button.textContent),
  ).toEqual([
    "Копировать JSON",
    "Скачать .json",
    "Импорт .json",
    "Инструкция для ИИ",
    "Скопировать поля источника",
  ]);
  fireEvent.click(screen.getByRole("button", { name: "Копировать JSON" }));
  await screen.findByText("JSON правила скопирован.");
  expect(writeText).toHaveBeenCalledTimes(1);
  expect(writeText.mock.calls[0][0]).toContain("9223372036854775807");
  expect(writeText.mock.calls[0][0]).toContain("1.0");
  expect(writeText.mock.calls[0][0]).not.toContain("source_id");
});

it("dismisses clipboard errors without removing the prepared manual text", async () => {
  vi.stubGlobal("navigator", {
    clipboard: { writeText: vi.fn().mockRejectedValue(new Error("denied")) },
  });
  render(<RuleExchange {...props} />);
  fireEvent.click(screen.getByRole("button", { name: "Копировать JSON" }));
  const manual = await screen.findByLabelText("Текст для ручного копирования");
  fireEvent.click(screen.getByRole("button", { name: /close/i }));
  expect(screen.queryByRole("alert")).toBeNull();
  expect((manual as HTMLTextAreaElement).value).toContain('"key": "n"');
  expect(
    screen.getByRole("button", { name: "Копировать подготовленный текст" }),
  ).toBeDefined();
});

it("lets success notices close and removes them after five seconds", async () => {
  vi.useFakeTimers();
  vi.stubGlobal("navigator", {
    clipboard: { writeText: vi.fn().mockResolvedValue(undefined) },
  });
  render(<RuleExchange {...props} />);
  const copy = screen.getByRole("button", {
    name: "Копировать JSON",
  });
  await act(async () => {
    fireEvent.click(copy);
  });
  expect(screen.getByText("JSON правила скопирован.")).toBeDefined();
  fireEvent.click(screen.getByRole("button", { name: /close/i }));
  expect(screen.queryByRole("status")).toBeNull();
  await act(async () => {
    fireEvent.click(copy);
  });
  expect(screen.getByText("JSON правила скопирован.")).toBeDefined();
  act(() => {
    vi.advanceTimersByTime(5000);
  });
  expect(screen.queryByRole("status")).toBeNull();
});
