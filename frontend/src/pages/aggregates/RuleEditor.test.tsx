import { useState } from "react";
import {
  render,
  screen,
  fireEvent,
  waitFor,
  within,
  cleanup,
} from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { RuleEditor } from "./RuleEditor";
import {
  parseAggregate,
  printAggregate,
  type JsonObject,
} from "../../app/api/aggregateJson";
import type { AggregatesApi, Operator } from "../../app/api/aggregates";
import { ApiError } from "../../app/api/client";
import { AggregateError } from "./AggregateError";
import userEvent from "@testing-library/user-event";
import { referenceErrors } from "../../features/aggregates/rule";

const operators = [
  { name: "derive", kind: "feature", parameters: ["expr"] },
] as Operator[];
afterEach(cleanup);
it.each(["count", "derive"])(
  "keeps coordinates after applying a renamed %s feature while its predecessor is disabled",
  async (op) => {
    render(
      <InteractiveEditor
        initial={{
          features: [
            { key: "a", name: "A", op: "count" },
            {
              key: "b",
              name: "B",
              op,
              ...(op === "derive" ? { expr: { ref: "a" } } : {}),
            },
          ],
        }}
      />,
    );
    fireEvent.click(screen.getByRole("switch", { name: "a: включён в выход" }));
    fireEvent.click(screen.getByRole("button", { name: "B · b" }));
    fireEvent.change(screen.getByLabelText("Название признака"), {
      target: { value: "B2" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Применить признак" }));
    await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
    fireEvent.click(screen.getByRole("switch", { name: "a: включён в выход" }));
    const wire = parseAggregate(
      screen.getByTestId("wire").textContent!,
    ) as JsonObject;
    expect((wire.features as JsonObject[]).map((f) => f.key)).toEqual([
      "a",
      "b",
    ]);
    expect((wire.features as JsonObject[])[1].name).toBe("B2");
    expect(referenceErrors(wire)).toEqual([]);
  },
);
it("preserves legacy dependency coordinates through off/on and follows imported output order", () => {
  const initial: JsonObject = {
    features: [
      { key: "a", name: "A", op: "count" },
      { key: "b", name: "B", op: "derive", expr: { ref: "a" } },
    ],
  };
  render(<InteractiveEditor initial={initial} />);
  const wire = () =>
    parseAggregate(screen.getByTestId("wire").textContent!) as JsonObject;
  fireEvent.click(screen.getByRole("switch", { name: "a: включён в выход" }));
  expect(referenceErrors(wire())).toEqual([]);
  expect((wire().features as JsonObject[]).map((f) => f.key)).toEqual(["b"]);
  fireEvent.click(screen.getByRole("switch", { name: "a: включён в выход" }));
  expect(wire()).toEqual(initial);
  expect(referenceErrors(wire())).toEqual([]);
  cleanup();
  const props = {
    operators,
    api: {} as AggregatesApi,
    source: "source",
    onChange: vi.fn(),
    onInvalid: vi.fn(),
  };
  const view = render(<RuleEditor {...props} rule={initial} />);
  const names = () =>
    within(screen.getByRole("region", { name: "Признаки" }))
      .getAllByRole("button", { name: / · / })
      .map((b) => b.getAttribute("aria-label"));
  expect(names()).toEqual(["A · a", "B · b"]);
  const replacement: JsonObject = {
    features: [
      { key: "b", name: "B", op: "count" },
      { key: "a", name: "A", op: "count" },
    ],
  };
  view.rerender(<RuleEditor {...props} rule={replacement} />);
  expect(names()).toEqual(["B · b", "A · a"]);
});
it("keeps incomplete numeric text editable, blocks its owner, and preserves exact constants", async () => {
  const invalid = vi.fn();
  function Editor() {
    const [rule, setRule] = useState(
      parseAggregate(
        '{"features":[{"key":"n","name":"N","op":"derive","expr":{"value":9007199254740993}}]}',
      ) as JsonObject,
    );
    return (
      <>
        <RuleEditor
          rule={rule}
          operators={operators}
          api={{} as AggregatesApi}
          source="source"
          onChange={setRule}
          onInvalid={invalid}
        />
        <output data-testid="wire">{printAggregate(rule)}</output>
      </>
    );
  }
  render(<Editor />);
  fireEvent.click(screen.getByRole("button", { name: "N · n" }));
  const input = screen.getByLabelText("Формула: константа");
  expect((input as HTMLInputElement).value).toBe("9007199254740993");
  fireEvent.change(input, { target: { value: "1e" } });
  expect((input as HTMLInputElement).value).toBe("1e");
  await waitFor(() =>
    expect(
      (
        screen.getByRole("button", {
          name: "Применить признак",
        }) as HTMLButtonElement
      ).disabled,
    ).toBe(true),
  );
  expect(screen.getByTestId("wire").textContent).toContain("9007199254740993");
  fireEvent.change(input, { target: { value: "1.0" } });
  await waitFor(() =>
    expect(
      (
        screen.getByRole("button", {
          name: "Применить признак",
        }) as HTMLButtonElement
      ).disabled,
    ).toBe(false),
  );
  expect(screen.getByTestId("wire").textContent).toContain(
    '"value":9007199254740993',
  );
  fireEvent.click(screen.getByRole("button", { name: "Применить признак" }));
  expect(screen.getByTestId("wire").textContent).toContain('"value":1.0');
});

const operatorInputs: (Pick<
  Operator,
  "name" | "kind" | "parameters" | "parameter_types"
> &
  Partial<Operator>)[] = [
  { name: "count", kind: "feature", parameters: [], parameter_types: {} },
  {
    name: "sum",
    kind: "feature",
    parameters: ["field"],
    parameter_types: { field: "number" },
  },
  {
    name: "exists",
    kind: "condition",
    parameters: ["field"],
    parameter_types: { field: "any" },
  },
  {
    name: "add",
    kind: "expression",
    parameters: ["args"],
    parameter_types: { args: "number" },
    arity: { min: 2, max: 2 },
  },
  {
    name: "formula",
    kind: "feature",
    parameters: ["expr"],
    parameter_types: {},
  },
  {
    name: "derive",
    kind: "feature",
    parameters: ["expr"],
    parameter_types: {},
  },
];
const extendedOperators: Operator[] = operatorInputs.map((operation) => ({
  inputs: [],
  compatible_fields: [],
  arity: null,
  weight: 1,
  weight_formula: "weight",
  result_nullable: true,
  result: "number",
  ...operation,
}));

function InteractiveEditor({ initial }: { initial: JsonObject }) {
  const [rule, setRule] = useState(initial);
  return (
    <>
      <RuleEditor
        rule={rule}
        operators={extendedOperators}
        api={{} as AggregatesApi}
        source=""
        onChange={setRule}
        onInvalid={() => {}}
      />
      <output data-testid="wire">{printAggregate(rule)}</output>
    </>
  );
}

it("opens output help with Enter and closes it with Escape without changing the rule", async () => {
  const user = userEvent.setup();
  const initial = {
    features: [{ key: "events", name: "События", op: "count" }],
  };
  render(<InteractiveEditor initial={initial} />);
  const outputHelp = screen.getByRole("button", {
    name: "Справка: Выход признака events",
  });
  outputHelp.focus();
  await user.keyboard("{Enter}");
  expect(outputHelp.getAttribute("aria-expanded")).toBe("true");
  await user.keyboard("{Escape}");
  expect(outputHelp.getAttribute("aria-expanded")).toBe("false");
  expect(screen.getByTestId("wire").textContent).toBe(printAggregate(initial));
});

it("opens modal help with Enter and closes only help with Escape without changing the rule", async () => {
  const user = userEvent.setup();
  const initial = {
    features: [{ key: "events", name: "События", op: "count" }],
  };
  render(<InteractiveEditor initial={initial} />);
  await user.click(screen.getByRole("button", { name: "События · events" }));
  const dialog = await screen.findByRole("dialog", { name: "Признак" });
  const nameHelp = within(dialog).getByRole("button", {
    name: "Справка: Название",
  });
  nameHelp.focus();
  await user.keyboard("{Enter}");
  expect(nameHelp.getAttribute("aria-expanded")).toBe("true");
  expect(document.activeElement).toBe(nameHelp);
  await user.keyboard("{Escape}");
  expect(nameHelp.getAttribute("aria-expanded")).toBe("false");
  expect(screen.getByRole("dialog", { name: "Признак" })).toBe(dialog);
  expect(
    within(dialog)
      .getByLabelText("Название признака")
      .getAttribute("aria-required"),
  ).toBe("true");
  expect(screen.getByTestId("wire").textContent).toBe(printAggregate(initial));
});

it("opens the feature named by a server error without toggling its output membership", () => {
  const initial = {
    features: [{ key: "events", name: "События", op: "count" }],
  };
  render(
    <>
      <AggregateError
        error={
          new ApiError(422, "invalid_rule", null, null, {
            path: "features[0]",
          })
        }
      />
      <InteractiveEditor initial={initial} />
    </>,
  );
  const output = screen.getByRole("switch", {
    name: "events: включён в выход",
  });
  fireEvent.click(screen.getByRole("button", { name: "features[0]" }));
  expect(screen.getByRole("dialog", { name: "Признак" })).toBeTruthy();
  expect(output.getAttribute("aria-checked")).toBe("true");
  expect(screen.getByTestId("wire").textContent).toBe(printAggregate(initial));
});

it("keeps one stable feature list across output switches without ordering controls", () => {
  render(
    <InteractiveEditor
      initial={{
        features: [
          { key: "events", name: "События", op: "count" },
          { key: "other", name: "Другой", op: "count" },
        ],
        auxiliary: [{ key: "internal", name: "Внутренний", op: "count" }],
      }}
    />,
  );
  const list = screen.getByRole("region", { name: "Признаки" });
  const names = () =>
    within(list)
      .getAllByRole("button", { name: / · / })
      .map((button) => button.getAttribute("aria-label"));
  const original = names();
  expect(original).toEqual([
    "События · events",
    "Другой · other",
    "Внутренний · internal",
  ]);
  expect(screen.queryByRole("region", { name: "Выходной вектор" })).toBeNull();
  expect(
    screen.queryByRole("region", { name: "Внутренние определения" }),
  ).toBeNull();
  expect(screen.queryByRole("button", { name: "Выше" })).toBeNull();
  expect(screen.queryByRole("button", { name: "Ниже" })).toBeNull();
  expect(
    screen.getAllByRole("button", { name: "Добавить признак" }),
  ).toHaveLength(1);
  fireEvent.click(
    screen.getByRole("switch", { name: "events: включён в выход" }),
  );
  expect(names()).toEqual(original);
  expect(screen.getByTestId("wire").textContent).toContain(
    '"features":[{"key":"other"',
  );
  expect(
    screen
      .getByRole("switch", { name: "events: включён в выход" })
      .getAttribute("aria-checked"),
  ).toBe("false");
  fireEvent.click(
    screen.getByRole("switch", { name: "events: включён в выход" }),
  );
  expect(names()).toEqual(original);
  expect(
    screen
      .getByRole("switch", { name: "events: включён в выход" })
      .getAttribute("aria-checked"),
  ).toBe("true");
  const document = parseAggregate(
    screen.getByTestId("wire").textContent!,
  ) as JsonObject;
  expect(Object.keys(document).sort()).toEqual(["auxiliary", "features"]);
  expect((document.features as JsonObject[]).map((f) => f.key)).toEqual([
    "events",
    "other",
  ]);
});

it("chooses the feature type inside a four-block modal and isolates cancel from apply", async () => {
  const initial = {
    features: [{ key: "events", name: "События", op: "count" }],
  };
  render(<InteractiveEditor initial={initial} />);
  const add = screen.getByRole("button", { name: "Добавить признак" });
  fireEvent.click(add);
  const modal = screen.getByRole("dialog", { name: "Признак" });
  // Evaluate visibility once, then check every exact accessible name against it.
  const headings = within(modal).getAllByRole("heading");
  for (const title of [
    "1. Настройки признака",
    "2. Входные данные",
    "3. Операции и отбор",
    "4. Итоговая формула",
  ])
    expect(headings).toContain(
      within(modal).getByRole("heading", { name: title, hidden: true }),
    );
  fireEvent.change(within(modal).getByLabelText("Название признака"), {
    target: { value: "Не применять" },
  });
  fireEvent.click(within(modal).getByRole("radio", { name: "Производный" }));
  expect(screen.getByTestId("wire").textContent).toBe(printAggregate(initial));
  fireEvent.click(within(modal).getByRole("button", { name: "Отмена" }));
  await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
  expect(screen.getByTestId("wire").textContent).toBe(printAggregate(initial));
  fireEvent.click(add);
  const reopened = within(screen.getByRole("dialog", { name: "Признак" }));
  expect(
    (
      reopened.getByRole("radio", {
        name: "Первичный",
      }) as HTMLInputElement
    ).checked,
  ).toBe(true);
  expect(
    (reopened.getByLabelText("Название признака") as HTMLInputElement).value,
  ).toBe("Новый признак");
  fireEvent.change(reopened.getByLabelText("Название признака"), {
    target: { value: "Частота" },
  });
  fireEvent.click(reopened.getByRole("radio", { name: "Производный" }));
  fireEvent.click(reopened.getByRole("button", { name: "Применить признак" }));
  await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
  const saved = screen.getByTestId("wire").textContent!;
  expect(saved).toContain('"name":"Частота"');
  expect(saved).toContain('"ref":"events"');
  fireEvent.click(screen.getByRole("button", { name: "Частота · feature_1" }));
  const editing = within(screen.getByRole("dialog", { name: "Признак" }));
  fireEvent.click(editing.getByRole("radio", { name: "Первичный" }));
  fireEvent.click(editing.getByRole("button", { name: "Отмена" }));
  expect(screen.getByTestId("wire").textContent).toBe(saved);
});

const globalWhere = { op: "exists", field: "event.kind" };
const featureWhere = { op: "exists", field: "host.name" };
const filteredRule = {
  where: globalWhere,
  features: [
    {
      key: "events",
      name: "События",
      op: "sum",
      field: "event.duration",
      where: featureWhere,
    },
  ],
};

async function applyFeature() {
  fireEvent.click(screen.getByRole("button", { name: "Применить признак" }));
  await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
  return parseAggregate(screen.getByTestId("wire").textContent!);
}

it("keeps a compound exact formula when changing and restoring its single operation selector", async () => {
  const initial = parseAggregate(
    '{"features":[{"key":"n","name":"N","op":"formula","expr":{"op":"add","args":[{"aggregate":{"op":"sum","field":"source.bytes"}},{"value":9007199254740993}]}}]}',
  ) as JsonObject;
  render(<InteractiveEditor initial={initial} />);
  fireEvent.click(screen.getByRole("button", { name: "N · n" }));
  expect(
    screen.queryByRole("combobox", { name: "Признак: агрегация" }),
  ).toBeNull();
  fireEvent.mouseDown(screen.getByRole("combobox", { name: "Операция входа" }));
  fireEvent.click(screen.getByRole("option", { name: /^Количество событий/ }));
  expect(screen.getByTestId("wire").textContent).toBe(printAggregate(initial));
  fireEvent.mouseDown(screen.getByRole("combobox", { name: "Операция входа" }));
  fireEvent.click(
    screen.getByRole("option", { name: /^Формула над итогами окна/ }),
  );
  expect(await applyFeature()).toEqual(initial);
});

it("keeps feature and global event filters when replacing its input", async () => {
  render(<InteractiveEditor initial={filteredRule} />);
  fireEvent.click(screen.getByRole("button", { name: "События · events" }));
  fireEvent.mouseDown(screen.getByRole("combobox", { name: "Операция входа" }));
  fireEvent.click(screen.getByRole("option", { name: /^Количество событий/ }));
  expect(await applyFeature()).toEqual({
    where: globalWhere,
    features: [
      {
        key: "events",
        name: "События",
        op: "count",
        where: featureWhere,
      },
    ],
  });
});

it.each(["Операция входа"])(
  "keeps event filters when changing the operation through %s",
  async (control) => {
    render(<InteractiveEditor initial={filteredRule} />);
    fireEvent.click(screen.getByRole("button", { name: "События · events" }));
    fireEvent.mouseDown(screen.getByRole("combobox", { name: control }));
    fireEvent.click(
      screen.getByRole("option", { name: /^Количество событий/ }),
    );
    expect(await applyFeature()).toEqual({
      where: globalWhere,
      features: [
        {
          key: "events",
          name: "События",
          op: "count",
          where: featureWhere,
        },
      ],
    });
  },
);

it("keeps the feature filter above both inputs when appending an ECS aggregate", async () => {
  render(<InteractiveEditor initial={filteredRule} />);
  fireEvent.click(screen.getByRole("button", { name: "События · events" }));
  fireEvent.click(
    screen.getByRole("button", { name: "Добавить итог к формуле" }),
  );
  expect(await applyFeature()).toEqual({
    where: globalWhere,
    features: [
      {
        key: "events",
        name: "События",
        op: "formula",
        expr: {
          op: "add",
          args: [
            { aggregate: { op: "sum", field: "event.duration" } },
            { aggregate: { op: "count" } },
          ],
        },
        where: featureWhere,
      },
    ],
  });
});

it("keeps the feature event filter when changing type and adding an ECS input", async () => {
  const base = { key: "base", name: "База", op: "count" };
  render(
    <InteractiveEditor
      initial={{ ...filteredRule, features: [base, ...filteredRule.features] }}
    />,
  );
  fireEvent.click(screen.getByRole("button", { name: "События · events" }));
  fireEvent.click(screen.getByRole("radio", { name: "Производный" }));
  fireEvent.click(
    screen.getByRole("button", { name: "Добавить итог к формуле" }),
  );
  expect(await applyFeature()).toEqual({
    where: globalWhere,
    features: [
      base,
      {
        key: "events",
        name: "События",
        op: "formula",
        expr: {
          op: "add",
          args: [{ ref: "base" }, { aggregate: { op: "count" } }],
        },
        where: featureWhere,
      },
    ],
  });
});
