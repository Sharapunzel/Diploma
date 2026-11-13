import { useState } from "react";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from "@testing-library/react";
import { afterEach, expect, it } from "vitest";
import type { AggregatesApi, Operator } from "../../app/api/aggregates";
import {
  parseAggregate,
  printAggregate,
  type JsonObject,
} from "../../app/api/aggregateJson";
import { referenceErrors } from "../../features/aggregates/rule";
import { RuleEditor } from "./RuleEditor";
afterEach(cleanup);
it.each(["auxiliary", "formula", "auxiliary-formula"])(
  "refuses deleting the last %s grammar marker without changing wire",
  (marker) => {
    const initial: JsonObject = {
      features: [
        { key: "b", name: "B", op: "derive", expr: { ref: "a" } },
        { key: "a", name: "A", op: "count" },
      ],
      ...(marker === "formula"
        ? {}
        : {
            auxiliary: [
              {
                key: "c",
                name: "C",
                op: marker === "auxiliary-formula" ? "formula" : "count",
                ...(marker === "auxiliary-formula"
                  ? { expr: { value: 1 } }
                  : {}),
              },
            ],
          }),
    };
    if (marker === "formula")
      (initial.features as JsonObject[]).push({
        key: "c",
        name: "C",
        op: "formula",
        expr: { value: 1 },
      });
    render(<InteractiveEditor initial={initial} />);
    const button = screen.getByRole("button", { name: "Удалить признак c" });
    button.focus();
    fireEvent.click(button);
    expect(screen.getByRole("alert").textContent).toContain(
      "Сначала исправьте",
    );
    expect(screen.getByTestId("wire").textContent).toBe(
      printAggregate(initial),
    );
    expect(document.activeElement).toBe(button);
    expect(referenceErrors(initial)).toEqual([]);
  },
);
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

it.each(["count", "derive"])(
  "keeps visible and wire %s order through visual add/delete while a is disabled",
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
            { key: "c", name: "C", op: "count" },
          ],
        }}
      />,
    );
    const check = (expected: string[]) => {
      const rule = parseAggregate(
        screen.getByTestId("wire").textContent!,
      ) as JsonObject;
      expect((rule.features as JsonObject[]).map((f) => f.key)).toEqual(
        expected,
      );
      expect(
        within(screen.getByRole("region", { name: "Признаки" }))
          .getAllByRole("switch")
          .filter((s) => s.getAttribute("aria-checked") === "true")
          .map((s) => s.getAttribute("aria-label")!.split(":")[0]),
      ).toEqual(expected);
      expect(
        ((rule.auxiliary ?? []) as JsonObject[]).map((f) => f.key),
      ).toEqual(expected.includes("a") ? [] : ["a"]);
      expect(referenceErrors(rule)).toEqual([]);
    };
    fireEvent.click(screen.getByRole("switch", { name: "a: включён в выход" }));
    check(["b", "c"]);
    expect(
      (
        screen.getByRole("button", {
          name: "Удалить признак a",
        }) as HTMLButtonElement
      ).disabled,
    ).toBe(op === "derive");
    fireEvent.click(screen.getByRole("button", { name: "Добавить признак" }));
    fireEvent.change(screen.getByLabelText("Название признака"), {
      target: { value: "D" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Применить признак" }));
    await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
    check(["b", "c", "feature_1"]);
    fireEvent.click(screen.getByRole("switch", { name: "a: включён в выход" }));
    check(["a", "b", "c", "feature_1"]);
    fireEvent.click(screen.getByRole("switch", { name: "a: включён в выход" }));
    fireEvent.click(screen.getByRole("button", { name: "Удалить признак c" }));
    check(["b", "feature_1"]);
    fireEvent.click(screen.getByRole("switch", { name: "a: включён в выход" }));
    check(["a", "b", "feature_1"]);
  },
);
it("explains refused promotion of an imported dependency without changing the wire", () => {
  const initial: JsonObject = {
    features: [{ key: "b", name: "B", op: "derive", expr: { ref: "a" } }],
    auxiliary: [{ key: "a", name: "A", op: "count" }],
  };
  render(<InteractiveEditor initial={initial} />);
  fireEvent.click(screen.getByRole("switch", { name: "a: включён в выход" }));
  expect(screen.getByRole("alert").textContent).toContain("Нельзя включить");
  expect(screen.getByTestId("wire").textContent).toBe(printAggregate(initial));
  expect(
    screen
      .getByRole("switch", { name: "a: включён в выход" })
      .getAttribute("aria-checked"),
  ).toBe("false");
});
