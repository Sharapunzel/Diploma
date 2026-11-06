import { useState } from "react";
import { render, screen, fireEvent, waitFor } from "@testing-library/react";
import { expect, it, vi } from "vitest";
import { RuleEditor } from "./RuleEditor";
import {
  parseAggregate,
  printAggregate,
  type JsonObject,
} from "../../app/api/aggregateJson";
import type { AggregatesApi, Operator } from "../../app/api/aggregates";

const operators = [
  { name: "derive", kind: "feature", parameters: ["expr"] },
] as Operator[];
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
  const input = screen.getByLabelText("features[0].expr: константа");
  expect((input as HTMLInputElement).value).toBe("9007199254740993");
  fireEvent.change(input, { target: { value: "1e" } });
  expect((input as HTMLInputElement).value).toBe("1e");
  await waitFor(() =>
    expect(invalid).toHaveBeenLastCalledWith(expect.any(String), true),
  );
  expect(screen.getByTestId("wire").textContent).toContain("9007199254740993");
  fireEvent.change(input, { target: { value: "1.0" } });
  await waitFor(() =>
    expect(invalid).toHaveBeenLastCalledWith(expect.any(String), false),
  );
  expect(screen.getByTestId("wire").textContent).toContain('"value":1.0');
});
