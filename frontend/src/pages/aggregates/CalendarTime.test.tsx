import { useState } from "react";
import { afterEach, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { CalendarTime } from "./CalendarTime";
afterEach(cleanup);
it("emits a boundary whose offset matches its input timezone", () => {
  const onChange = vi.fn();
  render(
    <CalendarTime
      label="Граница"
      value=""
      zone="Asia/Yekaterinburg"
      zoned
      scheduleAnchor="2026-01-01T00:00:00.123456Z"
      onChange={onChange}
    />,
  );
  fireEvent.change(screen.getByLabelText("Граница"), {
    target: { value: "2026-11-01T12:30:00" },
  });
  expect(onChange).toHaveBeenCalledWith("2026-11-01T12:30:00+05:00");
});
it("keeps an unchanged exact ISO moment when display zone changes", async () => {
  const onChange = vi.fn(),
    value = "2026-01-01T00:00:00.123456+05:00";
  const v = render(
    <CalendarTime
      label="Граница"
      value={value}
      zone="UTC"
      scheduleAnchor="2025-12-31T19:00:00.123456Z"
      onChange={onChange}
    />,
  );
  v.rerender(
    <CalendarTime
      label="Граница"
      value={value}
      zone="Asia/Yekaterinburg"
      scheduleAnchor="2025-12-31T19:00:00.654321Z"
      onChange={onChange}
    />,
  );
  expect((screen.getByLabelText("Граница") as HTMLInputElement).value).toBe(
    "2026-01-01T00:00",
  );
  fireEvent.click(screen.getByRole("button", { name: "Справка: Граница" }));
  expect(
    (await screen.findByText(value, { exact: false })).textContent,
  ).toContain(value);
  expect(onChange).not.toHaveBeenCalled();
});

it.each([
  ["2026-01-01T00:00:00.123456Z", "123456"],
  ["2026-01-01T00:00:00.123456+05:00:03.654321", "469135"],
])(
  "selects whole calendar seconds with the exact UTC phase of %s",
  async (anchor, fraction) => {
    const onChange = vi.fn();
    render(
      <CalendarTime
        label="Окно"
        value=""
        zone="Asia/Yekaterinburg"
        scheduleAnchor={anchor}
        onChange={onChange}
      />,
    );
    fireEvent.change(screen.getByLabelText("Окно"), {
      target: { value: "2026-11-01T12:30:07" },
    });
    expect(onChange).toHaveBeenCalledExactlyOnceWith(
      `2026-11-01T07:30:07.${fraction}Z`,
    );
    fireEvent.click(screen.getByRole("button", { name: "Справка: Окно" }));
    expect(
      (await screen.findByText(/Доля секунды расписания:/)).textContent,
    ).toContain(`0.${fraction}`);
  },
);

it("clears a window without a fabricated instant and retains the schedule phase for its next selection", () => {
  const changed = vi.fn();
  function Editor() {
    const [value, setValue] = useState("2026-01-01T00:00:00.123456Z");
    return (
      <CalendarTime
        label="Окно"
        value={value}
        zone="UTC"
        scheduleAnchor="2026-01-01T00:00:00.123456Z"
        onChange={(next) => {
          changed(next);
          setValue(next);
        }}
      />
    );
  }
  render(<Editor />);
  fireEvent.change(screen.getByLabelText("Окно"), { target: { value: "" } });
  expect(changed).toHaveBeenLastCalledWith("");
  expect((screen.getByLabelText("Окно") as HTMLInputElement).value).toBe("");
  fireEvent.change(screen.getByLabelText("Окно"), {
    target: { value: "2026-01-01T00:05:00" },
  });
  expect(changed).toHaveBeenLastCalledWith("2026-01-01T00:05:00.123456Z");
});

it.each(["2026-11-01T05:30:00.123456Z", "2026-11-01T06:30:00.123456Z"])(
  "keeps the exact schedule phase for the chosen DST fold instant %s",
  async (chosen) => {
    const changed = vi.fn();
    function Editor() {
      const [value, setValue] = useState("");
      return (
        <>
          <CalendarTime
            label="Окно"
            value={value}
            zone="America/New_York"
            scheduleAnchor="2026-01-01T00:00:00.123456Z"
            onChange={(next) => {
              changed(next);
              setValue(next);
            }}
          />
          <output data-testid="fold-instant">{value}</output>
        </>
      );
    }
    render(<Editor />);
    fireEvent.change(screen.getByLabelText("Окно"), {
      target: { value: "2026-11-01T01:30:00" },
    });
    expect(changed).toHaveBeenCalledExactlyOnceWith("");
    fireEvent.mouseDown(
      screen.getByRole("combobox", { name: "Окно: смещение" }),
    );
    fireEvent.click(
      await screen.findByText(chosen, {
        exact: false,
        selector: ".ant-select-item-option-content",
      }),
    );
    expect(changed).toHaveBeenLastCalledWith(chosen);
    expect(screen.getByTestId("fold-instant").textContent).toBe(chosen);
  },
);

it("retains gap input and offers both fold offsets before committing a new instant", async () => {
  function Editor() {
    const [value, setValue] = useState("2026-01-01T00:00:00Z");
    return (
      <>
        <CalendarTime
          label="Граница"
          value={value}
          zone="America/New_York"
          onChange={setValue}
        />
        <output data-testid="instant">{value}</output>
      </>
    );
  }
  render(<Editor />);
  fireEvent.change(screen.getByLabelText("Граница"), {
    target: { value: "2026-03-08T02:30:00" },
  });
  expect(screen.getByRole("alert").textContent).toContain(
    "Такого местного времени",
  );
  expect(
    (screen.getByLabelText("Граница") as HTMLInputElement).value,
  ).toContain("2026-03-08T02:30");
  fireEvent.change(screen.getByLabelText("Граница"), {
    target: { value: "2026-11-01T01:30:00" },
  });
  expect(
    screen.getByRole("combobox", { name: "Граница: смещение" }),
  ).toBeTruthy();
  expect(screen.getByTestId("instant").textContent).toBe("");
});
