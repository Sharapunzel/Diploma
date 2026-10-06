import { cleanup, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ConfirmationDialog } from "./ConfirmationDialog";

afterEach(cleanup);

describe("ConfirmationDialog", () => {
  it("uses caller-provided text and handlers for a second action", async () => {
    const onCancel = vi.fn();
    const onConfirm = vi.fn();
    const user = userEvent.setup();
    render(
      <ConfirmationDialog
        open
        title="Archive draft?"
        message="You can restore this draft later."
        confirmText="Archive"
        onCancel={onCancel}
        onConfirm={onConfirm}
      />,
    );

    const dialog = screen.getByRole("dialog", { name: "Archive draft?" });
    expect(dialog.textContent).toContain("You can restore this draft later.");
    await user.click(
      screen.getByRole("button", {
        name: /\u041e\u0442\u043c\u0435\u043d\u0430/i,
      }),
    );
    expect(onCancel).toHaveBeenCalledOnce();
    expect(onConfirm).not.toHaveBeenCalled();
    await user.click(screen.getByRole("button", { name: "Archive" }));
    expect(onConfirm).toHaveBeenCalledOnce();
  });

  it("disables cancel and confirmation while pending", () => {
    const onCancel = vi.fn();
    const onConfirm = vi.fn();
    render(
      <ConfirmationDialog
        open
        pending
        title="Archive draft?"
        message="The operation is running."
        confirmText="Archive"
        onCancel={onCancel}
        onConfirm={onConfirm}
      />,
    );

    expect(
      screen
        .getAllByRole("button", {
          name: /\u041e\u0442\u043c\u0435\u043d\u0430/i,
        })
        .some((button) => button.hasAttribute("disabled")),
    ).toBe(true);
    expect(
      screen
        .getAllByRole("button", { name: /Archive/ })
        .some((button) => button.hasAttribute("disabled")),
    ).toBe(true);
    expect(onCancel).not.toHaveBeenCalled();
    expect(onConfirm).not.toHaveBeenCalled();
  });
});
