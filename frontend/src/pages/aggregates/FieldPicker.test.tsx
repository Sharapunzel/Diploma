import { afterEach, expect, it, vi } from "vitest";
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import type { AggregatesApi, FieldCatalog } from "../../app/api/aggregates";
import { ApiError } from "../../app/api/client";
import userEvent from "@testing-library/user-event";
import { FieldPicker } from "./RuleNodes";
afterEach(cleanup);
function selectKey(input: HTMLElement, key: "ArrowDown" | "Enter") {
  // rc-select reads legacy `which`; jsdom userEvent leaves it at zero.
  const code = key === "ArrowDown" ? 40 : 13;
  fireEvent.keyDown(input, { key, code: key, keyCode: code, which: code });
  fireEvent.keyUp(input, { key, code: key, keyCode: code, which: code });
}
function deferred() {
  let resolve!: (p: FieldCatalog) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<FieldCatalog>((r, fail) => {
    resolve = r;
    reject = fail;
  });
  return { resolve, reject, promise };
}
const page = (name: string) =>
  ({
    items: [
      {
        name,
        description: "Описание ECS",
        type: "long",
        is_array: false,
        supported: true,
        reason: null,
        operators: ["sum"],
        provenance: ["normalizer"],
        normalizer_origin: ["mapping"],
      },
    ],
    total: 1,
    limit: 50,
    offset: 0,
    snapshot_at: "2026-01-01T00:00:00Z",
    catalog_hash: "a".repeat(64),
    observation: { scope: "all_retained_events", complete: true, rows: 0 },
    normalizer: { id: "n", version: 1 },
  }) as FieldCatalog;
it("retains an existing field while discarding late source/query results", async () => {
  const old = deferred(),
    current = deferred(),
    change = vi.fn();
  const api = {
    fieldCatalog: vi
      .fn()
      .mockReturnValueOnce(old.promise)
      .mockReturnValueOnce(current.promise),
  } as unknown as AggregatesApi;
  const context = { api, source: "old", operators: [] };
  const view = render(
    <FieldPicker
      context={context}
      operation="sum"
      value="saved.field"
      onChange={change}
    />,
  );
  await waitFor(() => expect(api.fieldCatalog).toHaveBeenCalledTimes(1));
  view.rerender(
    <FieldPicker
      context={{ ...context, source: "new" }}
      operation="sum"
      value="saved.field"
      onChange={change}
    />,
  );
  await waitFor(() => expect(api.fieldCatalog).toHaveBeenCalledTimes(2));
  expect(vi.mocked(api.fieldCatalog).mock.calls[0][3]?.aborted).toBe(true);
  current.resolve(page("new.field"));
  old.resolve(page("old.field"));
  fireEvent.mouseDown(screen.getByRole("combobox", { name: "Поле ECS" }));
  await screen.findByText(/new.field · Описание ECS/);
  expect(screen.queryByText(/old.field ·/)).toBeNull();
  expect(screen.getAllByText(/saved.field/).length).toBeGreaterThan(0);
  expect(change).not.toHaveBeenCalled();
});

it("keeps keyboard search and the current query result when an earlier response arrives late", async () => {
  const user = userEvent.setup();
  const initial = deferred(),
    old = deferred(),
    current = deferred(),
    change = vi.fn();
  const api = {
    fieldCatalog: vi
      .fn()
      .mockReturnValueOnce(initial.promise)
      .mockReturnValueOnce(old.promise)
      .mockReturnValueOnce(current.promise),
  } as unknown as AggregatesApi;
  render(
    <FieldPicker
      context={{ api, source: "source", operators: [] }}
      operation="sum"
      value="saved.field"
      onChange={change}
    />,
  );
  await waitFor(() => expect(api.fieldCatalog).toHaveBeenCalledTimes(1));
  await act(async () => initial.resolve(page("initial.field")));
  const input = screen.getByRole("combobox", { name: "Поле ECS" });
  input.focus();
  selectKey(input, "ArrowDown");
  await user.keyboard("source.bytes");
  expect(document.activeElement).toBe(input);
  expect((input as HTMLInputElement).value).toBe("source.bytes");
  expect(input.getAttribute("aria-expanded")).toBe("true");
  await waitFor(() => expect(api.fieldCatalog).toHaveBeenCalledTimes(2));
  expect(vi.mocked(api.fieldCatalog).mock.calls[1].slice(0, 3)).toEqual([
    "source",
    "source.bytes",
    0,
  ]);
  await user.keyboard("{Control>}a{/Control}{Backspace}destination.bytes");
  await waitFor(() => expect(api.fieldCatalog).toHaveBeenCalledTimes(3));
  const calls = vi.mocked(api.fieldCatalog).mock.calls;
  expect(calls[1][3]?.aborted).toBe(true);
  expect(calls[2].slice(0, 3)).toEqual(["source", "destination.bytes", 0]);
  current.resolve(page("destination.bytes"));
  await screen.findByRole("option", {
    name: /destination.bytes · Описание ECS/,
  });
  await act(async () => old.resolve(page("source.bytes")));
  expect(screen.queryByRole("option", { name: /source.bytes ·/ })).toBeNull();
  expect((input as HTMLInputElement).value).toBe("destination.bytes");
  expect(document.activeElement).toBe(input);
  expect(input.getAttribute("aria-expanded")).toBe("true");
  expect(screen.getAllByText(/saved.field/).length).toBeGreaterThan(0);
  expect(change).not.toHaveBeenCalled();
  selectKey(input, "ArrowDown");
  selectKey(input, "Enter");
  expect(change).toHaveBeenCalledExactlyOnceWith(
    "destination.bytes",
    expect.objectContaining({
      value: "destination.bytes",
      label: expect.stringContaining("destination.bytes · Описание ECS"),
    }),
  );
});

it("retries a failed catalog locally without replacing the saved field", async () => {
  const user = userEvent.setup();
  const failed = deferred(),
    retried = deferred(),
    change = vi.fn();
  const api = {
    fieldCatalog: vi
      .fn()
      .mockReturnValueOnce(failed.promise)
      .mockReturnValueOnce(retried.promise),
  } as unknown as AggregatesApi;
  render(
    <FieldPicker
      context={{ api, source: "source", operators: [] }}
      operation="sum"
      value="saved.field"
      onChange={change}
    />,
  );
  await waitFor(() => expect(api.fieldCatalog).toHaveBeenCalledTimes(1));
  failed.reject(new ApiError(503, "field_catalog_timeout"));
  await user.click(await screen.findByRole("button", { name: "Повторить" }));
  await waitFor(() => expect(api.fieldCatalog).toHaveBeenCalledTimes(2));
  const calls = vi.mocked(api.fieldCatalog).mock.calls;
  expect(calls[0][3]?.aborted).toBe(true);
  expect(calls[0].slice(0, 3)).toEqual(calls[1].slice(0, 3));
  expect(screen.getAllByText(/saved.field/).length).toBeGreaterThan(0);
  expect(change).not.toHaveBeenCalled();
  retried.resolve(page("source.bytes"));
  const input = screen.getByRole("combobox", { name: "Поле ECS" });
  input.focus();
  selectKey(input, "ArrowDown");
  await screen.findByRole("option", { name: /source.bytes · Описание ECS/ });
  expect(document.activeElement).toBe(input);
  expect(screen.queryByRole("button", { name: "Повторить" })).toBeNull();
  expect(screen.getAllByText(/saved.field/).length).toBeGreaterThan(0);
  expect(change).not.toHaveBeenCalled();
});
