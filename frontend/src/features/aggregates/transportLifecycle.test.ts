import { afterEach, expect, it, vi } from "vitest";
import { ApiClient } from "../../app/api/client";
import {
  AggregatesApi,
  type Operator,
  type Preview,
  type Validation,
} from "../../app/api/aggregates";
import { CardStore } from "./CardStore";
import { DraftStore } from "./DraftStore";

afterEach(() => vi.unstubAllGlobals());
const anchor = "2026-01-01T00:00:00Z";
const card = {
  id: "confirmed",
  name: "Aggregate",
  source_id: "source",
  source_name: "Source",
  source_type: "kafka",
  rule_hash: "a".repeat(64),
  anchor_utc: anchor,
  original_boundary: anchor,
  timezone: "UTC",
  created_at: anchor,
  created_by: "user",
  state: "deleted",
  next_scheduled_at: anchor,
  aggregate_rule: {
    features: [{ key: "events", name: "Events", op: "count" }],
  },
  skipped_ranges: [],
  duration_seconds: 300,
};
function empty(status: number) {
  return new Response(null, {
    status,
    headers: { "X-Request-ID": "empty-response" },
  });
}
it.each([200, 201, 204, 205])(
  "rejects empty %s through the real aggregate adapter with status and request ID",
  async (status) => {
    const fetch = vi.fn().mockResolvedValue(empty(status));
    vi.stubGlobal("fetch", fetch);
    const api = new AggregatesApi(new ApiClient());
    await expect(api.card("confirmed")).rejects.toMatchObject({
      status,
      code: "invalid_response",
      requestId: "empty-response",
    });
    await expect(api.list()).rejects.toMatchObject({
      status,
      code: "invalid_response",
    });
    await expect(api.historical("confirmed", anchor)).rejects.toMatchObject({
      status,
      code: "invalid_response",
    });
    expect(fetch).toHaveBeenCalledTimes(3);
  },
);
it.each([200, 204, 205])(
  "keeps an unknown mutation locked after empty %s until a verified read",
  async (status) => {
    const fetch = vi
      .fn()
      .mockResolvedValueOnce(Response.json({ ...card, state: "active" }))
      .mockResolvedValueOnce(
        Response.json(
          {
            code: "aggregate_database_error",
            message: "private",
            request_id: "mutation",
            details: null,
          },
          { status: 503 },
        ),
      )
      .mockResolvedValueOnce(empty(status))
      .mockResolvedValueOnce(Response.json({ ...card, state: "deleted" }));
    vi.stubGlobal("fetch", fetch);
    const store = new CardStore(new AggregatesApi(new ApiClient()), card.id);
    await store.load();
    const previous = store.card;
    await store.action("delete");
    expect(store.mutationOutcome).toBe("unknown");
    await store.load();
    expect(store.card).toBe(previous);
    expect(store.error).toMatchObject({
      status,
      code: "invalid_response",
      requestId: "empty-response",
    });
    expect(store.mutationOutcome).toBe("unknown");
    expect(store.canMutate).toBe(false);
    await store.action("delete");
    expect(fetch).toHaveBeenCalledTimes(3);
    await store.load();
    expect(store.card?.state).toBe("deleted");
    expect(store.mutationOutcome).toBeNull();
    expect(store.canMutate).toBe(true);
    expect(store.lifecycle.pending.size).toBe(0);
    expect(
      fetch.mock.calls.filter(([, options]) => options.method === "DELETE"),
    ).toHaveLength(1);
    store.dispose();
  },
);
it.each([200, 201, 204, 205])(
  "retains the exact create body/key after empty %s until a verified card",
  async (status) => {
    const fetch = vi
      .fn()
      .mockResolvedValueOnce(empty(status))
      .mockResolvedValueOnce(Response.json(card, { status: 201 }));
    vi.stubGlobal("fetch", fetch);
    const store = new DraftStore(new AggregatesApi(new ApiClient()));
    store.setOperators([
      { name: "count", kind: "feature", parameters: [] },
    ] as unknown as Operator[]);
    store.setField("name", card.name);
    store.setField("source", card.source_id);
    store.setField("boundary", anchor);
    store.setField("window", anchor);
    store.validation = { rule_hash: card.rule_hash } as Validation;
    store.preview = {} as Preview;
    const text = store.text;
    expect(await store.create()).toBeUndefined();
    expect(store.error).toMatchObject({
      status,
      code: "invalid_response",
      requestId: "empty-response",
    });
    expect(store.unknownCreate).toBe(true);
    expect(store.dirty).toBe(true);
    expect(store.text).toBe(text);
    expect(store.canCreate).toBe(true);
    expect(fetch).toHaveBeenCalledTimes(1);
    expect((await store.create())?.id).toBe(card.id);
    const attempts = fetch.mock.calls.map(([, options]) => ({
      body: options.body,
      key: options.headers.get("Idempotency-Key"),
    }));
    expect(attempts[0].key).toBeTruthy();
    expect(attempts[1]).toEqual(attempts[0]);
    expect(store.dirty).toBe(false);
    expect(store.unknownCreate).toBe(false);
    expect(store.lifecycle.pending.size).toBe(0);
    store.dispose();
  },
);
