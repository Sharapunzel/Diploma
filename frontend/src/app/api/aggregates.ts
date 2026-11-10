import { ApiClient, ApiError } from "./client";
import {
  object,
  parseAggregate,
  printAggregate,
  safeInteger,
  isLosslessNumber,
  type JsonObject,
  type Json,
} from "./aggregateJson";

export type Schedule = {
  boundary: string;
  timezone: string;
  duration_seconds: number;
};
export type AggregateInput = {
  source_id: string;
  schedule: Schedule;
  aggregate_rule: JsonObject;
};
export type PreviewInput = AggregateInput & {
  window_start_utc: string;
  time_basis: string;
};
export type CreateInput = PreviewInput & { name: string; rule_hash: string };
export type Operator = {
  name: string;
  kind: string;
  inputs: string[];
  parameters: string[];
  parameter_types: Record<string, string>;
  compatible_fields: { type: string; is_array: boolean }[];
  arity: { min: number; max: number | null } | null;
  weight: Json;
  weight_formula: string;
  result_nullable: boolean;
  result: string;
};
export type AggregateField = {
  name: string;
  type: string;
  is_array: boolean;
  description: string;
  supported: boolean;
  reason: string | null;
  operators: string[];
};
export type Validation = {
  rule_hash: string;
  anchor_utc: string;
  duration_seconds: number;
  feature_keys: string[];
  budget: JsonObject;
};
export type FeatureValue = {
  key: string;
  value: Json;
  reason: string | null;
  valid_count: Json;
  invalid_count: Json;
};
export type Preview = {
  rule_hash: string;
  start_utc: string;
  end_utc: string;
  scheduled_at: string;
  observed_rows: Json;
  event_count: Json;
  event_time_min: string | null;
  event_time_max: string | null;
  features: FeatureValue[];
  completeness: string;
  completeness_reason: string;
  time_basis: string;
  budget: JsonObject;
};
export type Aggregate = {
  id: string;
  name: string;
  source_id: string;
  source_name: string;
  source_type: string;
  aggregate_rule: JsonObject;
  rule_hash: string;
  schedule_id: string;
  anchor_utc: string;
  duration_seconds: number;
  original_boundary: string;
  timezone: string;
  created_at: string;
  created_by: string;
  state: string;
  next_scheduled_at: string;
  next_index: Json;
  skipped_ranges: Json[];
  lag_seconds: Json;
  last_error: string | null;
  last_error_at: string | null;
};
export type Point = Omit<
  Preview,
  "rule_hash" | "budget" | "observed_rows" | "features"
> & {
  id: string;
  aggregate_id: string;
  kind: string;
  actual_started_at: string;
  actual_completed_at: string;
  features: FeatureValue[] | null;
};
export type AggregatePage<T> = { items: T[]; total: number };

function invalid(): never {
  throw new ApiError(200, "invalid_response");
}
function numeric(value: unknown, nullable = false): boolean {
  if (nullable && value === null) return true;
  return typeof value === "number"
    ? Number.isFinite(value)
    : isLosslessNumber(value) && Number.isFinite(Number(value.value));
}
function strings(value: unknown): value is string[] {
  return Array.isArray(value) && value.every((v) => typeof v === "string");
}
function count(value: unknown): boolean {
  return (
    numeric(value) &&
    Number(exactNumeric(value)) >= 0 &&
    Number.isInteger(Number(exactNumeric(value)))
  );
}
function exactNumeric(value: unknown): unknown {
  return isLosslessNumber(value) ? value.value : value;
}
function eventRange(v: JsonObject) {
  for (const key of ["event_time_min", "event_time_max"])
    if (v[key] !== null && typeof v[key] !== "string") invalid();
  if (!count(v.event_count)) invalid();
}
function record(value: unknown): JsonObject {
  if (!object(value)) invalid();
  return value;
}
function features(value: unknown, nullable = false) {
  if (nullable && value === null) return;
  if (
    !Array.isArray(value) ||
    !value.every(
      (v) =>
        object(v) &&
        typeof v.key === "string" &&
        numeric(v.value, true) &&
        (v.reason === null || typeof v.reason === "string") &&
        count(v.valid_count) &&
        count(v.invalid_count),
    )
  )
    invalid();
}
function card(value: unknown): Aggregate {
  const v = record(value);
  for (const key of [
    "id",
    "name",
    "source_id",
    "source_name",
    "source_type",
    "rule_hash",
    "anchor_utc",
    "original_boundary",
    "timezone",
    "created_at",
    "created_by",
    "state",
    "next_scheduled_at",
  ])
    if (typeof v[key] !== "string") invalid();
  record(v.aggregate_rule);
  if (!Array.isArray(v.skipped_ranges)) invalid();
  return {
    ...v,
    duration_seconds: safeInteger(v.duration_seconds),
  } as unknown as Aggregate;
}
function preview(value: unknown): Preview {
  const v = record(value);
  for (const key of [
    "rule_hash",
    "start_utc",
    "end_utc",
    "scheduled_at",
    "completeness",
    "completeness_reason",
    "time_basis",
  ])
    if (typeof v[key] !== "string") invalid();
  features(v.features);
  eventRange(v);
  if (!count(v.observed_rows)) invalid();
  record(v.budget);
  return v as unknown as Preview;
}
function point(value: unknown): Point {
  const v = record(value);
  for (const key of [
    "id",
    "aggregate_id",
    "kind",
    "start_utc",
    "end_utc",
    "scheduled_at",
    "actual_started_at",
    "actual_completed_at",
    "completeness",
    "completeness_reason",
    "time_basis",
  ])
    if (typeof v[key] !== "string") invalid();
  features(v.features, true);
  eventRange(v);
  if (!["working", "historical"].includes(String(v.kind))) invalid();
  return v as unknown as Point;
}
function page<T>(value: unknown, decode: (v: unknown) => T): AggregatePage<T> {
  const v = record(value);
  if (!Array.isArray(v.items)) invalid();
  const total = safeInteger(v.total);
  if (total < 0) invalid();
  return { items: v.items.map(decode), total };
}
export class AggregatesApi {
  constructor(private api: ApiClient) {}
  private async request<T>(
    path: string,
    decode: (value: unknown) => T,
    signal?: AbortSignal,
    body?: unknown,
    method = "GET",
    key?: string,
  ) {
    return this.api.request<T>(`/aggregates${path}`, {
      signal,
      method,
      decodeResponse: (body) => decode(parseAggregate(body)),
      ...(body !== undefined
        ? {
            body: printAggregate(body),
            headers: {
              "Content-Type": "application/json",
              ...(key ? { "Idempotency-Key": key } : {}),
            },
          }
        : {}),
    });
  }
  list(
    offset = 0,
    signal?: AbortSignal,
    filters: { limit?: number; q?: string; state?: "active" | "archived" } = {},
  ) {
    const query = new URLSearchParams({
      limit: String(filters.limit ?? 50),
      offset: String(offset),
    });
    if (filters.q?.trim()) query.set("q", filters.q.trim());
    if (filters.state) query.set("state", filters.state);
    return this.request(`?${query}`, (v) => page(v, card), signal);
  }
  card(id: string, signal?: AbortSignal) {
    return this.request(`/${encodeURIComponent(id)}`, card, signal);
  }
  points(id: string, offset = 0, signal?: AbortSignal) {
    return this.request(
      `/${encodeURIComponent(id)}/points?limit=50&offset=${offset}`,
      (v) => page(v, point),
      signal,
    );
  }
  operators(signal?: AbortSignal) {
    return this.request(
      "/operators",
      (v) => {
        if (!Array.isArray(v)) invalid();
        return v.map((item) => {
          const o = record(item);
          if (
            typeof o.name !== "string" ||
            typeof o.kind !== "string" ||
            !strings(o.parameters) ||
            !strings(o.inputs) ||
            !Array.isArray(o.compatible_fields)
          )
            invalid();
          return {
            ...o,
            arity:
              o.arity === null
                ? null
                : {
                    min: safeInteger(record(o.arity).min),
                    max:
                      record(o.arity).max === null
                        ? null
                        : safeInteger(record(o.arity).max),
                  },
          } as unknown as Operator;
        });
      },
      signal,
    );
  }
  fields(id: string, q = "", offset = 0, signal?: AbortSignal) {
    return this.request(
      `/sources/${encodeURIComponent(id)}/fields?q=${encodeURIComponent(q)}&limit=100&offset=${offset}`,
      (v) =>
        page(v, (item) => {
          const f = record(item);
          if (
            typeof f.name !== "string" ||
            typeof f.type !== "string" ||
            typeof f.is_array !== "boolean" ||
            typeof f.supported !== "boolean" ||
            !strings(f.operators)
          )
            invalid();
          return f as unknown as AggregateField;
        }),
      signal,
    );
  }
  validate(body: AggregateInput, signal?: AbortSignal) {
    return this.request(
      "/validate",
      (v) => {
        const r = record(v);
        if (
          typeof r.rule_hash !== "string" ||
          typeof r.anchor_utc !== "string" ||
          !strings(r.feature_keys)
        )
          invalid();
        record(r.budget);
        return {
          ...r,
          duration_seconds: safeInteger(r.duration_seconds),
        } as unknown as Validation;
      },
      signal,
      body,
      "POST",
    );
  }
  preview(body: PreviewInput, signal?: AbortSignal) {
    return this.request("/preview", preview, signal, body, "POST");
  }
  create(body: CreateInput, key: string, signal?: AbortSignal) {
    return this.request("", card, signal, body, "POST", key);
  }
  copy(id: string, signal?: AbortSignal) {
    return this.request(
      `/${encodeURIComponent(id)}/copy`,
      (v) => {
        const r = record(v);
        if (typeof r.source_id !== "string") invalid();
        record(r.aggregate_rule);
        const s = record(r.schedule);
        if (typeof s.boundary !== "string" || typeof s.timezone !== "string")
          invalid();
        return {
          ...r,
          schedule: { ...s, duration_seconds: safeInteger(s.duration_seconds) },
        } as unknown as AggregateInput;
      },
      signal,
    );
  }
  lifecycle(
    id: string,
    action: "archive" | "resume" | "delete",
    signal?: AbortSignal,
  ) {
    return this.request(
      `/${encodeURIComponent(id)}${action === "delete" ? "" : `/${action}`}`,
      card,
      signal,
      undefined,
      action === "delete" ? "DELETE" : "POST",
    );
  }
  historical(id: string, start: string, signal?: AbortSignal) {
    return this.request(
      `/${encodeURIComponent(id)}/historical-points`,
      point,
      signal,
      { window_start_utc: start },
      "POST",
    );
  }
}
