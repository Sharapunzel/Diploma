import { ApiClient, ApiError } from "./client";
import {
  exact,
  isLosslessNumber,
  object,
  parseAggregate,
  printAggregate,
  safeInteger,
  type Json,
  type JsonObject,
} from "./aggregateJson";

export const executionStates = [
  "created",
  "requested",
  "starting",
  "running",
  "stopped",
  "failed",
  "terminated",
] as const;
export type ExecutionState = (typeof executionStates)[number];
export type ModelState = "waiting" | "warming" | "ready" | "error" | "stopped";
export type Reason = { code: string; message: string; factor: string | null };
export type DetectorWindow = {
  index: Json;
  kind: "evaluated" | "skipped";
  reason: Reason | null;
  start_utc: string;
  end_utc: string;
  scheduled_at: string;
  aggregation_started_at: string;
  aggregation_completed_at: string;
  evaluation_started_at: string | null;
  evaluation_completed_at: string | null;
  late: boolean;
};
export type DetectorSummary = {
  id: string;
  name: string;
  aggregate_id: string;
  aggregate_name: string;
  aggregate_state: "active" | "archived" | "deleted";
  source_id: string;
  source_name: string;
  created_at: string;
  execution_state: ExecutionState;
  model_state: ModelState;
  accelerated: boolean;
  accelerated_used: boolean;
  reason: Reason | null;
  last_consumed: DetectorWindow | null;
  last_evaluated: DetectorWindow | null;
  backlog: Json;
  lag_seconds: Json;
};
export type Warmup = {
  cutoff: string;
  step_seconds: Json;
  required_windows: Json;
  planned_windows: Json;
  materialized_windows: Json;
  feature_valid_windows: Json;
  eligible_windows: Json;
  ineligible_windows: Json;
  materialized_coverage_seconds: Json;
  eligible_coverage_seconds: Json;
  readiness: ModelState;
  reason: Reason | null;
  status: "preparing" | "prepared" | "waiting" | "ready";
};
export type DetectorCard = DetectorSummary & {
  config: {
    dimensions: Json;
    trees: Json;
    sample_size: Json;
    output_after: Json;
    seed: Json;
  };
  feature_keys: string[];
  feature_names: string[];
  anchor_utc: string;
  duration_seconds: Json;
  step_seconds: Json;
  revision: Json;
  execution_generation: Json;
  start_request_id: string | null;
  start_requested_at: string | null;
  execution_started_at: string | null;
  stopped_at: string | null;
  updated_at: string;
  last_failure: {
    reason: Reason;
    at: string;
    start_request_id: string | null;
  } | null;
  warmup: Warmup | null;
  recovery: {
    reason: "crash_recovery" | "manual_resume";
    first_index: Json;
    last_index: Json;
    started_at: string;
  } | null;
  next_scheduled_at: string | null;
  waiting_for_point: boolean | null;
};
export type Action = "create" | "start" | "stop";
export type Receipt = {
  operation_id: string;
  detector_id: string;
  action: Action;
  recorded_at: string;
  execution_generation: Json;
  start_request_id: string | null;
};
export type DetectorPage = {
  items: DetectorSummary[];
  total: number;
  limit: number;
  offset: number;
};
export type DetectorInput = {
  aggregate_id: string;
  name: string;
  accelerated: boolean;
  step_seconds: Json;
  config: { trees: Json; sample_size: Json; output_after: Json; seed: Json };
};

function invalid(): never {
  throw new ApiError(200, "invalid_response");
}
function record(v: unknown): JsonObject {
  if (!object(v)) invalid();
  return v;
}
function str(v: unknown) {
  if (typeof v !== "string") invalid();
}
function uuid(v: unknown) {
  if (
    typeof v !== "string" ||
    !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(v)
  )
    invalid();
}
function nullable(v: unknown, check: (v: unknown) => void) {
  if (v !== null) check(v);
}
function date(v: unknown) {
  if (
    typeof v !== "string" ||
    !/(Z|[+-]\d\d:\d\d)$/.test(v) ||
    !Number.isFinite(Date.parse(v))
  )
    invalid();
}
function bool(v: unknown) {
  if (typeof v !== "boolean") invalid();
}
function member(v: unknown, values: readonly string[]) {
  if (typeof v !== "string" || !values.includes(v)) invalid();
}
function integer(v: unknown, minimum?: bigint) {
  if (
    !(
      isLosslessNumber(v) ||
      (typeof v === "number" && Number.isSafeInteger(v))
    ) ||
    !/^-?\d+$/.test(exact(v))
  )
    invalid();
  if (minimum !== undefined && BigInt(exact(v)) < minimum) invalid();
}
function number(v: unknown) {
  if (
    !(isLosslessNumber(v) || typeof v === "number") ||
    !Number.isFinite(Number(exact(v))) ||
    Number(exact(v)) < 0
  )
    invalid();
}
function reason(v: unknown) {
  const r = record(v);
  str(r.code);
  str(r.message);
  nullable(r.factor, str);
}
const models = ["waiting", "warming", "ready", "error", "stopped"];
function windowDto(v: unknown) {
  const r = record(v);
  integer(r.index);
  member(r.kind, ["evaluated", "skipped"]);
  nullable(r.reason, reason);
  for (const k of [
    "start_utc",
    "end_utc",
    "scheduled_at",
    "aggregation_started_at",
    "aggregation_completed_at",
  ])
    date(r[k]);
  for (const k of ["evaluation_started_at", "evaluation_completed_at"])
    nullable(r[k], date);
  bool(r.late);
}
function summary(v: unknown) {
  const r = record(v);
  for (const k of ["id", "aggregate_id", "source_id"]) uuid(r[k]);
  for (const k of ["name", "aggregate_name", "source_name"]) str(r[k]);
  date(r.created_at);
  member(r.execution_state, executionStates);
  member(r.model_state, models);
  member(r.aggregate_state, ["active", "archived", "deleted"]);
  bool(r.accelerated);
  bool(r.accelerated_used);
  nullable(r.reason, reason);
  nullable(r.last_consumed, windowDto);
  nullable(r.last_evaluated, windowDto);
  nullable(r.backlog, (v) => integer(v, 0n));
  nullable(r.lag_seconds, number);
}
export function decodeCard(body: string, id: string): DetectorCard {
  const r = record(parseAggregate(body));
  summary(r);
  if ((r.id as string).toLowerCase() !== id.toLowerCase()) invalid();
  const c = record(r.config);
  for (const k of ["dimensions", "trees", "sample_size", "output_after"])
    integer(c[k], 1n);
  if (
    BigInt(exact(c.sample_size)) < 2n ||
    BigInt(exact(c.output_after)) < BigInt(exact(c.sample_size))
  )
    invalid();
  integer(c.seed);
  if (
    BigInt(exact(c.seed)) < -(2n ** 63n) ||
    BigInt(exact(c.seed)) > 2n ** 63n - 1n
  )
    invalid();
  for (const k of ["feature_keys", "feature_names"])
    if (
      !Array.isArray(r[k]) ||
      !(r[k] as Json[]).every((v) => typeof v === "string") ||
      BigInt((r[k] as Json[]).length) !== BigInt(exact(c.dimensions))
    )
      invalid();
  date(r.anchor_utc);
  date(r.updated_at);
  for (const k of ["duration_seconds", "step_seconds"]) integer(r[k], 1n);
  if (
    BigInt(exact(r.duration_seconds)) > 604800n ||
    BigInt(exact(r.step_seconds)) > BigInt(exact(r.duration_seconds))
  )
    invalid();
  for (const k of ["revision", "execution_generation"]) integer(r[k], 0n);
  nullable(r.start_request_id, uuid);
  for (const k of [
    "start_requested_at",
    "execution_started_at",
    "stopped_at",
    "next_scheduled_at",
  ])
    nullable(r[k], date);
  nullable(r.waiting_for_point, bool);
  nullable(r.last_failure, (v) => {
    const f = record(v);
    reason(f.reason);
    date(f.at);
    nullable(f.start_request_id, uuid);
  });
  nullable(r.recovery, (v) => {
    const f = record(v);
    member(f.reason, ["crash_recovery", "manual_resume"]);
    integer(f.first_index);
    integer(f.last_index);
    date(f.started_at);
  });
  nullable(r.warmup, (v) => {
    const w = record(v);
    date(w.cutoff);
    for (const k of [
      "step_seconds",
      "required_windows",
      "planned_windows",
      "materialized_windows",
      "feature_valid_windows",
      "eligible_windows",
      "ineligible_windows",
    ])
      integer(w[k], 0n);
    for (const k of [
      "materialized_coverage_seconds",
      "eligible_coverage_seconds",
    ])
      number(w[k]);
    member(w.readiness, models);
    member(w.status, ["preparing", "prepared", "waiting", "ready"]);
    nullable(w.reason, reason);
  });
  return r as unknown as DetectorCard;
}
export function decodePage(body: string): DetectorPage {
  const r = record(parseAggregate(body));
  if (!Array.isArray(r.items)) invalid();
  r.items.forEach(summary);
  const total = safeInteger(r.total),
    limit = safeInteger(r.limit),
    offset = safeInteger(r.offset);
  if (
    total < 0 ||
    limit < 1 ||
    limit > 100 ||
    offset < 0 ||
    offset > 100000 ||
    r.items.length > limit
  )
    invalid();
  return {
    items: r.items as unknown as DetectorSummary[],
    total,
    limit,
    offset,
  };
}
export function decodeReceipt(
  body: string,
  action: Action,
  id?: string,
): Receipt {
  const r = record(parseAggregate(body));
  uuid(r.operation_id);
  uuid(r.detector_id);
  if (
    r.action !== action ||
    (id && (r.detector_id as string).toLowerCase() !== id.toLowerCase())
  )
    invalid();
  date(r.recorded_at);
  integer(r.execution_generation, 0n);
  nullable(r.start_request_id, uuid);
  return r as unknown as Receipt;
}
export class DetectorsApi {
  constructor(private client: ApiClient) {}
  list(
    params: {
      limit: number;
      offset: number;
      aggregate_id?: string;
      execution_state?: string;
    },
    signal?: AbortSignal,
  ) {
    return this.client.request<DetectorPage>(
      `/detectors?${new URLSearchParams(Object.entries(params).map(([k, v]) => [k, String(v)]))}`,
      {
        signal,
        decodeResponse: (body) => {
          const page = decodePage(body);
          if (page.limit !== params.limit || page.offset !== params.offset)
            invalid();
          return page;
        },
      },
    );
  }
  card(id: string, signal?: AbortSignal) {
    return this.client.request<DetectorCard>(
      `/detectors/${encodeURIComponent(id)}`,
      { signal, decodeResponse: (body) => decodeCard(body, id) },
    );
  }
  command(
    action: Action,
    id: string | null,
    body: DetectorInput | Record<string, never>,
    key: string,
    signal?: AbortSignal,
  ) {
    return this.client.request<Receipt>(
      action === "create"
        ? "/detectors"
        : `/detectors/${encodeURIComponent(id!)}/${action}`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json", "Idempotency-Key": key },
        body: printAggregate(body),
        signal,
        decodeResponse: (text) => decodeReceipt(text, action, id ?? undefined),
      },
    );
  }
}
