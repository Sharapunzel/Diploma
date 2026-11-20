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
export type TimeBasis =
  | "event_time"
  | "availability_at"
  | "backend_processed_at_historical";
export type SemanticBasis = "event_time" | "availability_v1";
export type ExecutionMode = "current" | "retired";
export type ExecutionPolicy = {
  policy_version: "window_execution_v1";
  soft_latency_seconds: Json;
  hard_budget_seconds: Json;
};
export type ExecutionDiagnostics = ExecutionPolicy & {
  calculation_status: "computed" | "uncomputed";
  calculation_reason: "execution_budget_exceeded" | null;
  attempt_id: string;
  origin: "preview" | "initial_history" | "online";
  window_index: Json;
  start_utc: string;
  end_utc: string;
  started_at: string;
  execution_deadline_at: string;
  measured_at: string;
  measured_through: "before_commit" | "preview_complete" | "finalization";
  elapsed_seconds: Json;
  queue_seconds: Json;
  online_latency_seconds: Json;
  soft_latency_exceeded: boolean | null;
  timings: JsonObject;
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
export type SourceField = AggregateField & {
  provenance: ("observed" | "normalizer")[];
  normalizer_origin: ("mapping" | "system")[];
};
export type FieldCatalog = AggregatePage<SourceField> & {
  limit: number;
  offset: number;
  snapshot_at: string;
  catalog_hash: string;
  observation: { scope: "all_retained_events"; complete: true; rows: Json };
  normalizer: { id: string; version: number } | null;
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
  calculation_status: "computed";
  calculation_reason: null;
  execution: ExecutionDiagnostics;
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
  snapshot_reference_at: string | null;
  close_not_before_at: string | null;
  origin: "preview";
  in_process: boolean;
  snapshot_metadata: JsonObject;
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
  archived_at: string | null;
  deleted_at: string | null;
  deleted_by: string | null;
  semantic_basis: SemanticBasis;
  execution_mode: ExecutionMode;
  execution_reason: string | null;
  bootstrap_at: string | null;
  online_start_index: Json;
  paused: boolean;
  execution_generation: Json;
  active_cleanup_id: string | null;
  next_close_not_before_at: string | null;
  diagnostics: JsonObject;
  auto_cleanup: AutoCleanup;
  execution_policy: ExecutionPolicy;
  last_execution: ExecutionDiagnostics | null;
};
export type AutoCleanup = {
  policy_version: "model_history_v1" | null;
  status:
    | "scheduled"
    | "idle"
    | "compacting"
    | "protected"
    | "deferred"
    | "error"
    | "inactive";
  reason: string | null;
  checked_at: string | null;
  next_check_at: string | null;
  reserve_windows: Json;
  keep_from_index: Json;
  protected_from_index: Json;
  auto_cleared_total: Json;
  history_revision: Json;
  last_cleared_at: string | null;
  retained_from_index: Json;
  retained_to_index: Json;
  protected_total: Json;
  protected_by: {
    detector_id: string;
    name: string;
    reason:
      | "unconsumed_points"
      | "frozen_warmup"
      | "prepared_warmup_continuation"
      | "planned_warmup"
      | "insufficient_warmup_history";
    protected_from_index: Json;
    protected_to_index: Json;
  }[];
};
export type ComputedPoint = Omit<
  Preview,
  | "rule_hash"
  | "budget"
  | "observed_rows"
  | "features"
  | "origin"
  | "in_process"
  | "execution"
> & {
  execution: ExecutionDiagnostics | null;
  id: string;
  aggregate_id: string;
  kind: string;
  actual_started_at: string;
  actual_completed_at: string;
  features: FeatureValue[] | null;
  origin: "initial_history" | "online" | "legacy";
  window_index: Json;
  payload_state: "present" | "cleared";
  evidence_refs: JsonObject;
};
export type UncomputedPoint = Omit<
  ComputedPoint,
  | "calculation_status"
  | "calculation_reason"
  | "execution"
  | "event_count"
  | "event_time_min"
  | "event_time_max"
  | "features"
  | "completeness"
  | "completeness_reason"
  | "snapshot_reference_at"
  | "snapshot_metadata"
  | "evidence_refs"
  | "payload_state"
> & {
  calculation_status: "uncomputed";
  calculation_reason: "execution_budget_exceeded";
  execution: ExecutionDiagnostics;
  event_count: null;
  event_time_min: null;
  event_time_max: null;
  features: null;
  completeness: null;
  completeness_reason: null;
  snapshot_reference_at: null;
  snapshot_metadata: null;
  evidence_refs: null;
  payload_state: "present";
};
export type Point = ComputedPoint | UncomputedPoint;
export type AggregateAction = "pause" | "unpause" | "cleanup" | "cleanup_retry";
export type AggregateCommandReceipt = {
  operation_id: string;
  aggregate_id: string;
  action: AggregateAction;
  recorded_at: string;
  execution_generation: Json;
  cleanup_operation_id: string | null;
};
export type CleanupBlocker = {
  detector_id: string;
  name: string;
  reason: string;
  minimum_keep: Json;
  protected_start_index: Json;
  protected_end_index: Json;
};
export type CleanupProtection = {
  aggregate_id: string;
  reference_at: string;
  requested_keep: Json;
  total_points: Json;
  minimum_keep: Json;
  maximum_delete: Json;
  planned_delete: Json;
  tail_identity: JsonObject | null;
  can_cleanup: boolean;
  reason: string | null;
  blockers: CleanupBlocker[];
};
export type CleanupOperation = {
  id: string;
  aggregate_id: string;
  created_at: string;
  requested_keep: Json;
  reference_tail: JsonObject | null;
  planned_delete: Json;
  deleted_points: Json;
  deleted_bytes_if_measured: Json;
  completed_at: string | null;
  status: "pending" | "running" | "succeeded" | "failed";
  safe_error: string | null;
  progress: JsonObject;
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
  return integer(value) && BigInt(String(exactNumeric(value))) >= 0n;
}
function integer(value: unknown, nullable = false): boolean {
  if (value === null) return nullable;
  return typeof value === "number"
    ? Number.isSafeInteger(value)
    : isLosslessNumber(value) && /^-?(?:0|[1-9][0-9]*)$/.test(value.value);
}
function exactNumeric(value: unknown): unknown {
  return isLosslessNumber(value) ? value.value : value;
}
function uuid(value: unknown, nullable = false): boolean {
  return (
    (nullable && value === null) ||
    (typeof value === "string" &&
      /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(
        value,
      ))
  );
}
function instant(value: unknown, nullable = false): boolean {
  if (nullable && value === null) return true;
  if (typeof value !== "string") return false;
  const match =
    /^(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.[0-9]+)?(?:Z|\+00:00)$/.exec(value);
  if (!match || match[1].startsWith("0000")) return false;
  const whole = Date.parse(match[1] + "Z");
  return (
    Number.isFinite(whole) &&
    new Date(whole).toISOString().slice(0, 19) === match[1]
  );
}
function nullableString(value: unknown): boolean {
  return value === null || typeof value === "string";
}
/** User input is decimal only. Keep int64 as a decimal string until lossless JSON encoding. */
export function keepLastDecimal(value: string): string {
  if (!/^[0-9]+$/.test(value))
    throw new Error(
      "Укажите целое число от 0 до 9223372036854775807 без знака и дробной части.",
    );
  const normalized = value.replace(/^0+(?=.)/, "");
  if (normalized.length > 19 || BigInt(normalized) > 9223372036854775807n)
    throw new Error("Число сохраняемых точек превышает 9223372036854775807.");
  return normalized;
}
function snapshot(v: JsonObject) {
  if (
    !instant(v.snapshot_reference_at, true) ||
    !instant(v.close_not_before_at, true)
  )
    invalid();
  record(v.snapshot_metadata);
}
function eventRange(v: JsonObject) {
  for (const key of ["event_time_min", "event_time_max"])
    if (!instant(v[key], true)) invalid();
  if (!count(v.event_count)) invalid();
}
function record(value: unknown): JsonObject {
  if (!object(value)) invalid();
  return value;
}
function seconds(value: unknown, nullable = false): boolean {
  return value === null
    ? nullable
    : numeric(value) && Number(exactNumeric(value)) >= 0;
}
function executionPolicy(value: unknown, duration?: number) {
  const v = record(value);
  if (
    v.policy_version !== "window_execution_v1" ||
    !numeric(v.soft_latency_seconds) ||
    Number(exactNumeric(v.soft_latency_seconds)) !== 5 ||
    !seconds(v.hard_budget_seconds) ||
    Number(exactNumeric(v.hard_budget_seconds)) <= 0 ||
    Number(exactNumeric(v.hard_budget_seconds)) > 30 ||
    (duration !== undefined &&
      Number(exactNumeric(v.hard_budget_seconds)) !==
        Math.min(30, (4 * duration) / 5))
  )
    invalid();
}
function execution(value: unknown): ExecutionDiagnostics {
  const v = record(value);
  executionPolicy(v);
  if (
    !uuid(v.attempt_id) ||
    !["computed", "uncomputed"].includes(String(v.calculation_status)) ||
    (v.calculation_status === "computed"
      ? v.calculation_reason !== null
      : v.calculation_reason !== "execution_budget_exceeded") ||
    !["preview", "initial_history", "online"].includes(String(v.origin)) ||
    !integer(v.window_index, v.origin === "preview") ||
    (v.origin !== "preview" && !count(v.window_index)) ||
    !["before_commit", "preview_complete", "finalization"].includes(
      String(v.measured_through),
    ) ||
    !seconds(v.elapsed_seconds) ||
    !seconds(v.queue_seconds, true) ||
    !seconds(v.online_latency_seconds, true) ||
    (v.soft_latency_exceeded !== null &&
      typeof v.soft_latency_exceeded !== "boolean")
  )
    invalid();
  for (const key of [
    "start_utc",
    "end_utc",
    "started_at",
    "execution_deadline_at",
    "measured_at",
  ])
    if (!instant(v[key])) invalid();
  if (Date.parse(String(v.end_utc)) <= Date.parse(String(v.start_utc)))
    invalid();
  const timings = record(v.timings);
  const phases = [
    "selection",
    "manifest",
    "statistics",
    "interpret",
    "cohorts",
    "snapshot",
    "save",
    "commit",
  ];
  if (
    Object.entries(timings).some(
      ([key, value]) => !phases.includes(key) || !seconds(value),
    )
  )
    invalid();
  return v as unknown as ExecutionDiagnostics;
}
function calculation(v: JsonObject, nullableExecution: boolean) {
  if (!["computed", "uncomputed"].includes(String(v.calculation_status)))
    invalid();
  if (
    v.calculation_status === "computed"
      ? v.calculation_reason !== null
      : v.calculation_reason !== "execution_budget_exceeded"
  )
    invalid();
  if (v.execution === null) {
    if (!nullableExecution || v.calculation_status !== "computed") invalid();
    return;
  }
  const measured = execution(v.execution);
  if (
    measured.calculation_status !== v.calculation_status ||
    measured.calculation_reason !== v.calculation_reason ||
    measured.origin !== v.origin ||
    measured.start_utc !== v.start_utc ||
    measured.end_utc !== v.end_utc
  )
    invalid();
}
/** Only validated preview diagnostics can supply the draft's measured fact. */
export function previewExecutionFailure(
  error: unknown,
): ExecutionDiagnostics | null {
  if (
    !(error instanceof ApiError) ||
    error.status !== 422 ||
    error.code !== "execution_budget_exceeded" ||
    !object(error.details)
  )
    return null;
  try {
    const measured = execution(error.details.execution);
    return measured.origin === "preview" &&
      measured.calculation_status === "uncomputed"
      ? measured
      : null;
  } catch {
    return null;
  }
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
  ])
    if (typeof v[key] !== "string") invalid();
  for (const key of ["id", "source_id", "schedule_id", "created_by"])
    if (!uuid(v[key])) invalid();
  for (const key of ["anchor_utc", "created_at", "next_scheduled_at"])
    if (!instant(v[key])) invalid();
  for (const key of [
    "archived_at",
    "deleted_at",
    "last_error_at",
    "bootstrap_at",
    "next_close_not_before_at",
  ])
    if (!instant(v[key], true)) invalid();
  if (
    !uuid(v.deleted_by, true) ||
    !uuid(v.active_cleanup_id, true) ||
    !["active", "archived", "deleted"].includes(String(v.state)) ||
    !["event_time", "availability_v1"].includes(String(v.semantic_basis)) ||
    !["current", "retired"].includes(String(v.execution_mode)) ||
    !nullableString(v.execution_reason) ||
    !nullableString(v.last_error) ||
    typeof v.paused !== "boolean" ||
    !count(v.next_index) ||
    !integer(v.online_start_index, true) ||
    !count(v.execution_generation) ||
    !numeric(v.lag_seconds) ||
    Number(exactNumeric(v.lag_seconds)) < 0
  )
    invalid();
  record(v.aggregate_rule);
  autoCleanup(v.auto_cleanup);
  const diagnostics = record(v.diagnostics);
  if (
    diagnostics.exclusions !== undefined &&
    !Object.values(record(diagnostics.exclusions)).every(count)
  )
    invalid();
  if (diagnostics.late_count !== undefined && !count(diagnostics.late_count))
    invalid();
  for (const key of ["late_start", "late_end"])
    if (diagnostics[key] !== undefined && !instant(diagnostics[key], true))
      invalid();
  if (
    diagnostics.late_reason !== undefined &&
    !nullableString(diagnostics.late_reason)
  )
    invalid();
  if (!Array.isArray(v.skipped_ranges)) invalid();
  v.skipped_ranges.forEach(record);
  const duration = safeInteger(v.duration_seconds);
  if (duration < 1 || duration > 604800) invalid();
  executionPolicy(v.execution_policy, duration);
  if (v.last_execution !== null) {
    const last = execution(v.last_execution);
    if (last.origin === "preview") invalid();
    executionPolicy(last, duration);
  }
  return {
    ...v,
    duration_seconds: duration,
  } as unknown as Aggregate;
}
function autoCleanup(value: unknown) {
  const v = record(value);
  if (
    ![
      "scheduled",
      "idle",
      "compacting",
      "protected",
      "deferred",
      "error",
      "inactive",
    ].includes(String(v.status)) ||
    !nullableString(v.reason)
  )
    invalid();
  const times = ["checked_at", "next_check_at", "last_cleared_at"];
  const numbers = [
    "reserve_windows",
    "keep_from_index",
    "protected_from_index",
    "auto_cleared_total",
    "history_revision",
    "retained_from_index",
    "retained_to_index",
    "protected_total",
  ];
  for (const key of times) if (!instant(v[key], true)) invalid();
  const index = (item: unknown) =>
    item === null ||
    (integer(item) &&
      BigInt(String(exactNumeric(item))) >= 0n &&
      BigInt(String(exactNumeric(item))) <= 9223372036854775807n);
  for (const key of numbers) if (!index(v[key])) invalid();
  const range = (start: unknown, end: unknown) =>
    start === null ||
    end === null ||
    BigInt(String(exactNumeric(start))) <= BigInt(String(exactNumeric(end)));
  if (!Array.isArray(v.protected_by) || v.protected_by.length > 10) invalid();
  for (const item of v.protected_by) {
    const blocker = record(item);
    if (
      !uuid(blocker.detector_id) ||
      typeof blocker.name !== "string" ||
      !blocker.name.length ||
      blocker.name.length > 200 ||
      ![
        "unconsumed_points",
        "frozen_warmup",
        "prepared_warmup_continuation",
        "planned_warmup",
        "insufficient_warmup_history",
      ].includes(String(blocker.reason)) ||
      blocker.protected_from_index === null ||
      !index(blocker.protected_from_index) ||
      !index(blocker.protected_to_index) ||
      !range(blocker.protected_from_index, blocker.protected_to_index)
    )
      invalid();
  }
  if (v.policy_version === null) {
    if (
      v.status !== "error" ||
      v.reason !== "retention_state_missing" ||
      v.protected_by.length ||
      [...times, ...numbers].some((key) => v[key] !== null)
    )
      invalid();
  } else {
    if (
      v.policy_version !== "model_history_v1" ||
      v.auto_cleared_total === null ||
      v.history_revision === null ||
      (v.reserve_windows !== null &&
        (BigInt(String(exactNumeric(v.reserve_windows))) < 512n ||
          BigInt(String(exactNumeric(v.reserve_windows))) > 4096n)) ||
      BigInt(String(exactNumeric(v.auto_cleared_total))) >
        BigInt(String(exactNumeric(v.history_revision))) ||
      (v.retained_from_index === null) !== (v.retained_to_index === null) ||
      !range(v.retained_from_index, v.retained_to_index) ||
      (v.protected_total === null
        ? v.protected_by.length !== 0
        : BigInt(String(exactNumeric(v.protected_total))) <
          BigInt(v.protected_by.length))
    )
      invalid();
  }
}
function preview(value: unknown): Preview {
  const v = record(value);
  calculation(v, false);
  if (v.calculation_status !== "computed") invalid();
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
  for (const key of ["start_utc", "end_utc", "scheduled_at"])
    if (!instant(v[key])) invalid();
  if (
    v.origin !== "preview" ||
    typeof v.in_process !== "boolean" ||
    ![
      "event_time",
      "availability_at",
      "backend_processed_at_historical",
    ].includes(String(v.time_basis)) ||
    !["complete", "unknown", "interrupted", "unverified"].includes(
      String(v.completeness),
    )
  )
    invalid();
  snapshot(v);
  if (!count(v.observed_rows)) invalid();
  executionPolicy(record(v.budget).execution_policy);
  return v as unknown as Preview;
}
function point(value: unknown): Point {
  const v = record(value);
  calculation(v, true);
  const uncomputed = v.calculation_status === "uncomputed";
  for (const key of [
    "id",
    "aggregate_id",
    "kind",
    "start_utc",
    "end_utc",
    "scheduled_at",
    "actual_started_at",
    "actual_completed_at",
    "time_basis",
  ])
    if (typeof v[key] !== "string") invalid();
  if (uncomputed) {
    for (const key of [
      "event_count",
      "event_time_min",
      "event_time_max",
      "features",
      "completeness",
      "completeness_reason",
      "snapshot_reference_at",
      "snapshot_metadata",
      "evidence_refs",
    ])
      if (v[key] !== null) invalid();
    if (
      v.kind !== "working" ||
      v.time_basis !== "event_time" ||
      v.payload_state !== "present" ||
      v.origin === "legacy" ||
      !count(v.window_index)
    )
      invalid();
  } else {
    features(v.features, true);
    eventRange(v);
    if (
      typeof v.completeness !== "string" ||
      typeof v.completeness_reason !== "string"
    )
      invalid();
    snapshot(v);
    record(v.evidence_refs);
  }
  if (!["working", "historical"].includes(String(v.kind))) invalid();
  for (const key of ["id", "aggregate_id"]) if (!uuid(v[key])) invalid();
  for (const key of [
    "start_utc",
    "end_utc",
    "scheduled_at",
    "actual_started_at",
    "actual_completed_at",
  ])
    if (!instant(v[key])) invalid();
  if (
    ![
      "event_time",
      "availability_at",
      "backend_processed_at_historical",
    ].includes(String(v.time_basis)) ||
    !["initial_history", "online", "legacy"].includes(String(v.origin)) ||
    !["present", "cleared"].includes(String(v.payload_state)) ||
    (!uncomputed &&
      !["complete", "unknown", "interrupted", "unverified"].includes(
        String(v.completeness),
      )) ||
    !integer(v.window_index, true)
  )
    invalid();
  if (!instant(v.close_not_before_at, true)) invalid();
  if (
    v.execution !== null &&
    String((v.execution as JsonObject).window_index) !== String(v.window_index)
  )
    invalid();
  return v as unknown as Point;
}
function commandReceipt(value: unknown): AggregateCommandReceipt {
  const v = record(value);
  if (
    !uuid(v.operation_id) ||
    !uuid(v.aggregate_id) ||
    !uuid(v.cleanup_operation_id, true) ||
    !instant(v.recorded_at) ||
    !count(v.execution_generation) ||
    !["pause", "unpause", "cleanup", "cleanup_retry"].includes(String(v.action))
  )
    invalid();
  if (
    (v.action === "cleanup" || v.action === "cleanup_retry") !==
    (v.cleanup_operation_id !== null)
  )
    invalid();
  return v as unknown as AggregateCommandReceipt;
}
function cleanupProtection(value: unknown): CleanupProtection {
  const v = record(value);
  if (
    !uuid(v.aggregate_id) ||
    !instant(v.reference_at) ||
    typeof v.can_cleanup !== "boolean" ||
    !nullableString(v.reason) ||
    !Array.isArray(v.blockers)
  )
    invalid();
  for (const key of [
    "requested_keep",
    "total_points",
    "minimum_keep",
    "maximum_delete",
    "planned_delete",
  ])
    if (!count(v[key])) invalid();
  if (v.tail_identity !== null) record(v.tail_identity);
  for (const item of v.blockers) {
    const blocker = record(item);
    if (
      !uuid(blocker.detector_id) ||
      typeof blocker.name !== "string" ||
      typeof blocker.reason !== "string" ||
      !count(blocker.minimum_keep) ||
      !integer(blocker.protected_start_index, true) ||
      !integer(blocker.protected_end_index, true)
    )
      invalid();
  }
  return v as unknown as CleanupProtection;
}
function cleanupOperation(value: unknown): CleanupOperation {
  const v = record(value);
  if (
    !uuid(v.id) ||
    !uuid(v.aggregate_id) ||
    !instant(v.created_at) ||
    !instant(v.completed_at, true) ||
    !nullableString(v.safe_error) ||
    !["pending", "running", "succeeded", "failed"].includes(String(v.status))
  )
    invalid();
  for (const key of ["requested_keep", "planned_delete", "deleted_points"])
    if (!count(v[key])) invalid();
  if (
    v.deleted_bytes_if_measured !== null &&
    !count(v.deleted_bytes_if_measured)
  )
    invalid();
  if (v.reference_tail !== null) record(v.reference_tail);
  record(v.progress);
  return v as unknown as CleanupOperation;
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
      headers: {
        ...(body !== undefined ? { "Content-Type": "application/json" } : {}),
        ...(key ? { "Idempotency-Key": key } : {}),
      },
      ...(body !== undefined
        ? {
            body: printAggregate(body),
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
    return this.request(
      `/${encodeURIComponent(id)}`,
      (value) => {
        const result = card(value);
        if (result.id.toLowerCase() !== id.toLowerCase()) invalid();
        return result;
      },
      signal,
    );
  }
  points(id: string, offset = 0, signal?: AbortSignal, limit = 50) {
    return this.request(
      `/${encodeURIComponent(id)}/points?limit=${limit}&offset=${offset}`,
      (v) =>
        page(v, (item) => {
          const result = point(item);
          if (result.aggregate_id.toLowerCase() !== id.toLowerCase()) invalid();
          return result;
        }),
      signal,
    );
  }
  operators(signal?: AbortSignal, extended = false) {
    return this.request(
      extended ? "/operators?include_extended=true" : "/operators",
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
  fieldCatalog(
    id: string,
    q = "",
    offset = 0,
    signal?: AbortSignal,
    limit = 50,
  ) {
    return this.request(
      `/sources/${encodeURIComponent(id)}/field-catalog?${new URLSearchParams({ q, offset: String(offset), limit: String(limit) })}`,
      (value): FieldCatalog => {
        const v = record(value);
        const decoded = page(v, (item): SourceField => {
          const f = record(item);
          if (
            typeof f.name !== "string" ||
            typeof f.type !== "string" ||
            typeof f.description !== "string" ||
            typeof f.is_array !== "boolean" ||
            typeof f.supported !== "boolean" ||
            !(f.reason === null || typeof f.reason === "string") ||
            !strings(f.operators) ||
            !strings(f.provenance) ||
            !f.provenance.length ||
            f.provenance.some((p) => !["observed", "normalizer"].includes(p)) ||
            new Set(f.provenance).size !== f.provenance.length ||
            !strings(f.normalizer_origin) ||
            f.normalizer_origin.some(
              (p) => !["mapping", "system"].includes(p),
            ) ||
            new Set(f.normalizer_origin).size !== f.normalizer_origin.length
          )
            invalid();
          return f as unknown as SourceField;
        });
        const observation = record(v.observation);
        const pageLimit = safeInteger(v.limit),
          pageOffset = safeInteger(v.offset);
        if (
          pageLimit < 1 ||
          pageLimit > 100 ||
          pageOffset < 0 ||
          typeof v.snapshot_at !== "string" ||
          !/(?:Z|\+00:00)$/.test(v.snapshot_at) ||
          !Number.isFinite(Date.parse(v.snapshot_at)) ||
          typeof v.catalog_hash !== "string" ||
          !/^[a-f0-9]{64}$/.test(v.catalog_hash) ||
          observation.scope !== "all_retained_events" ||
          observation.complete !== true ||
          !count(observation.rows)
        )
          invalid();
        let normalizer: FieldCatalog["normalizer"] = null;
        if (v.normalizer !== null) {
          const n = record(v.normalizer);
          if (typeof n.id !== "string" || !/^[a-f0-9-]{36}$/i.test(n.id))
            invalid();
          const version = safeInteger(n.version);
          if (version < 1) invalid();
          normalizer = { id: n.id, version };
        }
        return {
          ...decoded,
          limit: pageLimit,
          offset: pageOffset,
          snapshot_at: v.snapshot_at,
          catalog_hash: v.catalog_hash,
          observation: observation as FieldCatalog["observation"],
          normalizer,
        };
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
        executionPolicy(
          record(r.budget).execution_policy,
          safeInteger(r.duration_seconds),
        );
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
  async preview(body: PreviewInput, signal?: AbortSignal) {
    const duration = body.schedule?.duration_seconds;
    const executionSeconds =
      Number.isSafeInteger(duration) && duration >= 1 && duration <= 604800
        ? Math.min(30, (4 * duration) / 5)
        : 30;
    const controller = new AbortController();
    let timedOut = false;
    const abort = () => controller.abort();
    signal?.addEventListener("abort", abort, { once: true });
    if (signal?.aborted) abort();
    // Five seconds are reserved for finalization and five for response delivery.
    // Aborting this read-only wait does not prove that server computation stopped.
    const timer = setTimeout(
      () => {
        timedOut = true;
        controller.abort();
      },
      (executionSeconds + 10) * 1000,
    );
    try {
      return await this.request(
        "/preview",
        preview,
        controller.signal,
        body,
        "POST",
      );
    } catch (error) {
      if (timedOut && !signal?.aborted)
        throw new ApiError(0, "preview_wait_timeout");
      throw error;
    } finally {
      clearTimeout(timer);
      signal?.removeEventListener("abort", abort);
    }
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
      (value) => {
        const result = card(value);
        if (result.id.toLowerCase() !== id.toLowerCase()) invalid();
        return result;
      },
      signal,
      undefined,
      action === "delete" ? "DELETE" : "POST",
    );
  }
  historical(id: string, start: string, signal?: AbortSignal) {
    return this.request(
      `/${encodeURIComponent(id)}/historical-points`,
      (value) => {
        const result = point(value);
        if (result.aggregate_id.toLowerCase() !== id.toLowerCase()) invalid();
        return result;
      },
      signal,
      { window_start_utc: start },
      "POST",
    );
  }
  pointAt(id: string, index: string, signal?: AbortSignal) {
    if (!/^-?[0-9]+$/.test(index)) throw new Error("Некорректный индекс окна.");
    return this.request(
      `/${encodeURIComponent(id)}/points/by-index/${encodeURIComponent(index)}`,
      (value) => {
        const result = point(value);
        if (
          result.aggregate_id.toLowerCase() !== id.toLowerCase() ||
          result.window_index === null ||
          BigInt(String(exactNumeric(result.window_index))) !== BigInt(index)
        )
          invalid();
        return result;
      },
      signal,
    );
  }
  pointById(id: string, pointId: string, signal?: AbortSignal) {
    return this.request(
      `/${encodeURIComponent(id)}/points/${encodeURIComponent(pointId)}`,
      (value) => {
        const result = point(value);
        if (
          result.aggregate_id.toLowerCase() !== id.toLowerCase() ||
          result.id.toLowerCase() !== pointId.toLowerCase()
        )
          invalid();
        return result;
      },
      signal,
    );
  }
  command(
    id: string,
    action: "pause" | "unpause",
    key: string,
    signal?: AbortSignal,
  ) {
    return this.request(
      `/${encodeURIComponent(id)}/${action}`,
      commandReceipt,
      signal,
      undefined,
      "POST",
      key,
    );
  }
  cleanupPreview(id: string, keepLast: string, signal?: AbortSignal) {
    return this.request(
      `/${encodeURIComponent(id)}/points/cleanup-preview?keep_last=${keepLastDecimal(keepLast)}`,
      cleanupProtection,
      signal,
    );
  }
  cleanup(id: string, keepLast: string, key: string, signal?: AbortSignal) {
    return this.request(
      `/${encodeURIComponent(id)}/points/cleanup`,
      commandReceipt,
      signal,
      { keep_last: parseAggregate(keepLastDecimal(keepLast)) },
      "POST",
      key,
    );
  }
  cleanupOperation(id: string, operationId: string, signal?: AbortSignal) {
    return this.request(
      `/${encodeURIComponent(id)}/cleanup-operations/${encodeURIComponent(operationId)}`,
      cleanupOperation,
      signal,
    );
  }
  retryCleanup(
    id: string,
    operationId: string,
    key: string,
    signal?: AbortSignal,
  ) {
    return this.request(
      `/${encodeURIComponent(id)}/cleanup-operations/${encodeURIComponent(operationId)}/retry`,
      commandReceipt,
      signal,
      undefined,
      "POST",
      key,
    );
  }
}
