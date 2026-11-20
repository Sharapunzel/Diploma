import type {
  Aggregate,
  Preview,
  AggregateCommandReceipt,
  CleanupProtection,
  CleanupOperation,
  AutoCleanup,
  ComputedPoint,
  UncomputedPoint,
  ExecutionPolicy,
  ExecutionDiagnostics,
} from "./aggregates";

export const aggregateId = "00000000-0000-4000-8000-000000000041";
export const cleanupId = "00000000-0000-4000-8000-000000000042";
export const aggregateTime = "2026-01-01T00:00:00Z";
export function executionPolicyFixture(hard = 30): ExecutionPolicy {
  return {
    policy_version: "window_execution_v1",
    soft_latency_seconds: 5,
    hard_budget_seconds: hard,
  };
}
export function executionFixture(
  overrides: Partial<ExecutionDiagnostics> = {},
): ExecutionDiagnostics {
  return {
    ...executionPolicyFixture(),
    calculation_status: "computed",
    calculation_reason: null,
    attempt_id: "00000000-0000-4000-8000-000000000043",
    origin: "preview",
    window_index: null,
    start_utc: aggregateTime,
    end_utc: "2026-01-01T00:05:00Z",
    started_at: "2026-01-01T00:10:00Z",
    execution_deadline_at: "2026-01-01T00:10:30Z",
    measured_at: "2026-01-01T00:10:01Z",
    measured_through: "preview_complete",
    elapsed_seconds: 1,
    queue_seconds: null,
    online_latency_seconds: null,
    soft_latency_exceeded: null,
    timings: {},
    ...overrides,
  };
}
export function autoCleanupFixture(
  overrides: Partial<AutoCleanup> = {},
): AutoCleanup {
  return {
    policy_version: "model_history_v1",
    status: "scheduled",
    reason: null,
    checked_at: null,
    next_check_at: aggregateTime,
    reserve_windows: null,
    keep_from_index: null,
    protected_from_index: null,
    auto_cleared_total: 0,
    history_revision: 0,
    last_cleared_at: null,
    retained_from_index: null,
    retained_to_index: null,
    protected_total: null,
    protected_by: [],
    ...overrides,
  };
}
export function missingAutoCleanupFixture(): AutoCleanup {
  return autoCleanupFixture({
    policy_version: null,
    status: "error",
    reason: "retention_state_missing",
    checked_at: null,
    next_check_at: null,
    reserve_windows: null,
    keep_from_index: null,
    protected_from_index: null,
    auto_cleared_total: null,
    history_revision: null,
    last_cleared_at: null,
    retained_from_index: null,
    retained_to_index: null,
    protected_total: null,
    protected_by: [],
  });
}
export function aggregateFixture(
  overrides: Partial<Aggregate> = {},
): Aggregate {
  return {
    id: aggregateId,
    name: "Агрегат",
    source_id: "00000000-0000-4000-8000-000000000001",
    source_name: "Источник",
    source_type: "kafka",
    aggregate_rule: {
      features: [{ key: "events", name: "События", op: "count" }],
    },
    rule_hash: "a".repeat(64),
    schedule_id: "00000000-0000-4000-8000-000000000002",
    anchor_utc: aggregateTime,
    duration_seconds: 300,
    original_boundary: aggregateTime,
    timezone: "UTC",
    created_at: aggregateTime,
    created_by: "00000000-0000-4000-8000-000000000003",
    state: "active",
    next_scheduled_at: "2026-01-01T00:05:00Z",
    next_index: 0,
    skipped_ranges: [],
    lag_seconds: 0,
    last_error: null,
    last_error_at: null,
    archived_at: null,
    deleted_at: null,
    deleted_by: null,
    semantic_basis: "event_time",
    execution_mode: "current",
    execution_reason: null,
    bootstrap_at: aggregateTime,
    online_start_index: 0,
    paused: false,
    execution_generation: 0,
    active_cleanup_id: null,
    next_close_not_before_at: "2026-01-01T00:10:00Z",
    diagnostics: {
      exclusions: {},
      late_count: 0,
      late_start: null,
      late_end: null,
      late_reason: null,
    },
    auto_cleanup: autoCleanupFixture(),
    execution_policy: executionPolicyFixture(
      Math.min(30, (4 * (overrides.duration_seconds ?? 300)) / 5),
    ),
    last_execution: null,
    ...overrides,
  };
}
export function previewFixture(overrides: Partial<Preview> = {}): Preview {
  return {
    calculation_status: "computed",
    calculation_reason: null,
    execution: executionFixture(),
    rule_hash: "a".repeat(64),
    start_utc: aggregateTime,
    end_utc: "2026-01-01T00:05:00Z",
    scheduled_at: "2026-01-01T00:05:00Z",
    observed_rows: 1,
    event_count: 1,
    event_time_min: aggregateTime,
    event_time_max: aggregateTime,
    features: [
      {
        key: "events",
        value: 1,
        reason: null,
        valid_count: 1,
        invalid_count: 0,
      },
    ],
    completeness: "complete",
    completeness_reason: "received_event_snapshot",
    time_basis: "event_time",
    budget: { execution_policy: executionPolicyFixture() },
    snapshot_reference_at: "2026-01-01T00:10:00Z",
    close_not_before_at: "2026-01-01T00:10:00Z",
    origin: "preview",
    in_process: true,
    snapshot_metadata: { delivery_coverage: "not_attested" },
    ...overrides,
  };
}
export function pointFixture(
  overrides: Partial<ComputedPoint> = {},
): ComputedPoint {
  const {
    rule_hash: _hash,
    budget: _budget,
    observed_rows: _rows,
    origin: _origin,
    in_process: _process,
    ...preview
  } = previewFixture();
  void _hash;
  void _budget;
  void _rows;
  void _origin;
  void _process;
  return {
    ...preview,
    id: "00000000-0000-4000-8000-000000000004",
    aggregate_id: aggregateId,
    kind: "working",
    actual_started_at: "2026-01-01T00:10:00Z",
    actual_completed_at: "2026-01-01T00:10:01Z",
    origin: "online",
    window_index: 0,
    payload_state: "present",
    evidence_refs: {},
    execution: null,
    ...overrides,
  };
}
export function uncomputedPointFixture(
  overrides: Partial<UncomputedPoint> = {},
): UncomputedPoint {
  return {
    ...pointFixture(),
    calculation_status: "uncomputed",
    calculation_reason: "execution_budget_exceeded",
    execution: executionFixture({
      calculation_status: "uncomputed",
      calculation_reason: "execution_budget_exceeded",
      origin: "online",
      window_index: 0,
      measured_through: "finalization",
    }),
    event_count: null,
    event_time_min: null,
    event_time_max: null,
    features: null,
    completeness: null,
    completeness_reason: null,
    snapshot_reference_at: null,
    snapshot_metadata: null,
    evidence_refs: null,
    payload_state: "present",
    ...overrides,
  };
}
export function receiptFixture(
  overrides: Partial<AggregateCommandReceipt> = {},
): AggregateCommandReceipt {
  return {
    operation_id: "00000000-0000-4000-8000-000000000005",
    aggregate_id: aggregateId,
    action: "pause",
    recorded_at: aggregateTime,
    execution_generation: 1,
    cleanup_operation_id: null,
    ...overrides,
  };
}
export function protectionFixture(
  overrides: Partial<CleanupProtection> = {},
): CleanupProtection {
  return {
    aggregate_id: aggregateId,
    reference_at: aggregateTime,
    requested_keep: 0,
    total_points: 10,
    minimum_keep: 0,
    maximum_delete: 10,
    planned_delete: 10,
    tail_identity: null,
    can_cleanup: true,
    reason: null,
    blockers: [],
    ...overrides,
  };
}
export function cleanupFixture(
  overrides: Partial<CleanupOperation> = {},
): CleanupOperation {
  return {
    id: cleanupId,
    aggregate_id: aggregateId,
    created_at: aggregateTime,
    requested_keep: 0,
    reference_tail: null,
    planned_delete: 10,
    deleted_points: 0,
    deleted_bytes_if_measured: null,
    completed_at: null,
    status: "pending",
    safe_error: null,
    progress: {},
    ...overrides,
  };
}
