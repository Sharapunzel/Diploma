export const id = "00000000-0000-4000-8000-000000000123";
export const at = "2026-11-10T00:00:00Z";
export function fixture() {
  return {
    id,
    name: "RCF",
    aggregate_id: id,
    aggregate_name: "Агрегат",
    aggregate_state: "active",
    source_id: id,
    source_name: "Источник",
    created_at: at,
    execution_state: "created",
    model_state: "waiting",
    accelerated: false,
    accelerated_used: false,
    reason: null,
    last_consumed: null,
    last_evaluated: null,
    backlog: null,
    lag_seconds: null,
    config: {
      dimensions: 1,
      trees: 100,
      sample_size: 256,
      output_after: 256,
      seed: 42,
    },
    feature_keys: ["n"],
    feature_names: ["События"],
    anchor_utc: at,
    duration_seconds: 2,
    step_seconds: 2,
    revision: 0,
    execution_generation: 0,
    start_request_id: null,
    start_requested_at: null,
    execution_started_at: null,
    stopped_at: null,
    updated_at: at,
    last_failure: null,
    warmup: null,
    recovery: null,
    next_scheduled_at: null,
    waiting_for_point: null,
  };
}
export function receipt(action = "create") {
  return {
    operation_id: id,
    detector_id: id,
    action,
    recorded_at: at,
    execution_generation: 1,
    start_request_id: null,
  };
}
