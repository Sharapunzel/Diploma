import type { ApiClient } from "./client";
import type { Page } from "./resources";

export type SourceState = {
  source_id: string;
  source_name: string;
  connection_id: string | null;
  connection_name: string | null;
  topic: string;
  topic_id: string | null;
  is_enabled: boolean;
  is_archived: boolean;
  worker_status: "running" | "retrying" | "stopped" | "not_running" | "unknown";
  reason_code: string | null;
  reason_description: string | null;
  captured_at: string;
  partitions: {
    partition: number;
    status: string;
    reason_code: string | null;
    reason_description: string | null;
  }[];
};
export type SourceTopology = {
  source_id: string;
  normalizer_id: string | null;
  normalizer_name: string | null;
  agent_id: string | null;
  agent_name: string | null;
  agent_state: string | null;
  assignment_state: string | null;
};
export type ProcessingError = {
  id: string;
  source_id: string | null;
  connection_id: string | null;
  normalizer_id: string | null;
  stage: "decode" | "envelope" | "normalization";
  diagnostics: {
    code?: string;
    variant?: string;
    block?: string;
    field?: string;
  }[];
  source_name: string;
  connection_name: string;
  normalizer_name: string;
  normalizer_version: number;
  kafka_topic: string;
  kafka_partition: number;
  kafka_offset: number;
  fluent_bit_collected_at: string | null;
  backend_received_at: string;
  backend_processed_at: string;
};
export type KafkaEvent = {
  id: string;
  kind: "retention_gap" | "topic_recreated";
  reason_code: string;
  source_id: string;
  source_name: string;
  connection_id: string | null;
  connection_name: string;
  topic: string;
  partition: number | null;
  offset_start: number | null;
  offset_end: number | null;
  detected_at: string;
};
export type RawPayload = {
  data: string;
  original_size: number;
  returned_size: number;
  truncated: boolean;
};
export type HistoryQuery = {
  sourceId?: string;
  type?: string;
  start?: string;
  end?: string;
  offset: number;
};
function query(filters: HistoryQuery, typeKey: string) {
  const params = new URLSearchParams({
    limit: "20",
    offset: String(filters.offset),
  });
  if (filters.sourceId) params.set("source_id", filters.sourceId);
  if (filters.type) params.set(typeKey, filters.type);
  if (filters.start) params.set("start", new Date(filters.start).toISOString());
  if (filters.end) params.set("end", new Date(filters.end).toISOString());
  return params.toString();
}
export class DiagnosticsApi {
  constructor(private api: ApiClient) {}
  sources(offset: number, q: string, signal?: AbortSignal) {
    return this.api.request<Page<SourceState>>(
      `/diagnostics/sources?limit=20&offset=${offset}&q=${encodeURIComponent(q)}`,
      { signal },
    );
  }
  source(id: string, signal?: AbortSignal) {
    return this.api.request<SourceState>(`/diagnostics/sources/${id}`, {
      signal,
    });
  }
  topology(id: string, signal?: AbortSignal) {
    return this.api.request<SourceTopology>(
      `/diagnostics/sources/${id}/topology`,
      { signal },
    );
  }
  errors(filters: HistoryQuery, signal?: AbortSignal) {
    return this.api.request<Page<ProcessingError>>(
      `/diagnostics/processing-errors?${query(filters, "stage")}`,
      { signal },
    );
  }
  events(filters: HistoryQuery, signal?: AbortSignal) {
    return this.api.request<Page<KafkaEvent>>(
      `/diagnostics/kafka-events?${query(filters, "kind")}`,
      { signal },
    );
  }
  raw(id: string) {
    return this.api.request<RawPayload>(
      `/diagnostics/processing-errors/${id}/raw`,
    );
  }
}
