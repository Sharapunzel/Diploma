import type { ApiClient } from "./client";
import {
  jsonRequest,
  type Normalizer,
  type Page,
  type KafkaSource,
} from "./resources";

export type Condition = {
  kind: "prefix" | "contains" | "regex";
  value: string;
};
export type Candidate = Record<string, unknown> & {
  set?: Record<string, string | number | boolean>;
};
export type ExtractBlock = {
  key: string;
  kind: "regex" | "template" | "columns" | "json";
  input?: string;
  candidates: Candidate[];
};
export type ValueSource =
  | { ref: string }
  | { literal: string | number | boolean | unknown[] | Record<string, unknown> }
  | { refs: string[]; join: string }
  | { refs: string[]; as_array: true };
export type MapBlock = {
  key: string;
  kind: "map_ecs";
  target: string;
  source: ValueSource;
  required: boolean;
  transform?: { date_format: string; timezone?: "UTC"; year?: number } | null;
};
export type Block = ExtractBlock | MapBlock;
export type RuleV1 = {
  format_version: 1;
  variants: {
    key: string;
    priority: number;
    when: Condition;
    blocks: Block[];
  }[];
};
export type EcsField = {
  name: string;
  type: string;
  description: string;
  is_array: boolean;
  mappable: boolean;
};
export type PreviewResult = {
  status: "complete" | "partial" | "failed";
  variant_key: string | null;
  ecs_data: Record<string, unknown> | null;
  fluent_bit_collected_at: string | null;
  diagnostics: {
    code: string;
    variant: string | null;
    block: string | null;
    field: string | null;
  }[];
  trace: {
    variant: string;
    block: string;
    candidate: number | null;
    result: string;
  }[];
};
export type SnapshotRecord = {
  position: number;
  timestamp: string | null;
  log: string | null;
  issue: "invalid_envelope" | "message_too_large" | "response_limit" | null;
};
export type SourceSnapshot = {
  records: SnapshotRecord[];
  skipped: number;
  truncated: boolean;
};
export type NormalizerInput = {
  name: string;
  description: string | null;
  rule: RuleV1;
};
export type BlockType = {
  kind: Block["kind"];
  description: string;
  parameters: string[];
  input: string;
  output: string;
  limits: Record<string, unknown>;
};

export class NormalizersApi {
  constructor(private api: ApiClient) {}
  sourceSnapshot(sourceId: string, signal?: AbortSignal) {
    return this.api.request<SourceSnapshot>(
      `/sources/${encodeURIComponent(sourceId)}/debug-snapshot`,
      { signal },
    );
  }
  kafkaSources(offset = 0, signal?: AbortSignal) {
    return this.api.request<Page<KafkaSource>>(
      `/sources?limit=50&offset=${offset}`,
      { signal },
    );
  }
  list(offset = 0, q = "", signal?: AbortSignal) {
    return this.api.request<Page<Normalizer>>(
      `/normalizers?limit=50&offset=${offset}&q=${encodeURIComponent(q)}`,
      { signal },
    );
  }
  detail(id: string, signal?: AbortSignal) {
    return this.api.request<Normalizer>(`/normalizers/${id}`, { signal });
  }
  create(data: NormalizerInput, signal?: AbortSignal) {
    return jsonRequest(
      this.api,
      "POST",
      "/normalizers",
      data,
      signal,
    ) as Promise<Normalizer>;
  }
  update(
    id: string,
    data: NormalizerInput & { version: number },
    signal?: AbortSignal,
  ) {
    return jsonRequest(
      this.api,
      "PATCH",
      `/normalizers/${id}`,
      data,
      signal,
    ) as Promise<Normalizer>;
  }
  remove(id: string, version: number, signal?: AbortSignal) {
    return this.api.request<void>(`/normalizers/${id}?version=${version}`, {
      method: "DELETE",
      signal,
    });
  }
  preview(
    rule: RuleV1,
    sample: { timestamp: string; log: string },
    signal?: AbortSignal,
  ) {
    return jsonRequest(
      this.api,
      "POST",
      "/normalizers/preview",
      { rule, sample },
      signal,
    ) as Promise<PreviewResult>;
  }
  fields(q = "", offset = 0, signal?: AbortSignal) {
    return this.api.request<Page<EcsField>>(
      `/ecs/fields?mappable=true&limit=50&offset=${offset}&q=${encodeURIComponent(q)}`,
      { signal },
    );
  }
  field(name: string, signal?: AbortSignal) {
    return this.api.request<EcsField>(
      `/ecs/fields/${encodeURIComponent(name)}`,
      { signal },
    );
  }
  sources(id: string, offset = 0, signal?: AbortSignal) {
    return this.api.request<Page<KafkaSource>>(
      `/sources?normalizer_id=${encodeURIComponent(id)}&limit=50&offset=${offset}`,
      { signal },
    );
  }
  blockTypes(signal?: AbortSignal) {
    return this.api.request<BlockType[]>("/normalizers/block-types", {
      signal,
    });
  }
}
