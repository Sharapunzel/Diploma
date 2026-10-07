import { ApiError, type ApiClient } from "./client";

export type Page<T> = {
  items: T[];
  total: number;
  limit: number;
  offset: number;
};
export type KafkaConnection = {
  id: string;
  name: string;
  bootstrap_servers: string[];
  security_protocol: "PLAINTEXT";
  created_at: string;
  updated_at: string;
};
export type ExternalConnection = {
  id: string;
  name: string;
  base_url: string;
  username: string;
  has_password: boolean;
  has_ca: boolean;
  created_at: string;
  updated_at: string;
};
export type KafkaSource = {
  id: string;
  source_type: "kafka";
  name: string;
  connection_id: string;
  topic_name: string;
  normalizer_id: string | null;
  is_enabled: boolean;
  is_archived: boolean;
  created_at: string;
  updated_at: string;
};
export type TargetType =
  | "index"
  | "index_pattern"
  | "data_stream"
  | "data_stream_pattern";
export type ExternalSource = {
  id: string;
  source_type: "external";
  name: string;
  external_connection_id: string;
  target_type: TargetType;
  index_name: string | null;
  index_pattern: string | null;
  data_stream_name: string | null;
  data_stream_pattern: string | null;
  is_enabled: boolean;
  created_at: string;
  updated_at: string;
};
export type Normalizer = {
  id: string;
  name: string;
  version: number;
  rule_status: "ready" | "legacy_incompatible";
};
export type Topic = {
  name: string;
  partition_count: number;
  is_registered: boolean;
};
export type KafkaConnectionInput = {
  name: string;
  bootstrap_servers: string[];
  security_protocol: "PLAINTEXT";
};
export type KafkaConnectionPatch = Partial<
  Pick<KafkaConnectionInput, "name" | "bootstrap_servers">
>;
export type ExternalConnectionInput = {
  name: string;
  base_url: string;
  username: string;
  password: string;
};
export type ExternalConnectionPatch = Partial<ExternalConnectionInput>;
export type KafkaSourceInput = {
  name: string;
  connection_id: string;
  topic_name: string;
};
export type KafkaSourcePatch = Partial<KafkaSourceInput>;
export type ExternalSourceInput = {
  name: string;
  external_connection_id: string;
  target_type: TargetType;
} & Partial<Record<(typeof targetField)[TargetType], string>>;
export type ExternalSourcePatch = Partial<ExternalSourceInput>;
export type ConnectionTestResult = {
  status: string;
  broker_count?: number;
  topic_count?: number;
  latency_ms?: number;
};
export const targetField: Record<
  TargetType,
  "index_name" | "index_pattern" | "data_stream_name" | "data_stream_pattern"
> = {
  index: "index_name",
  index_pattern: "index_pattern",
  data_stream: "data_stream_name",
  data_stream_pattern: "data_stream_pattern",
};
export const targetLabel: Record<TargetType, string> = {
  index: "Индекс",
  index_pattern: "Маска индексов",
  data_stream: "Поток данных",
  data_stream_pattern: "Маска потоков",
};
export function externalTargetPayload(
  targetType: TargetType,
  target: string,
): Pick<
  ExternalSourceInput,
  | "target_type"
  | "index_name"
  | "index_pattern"
  | "data_stream_name"
  | "data_stream_pattern"
> {
  switch (targetType) {
    case "index":
      return { target_type: targetType, index_name: target };
    case "index_pattern":
      return { target_type: targetType, index_pattern: target };
    case "data_stream":
      return { target_type: targetType, data_stream_name: target };
    case "data_stream_pattern":
      return { target_type: targetType, data_stream_pattern: target };
  }
}
export function targetName(source: ExternalSource): string {
  return source[targetField[source.target_type]] || "—";
}
export function matchPattern(pattern: string, name: string): boolean {
  let expression = "^";
  for (const char of pattern)
    expression +=
      char === "*"
        ? ".*"
        : char === "?"
          ? "."
          : char.replace(/[\\^$+?.()|{}[\]]/g, "\\$&");
  return new RegExp(`${expression}$`).test(name);
}
export function validationFields(
  error: unknown,
  fields: Record<string, string>,
): { name: string; errors: string[] }[] {
  if (
    !(error instanceof ApiError) ||
    error.code !== "validation_error" ||
    !error.details ||
    typeof error.details !== "object"
  )
    return [];
  const errors = (error.details as { errors?: unknown }).errors;
  if (!Array.isArray(errors)) return [];
  const names = new Set<string>();
  for (const issue of errors) {
    if (!issue || typeof issue !== "object") continue;
    const location = (issue as { loc?: unknown }).loc;
    if (!Array.isArray(location) || location[0] !== "body") continue;
    const field = fields[String(location[1])];
    if (field) names.add(field);
  }
  return [...names].map((name) => ({
    name,
    errors: ["Проверьте значение поля."],
  }));
}
export function jsonRequest(
  api: ApiClient,
  method: string,
  path: string,
  data?: unknown,
  signal?: AbortSignal,
) {
  return api.request(path, {
    method,
    headers: { "Content-Type": "application/json" },
    body: data === undefined ? undefined : JSON.stringify(data),
    signal,
  });
}
export class ResourcesApi {
  constructor(private api: ApiClient) {}
  kafkaConnection(id: string, signal?: AbortSignal) {
    return this.api.request<KafkaConnection>(`/kafka-connections/${id}`, {
      signal,
    });
  }
  externalConnection(id: string, signal?: AbortSignal) {
    return this.api.request<ExternalConnection>(`/external-connections/${id}`, {
      signal,
    });
  }
  kafkaSource(id: string, signal?: AbortSignal) {
    return this.api.request<KafkaSource>(`/sources/${id}`, { signal });
  }
  externalSource(id: string, signal?: AbortSignal) {
    return this.api.request<ExternalSource>(`/external-sources/${id}`, {
      signal,
    });
  }
  normalizer(id: string, signal?: AbortSignal) {
    return this.api.request<Normalizer>(`/normalizers/${id}`, { signal });
  }
  kafkaConnections(offset = 0, q = "", signal?: AbortSignal) {
    return this.api.request<Page<KafkaConnection>>(
      `/kafka-connections?limit=50&offset=${offset}&q=${encodeURIComponent(q)}`,
      { signal },
    );
  }
  externalConnections(offset = 0, signal?: AbortSignal) {
    return this.api.request<Page<ExternalConnection>>(
      `/external-connections?limit=50&offset=${offset}`,
      { signal },
    );
  }
  kafkaSources(offset = 0, connectionId?: string, signal?: AbortSignal) {
    return this.api.request<Page<KafkaSource>>(
      `/sources?limit=50&offset=${offset}${connectionId ? `&connection_id=${encodeURIComponent(connectionId)}` : ""}`,
      { signal },
    );
  }
  externalSources(offset = 0, signal?: AbortSignal) {
    return this.api.request<Page<ExternalSource>>(
      `/external-sources?limit=50&offset=${offset}`,
      { signal },
    );
  }
  normalizers(offset = 0, q = "", signal?: AbortSignal) {
    return this.api.request<Page<Normalizer>>(
      `/normalizers?limit=50&offset=${offset}&q=${encodeURIComponent(q)}`,
      { signal },
    );
  }
  topics(id: string, includeInternal = false, signal?: AbortSignal) {
    return this.api.request<{ items: Topic[]; total: number }>(
      `/kafka-connections/${id}/topics?include_internal=${includeInternal}`,
      { signal },
    );
  }
  discovered(
    id: string,
    kind: "indices" | "data-streams",
    offset = 0,
    signal?: AbortSignal,
  ) {
    return this.api.request<Page<string>>(
      `/external-connections/${id}/${kind}?limit=50&offset=${offset}`,
      { signal },
    );
  }
  createKafkaConnection(data: KafkaConnectionInput, signal?: AbortSignal) {
    return jsonRequest(
      this.api,
      "POST",
      "/kafka-connections",
      data,
      signal,
    ) as Promise<KafkaConnection>;
  }
  patchKafkaConnection(
    id: string,
    data: KafkaConnectionPatch,
    signal?: AbortSignal,
  ) {
    return jsonRequest(
      this.api,
      "PATCH",
      `/kafka-connections/${id}`,
      data,
      signal,
    ) as Promise<KafkaConnection>;
  }
  createExternalConnection(
    data: ExternalConnectionInput,
    signal?: AbortSignal,
  ) {
    return jsonRequest(
      this.api,
      "POST",
      "/external-connections",
      data,
      signal,
    ) as Promise<ExternalConnection>;
  }
  patchExternalConnection(
    id: string,
    data: ExternalConnectionPatch,
    signal?: AbortSignal,
  ) {
    return jsonRequest(
      this.api,
      "PATCH",
      `/external-connections/${id}`,
      data,
      signal,
    ) as Promise<ExternalConnection>;
  }
  deleteConnection(
    kind: "kafka" | "external",
    id: string,
    signal?: AbortSignal,
  ) {
    return jsonRequest(
      this.api,
      "DELETE",
      `${kind === "kafka" ? "/kafka-connections" : "/external-connections"}/${id}`,
      undefined,
      signal,
    ) as Promise<void>;
  }
  testConnection(kind: "kafka" | "external", id: string, signal?: AbortSignal) {
    return jsonRequest(
      this.api,
      "POST",
      `${kind === "kafka" ? "/kafka-connections" : "/external-connections"}/${id}/test`,
      undefined,
      signal,
    ) as Promise<ConnectionTestResult>;
  }
  createKafkaSource(data: KafkaSourceInput, signal?: AbortSignal) {
    return jsonRequest(
      this.api,
      "POST",
      "/sources",
      data,
      signal,
    ) as Promise<KafkaSource>;
  }
  patchKafkaSource(id: string, data: KafkaSourcePatch, signal?: AbortSignal) {
    return jsonRequest(
      this.api,
      "PATCH",
      `/sources/${id}`,
      data,
      signal,
    ) as Promise<KafkaSource>;
  }
  createExternalSource(data: ExternalSourceInput, signal?: AbortSignal) {
    return jsonRequest(
      this.api,
      "POST",
      "/external-sources",
      data,
      signal,
    ) as Promise<ExternalSource>;
  }
  patchExternalSource(
    id: string,
    data: ExternalSourcePatch,
    signal?: AbortSignal,
  ) {
    return jsonRequest(
      this.api,
      "PATCH",
      `/external-sources/${id}`,
      data,
      signal,
    ) as Promise<ExternalSource>;
  }
  sourceAction(
    kind: "kafka" | "external",
    id: string,
    action: "enable" | "disable" | "delete",
    signal?: AbortSignal,
  ) {
    const base = kind === "kafka" ? "/sources" : "/external-sources";
    return jsonRequest(
      this.api,
      action === "delete" ? "DELETE" : "POST",
      `${base}/${id}${action === "delete" ? "" : `/${action}`}`,
      undefined,
      signal,
    ) as Promise<void>;
  }
  assignNormalizer(
    id: string,
    normalizerId: string | null,
    signal?: AbortSignal,
  ) {
    return jsonRequest(
      this.api,
      "PUT",
      `/sources/${id}/normalizer`,
      { normalizer_id: normalizerId },
      signal,
    ) as Promise<KafkaSource>;
  }
  uploadCa(id: string, pem: string, signal?: AbortSignal) {
    return this.api.request<void>(`/external-connections/${id}/ca`, {
      method: "PUT",
      headers: { "Content-Type": "application/x-pem-file" },
      body: pem,
      signal,
    });
  }
  deleteCa(id: string, signal?: AbortSignal) {
    return this.api.request<void>(`/external-connections/${id}/ca`, {
      method: "DELETE",
      signal,
    });
  }
}
