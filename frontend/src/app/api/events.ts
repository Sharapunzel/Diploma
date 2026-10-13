import { ApiError, type ApiClient } from "./client";
import {
  jsonRequest,
  type ExternalSource,
  type KafkaSource,
  type Page,
} from "./resources";

export type EventSource = KafkaSource | ExternalSource;
export type EventField = {
  name: string;
  type: string;
  operators: string[];
  date_formats?: string[];
};
export type EventFilter = { field: string; operator: string; value?: unknown };
export type EventSearch = {
  timestamp_from: string;
  timestamp_to: string;
  sort: "asc" | "desc";
  filters: EventFilter[];
  limit: number;
  cursor?: string;
};
export type EventRow = {
  id: string;
  source_id: string;
  source_type: "kafka" | "external";
  event_timestamp: string;
  fields: Record<string, unknown>;
  normalization_status?: "complete" | "partial" | null;
};
export type EventCard = EventRow & {
  details?:
    | { raw: string; metadata: Record<string, unknown> }
    | Record<string, unknown>
    | null;
};
export type EventResults = {
  items: EventRow[];
  has_more: boolean;
  next_cursor: string | null;
};
export type DeleteScope = {
  source_id?: string;
  timestamp_from?: string;
  timestamp_to?: string;
};

export class EventsApi {
  constructor(private api: ApiClient) {}

  sources(kind: "kafka" | "external", offset: number, signal?: AbortSignal) {
    return this.api.request<Page<EventSource>>(
      `${kind === "kafka" ? "/sources" : "/external-sources"}?limit=50&offset=${offset}`,
      { signal },
    );
  }

  fields(id: string, offset: number, q = "", signal?: AbortSignal) {
    return this.api.request<Page<EventField>>(
      `/events/sources/${id}/fields?limit=100&offset=${offset}&q=${encodeURIComponent(q)}`,
      { signal },
    );
  }

  search(id: string, request: EventSearch, signal?: AbortSignal) {
    return jsonRequest(
      this.api,
      "POST",
      `/events/sources/${id}/search`,
      request,
      signal,
    ) as Promise<EventResults>;
  }

  card(id: string, eventId: string, signal?: AbortSignal) {
    return this.api.request<EventCard>(
      `/events/sources/${id}/events/${encodeURIComponent(eventId)}`,
      { signal },
    );
  }

  async deleteOne(id: string, eventId: string) {
    const result = await this.api.request<unknown>(
      `/events/sources/${id}/events/${encodeURIComponent(eventId)}`,
      { method: "DELETE" },
    );
    if (result !== undefined) throw new ApiError(200, "invalid_response");
  }

  async deleteMany(scope: DeleteScope) {
    const result = (await jsonRequest(
      this.api,
      "POST",
      "/parsed-logs/actions/delete",
      scope,
    )) as unknown;
    if (
      !result ||
      typeof result !== "object" ||
      !("deleted_count" in result) ||
      typeof result.deleted_count !== "number" ||
      !Number.isSafeInteger(result.deleted_count) ||
      result.deleted_count < 0
    )
      throw new ApiError(200, "invalid_response");
    return result as { deleted_count: number };
  }
}
