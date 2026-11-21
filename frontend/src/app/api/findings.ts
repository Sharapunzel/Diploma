import type { ApiClient } from "./client";

export type FindingFilters = {
  timestamp_from: string;
  timestamp_to: string;
  detector_id?: string;
  grade_min?: string;
  grade_max?: string;
  confidence_min?: string;
  confidence_max?: string;
};

export type TimeBasis =
  | "event_time"
  | "availability_at"
  | "backend_processed_at_historical";
export type FindingDetector = {
  detector_id: string;
  detector_name: string | null;
};
export type FindingRow = FindingDetector & {
  id: string;
  source_name: string | null;
  window_start: string | null;
  window_end: string | null;
  time_basis: TimeBasis;
  grade: number | null;
  confidence: number | null;
  late: boolean;
  accelerated_used: boolean;
};
export type FindingPage = {
  items: FindingRow[];
  total: number;
  offset: number;
  limit: number;
};
export type FindingLocation = { offset: number; anchorId?: string };
export type FindingFeature = {
  key: string;
  name: string;
  actual: string | null;
  expected: string | null;
  attribution: number | null;
};
export type FindingCard = FindingRow & {
  aggregate_name: string | null;
  source_id: string | null;
  event_range: Pick<FindingFilters, "timestamp_from" | "timestamp_to"> | null;
  event_range_reason: "empty_window" | "unavailable" | "unrepresentable" | null;
  aggregation_started_at: string | null;
  aggregation_completed_at: string | null;
  evaluation_started_at: string | null;
  evaluation_completed_at: string | null;
  expected_origin: "aws_rcf" | "preceding_median" | null;
  features: FindingFeature[];
};

export class FindingsApi {
  constructor(private api: ApiClient) {}

  page(
    filters: FindingFilters,
    limit: number,
    location: FindingLocation,
    signal: AbortSignal,
  ) {
    return this.readPage("/findings", filters, limit, location, signal);
  }

  timeline(
    filters: FindingFilters,
    limit: number,
    location: FindingLocation,
    signal: AbortSignal,
  ) {
    return this.readPage(
      "/findings/timeline",
      filters,
      limit,
      location,
      signal,
    );
  }

  private readPage(
    path: string,
    filters: FindingFilters,
    limit: number,
    { offset, anchorId }: FindingLocation,
    signal: AbortSignal,
  ) {
    const query = new URLSearchParams({
      ...filters,
      limit: String(limit),
      offset: String(offset),
    });
    if (anchorId) query.set("anchor_id", anchorId);
    return this.api.request<FindingPage>(`${path}?${query}`, { signal });
  }

  detectors(signal: AbortSignal) {
    return this.api.request<FindingDetector[]>("/findings/detectors", {
      signal,
    });
  }

  card(id: string, signal: AbortSignal) {
    return this.api.request<FindingCard>(`/findings/${id}`, { signal });
  }
}
