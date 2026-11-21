import { utcSortKey } from "../../shared/time/utc";

export type FindingLink = {
  sourceId: string;
  from: string;
  to: string;
};

const source = "source_id";
const lower = "timestamp_from";
const upper = "timestamp_to";
const uuid = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;

export function findingEventsPath(link: FindingLink) {
  return `/events?${new URLSearchParams({
    [source]: link.sourceId,
    [lower]: link.from,
    [upper]: link.to,
  })}`;
}

export function parseFindingLink(
  search: string,
): FindingLink | "invalid" | null {
  const params = new URLSearchParams(search);
  const keys = [source, lower, upper];
  if (!keys.some((key) => params.has(key))) return null;
  if (keys.some((key) => params.getAll(key).length !== 1)) return "invalid";
  const sourceId = params.get(source)!;
  const from = params.get(lower)!;
  const to = params.get(upper)!;
  const fromKey = utcSortKey(from);
  const toKey = utcSortKey(to);
  if (!uuid.test(sourceId) || !fromKey || !toKey || fromKey >= toKey)
    return "invalid";
  return { sourceId, from, to };
}
