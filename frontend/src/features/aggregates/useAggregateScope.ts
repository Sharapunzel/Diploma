import { useEffect, useMemo } from "react";
import { useStores } from "../../app/providers/StoresProvider";
import { AggregatesApi } from "../../app/api/aggregates";

export function useAggregateScope<T extends { dispose(): void }>(
  factory: (api: AggregatesApi) => T,
  key = "",
) {
  const { api, session } = useStores();
  const identity = `${session.version}|${session.data?.permissions.join("|")}|${key}`;
  const service = useMemo(() => new AggregatesApi(api), [api]);
  // The owner supplies a stable factory; identity changes discard all domain state.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  const store = useMemo(() => factory(service), [service, identity]);
  useEffect(() => () => store.dispose(), [store]);
  return { store, service, session };
}
