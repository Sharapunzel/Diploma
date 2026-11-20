import { useEffect, useMemo } from "react";
import { useStores } from "../../app/providers/StoresProvider";
import { AggregatesApi } from "../../app/api/aggregates";

export function useAggregateScope<
  T extends {
    dispose(): void;
    bindScope?(owner: object, actor: string): void;
    setVisible?(value: boolean): void;
  },
>(factory: (api: AggregatesApi) => T, key = "") {
  const { api, session } = useStores();
  const actorIdentity = `${session.version}|${session.data?.user?.id}|${session.data?.permissions.join("|")}`;
  const identity = `${actorIdentity}|${key}`;
  const service = useMemo(() => new AggregatesApi(api), [api]);
  // Route/actor identity owns the store; render-time factory closures do not replace it.
  const store = useMemo(() => {
    const created = factory(service);
    created.bindScope?.(api, actorIdentity);
    return created;
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [service, identity, api, actorIdentity]);
  useEffect(() => {
    const visibility = () => store.setVisible?.(!document.hidden);
    visibility();
    document.addEventListener("visibilitychange", visibility);
    return () => {
      document.removeEventListener("visibilitychange", visibility);
      store.setVisible?.(false);
    };
  }, [store]);
  useEffect(() => () => store.dispose(), [store]);
  return { store, service, session };
}
