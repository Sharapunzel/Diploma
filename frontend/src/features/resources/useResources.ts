import { useEffect, useMemo } from "react";
import {
  ResourcesApi,
  type ExternalConnection,
  type ExternalSource,
  type KafkaConnection,
  type KafkaSource,
  type Normalizer,
} from "../../app/api/resources";
import { useStores } from "../../app/providers/StoresProvider";
import { ResourceStore } from "./ResourceStore";
import { RequestLifecycle } from "./RequestLifecycle";
import { PagedOptionsStore } from "./PagedOptionsStore";
import { ConnectionsStore } from "./ConnectionsStore";
import { SourcesStore } from "./SourcesStore";

export function useResources() {
  const { api, session } = useStores();
  const identity = session.version;
  const permissions = session.data?.permissions.join("|") || "";
  const resources = useMemo(() => new ResourcesApi(api), [api]);
  const lists = useMemo(
    () => ({
      kafkaConnections: new ResourceStore<KafkaConnection>(
        resources,
        (offset, signal) => resources.kafkaConnections(offset, "", signal),
      ),
      externalConnections: new ResourceStore<ExternalConnection>(
        resources,
        (offset, signal) => resources.externalConnections(offset, signal),
      ),
      kafkaSources: new ResourceStore<KafkaSource>(
        resources,
        (offset, signal) => resources.kafkaSources(offset, undefined, signal),
      ),
      externalSources: new ResourceStore<ExternalSource>(
        resources,
        (offset, signal) => resources.externalSources(offset, signal),
      ),
    }),
    // Each identity or permission change receives fresh, empty stores.
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [resources, identity, permissions],
  );
  const lifecycle = useMemo(
    () => new RequestLifecycle(),
    // A permission or identity transition invalidates every in-flight scenario.
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [resources, identity, permissions],
  );
  const options = useMemo(
    () => ({
      connections: new PagedOptionsStore<KafkaConnection | ExternalConnection>(
        lifecycle,
        (item) => item.id,
      ),
      normalizers: new PagedOptionsStore<Normalizer>(
        lifecycle,
        (item) => item.id,
      ),
      targets: new PagedOptionsStore<string>(lifecycle, (item) => item),
    }),
    [lifecycle],
  );
  const connections = useMemo(
    () => new ConnectionsStore(resources, lifecycle),
    [resources, lifecycle],
  );
  const sources = useMemo(
    () => new SourcesStore(resources, lifecycle),
    [resources, lifecycle],
  );
  useEffect(
    () => () => {
      Object.values(lists).forEach((store) => store.dispose());
      connections.dispose();
      sources.clear();
      lifecycle.dispose();
    },
    [lists, lifecycle, connections, sources],
  );
  return {
    resources,
    lists,
    lifecycle,
    options,
    connections,
    sources,
    session,
  };
}
