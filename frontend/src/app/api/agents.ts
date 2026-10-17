import type { ApiClient } from "./client";
import type { Page } from "./resources";

export type AgentTopicInput = {
  connection_id: string;
  topic_name: string;
  action: "create" | "attach";
};
export type AgentTopic = AgentTopicInput & {
  id: string;
  source_id: string;
  topic_identity: string | null;
  state: string;
};
export type Agent = {
  id: string;
  name: string;
  description: string | null;
  state: string;
  access_revision: number;
  credentials_claimable: boolean;
  safe_error: string | null;
  operation_id: string | null;
  topics: AgentTopic[];
};
export type AgentOperation = {
  id: string;
  agent_id: string;
  kind: string;
  state: string;
  attempts: number;
  safe_error: string | null;
};
export type AgentTransport = {
  bootstrap_server: string;
  security_protocol: "SASL_SSL" | "SASL_PLAINTEXT";
  ca_pem: string | null;
};
export type AgentCredentials = {
  agent_id: string;
  revision: number;
  username: string;
  password: string;
};

export class AgentsApi {
  constructor(private api: ApiClient) {}
  list(offset = 0) {
    return this.api.request<Page<Agent>>(`/agents?limit=20&offset=${offset}`);
  }
  get(id: string) {
    return this.api.request<Agent>(`/agents/${id}`);
  }
  transport() {
    return this.api.request<AgentTransport>("/agents/transport", {
      cache: "no-store",
    });
  }
  operation(id: string, operationId: string) {
    return this.api.request<AgentOperation>(
      `/agents/${id}/operations/${operationId}`,
    );
  }
  create(
    data: {
      name: string;
      description: string | null;
      topics: AgentTopicInput[];
    },
    key: string,
  ) {
    return this.mutate<Agent>("POST", "/agents", data, key);
  }
  patch(id: string, data: { name: string; description: string | null }) {
    return this.mutate<Agent>("PATCH", `/agents/${id}`, data);
  }
  addTopic(id: string, topic: AgentTopicInput, key: string) {
    return this.mutate<AgentOperation>(
      "POST",
      `/agents/${id}/topics`,
      topic,
      key,
    );
  }
  removeTopic(id: string, topicId: string, key: string) {
    return this.mutate<AgentOperation>(
      "DELETE",
      `/agents/${id}/topics/${topicId}`,
      undefined,
      key,
    );
  }
  revoke(id: string, key: string) {
    return this.mutate<AgentOperation>(
      "POST",
      `/agents/${id}/revoke`,
      undefined,
      key,
    );
  }
  rotate(id: string, key: string) {
    return this.mutate<AgentOperation>(
      "POST",
      `/agents/${id}/credentials/rotate`,
      undefined,
      key,
    );
  }
  claim(id: string) {
    return this.mutate<AgentCredentials>(
      "POST",
      `/agents/${id}/credentials/claim`,
    );
  }
  private mutate<T>(
    method: string,
    path: string,
    data?: unknown,
    key?: string,
  ) {
    return this.api.request<T>(path, {
      method,
      headers: {
        ...(data === undefined ? {} : { "Content-Type": "application/json" }),
        ...(key ? { "Idempotency-Key": key } : {}),
      },
      body: data === undefined ? undefined : JSON.stringify(data),
      cache: "no-store",
    });
  }
}
