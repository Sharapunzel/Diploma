import { describe, expect, it } from "vitest";
import { fluentBitConfig } from "./fluentBitConfig";
import type {
  Agent,
  AgentCredentials,
  AgentTransport,
} from "../../app/api/agents";

const agent: Agent = {
  id: "agent",
  name: "edge",
  description: null,
  state: "ready",
  access_revision: 1,
  credentials_claimable: true,
  safe_error: null,
  operation_id: null,
  topics: [
    {
      id: "a",
      connection_id: "c",
      source_id: "s1",
      topic_identity: "t1",
      topic_name: "auth.logs",
      action: "create",
      state: "ready",
    },
    {
      id: "b",
      connection_id: "c",
      source_id: "s2",
      topic_identity: "t2",
      topic_name: "web.logs",
      action: "attach",
      state: "ready",
    },
  ],
};
const credentials: AgentCredentials = {
  agent_id: "agent",
  revision: 1,
  username: "agent-user",
  password: "one-time-secret",
};

describe("Fluent Bit configuration", () => {
  it("routes every tag to its own authorized topic and keeps the envelope and TLS verification", () => {
    const transport: AgentTransport = {
      bootstrap_server: "broker.example:9094",
      security_protocol: "SASL_SSL",
      ca_pem: "public CA",
    };
    const config = fluentBitConfig(agent, credentials, transport);
    expect(config.match(/\[INPUT\]/g)).toHaveLength(2);
    expect(config.match(/\[OUTPUT\]/g)).toHaveLength(2);
    expect(config).toContain(
      "Match diploma.agent.1\n    Brokers broker.example:9094\n    Topics auth.logs",
    );
    expect(config).toContain(
      "Match diploma.agent.2\n    Brokers broker.example:9094\n    Topics web.logs",
    );
    expect(config).toContain("Timestamp_Key timestamp");
    expect(config).toContain("Timestamp_Format iso8601");
    expect(config).toContain(
      "rdkafka.ssl.endpoint.identification.algorithm https",
    );
    expect(config).toContain("rdkafka.sasl.password one-time-secret");
    expect(config).not.toContain("kafka:9092");
  });
  it("rejects a configuration for an agent without confirmed access", () => {
    expect(() =>
      fluentBitConfig({ ...agent, state: "revoked" }, credentials, {
        bootstrap_server: "127.0.0.1:9092",
        security_protocol: "SASL_PLAINTEXT",
        ca_pem: null,
      }),
    ).toThrow();
  });
});
