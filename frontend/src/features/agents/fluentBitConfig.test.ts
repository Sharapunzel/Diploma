import { describe, expect, it } from "vitest";
import { fluentBitConfig, type AgentInputKind } from "./fluentBitConfig";
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

  it.each<AgentInputKind>(["tail", "winevtlog", "systemd", "other"])(
    "renders the %s input while preserving per-topic output routing",
    (kind) => {
      const config = fluentBitConfig(
        agent,
        null,
        {
          bootstrap_server: "broker.example:9094",
          security_protocol: "SASL_SSL",
          ca_pem: "public CA",
        },
        { a: kind, b: "tail" },
      );
      expect(config).toContain("__REPLACE_WITH_AGENT_PASSWORD__");
      expect(config).not.toContain("one-time-secret");
      expect(config).toContain(
        "Match diploma.agent.1\n    Brokers broker.example:9094\n    Topics auth.logs",
      );
      expect(config).toContain(
        "Match diploma.agent.2\n    Brokers broker.example:9094\n    Topics web.logs",
      );
      const suffix: Record<AgentInputKind, string> = {
        tail: "t",
        winevtlog: "w",
        systemd: "s",
        other: "o",
      };
      if (kind !== "other")
        expect(config).toContain(`auth.logs-${suffix[kind]}.db`);
      if (kind === "winevtlog") {
        expect(config).toContain("Render_Event_As_Text true");
        expect(config).toContain("Render_Event_Text_Key log");
        expect(config).toContain(
          "DB C:/ProgramData/FluentBit/state/auth.logs-w.db",
        );
        expect(config).toContain("persistent directory");
      }
      if (kind === "tail" || kind === "winevtlog") {
        expect(config).toContain("Name record_modifier");
        expect(config).toContain("Allowlist_key log");
        expect(config).not.toContain("Keep log");
      }
      if (kind === "systemd") {
        expect(config).toContain(
          "Systemd_Filter _SYSTEMD_UNIT=REPLACE_WITH_UNIT.service",
        );
        expect(config).toContain("Name lua");
        expect(config).toContain("Call diploma_systemd_envelope");
        expect(config).toContain("journal_time=");
        expect(config).toContain("os.time()");
        expect(config).toContain("SYSLOG_IDENTIFIER");
      }
      if (kind === "other") {
        expect(config).toContain("# [FILTER]");
        expect(config).toContain("REPLACE_WITH_RAW_TEXT_FIELD log");
        expect(config).toContain("TEMPLATE ONLY");
      }
    },
  );
  it("allows compatible Linux input kinds on separate topic routes", () => {
    const config = fluentBitConfig(
      agent,
      null,
      {
        bootstrap_server: "broker.example:9094",
        security_protocol: "SASL_SSL",
        ca_pem: "public CA",
      },
      { a: "systemd", b: "tail" },
    );
    expect(config).toContain(
      "Match diploma.agent.1\n    Brokers broker.example:9094\n    Topics auth.logs",
    );
    expect(config).toContain(
      "Match diploma.agent.2\n    Brokers broker.example:9094\n    Topics web.logs",
    );
    expect(config.match(/Name lua/g)).toHaveLength(1);
    expect(config.match(/Name tail/g)).toHaveLength(1);
    expect(config).toContain("DB /var/lib/fluent-bit/web.logs-t.db");
    expect(config).toContain(
      "rdkafka.ssl.ca.location /etc/fluent-bit/diploma-kafka-ca.pem",
    );
  });

  it("rejects Windows and Linux journal inputs in one config", () => {
    expect(() =>
      fluentBitConfig(
        agent,
        null,
        {
          bootstrap_server: "broker.example:9094",
          security_protocol: "SASL_SSL",
          ca_pem: "public CA",
        },
        { a: "winevtlog", b: "systemd" },
      ),
    ).toThrow(/одной ОС/);
  });

  it("keeps all Windows paths on Windows for winevtlog plus tail, including full rerender", () => {
    const transport: AgentTransport = {
      bootstrap_server: "broker.example:9094",
      security_protocol: "SASL_SSL",
      ca_pem: "public CA",
    };
    const selections: Record<string, AgentInputKind> = {
      a: "winevtlog",
      b: "tail",
    };
    const preview = fluentBitConfig(agent, null, transport, selections);
    const full = fluentBitConfig(agent, credentials, transport, selections);
    for (const config of [preview, full]) {
      expect(config).toContain(
        "DB C:/ProgramData/FluentBit/state/auth.logs-w.db",
      );
      expect(config).toContain(
        "DB C:/ProgramData/FluentBit/state/web.logs-t.db",
      );
      expect(config).toContain("Path C:/REPLACE/WITH/SERVICE_2.log");
      expect(config).toContain(
        "rdkafka.ssl.ca.location C:/ProgramData/FluentBit/diploma-kafka-ca.pem",
      );
      expect(config).not.toContain("/var/lib/fluent-bit");
      expect(config).not.toContain("/etc/fluent-bit");
    }
    expect(preview).toContain("__REPLACE_WITH_AGENT_PASSWORD__");
    expect(full).toContain("rdkafka.sasl.password one-time-secret");
  });

  it("supports a Windows host with only universal tail inputs", () => {
    const config = fluentBitConfig(
      agent,
      null,
      {
        bootstrap_server: "broker.example:9094",
        security_protocol: "SASL_SSL",
        ca_pem: "public CA",
      },
      {},
      "windows",
    );
    expect(config).toContain(
      "DB C:/ProgramData/FluentBit/state/auth.logs-t.db",
    );
    expect(config).toContain("Path C:/REPLACE/WITH/SERVICE_1.log");
    expect(config).toContain(
      "rdkafka.ssl.ca.location C:/ProgramData/FluentBit/diploma-kafka-ca.pem",
    );
    expect(config).not.toContain("/var/lib/fluent-bit");
  });

  it("keeps existing topic cursor paths when an earlier sorted topic is added", () => {
    const transport: AgentTransport = {
      bootstrap_server: "broker.example:9094",
      security_protocol: "SASL_SSL",
      ca_pem: "public CA",
    };
    const existing = { ...agent, topics: [agent.topics[1]] };
    const before = fluentBitConfig(existing, null, transport, { b: "tail" });
    const after = fluentBitConfig(
      {
        ...agent,
        topics: [
          {
            ...agent.topics[0],
            id: "earlier",
            topic_name: "aaa.logs",
          },
          agent.topics[1],
        ],
      },
      null,
      transport,
      { earlier: "systemd", b: "tail" },
    );
    const topicDbPath = (config: string) =>
      config.split("# web.logs:")[1]?.match(/DB ([^\r\n]+)/)?.[1];
    expect(topicDbPath(after)).toBe(topicDbPath(before));
    expect(topicDbPath(after)).toBe("/var/lib/fluent-bit/web.logs-t.db");
  });
});
