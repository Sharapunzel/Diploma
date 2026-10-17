import type {
  Agent,
  AgentCredentials,
  AgentTransport,
} from "../../app/api/agents";

/** Each input tag has exactly one output, so an unrecognized topic cannot receive records. */
export function fluentBitConfig(
  agent: Agent,
  credentials: AgentCredentials,
  transport: AgentTransport,
): string {
  if (
    agent.state !== "ready" ||
    agent.topics.length === 0 ||
    agent.topics.some((topic) => topic.state !== "ready")
  )
    throw new Error("Agent is not ready");
  if (
    agent.topics.some(
      (topic) => !/^[A-Za-z0-9._-]{1,249}$/.test(topic.topic_name),
    )
  )
    throw new Error("Agent topic name is invalid");
  if (
    !/^[^\s]+:\d{1,5}$/.test(transport.bootstrap_server) ||
    (transport.security_protocol === "SASL_SSL" && !transport.ca_pem)
  )
    throw new Error("Agent transport is incomplete");
  const lines = [
    "# Tested with Fluent Bit 4.2.3. Replace each INPUT Path with a real log path on the agent host.",
    "# Keep the generated Tag and OUTPUT Match pairs unchanged.",
    "[SERVICE]",
    "    Flush 1",
    "    Log_Level info",
    "",
  ];
  agent.topics.forEach((topic, index) => {
    const tag = `diploma.agent.${index + 1}`;
    lines.push(
      `# ${topic.topic_name}: replace the path with the log file for this service.`,
      "[INPUT]",
      "    Name tail",
      `    Tag ${tag}`,
      `    Path /REPLACE/WITH/SERVICE_${index + 1}.log`,
      "    Read_from_Head On",
      "",
      "[OUTPUT]",
      "    Name kafka",
      `    Match ${tag}`,
      `    Brokers ${transport.bootstrap_server}`,
      `    Topics ${topic.topic_name}`,
      "    Format json",
      "    Timestamp_Key timestamp",
      "    Timestamp_Format iso8601",
      `    rdkafka.security.protocol ${transport.security_protocol}`,
      "    rdkafka.sasl.mechanisms SCRAM-SHA-512",
      `    rdkafka.sasl.username ${credentials.username}`,
      `    rdkafka.sasl.password ${credentials.password}`,
    );
    if (transport.security_protocol === "SASL_SSL") {
      lines.push(
        "    rdkafka.ssl.ca.location /etc/fluent-bit/diploma-kafka-ca.pem",
        "    rdkafka.enable.ssl.certificate.verification true",
        "    rdkafka.ssl.endpoint.identification.algorithm https",
      );
    }
    lines.push("");
  });
  return lines.join("\n");
}
