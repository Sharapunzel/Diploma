import type {
  Agent,
  AgentCredentials,
  AgentTransport,
} from "../../app/api/agents";

export type AgentInputKind = "tail" | "winevtlog" | "systemd" | "other";
export type AgentInputSelection = Record<string, AgentInputKind>;
export type AgentHostPlatform = "linux" | "windows";

export function agentHostPlatform(
  selections: AgentInputKind[],
  preferred: AgentHostPlatform = "linux",
): AgentHostPlatform {
  if (selections.includes("winevtlog") && selections.includes("systemd"))
    throw new Error(
      "Один конфиг предназначен для одной ОС: Windows winevtlog и Linux systemd нельзя совмещать.",
    );
  if (selections.includes("winevtlog") && preferred !== "windows")
    throw new Error("Для winevtlog выберите ОС Windows.");
  if (selections.includes("systemd") && preferred !== "linux")
    throw new Error("Для systemd выберите ОС Linux.");
  return preferred;
}

/** Each input tag has exactly one output, so an unrecognized topic cannot receive records. */
export function fluentBitConfig(
  agent: Agent,
  credentials: AgentCredentials | null,
  transport: AgentTransport,
  inputSelections: AgentInputSelection = {},
  hostPlatform: AgentHostPlatform = Object.values(inputSelections).includes(
    "winevtlog",
  )
    ? "windows"
    : "linux",
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
  const platform = agentHostPlatform(
    agent.topics.map((topic) => inputSelections[topic.id] ?? "tail"),
    hostPlatform,
  );
  const template = credentials === null;
  const lines = [
    template
      ? "# TEMPLATE ONLY: replace the explicit credential placeholders before use. This file cannot authorize an agent."
      : "# Tested with Fluent Bit 4.2.3. Fill every marked input setting before starting the agent.",
    `# One agent host OS: ${platform}. Keep each generated Tag and OUTPUT Match pair unchanged.`,
    "# Create the state and CA directories and grant the Fluent Bit account access before starting.",
    "[SERVICE]",
    "    Flush 1",
    "    Log_Level info",
    "",
  ];
  agent.topics.forEach((topic, index) => {
    const tag = `diploma.agent.${index + 1}`;
    const kind = inputSelections[topic.id] ?? "tail";
    // Topic names have already been restricted to filesystem-safe ASCII chars.
    // A one-letter input marker keeps different cursors distinct without adding
    // a parent directory that Fluent Bit would need to create itself.
    const stateSuffix: Record<AgentInputKind, string> = {
      tail: "t",
      winevtlog: "w",
      systemd: "s",
      other: "o",
    };
    const stateDir =
      platform === "windows"
        ? "C:/ProgramData/FluentBit/state"
        : "/var/lib/fluent-bit";
    const stateDb = `${stateDir}/${topic.topic_name}-${stateSuffix[kind]}.db`;
    lines.push(`# ${topic.topic_name}: selected input ${kind}.`);
    if (kind === "tail") {
      lines.push(
        "[INPUT]",
        "    Name tail",
        `    Tag ${tag}`,
        `    Path ${platform === "windows" ? `C:/REPLACE/WITH/SERVICE_${index + 1}.log` : `/REPLACE/WITH/SERVICE_${index + 1}.log`}`,
        `    DB ${stateDb}`,
        "    Read_from_Head On",
      );
    } else if (kind === "winevtlog") {
      lines.push(
        "[INPUT]",
        "    Name winevtlog",
        `    Tag ${tag}`,
        "    Channels /REPLACE/WITH/WINDOWS_CHANNELS",
        "    # Create this persistent directory and grant write access to the agent account.",
        "    # Replace the DB path if ProgramData is not the chosen durable location.",
        `    DB ${stateDb}`,
        "    Read_Existing_Events false",
        "    Render_Event_As_Text true",
        "    Render_Event_Text_Key log",
      );
    } else if (kind === "systemd") {
      lines.push(
        "[INPUT]",
        "    Name systemd",
        `    Tag ${tag}`,
        `    Systemd_Filter _SYSTEMD_UNIT=REPLACE_WITH_UNIT.service`,
        `    DB ${stateDb}`,
        "    Read_From_Tail Off",
        "",
        "# Preserve the original journal timestamp and available metadata inside log.",
        "# Reset the envelope timestamp to the time this agent processes the record.",
        "[FILTER]",
        "    Name lua",
        `    Match ${tag}`,
        "    Call diploma_systemd_envelope",
        "    Code function diploma_systemd_envelope(tag, timestamp, record) local original = os.date('!%Y-%m-%dT%H:%M:%SZ', math.floor(timestamp)); local function field(key) local value = record[key]; if value == nil then return '' end; return tostring(value):gsub('[\\r\\n]', ' ') end; local message = field('MESSAGE'); local log = 'journal_time=' .. original .. ' unit=' .. field('_SYSTEMD_UNIT') .. ' priority=' .. field('PRIORITY') .. ' pid=' .. field('_PID') .. ' identifier=' .. field('SYSLOG_IDENTIFIER') .. ' MESSAGE=' .. message; return 1, os.time(), {log = log} end",
      );
    } else if (kind === "other") {
      lines.push(
        "# Replace this INPUT with a plugin supported by the target Fluent Bit build.",
        "# Ensure it produces a string field named log; adapt the filters below to that input.",
        "[INPUT]",
        "    Name REPLACE_WITH_SUPPORTED_INPUT_PLUGIN",
        `    Tag ${tag}`,
        "    # Add plugin-specific required settings here.",
        "",
        "# Example only: rename an input field to log, then retain only log.",
        "# Adapt the field name and rules to the actual plugin output.",
        "# [FILTER]",
        "#     Name modify",
        `#     Match ${tag}`,
        "#     Rename REPLACE_WITH_RAW_TEXT_FIELD log",
        "# [FILTER]",
        "#     Name record_modifier",
        `#     Match ${tag}`,
        "#     Allowlist_key log",
      );
    } else {
      throw new Error("Unsupported Fluent Bit input");
    }
    if (kind !== "other" && kind !== "systemd") {
      lines.push(
        "",
        "[FILTER]",
        "    Name record_modifier",
        `    Match ${tag}`,
        "    Allowlist_key log",
      );
    }
    lines.push(
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
      `    rdkafka.sasl.username ${credentials?.username ?? "__REPLACE_WITH_AGENT_USERNAME__"}`,
      `    rdkafka.sasl.password ${credentials?.password ?? "__REPLACE_WITH_AGENT_PASSWORD__"}`,
    );
    if (transport.security_protocol === "SASL_SSL") {
      lines.push(
        `    rdkafka.ssl.ca.location ${platform === "windows" ? "C:/ProgramData/FluentBit/diploma-kafka-ca.pem" : "/etc/fluent-bit/diploma-kafka-ca.pem"}`,
        "    rdkafka.enable.ssl.certificate.verification true",
        "    rdkafka.ssl.endpoint.identification.algorithm https",
      );
    }
    lines.push("");
  });
  return lines.join("\n");
}
