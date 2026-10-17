/** Production server/TLS smoke through the same config renderer used by /agents. */
import { writeFileSync } from "node:fs";
import { join } from "node:path";
import { randomUUID } from "node:crypto";
import { expect, test } from "@playwright/test";
import { fluentBitConfig } from "../src/features/agents/fluentBitConfig";
import type {
  Agent,
  AgentCredentials,
  AgentTransport,
} from "../src/app/api/agents";

const run = process.env.TASK21_RUN_DIR;
const password = process.env.TASK21_ADMIN_PASSWORD;
const topic = process.env.TASK21_TOPIC;
test.skip(!run || !password || !topic, "requires the isolated TASK-21 profile");
test.use({ ignoreHTTPSErrors: true });
test.setTimeout(120_000);

test("renders and uses the production TLS agent configuration", async ({
  request,
}) => {
  const login = await request.post("/api/v1/auth/local/login", {
    data: { username: "task21admin", password },
  });
  expect(login.ok()).toBeTruthy();
  const csrf = (await login.json()).csrf_token as string;
  const connection = await request.post("/api/v1/kafka-connections", {
    headers: { "X-CSRF-Token": csrf },
    data: { name: "Task 21 TLS Kafka", bootstrap_servers: ["kafka:9092"] },
  });
  expect(connection.status()).toBe(201);
  const connectionId = (await connection.json()).id as string;
  const created = await request.post("/api/v1/agents", {
    headers: { "X-CSRF-Token": csrf, "Idempotency-Key": randomUUID() },
    data: {
      name: "Task 21 TLS agent",
      topics: [
        { connection_id: connectionId, topic_name: topic, action: "create" },
      ],
    },
  });
  expect(created.status()).toBe(202);
  const agentId = (await created.json()).id as string;
  let agent: Agent | undefined;
  for (let attempt = 0; attempt < 60; attempt++) {
    const response = await request.get(`/api/v1/agents/${agentId}`);
    expect(response.ok()).toBeTruthy();
    agent = (await response.json()) as Agent;
    if (agent.state === "ready") break;
    if (agent.state === "error") throw new Error("Agent provisioning failed");
    await new Promise((resolve) => setTimeout(resolve, 1000));
  }
  expect(agent?.state).toBe("ready");
  const transportResponse = await request.get("/api/v1/agents/transport");
  expect(transportResponse.ok()).toBeTruthy();
  const transport = (await transportResponse.json()) as AgentTransport;
  expect(transport.security_protocol).toBe("SASL_SSL");
  expect(transport.ca_pem).toContain("BEGIN CERTIFICATE");
  const claim = await request.post(
    `/api/v1/agents/${agentId}/credentials/claim`,
    {
      headers: { "X-CSRF-Token": csrf },
    },
  );
  expect(claim.ok()).toBeTruthy();
  const credentials = (await claim.json()) as AgentCredentials;
  const config = fluentBitConfig(agent!, credentials, transport);
  expect(config).toContain(`Brokers ${transport.bootstrap_server}`);
  expect(config).toContain(
    "rdkafka.ssl.endpoint.identification.algorithm https",
  );
  writeFileSync(join(run!, "fluent-bit.conf"), config, { mode: 0o600 });
  writeFileSync(join(run!, "diploma-kafka-ca.pem"), transport.ca_pem!, {
    mode: 0o600,
  });
});
