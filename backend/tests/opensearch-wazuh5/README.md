# TASK-12 Wazuh Indexer-only stand

This stand runs the official `wazuh/wazuh-indexer:5.0.0-beta5` image only. The Docker image is pinned to digest `sha256:70d7c42518e4e295a73947ef916fc26e67f81658dd0c892d7032018b296607ce`. It does not add a Wazuh Manager, Dashboard, or Agent and does not alter the main Compose file.

The indexer uses one GiB of JVM heap, a dedicated named volume, and publishes HTTPS only on `127.0.0.1:19200`. The supplied scripts generate a local test CA and localhost certificate under the ignored `runtime/` directory. The CA private key remains in `runtime/authority/` and is not mounted in the container. They also create a randomly generated account with document read and PIT lifecycle permissions scoped to `task12-*` and `wazuh-findings-v5-*`. The administrator password is prompted and is never written to this directory. The reader password is written only to ignored `runtime/reader-password.txt`.

## Start and seed

Run these commands in this directory with Python 3.11+ and Docker Compose:

```powershell
python generate_certs.py
docker compose up -d
docker compose ps
python bootstrap_readonly.py
python seed_synthetic.py
```

`bootstrap_readonly.py` prompts for the indexer's `admin` password. On a fresh image, enter its configured initial password. The API role grants document reads and the required field-mapping/PIT actions only over the two test patterns. Cluster-wide access is limited to index metadata monitoring and resolving logical names; it does not grant document reads or writes there. `seed_synthetic.py` prompts again and uses the admin account to create two data streams and bulk insert synthetic events. Re-running it checks existing synthetic documents in the streams' backing indices, creates only missing IDs, and updates the timestamp of existing `event-0000` and `event-0020` in place; it does not remove a data stream or its volume. `smoke_indexer.py` exercises HTTPS, mappings, field capabilities, PIT paging and close, and confirms a document write is denied.

Configure an external connection in the application with URL `https://127.0.0.1:19200`, username `diploma_task12_reader`, the contents of `runtime/reader-password.txt`, and `runtime/certs/root-ca.pem`. TLS hostname and CA checks must remain enabled. Add these four external sources and enable each one:

| Target type | Target |
| --- | --- |
| Index | `task12-concrete` |
| Index pattern | `task12-logs-*` |
| Data stream | `wazuh-findings-v5-security` |
| Data stream pattern | `wazuh-findings-v5-*` |

The backend live API smoke uses the same connection settings through environment variables. With `TEST_DATABASE_URL` set strictly to `diploma_test`, configure these values in PowerShell and run the backend runner from `backend/`. `--temporary` creates and removes its own test database for an independent review run:

```powershell
$env:WAZUH_SMOKE_URL = "https://127.0.0.1:19200"
$env:WAZUH_SMOKE_USERNAME = "diploma_task12_reader"
$env:WAZUH_SMOKE_PASSWORD = Get-Content -Raw "tests/opensearch-wazuh5/runtime/reader-password.txt"
$env:WAZUH_SMOKE_CA = (Resolve-Path "tests/opensearch-wazuh5/runtime/certs/root-ca.pem").Path
python scripts/run_tests.py --temporary
```

When these variables are present, `test_external_live_wazuh_smoke` checks source registration and activation for all four target types, catalog, time/filter search, cursor paging, event cards, a numeric epoch-millis `@timestamp`, guest access, disable behavior, and unchanged local/Kafka tables. Re-run `seed_synthetic.py` with the stand's admin password before this check when upgrading an existing stand from the earlier all-string seed.

The test index has 24 documents, the index family has 128 documents, and the two stream family has 144 documents. The records include repeated timestamps for stable-order pagination, standard ECS-like fields, and WCS-shaped `wazuh.*` fields in mappings seeded by this stand. They are synthetic documents in the real Wazuh Indexer beta5 service, but they are not documents or schema from the Wazuh findings pipeline; this stand has no Manager and does not verify that pipeline's output.

## Scope and limits

These `wazuh.*` documents are synthetic WCS-shaped data in a real Wazuh Indexer 5 beta5 cluster. This stand creates its own test index template and simplified mappings. The documents exercise the indexer's HTTP, field capabilities, mapping, data stream, security, and PIT behavior. They are not findings emitted by a Wazuh Manager and do not verify the real findings pipeline or its schema. The indexer-only image contains no Manager to generate findings.

The external event catalog lists mappings and only advertises operators that are safe for their mapping. It does not offer `exists`/`not_exists`, because indexed presence cannot distinguish an explicit JSON `null`; it does not offer `neq`, because ignored malformed array members can change its meaning. OpenSearch mappings do not expose whether a field is scalar or an array, so generic numeric/date ranges are also omitted. The ECS `@timestamp` range remains available for the event time interval. Literal string operations require an unnormalized keyword-like field; text fields are listed with no operators rather than searched with full-text `match`. `contains` and `ends_with` require at least three characters to bound leading-wildcard cost.

The image's configured map-count requirement was checked before start (`vm.max_map_count=262144`). At the time the stand was prepared, Docker reported 8 CPUs and 7.7 GiB RAM; the existing Kafka container used about 1.1 GiB and PostgreSQL about 84 MiB. The stand sets a 1 GiB JVM heap.

## Stop

Stop only this service while keeping its test data:

```powershell
docker compose stop
```

Start it again with `docker compose up -d`. Do not use `down -v` as part of routine cleanup. The named volume is dedicated to this stand and survives container removal. The existing project containers and volumes are not managed by this Compose project.
