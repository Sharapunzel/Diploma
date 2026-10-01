import base64
import getpass
import json
import ssl
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parent
CERTS = ROOT / "runtime" / "certs"
BASE_URL = "https://localhost:19200"


def call(method, path, username, password, payload=None, content_type="application/json"):
    if content_type == "application/x-ndjson" and payload is not None:
        body = payload.encode()
    else:
        body = None if payload is None else json.dumps(payload).encode()
    token = base64.b64encode(f"{username}:{password}".encode()).decode()
    request = Request(
        BASE_URL + path,
        data=body,
        method=method,
        headers={"Authorization": f"Basic {token}", "Content-Type": content_type},
    )
    context = ssl.create_default_context(cafile=str(CERTS / "root-ca.pem"))
    try:
        with urlopen(request, context=context, timeout=20) as response:
            content = response.read(5_242_881)
            if len(content) > 5_242_880:
                raise RuntimeError("Unexpectedly large response")
            return json.loads(content) if content else None
    except HTTPError as error:
        detail = error.read(2048).decode("utf-8", errors="replace")
        raise RuntimeError(f"Indexer request failed ({error.code}): {detail}") from None


def document(sequence: int, group: str):
    minute = sequence % 4
    timestamp = f"2026-06-15T12:{minute:02d}:00Z"
    if sequence == 20:
        timestamp = 1781524800000  # 2026-06-15T12:00:00Z, epoch millis
    elif sequence == 0:
        timestamp = "2026-06-15T15:00:00+03:00"
    return {
        "@timestamp": timestamp,
        "event": {"action": "login" if sequence % 2 == 0 else "logout",
                  "category": ["authentication"], "sequence": sequence},
        "host": {"hostname": f"synthetic-{sequence % 7:02d}"},
        "source": {"ip": f"192.0.2.{sequence % 200 + 1}"},
        "wazuh": {"agent": {"id": f"{sequence % 5:03d}", "name": f"agent-{sequence % 5}"},
                  "rule": {"id": f"{1000 + sequence % 3}"}},
        "message": f"Synthetic security event {sequence} ({group})",
    }


def target_exists(path, username, password):
    try:
        call("GET", path, username, password)
        return True
    except RuntimeError as error:
        if "(404)" in str(error):
            return False
        raise


def bulk_index(index, records, operation="index"):
    rows = []
    for sequence, item in records:
        rows.append(json.dumps({operation: {"_index": index, "_id": f"event-{sequence:04d}"}}))
        rows.append(json.dumps(item))
    return "\n".join(rows) + "\n"


def backing_indices(stream, username, password):
    result = call("GET", f"/_data_stream/{stream}", username, password)
    streams = result.get("data_streams")
    if not isinstance(streams, list) or len(streams) != 1 or streams[0].get("name") != stream:
        raise RuntimeError("Unexpected data stream metadata")
    indices = [item.get("index_name") for item in streams[0].get("indices", [])]
    if not indices or len(indices) > 50 or any(
        not isinstance(index, str) or not index.startswith(f".ds-{stream}-")
        for index in indices
    ):
        raise RuntimeError("Unexpected data stream backing indices")
    return indices


def existing_stream_documents(stream, count, group, username, password):
    found = {}
    ids = [f"event-{sequence:04d}" for sequence in range(count)]
    for backing in backing_indices(stream, username, password):
        result = call("POST", f"/{backing}/_mget", username, password, {"ids": ids})
        docs = result.get("docs")
        if not isinstance(docs, list) or len(docs) != count:
            raise RuntimeError("Unexpected data stream documents response")
        for sequence, item in enumerate(docs):
            if item.get("found") is not True:
                continue
            expected = document(sequence, group)
            actual = item.get("_source")
            if not isinstance(actual, dict) or {
                name: value for name, value in actual.items() if name != "@timestamp"
            } != {name: value for name, value in expected.items() if name != "@timestamp"}:
                raise RuntimeError("Existing stream document is not from this synthetic seed")
            found.setdefault(sequence, []).append(backing)
    return found


def check_bulk_result(result):
    if result.get("errors"):
        errors = [detail["error"] for row in result["items"] for detail in row.values()
                  if detail.get("error")]
        raise RuntimeError(f"Indexer rejected {len(errors)} synthetic documents: {errors[:3]}")


def seed_stream(stream, count, group, username, password):
    existing = existing_stream_documents(stream, count, group, username, password)
    missing = [(sequence, document(sequence, group)) for sequence in range(count)
               if sequence not in existing]
    if missing:
        result = call("POST", "/_bulk?refresh=true", username, password,
                      bulk_index(stream, missing, "create"), "application/x-ndjson")
        check_bulk_result(result)
    for sequence in (0, 20):
        for backing in existing.get(sequence, []):
            payload = {"doc": {"@timestamp": document(sequence, group)["@timestamp"]}}
            call("POST", f"/{backing}/_update/event-{sequence:04d}?refresh=true",
                 username, password, payload)


def main():
    admin_password = getpass.getpass("Wazuh Indexer admin password: ")
    common_mapping = {
        "properties": {
            "@timestamp": {"type": "date"},
            "event": {"properties": {
                "action": {"type": "keyword"}, "category": {"type": "keyword"},
                "sequence": {"type": "long"},
            }},
            "host": {"properties": {"hostname": {"type": "keyword"}}},
            "source": {"properties": {"ip": {"type": "ip"}}},
            "wazuh": {"properties": {
                "agent": {"properties": {
                    "id": {"type": "keyword"}, "name": {"type": "keyword"},
                }},
                "rule": {"properties": {"id": {"type": "keyword"}}},
            }},
            "message": {"type": "text"},
        }
    }
    stream_template = {
        "index_patterns": ["wazuh-findings-v5-*"],
        "data_stream": {},
        "template": {"mappings": common_mapping},
    }
    call("PUT", "/_index_template/task12-wazuh-findings-v5", "admin", admin_password,
         stream_template)
    for stream in ("wazuh-findings-v5-security", "wazuh-findings-v5-audit"):
        path = f"/_data_stream/{stream}"
        if not target_exists(path, "admin", admin_password):
            call("PUT", path, "admin", admin_password, {})

    for index in ("task12-concrete", "task12-logs-2026-06", "task12-logs-2026-07"):
        if not target_exists(f"/{index}", "admin", admin_password):
            call("PUT", f"/{index}", "admin", admin_password,
                 {"mappings": common_mapping})

    groups = [
        ("task12-concrete", 24, "concrete"),
        ("task12-logs-2026-06", 64, "family-june"),
        ("task12-logs-2026-07", 64, "family-july"),
        ("wazuh-findings-v5-security", 72, "wazuh-security"),
        ("wazuh-findings-v5-audit", 72, "wazuh-audit"),
    ]
    for index, count, group in groups:
        if index.startswith("wazuh-findings"):
            seed_stream(index, count, group, "admin", admin_password)
            continue
        records = [(i, document(i, group)) for i in range(count)]
        body = bulk_index(index, records, "index")
        result = call("POST", "/_bulk?refresh=true", "admin", admin_password,
                      body, "application/x-ndjson")
        check_bulk_result(result)
    print("Seeded 296 synthetic documents across the concrete index, index family, and two WCS-like data streams.")


if __name__ == "__main__":
    main()
