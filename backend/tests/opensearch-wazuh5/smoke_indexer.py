import base64
import json
import ssl
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parent
CERTS = ROOT / "runtime" / "certs"
BASE_URL = "https://127.0.0.1:19200"
USERNAME = "diploma_task12_reader"


def call(method, path, password, payload=None):
    body = None if payload is None else json.dumps(payload).encode()
    token = base64.b64encode(f"{USERNAME}:{password}".encode()).decode()
    request = Request(BASE_URL + path, data=body, method=method, headers={
        "Authorization": f"Basic {token}", "Content-Type": "application/json",
    })
    context = ssl.create_default_context(cafile=str(CERTS / "root-ca.pem"))
    try:
        with urlopen(request, context=context, timeout=10) as response:
            data = response.read(5_242_881)
            if len(data) > 5_242_880:
                raise RuntimeError("Response exceeded smoke limit")
            return response.status, json.loads(data) if data else None
    except HTTPError as error:
        data = error.read(2048).decode("utf-8", errors="replace")
        return error.code, data


def main():
    password = (ROOT / "runtime" / "reader-password.txt").read_text(encoding="utf-8")
    version = call("GET", "/", password)
    assert version[0] == 200
    indices = call("GET", "/_cat/indices?format=json&h=index&expand_wildcards=open", password)
    assert indices[0] == 200 and "task12-concrete" in {row["index"] for row in indices[1]}
    streams = call("GET", "/_resolve/index/*?expand_wildcards=open", password)
    assert streams[0] == 200
    stream_names = {item["name"] for item in streams[1]["data_streams"]}
    assert {"wazuh-findings-v5-security", "wazuh-findings-v5-audit"} <= stream_names

    targets = ["wazuh-findings-v5-security", "wazuh-findings-v5-audit"]
    caps = call("GET", "/" + ",".join(targets) + "/_field_caps?fields=*&include_unmapped=true", password)
    assert caps[0] == 200
    fields = caps[1]["fields"]
    assert "@timestamp" in fields and "wazuh.agent.name" in fields
    mappings = call("GET", "/" + ",".join(targets) + "/_mapping", password)
    assert mappings[0] == 200
    assert mappings[1]

    opened = call("POST", "/" + ",".join(targets) + "/_search/point_in_time?keep_alive=2m", password)
    assert opened[0] == 200 and opened[1].get("pit_id")
    result = call("POST", "/_search?allow_partial_search_results=false", password, {
        "size": 3,
        "pit": {"id": opened[1]["pit_id"], "keep_alive": "2m"},
        "query": {"bool": {"filter": [{"exists": {"field": "@timestamp"}}]}},
        "sort": [{"@timestamp": {"order": "desc", "missing": "_last"}},
                 {"_shard_doc": "desc"}],
        "track_total_hits": False,
    })
    assert result[0] == 200 and result[1]["_shards"]["failed"] == 0
    hits = result[1]["hits"]["hits"]
    assert len(hits) == 3 and all(hit["_source"].get("wazuh") for hit in hits)
    close = call("DELETE", "/_search/point_in_time", password,
                 {"pit_id": result[1].get("pit_id", opened[1]["pit_id"])})
    assert close[0] == 200

    denied = call("PUT", "/task12-concrete/_doc/read-only-check", password,
                  {"@timestamp": "2026-06-15T12:00:00Z"})
    assert denied[0] == 403, f"Read-only account write check returned {denied[0]}"
    print("Read-only HTTPS, field caps, WCS data streams, PIT paging, and denied document writes passed.")
    print(f"OpenSearch {version[1]['version']['number']}; Wazuh Indexer 5.0.0-beta5; streams: {len(stream_names)}")


if __name__ == "__main__":
    main()
