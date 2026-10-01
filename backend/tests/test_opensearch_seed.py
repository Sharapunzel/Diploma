import importlib.util
import json
from pathlib import Path


def test_reseed_updates_existing_stream_backings_and_creates_only_missing(monkeypatch):
    script = Path(__file__).parent / "opensearch-wazuh5" / "seed_synthetic.py"
    spec = importlib.util.spec_from_file_location("task12_seed_synthetic", script)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    stream = "wazuh-findings-v5-security"
    backings = [f".ds-{stream}-000001", f".ds-{stream}-000002"]
    stored = {
        backing: {
            sequence: {**module.document(sequence, "wazuh-security"),
                       "@timestamp": "2026-06-15T12:00:00Z"}
            for sequence in range(start, end)
        }
        for backing, start, end in ((backings[0], 0, 36), (backings[1], 36, 71))
    }
    writes = []

    def call(method, path, username, password, payload=None, content_type="application/json"):
        assert username == "admin" and password == "test-only"
        if method == "GET" and path == f"/_data_stream/{stream}":
            return {"data_streams": [{"name": stream,
                                      "indices": [{"index_name": name} for name in backings]}]}
        if method == "POST" and path.endswith("/_mget"):
            backing = path.split("/")[1]
            return {"docs": [{"found": sequence in stored[backing],
                              "_source": stored[backing].get(sequence)}
                             for sequence in range(72)]}
        if method == "POST" and path == "/_bulk?refresh=true":
            lines = [json.loads(line) for line in payload.splitlines()]
            created = []
            for header, source in zip(lines[::2], lines[1::2], strict=True):
                sequence = int(header["create"]["_id"].split("-")[1])
                stored[backings[-1]][sequence] = source
                created.append(sequence)
            writes.append(("create", created))
            return {"errors": False}
        if method == "POST" and "/_update/" in path:
            backing = path.split("/")[1]
            sequence = int(path.split("event-")[1].split("?")[0])
            stored[backing][sequence].update(payload["doc"])
            writes.append(("update", backing, sequence))
            return {"result": "updated"}
        raise AssertionError((method, path))

    monkeypatch.setattr(module, "call", call)
    module.seed_stream(stream, 72, "wazuh-security", "admin", "test-only")
    assert ("create", [71]) in writes
    assert ("update", backings[0], 0) in writes
    assert ("update", backings[0], 20) in writes
    assert stored[backings[0]][20]["@timestamp"] == 1781524800000
    writes.clear()
    module.seed_stream(stream, 72, "wazuh-security", "admin", "test-only")
    assert not any(write[0] == "create" for write in writes)
