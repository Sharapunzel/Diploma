"""Server HTTP/runtime scenario within the existing isolated smoke profiles."""

import json
import secrets
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from subprocess import SubprocessError
from uuid import uuid4

import httpx


def validate_runtime_arguments(args):
    if not args.aggregate_only:
        return
    if args.profile not in {"end-to-end", "smoke-development", "smoke-production"}:
        raise ValueError("--aggregate-only requires end-to-end or a smoke profile")
    if args.detector_only or args.rcf or args.wal or args.pytest is not None or args.browser_only:
        raise ValueError("--aggregate-only cannot combine with other test selections")


def runtime_card(identity, card):
    return {"detector_id": identity,
        "consumed_index": (card.get("last_consumed") or {}).get("index"),
        "evaluated_index": (card.get("last_evaluated") or {}).get("index"),
        **{key: card.get(key) for key in (
        "aggregate_id", "source_id", "execution_state", "execution_started_at",
        "backlog", "next_scheduled_at", "last_consumed", "last_evaluated")}}


def aggregate_event_time_smoke(compose, env, production, command, lifecycle):
    username, password = "event-time-" + uuid4().hex, secrets.token_urlsafe(24)

    def fixture(action, payload=None):
        script = (Path(__file__).resolve().parents[1] / "backend/tests/ml/event_time_runtime.py").read_text(encoding="utf-8")
        output = command([*compose, "exec", "-T", "-e", f"EVENT_TIME_USER={username}",
            "-e", f"EVENT_TIME_PASSWORD={password}", "-e", "EVENT_TIME_PAYLOAD=" + json.dumps(payload or {}),
            "api", "python", "-c", script, action], env=env, capture=True, timeout=30)
        return json.loads(output.strip().splitlines()[-1])

    seeded = fixture("setup")
    port = env["HTTPS_BIND" if production else "PROXY_BIND"].split(":")[1]
    origin = f"{'https' if production else 'http'}://localhost:{port}"
    offsets, last_write = seeded["offsets"], 0
    ready_at, detectors, ids = {}, [], []

    def diagnostics():
        # Only explicit safe fields, never raw data, tokens or model state.
        for service, path in (("detector-worker", "/tmp/detector-worker.json"),):
            try:
                result = command([*compose, "exec", "-T", service, "cat", path], env=env, capture=True)
                print("Runtime worker diagnostics: " + result.strip(), flush=True)
            except (RuntimeError, OSError, SubprocessError) as error:
                print("Runtime diagnostics unavailable: " + type(error).__name__, flush=True)
        try:
            containers = command([*compose, "ps", "-q"], env=env, capture=True).split()
            result = command(["docker", "stats", "--no-stream", "--format", "{{json .}}", *containers], env=env, capture=True)
            print("Runtime resource observations: " + result.strip(), flush=True)
        except (RuntimeError, OSError, SubprocessError) as error:
            print("Runtime resource observations unavailable: " + type(error).__name__, flush=True)

    def write(count=None, timestamp=None):
        nonlocal offsets, last_write
        now = time.monotonic()
        if count is None and now - last_write < 1:
            return
        payload = {"sources": seeded["sources"], "offsets": offsets,
                   "count": count if count is not None else 1 + int(now) % 3}
        if timestamp:
            payload["timestamp"] = timestamp
        offsets = fixture("write", payload)["offsets"]
        last_write = time.monotonic()

    with httpx.Client(base_url=origin, verify=env["KAFKA_CA_FILE"] if production else True, timeout=30) as client:
        def request(method, path, body=None, status=200, key=None):
            headers = {**csrf} if method != "GET" else {}
            if key:
                headers["Idempotency-Key"] = key
            response = client.request(method, "/api/v1/" + path, json=body, headers=headers)
            assert response.status_code == status, (path, response.status_code, response.text[:500])
            return response.json()

        try:
            csrf = {}
            request("POST", "auth/local/login", {"username": username, "password": password})
            csrf = {"X-CSRF-Token": request("GET", "auth/session")["csrf_token"]}
            ids = []
            for index, source in enumerate(seeded["sources"]):
                rule = {"where": {"op": "has_any", "field": "event.category",
                    "value": ["authentication" if index == 0 else "web"]},
                    "features": [{"key": "n", "name": "Events", "op": "count"}]}
                body = {"source_id": source, "aggregate_rule": rule,
                    "schedule": {"boundary": seeded["anchor"], "timezone": "UTC", "duration_seconds": 5},
                    "window_start_utc": seeded["anchor"], "time_basis": "event_time"}
                checked = request(
                    "POST", "aggregates/validate", {k: body[k] for k in ("source_id", "aggregate_rule", "schedule")})
                request("POST", "aggregates/preview", body)
                card = request("POST", "aggregates", body | {"name": "Runtime " + str(index), "rule_hash": checked["rule_hash"]},
                    status=201, key=str(uuid4()))
                assert card["semantic_basis"] == "event_time"
                ids.append(card["id"])

            def wait(predicate, seconds=45, pump=True):
                deadline = time.monotonic() + seconds
                while True:
                    if pump:
                        write()
                    result = predicate()
                    if result:
                        return result
                    assert time.monotonic() < deadline, "Event-time runtime condition timed out"
                    time.sleep(.1)

            def points(identity):
                return request("GET", f"aggregates/{identity}/points?limit=100")["items"]

            wait(lambda: all(sum(p["origin"] == "initial_history" for p in points(a)) == 16 for a in ids))
            old = next(p for p in points(ids[0]) if p["window_index"] == 0)
            write(1, seeded["anchor"])
            wait(lambda: request("GET", f"aggregates/{ids[0]}")["diagnostics"]["late_count"] >= 1)
            assert request("GET", f"aggregates/{ids[0]}/points/by-index/0") == old
            # Fresh online history is continuous; the unknown archive-to-now tail
            # is never silently spliced into a model batch.
            wait(lambda: all(sum(p["origin"] == "online" and p["completeness"] == "complete"
                for p in points(a)) >= 16 for a in ids), seconds=120)
            detectors = []
            for aggregate, accelerated in ((ids[0], False), (ids[0], True), (ids[1], False)):
                body = {"aggregate_id": aggregate, "name": "Runtime RCF", "accelerated": accelerated,
                    "config": {"trees": 20, "sample_size": 8, "output_after": 8}}
                if accelerated:
                    body["step_seconds"] = 1
                receipt = request("POST", "detectors", body, status=201, key=str(uuid4()))
                detectors.append(receipt["detector_id"])
                request("POST", f"detectors/{detectors[-1]}/start", status=202, key=str(uuid4()))

            def running():
                cards = [request("GET", f"detectors/{d}") for d in detectors]
                assert all(c["execution_state"] != "failed" for c in cards), cards
                for identity, card in zip(detectors, cards):
                    if card["execution_state"] == "running":
                        ready_at.setdefault(identity, datetime.now(UTC).isoformat())
                return all(c["execution_state"] == "running" for c in cards)
            wait(running, seconds=90)
            # Measure windows opened after every forest became running. Windows
            # accumulated during the three initial Java warmups are startup backlog.
            measured = max(request("GET", f"aggregates/{a}")["next_index"] for a in ids) + 4
            print("Runtime measurement boundary: " + json.dumps({"ready_at": ready_at, "measured": measured,
                "sources": seeded["sources"], "aggregates": [request("GET", f"aggregates/{a}")["next_index"] for a in ids], "aggregate_ids": ids, "detectors": [runtime_card(d, request("GET", f"detectors/{d}")) for d in detectors]}), flush=True)
            for target in range(measured, measured + 2):
                wait(lambda target=target: all((request("GET", f"detectors/{d}")["last_evaluated"] or {}).get("index", -1)
                    >= target for d in detectors), seconds=45)
                windows = fixture("timings", {"detectors": detectors, "index": target})
                for d in detectors:
                    print("Runtime measured window: " + json.dumps({"target": target, "detector_id": d, "window": windows[d]}), flush=True)
                for d in detectors:
                    window = windows[d]
                    assert window is not None and window["index"] == target and window["kind"] == "evaluated", windows
                    assert not window["late"], {"detector_id": d, **window}
                    completed = datetime.fromisoformat(window["evaluation_completed_at"])
                    deadline = datetime.fromisoformat(window["close_not_before_at"])
                    assert completed <= deadline + timedelta(seconds=5), {"detector_id": d, **window}
            write(100)
            def anomaly_seen_for_both_sources():
                findings = fixture("inspect", {"aggregates": ids})["findings"].values()
                return {f["source_id"] for f in findings if f["grade"] > 0 and f["exact_values"][0] >= 100} == set(seeded["sources"])
            wait(anomaly_seen_for_both_sources, seconds=45)
            shock = next(f for f in fixture("inspect", {"aggregates": ids})["findings"].values()
                if f["source_id"] == seeded["sources"][0] and f["exact_values"][0] >= 100)
            wait(lambda: all((request("GET", f"detectors/{d}")["last_evaluated"] or {}).get("index", -1)
                >= shock["window_index"] for d in detectors), seconds=30)
            # A confirmed pause cancels commits, including across both restarts.
            receipt = request("POST", f"aggregates/{ids[0]}/pause", key="pause")
            position = request("GET", f"aggregates/{ids[0]}")["next_index"]
            command([*compose, "restart", "aggregate-worker", "detector-worker"], env=env)
            until = time.monotonic() + 11
            while time.monotonic() < until:
                write()
                time.sleep(.2)
            paused = request("GET", f"aggregates/{ids[0]}")
            assert paused["paused"] and paused["next_index"] == position
            request("POST", f"aggregates/{ids[0]}/unpause", key="unpause")
            assert request("POST", f"aggregates/{ids[0]}/pause", key="pause") == receipt
            wait(lambda: request("GET", f"aggregates/{ids[0]}")["next_index"] > position)
            request("POST", f"detectors/{detectors[1]}/stop", key=str(uuid4()))
            until = time.monotonic() + 11
            while time.monotonic() < until:
                write()
                time.sleep(.2)
            request("POST", f"aggregates/{ids[0]}/points/cleanup", {"keep_last": 0}, status=409, key="running-cleanup")
            request("POST", f"detectors/{detectors[0]}/stop", key=str(uuid4()))
            request("POST", f"aggregates/{ids[0]}/pause", key="cleanup-pause")
            plan = request("GET", f"aggregates/{ids[0]}/points/cleanup-preview?keep_last=0")
            assert len(plan["blockers"]) >= 1 and plan["minimum_keep"] > 0
            individual = {d: 0 for d in detectors[:2]}
            individual.update({b["detector_id"]: b["minimum_keep"] for b in plan["blockers"]})
            assert individual[detectors[1]] > individual[detectors[0]], individual
            assert plan["minimum_keep"] == max(individual.values())
            request("POST", f"aggregates/{ids[0]}/points/cleanup", {"keep_last": 0}, status=409, key="protected-cleanup")
            before = fixture("inspect", {"aggregates": ids})
            accepted = request("POST", f"aggregates/{ids[0]}/points/cleanup", {"keep_last": plan["minimum_keep"]},
                status=202, key="cleanup")
            operation = accepted["cleanup_operation_id"]
            def progress():
                result = request("GET", f"aggregates/{ids[0]}/cleanup-operations/{operation}")
                assert result["status"] != "failed", result
                return result if result["deleted_points"] else None
            started = wait(progress)
            assert 0 < started["deleted_points"] < started["planned_delete"], started
            command([*compose, "restart", "aggregate-worker"], env=env)
            restored = request("GET", f"aggregates/{ids[0]}/cleanup-operations/{operation}")
            assert restored["deleted_points"] >= started["deleted_points"]
            for key in ("id", "requested_keep", "reference_tail", "planned_delete"):
                assert restored[key] == started[key]
            wait(lambda: request("GET", f"aggregates/{ids[0]}/cleanup-operations/{operation}")["status"] == "succeeded")
            assert request("GET", f"aggregates/{ids[0]}/cleanup-operations/{operation}")["deleted_points"] == started["planned_delete"]
            assert request("POST", f"aggregates/{ids[0]}/points/cleanup", {"keep_last": plan["minimum_keep"]},
                status=202, key="cleanup") == accepted
            after = fixture("inspect", {"aggregates": ids})
            assert all(after["findings"].get(k) == v for k, v in before["findings"].items())
            assert all(after["checkpoints"][d] == before["checkpoints"][d] for d in detectors[:2])
            assert after["payloads"][ids[0]] < before["payloads"][ids[0]]
            request("GET", f"aggregates/{ids[0]}/points/{shock['origin_point_id']}", status=410)
            for d in detectors:
                request("POST", f"detectors/{d}/stop", key=str(uuid4()))
            for a in ids:
                request("POST", f"aggregates/{a}/pause", key=str(uuid4()))
            wait(lambda: fixture("inspect", {"aggregates": ids})["capture_pending"] == 0, pump=False)
            stable = fixture("inspect", {"aggregates": ids})
            lifecycle()
            assert fixture("inspect", {"aggregates": ids}) == stable
            assert request("GET", f"aggregates/{ids[0]}/cleanup-operations/{operation}")["status"] == "succeeded"
            print("Event-time HTTP/worker smoke passed: archive, late, both RCF warmups, pause/restart/catchup, protected cleanup and independent findings", flush=True)
        except Exception:
            for identity in detectors:
                try:
                    print("Runtime failure state: " + json.dumps(runtime_card(identity, request("GET", f"detectors/{identity}"))), flush=True)
                except (AssertionError, RuntimeError, httpx.HTTPError) as error:
                    print("Runtime failure readback unavailable: " + type(error).__name__, flush=True)
            for identity in ids:
                try:
                    card = request("GET", f"aggregates/{identity}")
                    print("Runtime aggregate position: " + json.dumps({"aggregate_id": identity, "next_index": card["next_index"], "paused": card["paused"]}), flush=True)
                except (AssertionError, RuntimeError, httpx.HTTPError) as error:
                    print("Runtime aggregate position unavailable: " + type(error).__name__, flush=True)
            diagnostics()
            raise
