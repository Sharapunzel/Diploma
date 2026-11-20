"""Authenticated HTTP serialization of honest preview/canonical refusals.

Run using backend-db selecting this module. The clock/barrier helpers belong to
the companion real-transaction timeout suite; no synthetic success is inserted.
"""

from sqlalchemy import func, select
from test_aggregate_execution_budget import budget_case, controlled_budget, timed_service
from test_aggregate_retention_boundaries import http_lookup_client
from test_aggregate_service import request
from test_event_time_pipeline import context, migrated

from app.models import AggregateClosure
from app.services.implementations.aggregate_snapshot import AggregateSnapshotService

__all__ = ["context", "http_lookup_client", "migrated"]


def expire_after_complete_candidate(monkeypatch, clock):
    original = AggregateSnapshotService.build

    def expired(self, source_id, rule, start, end, duration, origin, cohort, budget, preview=False):
        original(self, source_id, rule, start, end, duration, origin, cohort, budget, preview)
        clock[0] += budget.seconds + .01
        budget.check()

    monkeypatch.setattr(AggregateSnapshotService, "build", expired)


def test_http_terminal_refusal_is_available_by_list_id_and_index(context, monkeypatch, http_lookup_client):
    case = budget_case(context)
    clock = controlled_budget(monkeypatch)
    expire_after_complete_candidate(monkeypatch, clock)
    with timed_service(context, case, clock) as api:
        outcome = api.advance(case.card.id)
        assert outcome["calculation_status"] == "uncomputed"
    client = http_lookup_client
    prefix = f"/api/v1/aggregates/{case.card.id}/points"
    results = [client.get(prefix + "/by-index/2"), client.get(prefix + "/" + str(outcome["point_id"]))]
    for response in results:
        assert response.status_code == 200, response.text
        point = response.json()
        assert point["id"] == str(outcome["point_id"]) and point["window_index"] == 2
        assert point["calculation_status"] == "uncomputed"
        assert point["calculation_reason"] == "execution_budget_exceeded"
        assert point["payload_state"] == "present"
        assert point["execution"]["calculation_status"] == "uncomputed"
        for key in ("event_count", "features", "event_time_min", "event_time_max",
                "snapshot_reference_at", "snapshot_metadata", "completeness", "completeness_reason", "evidence_refs"):
            assert key in point and point[key] is None
    page = client.get(prefix)
    assert page.status_code == 200, page.text
    assert page.json()["total"] == 1 and page.json()["items"] == [results[0].json()]
    pending = client.get(prefix + "/by-index/3")
    assert pending.status_code == 409 and pending.json()["code"] == "canonical_point_not_ready"


def test_http_preview_timeout_has_policy_diagnostics_without_canonical_state(context, monkeypatch, http_lookup_client):
    clock = controlled_budget(monkeypatch)
    expire_after_complete_candidate(monkeypatch, clock)
    client = http_lookup_client
    csrf = {"X-CSRF-Token": client.get("/api/v1/auth/session").json()["csrf_token"]}
    body = request(context[2], duration=1).model_dump(mode="json")
    response = client.post("/api/v1/aggregates/preview", json=body, headers=csrf)
    assert response.status_code == 422, response.text
    data = response.json()
    assert data["code"] == "execution_budget_exceeded"
    assert data["request_id"] == response.headers["X-Request-ID"]
    diagnostic = data["details"]["execution"]
    assert diagnostic["calculation_status"] == "uncomputed"
    assert diagnostic["calculation_reason"] == "execution_budget_exceeded"
    assert diagnostic["hard_budget_seconds"] == .8
    assert diagnostic["origin"] == "preview" and diagnostic["elapsed_seconds"] >= .8
    for key in ("event_count", "features", "snapshot_reference_at"):
        assert key not in diagnostic
    with context[0].session_factory() as session:
        assert session.scalar(select(func.count()).select_from(AggregateClosure).where(
            AggregateClosure.source_identity == context[2])) == 0
