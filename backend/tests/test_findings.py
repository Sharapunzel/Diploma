from datetime import datetime, timedelta
from uuid import uuid4

from fastapi.testclient import TestClient
from findings_fixture import ADMIN, GUEST, seed_findings
from sqlalchemy import func, select, text
from test_event_time_migration import legacy_database

from app.config import Settings
from app.core.security import hash_password
from app.db import Database
from app.main import create_app
from app.models import ParsedLog, Role, User, WorkingWindowPoint

__all__ = ["legacy_database"]
PATH = "/api/v1/findings"
PASSWORD = "findings-test-password"


def test_findings_permission_migration(legacy_database):
    engine, _, migrate = legacy_database
    migrate("upgrade", "0024_aggregate_execution_budget")

    def permissions():
        with engine.connect() as connection:
            return dict(connection.execute(select(Role.id, Role.permissions)).all())

    before = permissions()
    migrate("upgrade", "head")
    assert permissions() == {
        identity: [*before[identity], "findings.read"] for identity in (ADMIN, GUEST)
    }
    migrate("check")
    migrate("downgrade", "0024_aggregate_execution_budget")
    assert permissions() == before
    migrate("upgrade", "head")


def test_findings_list_timeline_cards_and_exact_events(legacy_database):
    engine, url, migrate = legacy_database
    migrate("upgrade", "head")
    db = Database(url)
    try:
        with db.session_factory.begin() as session:
            admin_id = uuid4()
            password = hash_password(PASSWORD)
            session.add_all(
                User(
                    id=identity,
                    username=name,
                    display_name=name,
                    role_id=role,
                    password_hash=password,
                )
                for identity, name, role in ((admin_id, "admin", ADMIN), (uuid4(), "guest", GUEST))
            )
            fixture = seed_findings(session, admin_id)
        assert fixture["source"]
        ids = fixture["findings"]
        params = {"timestamp_from": fixture["start"], "timestamp_to": fixture["stop"]}
        start = datetime.fromisoformat(fixture["start"])
        event_path = f"/api/v1/events/sources/{fixture['source']}/search"
        settings = Settings(environment="test", database_url=url)
        with TestClient(create_app(settings, database=db)) as client:
            assert client.get(PATH, params=params).status_code == 401
            for username in ("admin", "guest"):
                client.cookies.clear()
                response = client.post(
                    "/api/v1/auth/local/login", json={"username": username, "password": PASSWORD}
                )
                assert response.status_code == 200, response.text
                assert "findings.read" in response.json()["permissions"]
                first = client.get(PATH, params={**params, "limit": 1})
                assert first.status_code == 200, first.text
                assert first.json()["total"] == 3
                assert first.json()["items"][0]["id"] == ids["legacy"]
                rest = client.get(PATH, params={**params, "limit": 2, "offset": 1}).json()
                assert [item["id"] for item in rest["items"]] == sorted(
                    [ids["primary"], ids["single"]], reverse=True
                )
                assert client.get(PATH, params={**params, "offset": 100}).json()["total"] == 3
                timeline = client.get(f"{PATH}/timeline", params=params)
                assert timeline.status_code == 200, timeline.text
                chart = timeline.json()
                assert chart["total"] == first.json()["total"]
                assert chart["items"] == first.json()["items"] + rest["items"]
                assert chart["items"][0]["time_basis"] == "availability_at"
                assert chart["items"][1]["window_start"] < chart["items"][1]["window_end"]
                for path in (PATH, f"{PATH}/timeline"):
                    anchor = {**params, "anchor_id": ids["primary"], "limit": 1}
                    located = client.get(path, params=anchor)
                    assert located.status_code == 200, located.text
                    assert located.json()["items"][0]["id"] == ids["primary"]
                    assert located.json()["offset"] == next(
                        index
                        for index, row in enumerate(chart["items"])
                        if row["id"] == ids["primary"]
                    )
                    assert (
                        client.get(path, params={**anchor, "anchor_id": uuid4()}).status_code == 404
                    )
                filtered = {
                    **params,
                    "detector_id": fixture["detectors"][0],
                    "grade_min": 0.8,
                    "grade_max": 0.8,
                    "confidence_min": 0.9,
                    "confidence_max": 0.9,
                }
                assert [row["id"] for row in client.get(PATH, params=filtered).json()["items"]] == [
                    ids["primary"]
                ]
                assert client.get(f"{PATH}/timeline", params=filtered).json()["total"] == 1
                assert (
                    client.get(PATH, params={**filtered, "anchor_id": ids["legacy"]}).status_code
                    == 404
                )
                long_timeline = client.get(
                    f"{PATH}/timeline",
                    params={**params, "limit": 1, "timestamp_from": "1900-01-01T00:00:00Z"},
                )
                assert long_timeline.status_code == 200 and len(long_timeline.json()["items"]) == 1
                catalog = client.get(f"{PATH}/detectors").json()
                assert {row["detector_id"] for row in catalog} == set(fixture["detectors"])
                assert {row["detector_name"] for row in catalog} == {fixture["detector_name"]}

                for invalid in (
                    {"detector_id": "invalid"},
                    {"timestamp_from": "invalid"},
                    {"timestamp_from": start.isoformat().split("+")[0]},
                    {"timestamp_from": fixture["stop"]},
                    {"grade_min": 0.9, "grade_max": 0.1},
                    {"confidence_min": -1},
                    {"anchor_id": "invalid"},
                ):
                    for path in (PATH, f"{PATH}/timeline"):
                        assert client.get(path, params={**params, **invalid}).status_code == 422
                assert client.get(f"{PATH}/not-a-uuid").status_code == 422
                assert client.get(f"{PATH}/{uuid4()}").status_code == 404
                assert client.get(PATH, params={**params, "limit": 101}).status_code == 422
                response = client.get(f"{PATH}/{ids['primary']}")
                assert response.status_code == 200, response.text
                card = response.json()
                assert card["detector_name"] == fixture["detector_name"]
                assert card["aggregate_name"] == "Сохранённый агрегат"
                assert card["late"] and card["accelerated_used"]
                assert card["features"][0]["actual"] == "1152921504606846977"
                assert card["features"][0]["expected"] == "1152921504606846976.75"
                assert [feature["attribution"] for feature in card["features"]] == [3, 1, 0]
                assert "private" not in response.text and "checkpoint" not in response.text
                assert card["event_range"] == {
                    "timestamp_from": fixture["from"],
                    "timestamp_to": fixture["to"],
                }
                assert card["event_range_reason"] is None
                legacy = client.get(f"{PATH}/{ids['legacy']}").json()
                assert legacy["time_basis"] == "availability_at" and legacy["event_range"] is None
                assert legacy["event_range_reason"] == "unavailable"
                assert legacy["evaluation_started_at"] is None
                assert legacy["features"][1]["expected"] is None
                assert legacy["features"][0]["attribution"] is None
                assert (
                    client.get(f"{PATH}/{ids['max']}").json()["event_range_reason"]
                    == "unrepresentable"
                )

                csrf = client.get("/api/v1/auth/session").json()["csrf_token"]
                headers = {"X-CSRF-Token": csrf}
                result = client.post(event_path, json=card["event_range"], headers=headers)
                assert result.status_code == 200, result.text
                events = result.json()["items"]
                assert {event["fields"]["message"] for event in events} == {
                    "findings-min",
                    "findings-max",
                    "findings-late",
                }
                max_event = next(
                    event for event in events if event["fields"]["message"] == "findings-max"
                )
                result = client.get(
                    f"/api/v1/events/sources/{fixture['source']}/events/{max_event['id']}"
                )
                assert result.status_code == 200, result.text
                single = client.get(f"{PATH}/{ids['single']}").json()["event_range"]
                assert datetime.fromisoformat(single["timestamp_to"]) - datetime.fromisoformat(
                    single["timestamp_from"]
                ) == timedelta(microseconds=1)
                result = client.post(event_path, json=single, headers=headers)
                assert [event["fields"]["message"] for event in result.json()["items"]] == [
                    "findings-min"
                ]

            with engine.begin() as conn:
                assert conn.scalar(select(func.count()).select_from(WorkingWindowPoint)) == 0
                conn.execute(ParsedLog.__table__.delete())
                conn.execute(
                    text(
                        "UPDATE app.roles SET permissions=permissions-'findings.read' WHERE id=:role"
                    ),
                    {"role": GUEST},
                )
            assert (
                client.post(event_path, json=card["event_range"], headers=headers).json()["items"]
                == []
            )
            for path in (PATH, f"{PATH}/timeline", f"{PATH}/detectors", f"{PATH}/{ids['primary']}"):
                assert (
                    client.get(
                        path, params=params if path in (PATH, f"{PATH}/timeline") else {}
                    ).status_code
                    == 403
                )
    finally:
        db.dispose()
