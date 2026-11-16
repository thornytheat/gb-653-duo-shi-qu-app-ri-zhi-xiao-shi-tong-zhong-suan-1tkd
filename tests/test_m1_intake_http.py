"""M1 采集 HTTP 集成：页面渲染、采集动作、过期推进 422 与重试幂等。"""
import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


@pytest.fixture
def client(tmp_path):
    os.environ["DB_PATH"] = str(tmp_path / "http.db")
    from fastapi.testclient import TestClient
    from app.main import create_app
    with TestClient(create_app()) as value:
        yield value


def test_logs_page_renders_intake_ledger(client):
    res = client.get("/logs")
    assert res.status_code == 200
    assert "来源与文件代次台账" in res.text
    assert "/api/logs/intake" in res.text


def test_intake_lifecycle_over_http(client):
    r = client.post("/api/logs/intake", json={"action": "open", "source": "pay/a.log"})
    assert r.status_code == 200 and r.json()["generation_view"]["generation"] == 1

    r = client.post("/api/logs/intake", json={
        "action": "upload", "source": "pay/a.log", "generation": 1, "seq": 0,
        "text": '{"request_id":"a"}\n{"reque', "expected_offset": 0})
    assert r.status_code == 200
    assert r.json()["generation_view"]["pending_tail"] == '{"reque'

    # 换代：旧代待补齐，新代立即接收
    r = client.post("/api/logs/intake", json={
        "action": "rotate", "source": "pay/a.log", "generation": 1})
    assert r.status_code == 200
    states = [g["state"] for g in r.json()["snapshot"]["sources"][0]["generations"]]
    assert states == ["PENDING_TAIL", "RECEIVING"]

    # 过期换代被拒绝（STALE_VERSION → 422）
    r = client.post("/api/logs/intake", json={
        "action": "rotate", "source": "pay/a.log", "generation": 1})
    assert r.status_code == 422 and r.json()["error"] == "STALE_VERSION"

    # 旧代补写后明确结尾，尾行归入原批次
    client.post("/api/logs/intake", json={
        "action": "upload", "source": "pay/a.log", "generation": 1, "seq": 1,
        "text": 'st_id":"b"}\n'})
    r = client.post("/api/logs/intake", json={
        "action": "end", "source": "pay/a.log", "generation": 1})
    assert r.status_code == 200
    g1 = r.json()["snapshot"]["sources"][0]["generations"][0]
    assert g1["state"] == "COMPLETE" and g1["valid_count"] == 2
    assert g1["batch_id"] == "pay/a.log#g1"


def test_http_retry_is_idempotent_and_conflict_422(client):
    client.post("/api/logs/intake", json={"action": "open", "source": "s"})
    payload = {"action": "upload", "source": "s", "generation": 1, "seq": 0,
               "text": '{"request_id":"r"}\n', "expected_offset": 0}
    first = client.post("/api/logs/intake", json=payload).json()
    again = client.post("/api/logs/intake", json=payload).json()
    assert again["replayed"] is True
    assert again["generation_view"]["valid_count"] == 1
    assert again["generation_view"]["received_offset"] == \
        first["generation_view"]["received_offset"]

    bad = dict(payload, text="different\n")
    r = client.post("/api/logs/intake", json=bad)
    assert r.status_code == 422 and r.json()["error"] == "SEGMENT_CONFLICT"


def test_http_position_conflict_and_closed(client):
    client.post("/api/logs/intake", json={"action": "open", "source": "s"})
    client.post("/api/logs/intake", json={
        "action": "upload", "source": "s", "generation": 1, "seq": 0, "text": "a\n"})
    r = client.post("/api/logs/intake", json={
        "action": "upload", "source": "s", "generation": 1, "seq": 1,
        "text": "b\n", "expected_offset": 0})
    assert r.status_code == 422 and r.json()["error"] == "POSITION_CONFLICT"

    client.post("/api/logs/intake", json={
        "action": "end", "source": "s", "generation": 1})
    r = client.post("/api/logs/intake", json={
        "action": "upload", "source": "s", "generation": 1, "seq": 1, "text": "c\n"})
    assert r.status_code == 422 and r.json()["error"] == "GENERATION_CLOSED"


def test_http_unknown_action_and_missing_source(client):
    r = client.post("/api/logs/intake", json={"action": "nope", "source": "s"})
    assert r.status_code == 422 and r.json()["error"] == "UNKNOWN_ACTION"
    r = client.post("/api/logs/intake", json={"action": "upload", "source": "ghost",
                                              "generation": 1, "seq": 0, "text": "x"})
    assert r.status_code == 422 and r.json()["error"] == "SOURCE_NOT_FOUND"


def test_snapshot_endpoint_and_legacy_import_unchanged(client):
    assert client.get("/api/logs/intake").json()["totals"]["generation_count"] == 0
    legacy = client.post("/api/logs/import", json={}).json()
    assert legacy["valid_count"] == 2 and legacy["quarantine_count"] == 1


def test_invalid_arguments_return_422_not_500(client):
    r = client.post("/api/logs/intake", json={"action": "open", "source": "  "})
    assert r.status_code == 422 and r.json()["error"] == "INVALID_ARGUMENT"
    r = client.post("/api/logs/intake", json={
        "action": "upload", "source": "s", "generation": "x", "seq": 0, "text": "a"})
    assert r.status_code == 422 and r.json()["error"] == "INVALID_ARGUMENT"
    r = client.post("/api/logs/intake", json={"action": "open"})
    assert r.status_code == 422 and r.json()["error"] == "INVALID_ARGUMENT"
