"""M1 分段采集：代次登记、半行补齐、换代并发、恢复续传与幂等重试。"""
import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app.modules.m1_logs.core import (  # noqa: E402
    COMPLETE, PENDING_TAIL, RECEIVING, IngestService, hour_bucket, split_segment,
)
from app.modules.m1_logs.repository import (  # noqa: E402
    MemoryIngestRepository, SqliteIngestRepository,
)


@pytest.fixture(params=["memory", "sqlite"])
def service(request, tmp_path):
    if request.param == "memory":
        return IngestService(MemoryIngestRepository())
    import sqlite3
    db_path = tmp_path / "intake.db"
    repo = SqliteIngestRepository(lambda: sqlite3.connect(db_path))
    return IngestService(repo)


def open_and_seed(service):
    service.open_source("pay/app.log")
    return service.upload("pay/app.log", 1, 0, '{"request_id":"a"}\n{"reque')


# --- 陷阱 1：轮转时半行不得当坏数据丢掉 ------------------------------------
def test_half_line_is_retained_not_quarantined(service):
    out = open_and_seed(service)
    g = out["generation_view"]
    assert g["state"] == RECEIVING
    assert g["pending_tail"] == '{"reque'
    assert g["valid_count"] == 1
    assert g["quarantine_count"] == 0
    assert g["received_offset"] == len(('{"request_id":"a"}\n{"reque').encode())


def test_split_segment_carries_tail():
    lines, carry = split_segment('{"reque', "")
    assert lines == [] and carry == '{"reque'
    lines, carry = split_segment('st_id":"b"}\n', '{"reque')
    assert carry == ""
    assert lines == [(1, '{"request_id":"b"}', False)]


# --- 陷阱 2：续沿用旧位置漏读 —— 新代次立即接收，旧代次待补齐 ---------------
def test_rotation_new_generation_receives_immediately(service):
    open_and_seed(service)
    rot = service.rotate("pay/app.log", 1)
    assert rot["previous_generation"] == 1
    snap = rot["snapshot"]["sources"][0]
    old, new = snap["generations"]
    assert old["state"] == PENDING_TAIL and old["pending_tail"] == '{"reque'
    assert new["state"] == RECEIVING and new["generation"] == 2
    assert new["batch_id"] == "pay/app.log#g2"  # 各代独立批次
    # 新文件立即写入新代次，不被旧代尾行阻塞
    up = service.upload("pay/app.log", 2, 0, '{"request_id":"n1"}\n', 0)
    assert up["generation_view"]["valid_count"] == 1


# --- 陷阱 3：两代拼接会伪造事件 —— 尾行补齐后归入原批次 ---------------------
def test_old_generation_tail_completes_into_original_batch(service):
    open_and_seed(service)
    service.rotate("pay/app.log", 1)
    service.upload("pay/app.log", 2, 0, '{"request_id":"n1"}\n', 0)
    # 旧代补写：半行与续片拼接，物理行号在旧代内连续
    done = service.upload("pay/app.log", 1, 1, 'st_id":"b"}\n', None)
    assert done["generation_view"]["pending_tail"] == ""
    view = done["snapshot"]["sources"][0]["generations"][0]
    assert view["valid_count"] == 2 and view["line_count"] == 2
    end = service.end_generation("pay/app.log", 1)
    assert end["generation_view"]["state"] == COMPLETE
    evidence = end["snapshot"]["sources"][0]["evidence"]
    old_rows = [e for e in evidence if e["generation"] == 1 and e["kind"] == "VALID"]
    assert [r["request_id"] for r in old_rows] == ["a", "b"]
    assert all(r["batch_id"] == "pay/app.log#g1" for r in old_rows)
    # 两代证据各自独立，不存在跨代拼接出的伪造事件
    assert {r["request_id"] for r in evidence} == {"a", "b", "n1"}


def test_explicit_end_flushes_last_half_line(service):
    service.open_source("s")
    # 半行本身在收到结尾时已构成合法 JSON（无尾换行不等于坏数据）
    service.upload("s", 1, 0, '{"request_id":"z"}')
    ended = service.end_generation("s", 1)
    g = ended["generation_view"]
    assert g["state"] == COMPLETE and g["pending_tail"] == ""
    assert g["valid_count"] == 1 and g["line_count"] == 1


def test_end_quarantines_broken_final_line(service):
    service.open_source("s")
    service.upload("s", 1, 0, '{"request_id":')
    ended = service.end_generation("s", 1)
    assert ended["generation_view"]["quarantine_count"] == 1
    q = ended["snapshot"]["sources"][0]["evidence"][0]
    assert q["kind"] == "QUARANTINE" and q["reason"] == "TRUNCATED"


# --- 陷阱 4：换代与上传并发 —— 拒绝过期推进 --------------------------------
def test_stale_rotation_rejected(service):
    service.open_source("s")
    service.rotate("s", 1)  # 当前已到 g2
    with pytest.raises(ValueError, match="STALE_VERSION"):
        service.rotate("s", 1)  # 过期代次再次换代
    snap = service.snapshot("s")
    assert snap["sources"][0]["current_generation"] == 2


def test_upload_after_close_rejected(service):
    service.open_source("s")
    service.end_generation("s", 1)
    with pytest.raises(ValueError, match="GENERATION_CLOSED"):
        service.upload("s", 1, 0, "x")


def test_retry_after_close_replays_but_new_segment_rejected(service):
    service.open_source("s")
    service.upload("s", 1, 0, '{"request_id":"r"}\n', 0)
    service.end_generation("s", 1)
    # 已收过的分片：结尾后重试仍幂等回放，不重复、不误拒
    replay = service.upload("s", 1, 0, '{"request_id":"r"}\n', 0)
    assert replay["replayed"] is True
    assert replay["generation_view"]["valid_count"] == 1
    # 未收过的新分片：拒绝写入已完成代次
    with pytest.raises(ValueError, match="GENERATION_CLOSED"):
        service.upload("s", 1, 1, "new\n")


def test_position_conflict_rejected(service):
    service.open_source("s")
    service.upload("s", 1, 0, "abc\n")
    with pytest.raises(ValueError, match="POSITION_CONFLICT"):
        service.upload("s", 1, 1, "d\n", expected_offset=0)  # 偏移落后于已接收位置


def test_unknown_source_and_generation(service):
    with pytest.raises(ValueError, match="SOURCE_NOT_FOUND"):
        service.upload("nope", 1, 0, "x")
    service.open_source("s")
    with pytest.raises(ValueError, match="GENERATION_NOT_FOUND"):
        service.upload("s", 9, 0, "x")


# --- 陷阱 5：重试不重复入桶 ------------------------------------------------
def test_retry_same_segment_is_idempotent(service):
    service.open_source("s")
    first = service.upload("s", 1, 0, '{"request_id":"r"}\n', 0)
    again = service.upload("s", 1, 0, '{"request_id":"r"}\n', 0)
    assert again["replayed"] is True
    g = again["generation_view"]
    assert g["valid_count"] == 1 and g["segment_count"] == 1
    assert g["received_offset"] == first["generation_view"]["received_offset"]


def test_same_seq_different_checksum_conflicts(service):
    service.open_source("s")
    service.upload("s", 1, 0, "a\n")
    with pytest.raises(ValueError, match="SEGMENT_CONFLICT"):
        service.upload("s", 1, 0, "b\n")


# --- 陷阱 6：中断后恢复续传不漏行 ------------------------------------------
def test_recovery_resumes_without_gap_or_duplicate(tmp_path):
    import sqlite3
    db_path = tmp_path / "r.db"
    svc1 = IngestService(SqliteIngestRepository(lambda: sqlite3.connect(db_path)))
    svc1.open_source("s")
    svc1.upload("s", 1, 0, '{"request_id":"r1"}\n{"reque', 0)
    offset = svc1.snapshot("s")["sources"][0]["generations"][0]["received_offset"]

    # 模拟进程中断：用新连接重建服务，已接收位置与待补尾行从 SQLite 恢复
    svc2 = IngestService(SqliteIngestRepository(lambda: sqlite3.connect(db_path)))
    resumed = svc2.upload("s", 1, 1, 'st_id":"r2"}\n', offset)
    g = resumed["generation_view"]
    assert g["pending_tail"] == "" and g["valid_count"] == 2
    assert g["received_offset"] == offset + len('st_id":"r2"}\n'.encode())
    # 再重试恢复前的最后一片，不产生重复
    replay = svc2.upload("s", 1, 1, 'st_id":"r2"}\n', offset)
    assert replay["replayed"] is True
    assert replay["generation_view"]["valid_count"] == 2


# --- 守恒与展示 ------------------------------------------------------------
def test_accounting_conservation(service):
    service.open_source("s")
    service.upload("s", 1, 0, '{"request_id":"a"}\n\n{"bad\n', 0)
    service.rotate("s", 1)
    service.end_generation("s", 1)
    snap = service.snapshot()
    t = snap["totals"]
    src = snap["sources"][0]
    assert t["row_count"] == src["valid_count"]
    assert t["quarantine_count"] == src["quarantine_count"]
    g1 = src["generations"][0]
    # 物理行含空行：有效1 + 隔离1 + 空行1 = 3
    assert g1["line_count"] == 3
    assert g1["valid_count"] + g1["quarantine_count"] == 2


def test_hour_buckets_follow_m4_convention_without_new_tz_rules():
    assert hour_bucket("2026-11-15T01:05:00Z") == "2026-11-15T01:00:00Z"
    # 与 M4 一致：带偏移的时间戳按其挂钟小时取桶，不新增时区修正
    assert hour_bucket("2026-11-15T09:10:00+08:00") == "2026-11-15T09:00:00Z"
    assert hour_bucket(None) == "UNBUCKETED"
    service = IngestService(MemoryIngestRepository())
    service.open_source("s")
    service.upload("s", 1, 0,
                   '{"request_id":"a","timestamp":"2026-11-15T01:05:00Z"}\n'
                   '{"request_id":"b","timestamp":"2026-11-15T01:40:00Z"}\n', 0)
    g = service.snapshot("s")["sources"][0]["generations"][0]
    assert g["hour_buckets"] == ["2026-11-15T01:00:00Z"]


def test_open_is_idempotent(service):
    first = service.open_source("s")
    again = service.open_source("s")
    assert again["reopened"] is True
    assert service.snapshot()["totals"]["generation_count"] == 1


def test_blank_lines_keep_physical_line_numbers(service):
    service.open_source("s")
    out = service.upload("s", 1, 0, '\n{"request_id":"a"}\n', 0)
    ev = out["snapshot"]["sources"][0]["evidence"][0]
    assert ev["source_line"] == 2


def test_concurrent_rotation_single_winner_no_duplicate_generation(service):
    import threading
    service.open_source("s")
    service.upload("s", 1, 0, '{"request_id":"a"}\n', 0)
    barrier = threading.Barrier(8)
    errors, ok = [], []

    def worker():
        barrier.wait()
        try:
            service.rotate("s", 1)
            ok.append(1)
        except ValueError as exc:
            errors.append(str(exc))

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(ok) == 1
    assert errors.count("STALE_VERSION") == 7
    snap = service.snapshot("s")["sources"][0]
    assert snap["current_generation"] == 2
    assert [g["generation"] for g in snap["generations"]] == [1, 2]
    # 旧代证据未被并发重复写入
    assert snap["valid_count"] == 1


def test_concurrent_upload_same_seq_single_commit(service):
    import threading
    service.open_source("s")
    barrier = threading.Barrier(6)
    outcomes = []

    def worker():
        barrier.wait()
        try:
            r = service.upload("s", 1, 0, '{"request_id":"r"}\n', 0)
            outcomes.append(("ok", r.get("replayed", False)))
        except ValueError as exc:
            outcomes.append(("err", str(exc)))

    threads = [threading.Thread(target=worker) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    g = service.snapshot("s")["sources"][0]["generations"][0]
    assert g["valid_count"] == 1 and g["segment_count"] == 1
    # 首个提交落盘，其余全部幂等回放，无任何冲突或重复入桶
    assert {kind for kind, _ in outcomes} == {"ok"}
    assert sum(1 for kind, replayed in outcomes if kind == "ok" and not replayed) == 1
    assert sum(1 for kind, replayed in outcomes if replayed) == 5
