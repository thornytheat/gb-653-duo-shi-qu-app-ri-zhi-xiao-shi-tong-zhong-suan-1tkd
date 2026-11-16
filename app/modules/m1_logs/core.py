"""M1 日志采信。

在原始整段解析之外，提供按来源登记文件代次、分段上传的有状态采集：

- 轮转产生新代次，新文件立即以 RECEIVING 接收，旧代次进入 PENDING_TAIL；
- 半行保留为待补尾行（pending_tail），不立即判坏数据，续传拼接补齐；
- 旧代次补写完成后显式结尾（COMPLETE），尾行按物理行号归入原批次；
- 已接收位置按字节偏移登记，重试以 (代次, 分片序号, 校验值) 幂等，不入桶两次；
- 换代与上传并发时，过期代次/位置一律拒绝。
"""
from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from hashlib import sha256

# ---------------------------------------------------------------------------
# 向后兼容的整段解析（旧入口 /api/logs/import 与原测试依赖，行为保持不变）
# ---------------------------------------------------------------------------
SAMPLE = {"text": '{"request_id":"9007199254740993","timestamp":"2026-11-15T01:05:00Z","service":"pay","status":200}\n{"request_id":"r2","timestamp":"2026-11-15T09:10:00+08:00","service":"pay","status":500}\n{"request_id":"r3"'}


def parse(text):
    rows = []
    quarantine = []
    for line_no, line in enumerate(text.splitlines(), 1):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
            row["request_id"] = str(row.get("request_id", ""))
            row["source_line"] = line_no
            rows.append(row)
        except json.JSONDecodeError:
            quarantine.append({"source_line": line_no, "reason": "TRUNCATED", "raw": line})
    return {"rows": rows, "quarantine": quarantine}


def run(payload):
    out = parse(payload.get("text", SAMPLE["text"]))
    return {
        "status": "READY",
        "input_count": len(out["rows"]) + len(out["quarantine"]),
        "valid_count": len(out["rows"]),
        "quarantine_count": len(out["quarantine"]),
        "first_id": out["rows"][0]["request_id"],
        "reason": out["quarantine"][0]["reason"] if out["quarantine"] else "OK",
    }

# ---------------------------------------------------------------------------
# 代次状态机
# ---------------------------------------------------------------------------
RECEIVING = "RECEIVING"          # 接收中：当前代次，持续接收分片
PENDING_TAIL = "PENDING_TAIL"    # 待补齐：已换代，旧代次仍有尾行待补写
COMPLETE = "COMPLETE"            # 完成：已明确结尾，拒绝再写入

GENERATION_STATES = (RECEIVING, PENDING_TAIL, COMPLETE)
STATE_LABELS = {
    RECEIVING: "接收中",
    PENDING_TAIL: "待补齐",
    COMPLETE: "完成",
}


def split_segment(text: str, carry: str):
    """把本次分片文本与上轮待补尾行拼接后按物理行切分。

    返回 ``(lines, new_carry)``：``lines`` 为 ``(line_no, raw, is_blank)``，
    行号在本代次内连续（含空行）；末尾不含换行的半截行成为新的待补尾行。
    """
    joined = carry + text
    parts = joined.split("\n")
    new_carry = parts.pop()  # 末尾片段：无尾换行时是半行，有尾换行时是空串
    base_line = 0
    lines = []
    for raw in parts:
        base_line += 1
        lines.append((base_line, raw, not raw.strip()))
    return lines, new_carry


def parse_line(raw: str, line_no: int):
    """解析一条完整物理行，返回 (row|None, bad|None)；空行返回 (None, None)。"""
    if not raw.strip():
        return None, None
    try:
        row = json.loads(raw)
    except json.JSONDecodeError:
        return None, {"source_line": line_no, "reason": "TRUNCATED", "raw": raw}
    row["request_id"] = str(row.get("request_id", ""))
    row["source_line"] = line_no
    return row, None


def hour_bucket(timestamp):
    """对应小时桶（UTC 键，与 M4 小时分桶同一约定；不做额外时区修正）。

    无法解析或缺少时间戳的记录归 UNBUCKETED，仅用于页面展示。
    """
    if not timestamp:
        return "UNBUCKETED"
    try:
        dt = datetime.fromisoformat(str(timestamp).replace("Z", "+00:00"))
    except ValueError:
        return "UNBUCKETED"
    return dt.strftime("%Y-%m-%dT%H:00:00Z")


def checksum(text: str) -> str:
    return sha256(text.encode("utf-8")).hexdigest()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _view(gen: dict, rows: list[dict], quarantine: list[dict]) -> dict:
    """组装单个代次的对外视图，附本代次涉及的小时桶。"""
    buckets = sorted({
        row["hour_bucket"]
        for row in rows
        if row.get("generation") == gen["generation"]
    })
    gen_rows = sum(1 for r in rows if r["generation"] == gen["generation"])
    gen_bad = sum(1 for r in quarantine if r["generation"] == gen["generation"])
    return {
        "source": gen["source"],
        "generation": gen["generation"],
        "batch_id": gen["batch_id"],
        "state": gen["state"],
        "state_label": STATE_LABELS[gen["state"]],
        "received_offset": gen["received_offset"],
        "pending_tail": gen["pending_tail"],
        "line_count": gen["line_count"],
        "valid_count": gen_rows,
        "quarantine_count": gen_bad,
        "segment_count": gen["segment_count"],
        "hour_buckets": buckets,
        "opened_at": gen["opened_at"],
        "updated_at": gen["updated_at"],
        "ended_at": gen.get("ended_at"),
    }


def _evidence(rows: list[dict], quarantine: list[dict]) -> list[dict]:
    """有效行与隔离行合并为统一证据清单，按代次、物理行号排序。"""
    items = [{
        "kind": "VALID", "source": r["source"], "generation": r["generation"],
        "batch_id": r["batch_id"], "source_line": r["source_line"],
        "request_id": r["request_id"], "hour_bucket": r["hour_bucket"],
        "reason": "", "raw": r["payload"],
    } for r in rows]
    items += [{
        "kind": "QUARANTINE", "source": q["source"], "generation": q["generation"],
        "batch_id": q["batch_id"], "source_line": q["source_line"],
        "request_id": "", "hour_bucket": "UNBUCKETED",
        "reason": q["reason"], "raw": q["raw"],
    } for q in quarantine]
    return sorted(items, key=lambda x: (x["generation"], x["source_line"], x["kind"]))


class IngestService:
    """按来源/代次管理分段上传的采集服务。

    仓库只需提供内存实现（测试）或 SQLite 实现（应用）的同名方法；
    服务层以一把进程锁把"检查—裁决—写入"串行化，拒绝过期推进。
    """

    def __init__(self, repo):
        self.repo = repo
        self._lock = threading.RLock()

    # -- 只读 ----------------------------------------------------------------
    def snapshot(self, source: str | None = None) -> dict:
        with self._lock:
            sources = self.repo.list_sources()
            out = {"sources": []}
            for src in sources:
                if source and src != source:
                    continue
                gens = self.repo.list_generations(src)
                rows = self.repo.list_rows(src)
                quarantine = self.repo.list_quarantine(src)
                out["sources"].append({
                    "source": src,
                    "current_generation": gens[-1]["generation"] if gens else 0,
                    "generation_count": len(gens),
                    "valid_count": len(rows),
                    "quarantine_count": len(quarantine),
                    "generations": [_view(g, rows, quarantine) for g in gens],
                    "evidence": _evidence(rows, quarantine),
                })
            if source:
                out["source"] = source
                out["generation"] = (
                    out["sources"][0]["current_generation"] if out["sources"] else 0
                )
            totals = self.repo.totals()
            out["totals"] = totals
            return out

    # -- 写动作 --------------------------------------------------------------
    def open_source(self, source: str, batch_id: str | None = None) -> dict:
        if not source or not str(source).strip():
            raise ValueError("INVALID_ARGUMENT")
        with self._lock:
            existing = self.repo.get_source(source)
            if existing is None:
                self.repo.upsert_source(source, current_generation=1)
                self._create_generation(source, 1, batch_id)
                return self._result(source, 1, action="open", reopened=False)
            # 幂等重开：来源与当前代次已登记，直接回视图，不重复建代
            generation = existing["current_generation"]
            self._require_generation(source, generation)
            return self._result(source, generation, action="open", reopened=True)

    def upload(self, source: str, generation: int, seq: int, text: str,
               expected_offset: int | None = None) -> dict:
        if not source or not isinstance(generation, int) or generation < 1:
            raise ValueError("INVALID_ARGUMENT")
        if not isinstance(seq, int) or seq < 0:
            raise ValueError("INVALID_ARGUMENT")
        text = text or ""
        digest = checksum(text)
        with self._lock:
            gen = self._require_generation(source, generation)
            # 1) 幂等重试优先：同代次同分片序号且校验值一致，原样回放，
            #    即使代次已结尾也回放成功，保证重试不重复入桶也不被误拒
            existing = self.repo.get_segment(source, generation, seq)
            if existing is not None:
                if existing["checksum"] != digest:
                    raise ValueError("SEGMENT_CONFLICT")
                return self._result(source, generation, action="upload",
                                    replayed=True, seq=seq)
            if gen["state"] == COMPLETE:
                raise ValueError("GENERATION_CLOSED")
            # 2) 位置校验：分片必须紧接已接收位置，否则拒绝过期/错位推进
            if expected_offset is not None and expected_offset != gen["received_offset"]:
                raise ValueError("POSITION_CONFLICT")
            data = text.encode("utf-8")
            start_offset = gen["received_offset"]
            end_offset = start_offset + len(data)

            lines, new_carry = split_segment(text, gen["pending_tail"])
            rows, quarantine, line_count = [], [], 0
            for line_no, raw, is_blank in lines:
                line_count += 1
                if is_blank:
                    continue
                row, bad = parse_line(raw, line_no)
                if row is not None:
                    rows.append(row)
                elif bad is not None:
                    quarantine.append(bad)

            self.repo.commit_segment(
                source=source, generation=generation, seq=seq, checksum=digest,
                start_offset=start_offset, end_offset=end_offset, byte_len=len(data),
                pending_tail=new_carry, line_count=line_count,
                rows=rows, quarantine=quarantine, updated_at=_now(),
            )
            return self._result(source, generation, action="upload",
                                replayed=False, seq=seq,
                                accepted_lines=len(rows) + len(quarantine),
                                pending_tail=new_carry)

    def rotate(self, source: str, expected_generation: int,
               batch_id: str | None = None) -> dict:
        """换代：旧代次转待补齐，新代次立即进入接收中。"""
        with self._lock:
            old = self._require_generation(source, expected_generation)
            current = self.repo.get_source(source)["current_generation"]
            if current != expected_generation:
                raise ValueError("STALE_VERSION")  # 并发换代：提交版本已过期
            if old["state"] == COMPLETE:
                raise ValueError("GENERATION_CLOSED")
            new_generation = expected_generation + 1
            self.repo.apply_rotation(
                source=source, old_generation=expected_generation,
                new_generation=new_generation, updated_at=_now(),
            )
            self._create_generation(source, new_generation, batch_id)
            return self._result(source, new_generation, action="rotate",
                                previous_generation=expected_generation)

    def end_generation(self, source: str, generation: int) -> dict:
        """明确结尾：冲刷待补尾行（按原物理行号归入本代次/原批次），代次置完成。"""
        with self._lock:
            gen = self._require_generation(source, generation)
            if gen["state"] == COMPLETE:
                # 结尾本身幂等：崩溃恢复后重复结尾直接回放
                return self._result(source, generation, action="end", replayed=True)
            rows, quarantine = [], []
            tail = gen["pending_tail"]
            if tail.strip():
                line_no = gen["line_count"] + 1
                row, bad = parse_line(tail, line_no)
                if row is not None:
                    rows.append(row)
                elif bad is not None:
                    quarantine.append(bad)
            self.repo.commit_end(
                source=source, generation=generation, rows=rows,
                quarantine=quarantine, updated_at=_now(), ended_at=_now(),
            )
            return self._result(source, generation, action="end",
                                flushed_tail=bool(tail.strip()))

    # -- 内部辅助 ------------------------------------------------------------
    def _create_generation(self, source: str, generation: int,
                           batch_id: str | None) -> None:
        bid = batch_id or f"{source}#g{generation}"
        self.repo.create_generation({
            "source": source,
            "generation": generation,
            "batch_id": bid,
            "state": RECEIVING,
            "received_offset": 0,
            "pending_tail": "",
            "line_count": 0,
            "segment_count": 0,
            "opened_at": _now(),
            "updated_at": _now(),
            "ended_at": None,
        })

    def _require_generation(self, source: str, generation: int) -> dict:
        if self.repo.get_source(source) is None:
            raise ValueError("SOURCE_NOT_FOUND")
        gen = self.repo.get_generation(source, generation)
        if gen is None:
            raise ValueError("GENERATION_NOT_FOUND")
        return gen

    def _result(self, source: str, generation: int, **extra) -> dict:
        gen = self.repo.get_generation(source, generation)
        view = _view(gen, self.repo.list_rows(source), self.repo.list_quarantine(source))
        return {"action": extra.pop("action"), **extra, "generation_view": view,
                "snapshot": self.snapshot(source)}
