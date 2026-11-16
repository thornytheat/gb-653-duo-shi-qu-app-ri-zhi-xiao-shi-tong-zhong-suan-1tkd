"""M1 采集状态仓库：内存实现（单元测试）与 SQLite 实现（应用持久化）。

所有写方法在 SQLite 上以单事务提交；进程中断只会落在事务边界，
恢复后续传依靠已接收偏移与分片唯一约束做到不漏行、不重复入桶。
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone

from .core import COMPLETE, PENDING_TAIL, RECEIVING, hour_bucket


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class MemoryIngestRepository:
    def __init__(self):
        self.sources: dict[str, dict] = {}
        self.generations: dict[tuple, dict] = {}
        self.segments: dict[tuple, dict] = {}
        self.rows: list[dict] = []
        self.quarantine: list[dict] = []

    def get_source(self, source):
        return self.sources.get(source)

    def upsert_source(self, source, current_generation):
        now = _now()
        self.sources.setdefault(source, {
            "source": source, "current_generation": current_generation,
            "created_at": now,
        })
        self.sources[source]["current_generation"] = current_generation
        self.sources[source]["updated_at"] = now

    def list_sources(self):
        return sorted(self.sources)

    def create_generation(self, gen):
        self.generations[(gen["source"], gen["generation"])] = dict(gen)

    def get_generation(self, source, generation):
        gen = self.generations.get((source, generation))
        return dict(gen) if gen else None

    def list_generations(self, source):
        return [dict(g) for key, g in sorted(self.generations.items()) if key[0] == source]

    def get_segment(self, source, generation, seq):
        seg = self.segments.get((source, generation, seq))
        return dict(seg) if seg else None

    def commit_segment(self, *, source, generation, seq, checksum, start_offset,
                       end_offset, byte_len, pending_tail, line_count, rows,
                       quarantine, updated_at):
        key = (source, generation, seq)
        if key in self.segments:
            raise ValueError("SEGMENT_CONFLICT")
        self.segments[key] = {
            "source": source, "generation": generation, "seq": seq,
            "checksum": checksum, "start_offset": start_offset,
            "end_offset": end_offset, "byte_len": byte_len, "created_at": _now(),
        }
        gen = self.generations[(source, generation)]
        gen["received_offset"] = end_offset
        gen["pending_tail"] = pending_tail
        gen["line_count"] += line_count
        gen["segment_count"] += 1
        gen["updated_at"] = updated_at
        self._append_rows(source, gen, rows, quarantine)

    def apply_rotation(self, *, source, old_generation, new_generation, updated_at):
        old = self.generations[(source, old_generation)]
        old["state"] = PENDING_TAIL
        old["updated_at"] = updated_at
        self.sources[source]["current_generation"] = new_generation
        self.sources[source]["updated_at"] = updated_at

    def commit_end(self, *, source, generation, rows, quarantine, updated_at, ended_at):
        gen = self.generations[(source, generation)]
        gen["state"] = COMPLETE
        gen["pending_tail"] = ""
        gen["line_count"] += len(rows) + len(quarantine)
        gen["updated_at"] = updated_at
        gen["ended_at"] = ended_at
        self._append_rows(source, gen, rows, quarantine)

    def _append_rows(self, source, gen, rows, quarantine):
        for row in rows:
            self.rows.append({
                "source": source,
                "generation": gen["generation"],
                "batch_id": gen["batch_id"],
                "source_line": row["source_line"],
                "request_id": row.get("request_id", ""),
                "hour_bucket": hour_bucket(row.get("timestamp")),
                "payload": json.dumps(row, ensure_ascii=False, sort_keys=True),
                "created_at": _now(),
            })
        for bad in quarantine:
            self.quarantine.append({
                "source": source,
                "generation": gen["generation"],
                "batch_id": gen["batch_id"],
                "source_line": bad["source_line"],
                "reason": bad["reason"],
                "raw": bad["raw"],
                "created_at": _now(),
            })

    def list_rows(self, source):
        return [dict(r) for r in self.rows if r["source"] == source]

    def list_quarantine(self, source):
        return [dict(q) for q in self.quarantine if q["source"] == source]

    def totals(self):
        gens = list(self.generations.values())
        return {
            "source_count": len(self.sources),
            "generation_count": len(gens),
            "receiving": sum(1 for g in gens if g["state"] == RECEIVING),
            "pending_tail": sum(1 for g in gens if g["state"] == PENDING_TAIL),
            "complete": sum(1 for g in gens if g["state"] == COMPLETE),
            "row_count": len(self.rows),
            "quarantine_count": len(self.quarantine),
        }


class SqliteIngestRepository:
    def __init__(self, connect):
        self.connect = connect
        with self.connect() as db:
            db.executescript(
                """
                CREATE TABLE IF NOT EXISTS ingest_sources (
                    source TEXT PRIMARY KEY,
                    current_generation INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS ingest_generations (
                    source TEXT NOT NULL,
                    generation INTEGER NOT NULL,
                    batch_id TEXT NOT NULL,
                    state TEXT NOT NULL,
                    received_offset INTEGER NOT NULL,
                    pending_tail TEXT NOT NULL,
                    line_count INTEGER NOT NULL,
                    segment_count INTEGER NOT NULL,
                    opened_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    ended_at TEXT,
                    PRIMARY KEY (source, generation)
                );
                CREATE TABLE IF NOT EXISTS ingest_segments (
                    source TEXT NOT NULL,
                    generation INTEGER NOT NULL,
                    seq INTEGER NOT NULL,
                    checksum TEXT NOT NULL,
                    start_offset INTEGER NOT NULL,
                    end_offset INTEGER NOT NULL,
                    byte_len INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (source, generation, seq)
                );
                CREATE TABLE IF NOT EXISTS ingest_rows (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source TEXT NOT NULL,
                    generation INTEGER NOT NULL,
                    batch_id TEXT NOT NULL,
                    source_line INTEGER NOT NULL,
                    request_id TEXT NOT NULL,
                    hour_bucket TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS ingest_quarantine (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source TEXT NOT NULL,
                    generation INTEGER NOT NULL,
                    batch_id TEXT NOT NULL,
                    source_line INTEGER NOT NULL,
                    reason TEXT NOT NULL,
                    raw TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                """
            )

    # -- 读 ------------------------------------------------------------------
    def get_source(self, source):
        with self.connect() as db:
            row = db.execute(
                "SELECT source, current_generation, created_at, updated_at"
                " FROM ingest_sources WHERE source=?", (source,)).fetchone()
        return dict(zip(["source", "current_generation", "created_at", "updated_at"], row)) if row else None

    def list_sources(self):
        with self.connect() as db:
            return [r[0] for r in db.execute(
                "SELECT source FROM ingest_sources ORDER BY source").fetchall()]

    def create_generation(self, gen):
        with self.connect() as db:
            db.execute(
                "INSERT INTO ingest_generations (source, generation, batch_id, state,"
                " received_offset, pending_tail, line_count, segment_count, opened_at,"
                " updated_at, ended_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (gen["source"], gen["generation"], gen["batch_id"], gen["state"],
                 gen["received_offset"], gen["pending_tail"], gen["line_count"],
                 gen["segment_count"], gen["opened_at"], gen["updated_at"],
                 gen["ended_at"]))
            db.execute(
                "INSERT OR IGNORE INTO ingest_sources (source, current_generation,"
                " created_at, updated_at) VALUES (?,?,?,?)",
                (gen["source"], gen["generation"], gen["opened_at"], gen["updated_at"]))

    def get_generation(self, source, generation):
        cols = ["source", "generation", "batch_id", "state", "received_offset",
                "pending_tail", "line_count", "segment_count", "opened_at",
                "updated_at", "ended_at"]
        with self.connect() as db:
            row = db.execute(
                f"SELECT {', '.join(cols)} FROM ingest_generations"
                " WHERE source=? AND generation=?", (source, generation)).fetchone()
        return dict(zip(cols, row)) if row else None

    def list_generations(self, source):
        cols = ["source", "generation", "batch_id", "state", "received_offset",
                "pending_tail", "line_count", "segment_count", "opened_at",
                "updated_at", "ended_at"]
        with self.connect() as db:
            rows = db.execute(
                f"SELECT {', '.join(cols)} FROM ingest_generations"
                " WHERE source=? ORDER BY generation", (source,)).fetchall()
        return [dict(zip(cols, r)) for r in rows]

    def get_segment(self, source, generation, seq):
        cols = ["source", "generation", "seq", "checksum", "start_offset",
                "end_offset", "byte_len", "created_at"]
        with self.connect() as db:
            row = db.execute(
                f"SELECT {', '.join(cols)} FROM ingest_segments"
                " WHERE source=? AND generation=? AND seq=?",
                (source, generation, seq)).fetchone()
        return dict(zip(cols, row)) if row else None

    def list_rows(self, source):
        cols = ["source", "generation", "batch_id", "source_line", "request_id",
                "hour_bucket", "payload", "created_at"]
        with self.connect() as db:
            rows = db.execute(
                f"SELECT {', '.join(cols)} FROM ingest_rows WHERE source=?"
                " ORDER BY generation, source_line, id", (source,)).fetchall()
        return [dict(zip(cols, r)) for r in rows]

    def list_quarantine(self, source):
        cols = ["source", "generation", "batch_id", "source_line", "reason",
                "raw", "created_at"]
        with self.connect() as db:
            rows = db.execute(
                f"SELECT {', '.join(cols)} FROM ingest_quarantine WHERE source=?"
                " ORDER BY generation, source_line, id", (source,)).fetchall()
        return [dict(zip(cols, r)) for r in rows]

    def totals(self):
        with self.connect() as db:
            src = db.execute("SELECT COUNT(*) FROM ingest_sources").fetchone()[0]
            state_rows = db.execute(
                "SELECT state, COUNT(*) FROM ingest_generations GROUP BY state").fetchall()
            states = dict(state_rows)
            rows = db.execute("SELECT COUNT(*) FROM ingest_rows").fetchone()[0]
            bad = db.execute("SELECT COUNT(*) FROM ingest_quarantine").fetchone()[0]
        return {
            "source_count": src,
            "generation_count": sum(states.values()),
            "receiving": states.get(RECEIVING, 0),
            "pending_tail": states.get(PENDING_TAIL, 0),
            "complete": states.get(COMPLETE, 0),
            "row_count": rows,
            "quarantine_count": bad,
        }

    # -- 写（单事务） --------------------------------------------------------
    def upsert_source(self, source, current_generation):
        now = _now()
        with self.connect() as db:
            db.execute(
                "INSERT INTO ingest_sources (source, current_generation, created_at,"
                " updated_at) VALUES (?,?,?,?) ON CONFLICT(source) DO UPDATE SET"
                " current_generation=excluded.current_generation, updated_at=excluded.updated_at",
                (source, current_generation, now, now))

    def commit_segment(self, *, source, generation, seq, checksum, start_offset,
                       end_offset, byte_len, pending_tail, line_count, rows,
                       quarantine, updated_at):
        with self.connect() as db:
            gen = db.execute(
                "SELECT batch_id FROM ingest_generations WHERE source=? AND generation=?",
                (source, generation)).fetchone()
            if gen is None:
                raise ValueError("GENERATION_NOT_FOUND")
            batch_id = gen[0]
            db.execute(
                "INSERT INTO ingest_segments (source, generation, seq, checksum,"
                " start_offset, end_offset, byte_len, created_at)"
                " VALUES (?,?,?,?,?,?,?,?)",
                (source, generation, seq, checksum, start_offset, end_offset,
                 byte_len, _now()))
            db.execute(
                "UPDATE ingest_generations SET received_offset=?, pending_tail=?,"
                " line_count=line_count+?, segment_count=segment_count+1, updated_at=?"
                " WHERE source=? AND generation=?",
                (end_offset, pending_tail, line_count, updated_at, source, generation))
            self._insert_evidence(db, source, generation, batch_id, rows, quarantine)

    def apply_rotation(self, *, source, old_generation, new_generation, updated_at):
        with self.connect() as db:
            db.execute(
                "UPDATE ingest_generations SET state=?, updated_at=?"
                " WHERE source=? AND generation=?",
                (PENDING_TAIL, updated_at, source, old_generation))
            db.execute(
                "UPDATE ingest_sources SET current_generation=?, updated_at=?"
                " WHERE source=?", (new_generation, updated_at, source))

    def commit_end(self, *, source, generation, rows, quarantine, updated_at, ended_at):
        with self.connect() as db:
            gen = db.execute(
                "SELECT batch_id FROM ingest_generations WHERE source=? AND generation=?",
                (source, generation)).fetchone()
            if gen is None:
                raise ValueError("GENERATION_NOT_FOUND")
            batch_id = gen[0]
            db.execute(
                "UPDATE ingest_generations SET state=?, pending_tail='',"
                " line_count=line_count+?, updated_at=?, ended_at=?"
                " WHERE source=? AND generation=?",
                (COMPLETE, len(rows) + len(quarantine), updated_at, ended_at,
                 source, generation))
            self._insert_evidence(db, source, generation, batch_id, rows, quarantine)

    def _insert_evidence(self, db, source, generation, batch_id, rows, quarantine):
        now = _now()
        for row in rows:
            db.execute(
                "INSERT INTO ingest_rows (source, generation, batch_id, source_line,"
                " request_id, hour_bucket, payload, created_at) VALUES (?,?,?,?,?,?,?,?)",
                (source, generation, batch_id, row["source_line"],
                 row.get("request_id", ""), hour_bucket(row.get("timestamp")),
                 json.dumps(row, ensure_ascii=False, sort_keys=True), now))
        for bad in quarantine:
            db.execute(
                "INSERT INTO ingest_quarantine (source, generation, batch_id,"
                " source_line, reason, raw, created_at) VALUES (?,?,?,?,?,?,?)",
                (source, generation, batch_id, bad["source_line"], bad["reason"],
                 bad["raw"], now))
