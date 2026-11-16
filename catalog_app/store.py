"""On-disk version store with publish fencing and resumable task records.

Layout (all writes are temp-file + os.replace, so a crash never leaves a
half-written JSON document)::

    data/
      tasks/<task_id>.json        # one collection task, updated on every event
      versions/v000001.json       # immutable closed snapshot
      versions/latest.json        # pointer; only ever moves forward
      index.json                  # task sequence + epoch fence

Concurrency rules:

* Every task receives a monotonic ``seq`` at creation time.
* Publishing requires ``task.seq == epoch + 1``.  Two tasks that discovered the
  same shared dependency therefore cannot both publish: the earlier-seq task
  commits first and bumps the epoch, while the late task's publish is rejected
  — an old task can never roll back a newer published version.
* A failed task keeps its partial pins on disk and can be resumed; a request
  failure never deletes an already-pinned reference (the collector only adds
  or replaces its own pins, never removes).
"""

from __future__ import annotations

import json
import os
import threading
import time
import uuid
from pathlib import Path

from .collector import Collector
from .client import LocalClient
from .source import SourceState


def _atomic_write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


def _read_json(path: Path) -> dict | None:
    if not path.exists():
        return None
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


class FenceError(Exception):
    pass


class VersionStore:
    def __init__(self, root: str | Path, state: SourceState):
        self.root = Path(root)
        (self.root / "tasks").mkdir(parents=True, exist_ok=True)
        (self.root / "versions").mkdir(parents=True, exist_ok=True)
        self.state = state
        self._lock = threading.RLock()
        # global publish mutex: closes the check-then-set TOCTOU window so
        # parallel workers can never both pass the epoch check.
        self._publish_lock = threading.Lock()
        self.index_path = self.root / "index.json"
        self.index = _read_json(self.index_path) or {
            "next_seq": 1, "epoch": 0, "latest_version": None,
            "version_counter": 0}
        self._collectors: dict[str, Collector] = {}

    # ------------------------------------------------------------------ #
    # Task lifecycle
    # ------------------------------------------------------------------ #
    def create_task(self, catalog_id: str, strategy: str,
                    note: str = "") -> dict:
        with self._lock:
            task_id = uuid.uuid4().hex[:12]
            seq = self.index["next_seq"]
            self.index["next_seq"] += 1
            record = {
                "task_id": task_id, "seq": seq, "catalog_id": catalog_id,
                "strategy": strategy, "note": note,
                "status": "created",       # created|running|closed|failed
                "created_at": time.time(), "updated_at": time.time(),
                "attempts": [], "progress": {"visited": [], "edges": []},
                "cost": {}, "error": None, "snapshot": None,
                "published_version": None, "resume_count": 0}
            self._save_task(record)
            client = LocalClient(self.state, task_id=task_id)
            self._collectors[task_id] = Collector(client, catalog_id, strategy)
            return record

    def get_task(self, task_id: str) -> dict | None:
        return _read_json(self.root / "tasks" / f"{task_id}.json")

    def list_tasks(self) -> list[dict]:
        out = []
        for p in sorted((self.root / "tasks").glob("*.json"),
                        key=lambda x: x.stat().st_mtime, reverse=True):
            rec = _read_json(p)
            if rec:
                out.append({k: rec[k] for k in (
                    "task_id", "seq", "catalog_id", "strategy", "status",
                    "created_at", "updated_at", "error",
                    "published_version", "resume_count")})
        return out

    def run_task(self, task_id: str, resume: bool = False) -> dict:
        record = self.get_task(task_id)
        if record is None:
            raise KeyError(task_id)
        if record["status"] == "closed":
            return record
        if resume:
            # Rebuild from the persisted pins rather than reusing the live
            # collector: the previous attempt's in-memory state may be stale
            # relative to the source, and rebuilding guarantees every cached
            # object is re-validated by a conditional read before reuse.
            collector = self._restore_collector(record)
            self._collectors[task_id] = collector
        else:
            collector = self._collectors.get(task_id)
            if collector is None:
                # process restart: rebuild collector, pins are re-validated
                # by conditional reads on the first resumed fetch.
                collector = self._restore_collector(record)
                self._collectors[task_id] = collector
        record["status"] = "running"
        if resume:
            record["resume_count"] += 1
        record["updated_at"] = time.time()
        self._save_task(record)
        try:
            snapshot = collector.run(resume=resume)
            record["status"] = "closed"
            record["snapshot"] = snapshot
            record["cost"] = snapshot["cost"]
            record["error"] = None
        except Exception as exc:  # ClosureError, SourceError
            record["status"] = "failed"
            record["error"] = {
                "reason": str(exc),
                "kind": getattr(exc, "kind", "source"),
                "details": getattr(exc, "details", {})}
            record["cost"] = collector.client.cost.as_dict()
        record["attempts"] = [self._attempt_dict(a)
                              for a in collector.attempts]
        record["progress"] = collector.progress()
        record["updated_at"] = time.time()
        self._save_task(record)
        return record

    def _restore_collector(self, record: dict) -> Collector:
        from .collector import Pinned
        client = LocalClient(self.state, task_id=record["task_id"])
        c = Collector(client, record["catalog_id"], record["strategy"])
        for key, obj in (record.get("progress", {}).get("visited_pins", {})
                         or {}).items():
            c.pinned[key] = Pinned(obj["type"], obj["id"], obj["rev"],
                                   obj["etag"], obj["body"])
        # every restored pin predates the next attempt, so its drift on
        # resume is catch-up work rather than a same-attempt conflict.
        c.restored_keys = set(c.pinned)
        c._attempt_start_keys = set(c.pinned)
        return c

    # ------------------------------------------------------------------ #
    # Publishing with the epoch fence
    # ------------------------------------------------------------------ #
    def publish(self, task_id: str) -> dict:
        if self.get_task(task_id) is None:
            raise KeyError(task_id)
        # single global critical section for the fence check and every
        # state mutation it guards.  Re-read the record *inside* the lock so
        # a task cannot publish against a stale snapshot taken before another
        # worker's commit.
        with self._publish_lock:
            with self._lock:
                record = self.get_task(task_id)
                if record is None:
                    raise KeyError(task_id)
                if record["status"] != "closed":
                    raise FenceError("任务未闭合，不能发布")
                if record["seq"] <= self.index["epoch"]:
                    latest = self.index.get("latest_version")
                    raise FenceError(
                        f"发布围栏拒绝：任务 seq={record['seq']} 不大于"
                        f"已发布围栏 epoch={self.index['epoch']}（最新发布 "
                        f"{latest}）。迟到的旧任务不能回退发布版；"
                        "请基于最新来源重新采集。")
                self.index["version_counter"] += 1
                version = f"v{self.index['version_counter']:06d}"
                snap = record["snapshot"]
                doc = {"version": version, "published_at": time.time(),
                       "task_id": task_id, "seq": record["seq"],
                       "catalog_id": record["catalog_id"],
                       "strategy": record["strategy"], **snap}
                _atomic_write(self.root / "versions" / f"{version}.json", doc)
                _atomic_write(self.root / "versions" / "latest.json",
                              {"version": version,
                               "published_at": doc["published_at"],
                               "task_id": task_id})
                self.index["epoch"] = max(self.index["epoch"], record["seq"])
                self.index["latest_version"] = version
                _atomic_write(self.index_path, self.index)
                record["published_version"] = version
                record["updated_at"] = time.time()
                self._save_task(record)
                return doc

    def get_version(self, version: str) -> dict | None:
        if version == "latest":
            ptr = _read_json(self.root / "versions" / "latest.json")
            if not ptr:
                return None
            version = ptr["version"]
        return _read_json(self.root / "versions" / f"{version}.json")

    def list_versions(self) -> list[dict]:
        out = []
        for p in sorted((self.root / "versions").glob("v*.json")):
            doc = _read_json(p)
            if doc:
                out.append({"version": doc["version"],
                            "published_at": doc["published_at"],
                            "task_id": doc["task_id"], "seq": doc["seq"],
                            "catalog_id": doc["catalog_id"],
                            "strategy": doc["strategy"],
                            "object_count": len(doc["objects"]),
                            "cost": doc["cost"]})
        return out

    # ------------------------------------------------------------------ #
    def _save_task(self, record: dict) -> None:
        # persist pins too, so a process restart can resume conditionally
        collector = self._collectors.get(record["task_id"])
        if collector is not None:
            record["progress"] = collector.progress()
            record["progress"]["visited_pins"] = {
                k: {"type": p.otype, "id": p.oid, "rev": p.rev,
                    "etag": p.etag, "body": p.body}
                for k, p in collector.pinned.items()}
        _atomic_write(self.root / "tasks" / f"{record['task_id']}.json",
                      record)

    @staticmethod
    def _attempt_dict(a) -> dict:
        return {"started_at": a.started_at, "finished_at": a.finished_at,
                "strategy": a.strategy, "status": a.status,
                "reason": a.reason, "kind": a.kind,
                "cost": a.cost, "rounds": a.rounds,
                "visited_path": a.visited_path}
