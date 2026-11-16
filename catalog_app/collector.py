"""Catalog collector: recursive pinning vs. re-check-to-stable.

The source updates each object independently with no cross-object transaction,
so a naive traversal can stitch an *old* product onto a *new* dataset.  Two
closure strategies are offered, both producing the same immutable snapshot
shape when they close:

``pin`` — recursive revision pinning
    Every object is pinned (rev + ETag + body) on first visit.  Shared objects
    are re-validated on each later visit with ``If-Match``.  A final closure
    pass conditionally re-validates *every* pinned object with
    ``If-None-Match`` (304 = bytes saved).  Any 412/changed object means the
    graph moved under us: the attempt fails with an explicit conflict reason
    instead of publishing a mixed-revision catalog.

``recheck`` — repeated full verification until stable
    Whole-graph rounds.  Round 1 fetches unconditionally; later rounds send
    ``If-None-Match`` and only a round in which every object answers 304 is
    stable.  Bounded rounds cap the cost of a continuously changing source.

Both strategies detect missing references (404 / tombstone) and circular
references (colored DFS) and record per-object request cost.  A failed attempt
persists its partial pins, so a retry resumes: cached objects are validated
with conditional reads and only drift is re-fetched.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field

from .client import BaseClient, SourceError

STRATEGIES = ("pin", "recheck")
MAX_RECHECK_ROUNDS = 6


class ClosureError(Exception):
    def __init__(self, reason: str, kind: str, details: dict | None = None):
        super().__init__(reason)
        self.reason = reason
        self.kind = kind          # missing | cycle | conflict | unstable | source
        self.details = details or {}


@dataclass
class Pinned:
    otype: str
    oid: str
    rev: int
    etag: str
    body: dict


@dataclass
class Attempt:
    started_at: float
    finished_at: float | None = None
    strategy: str = ""
    status: str = "running"          # running | closed | failed
    reason: str = ""
    kind: str = ""
    cost: dict = field(default_factory=dict)
    rounds: list = field(default_factory=list)
    visited_path: list = field(default_factory=list)


def _key(otype: str, oid: str) -> str:
    return f"{otype}/{oid}"


def _split_ref(ref: str) -> tuple[str, str]:
    otype, oid = ref.split("/", 1)
    return otype, oid


def outgoing_refs(otype: str, body: dict) -> list[str]:
    """Reference edges declared by one object."""
    refs: list[str] = []
    if otype == "catalog":
        refs.extend(body.get("products", []))
    elif otype == "product":
        if body.get("dataset"):
            refs.append(body["dataset"])
    elif otype == "dataset":
        refs.extend(body.get("fields", []))
        refs.extend(body.get("rules", []))
        refs.extend(body.get("depends_on", []))
    return refs


class Collector:
    """One collector instance per collection task."""

    def __init__(self, client: BaseClient, catalog_id: str, strategy: str,
                 max_rounds: int = MAX_RECHECK_ROUNDS):
        if strategy not in STRATEGIES:
            raise ValueError(f"unknown strategy {strategy}")
        self.client = client
        self.catalog_id = catalog_id
        self.strategy = strategy
        self.max_rounds = max_rounds
        self.pinned: dict[str, Pinned] = {}
        self.edges: list[list[str]] = []
        self.attempts: list[Attempt] = []
        self.current_path: list[str] = []
        self._lock = threading.RLock()
        # keys whose pin was restored from a previous failed attempt.
        # Drift on these keys during the resumed traversal is the *old*
        # drift we are explicitly catching up on, so the closure pass must
        # not treat their refresh as a same-attempt conflict.
        self.restored_keys: set[str] = set()
        self._attempt_start_keys: set[str] = set()

    # ------------------------------------------------------------------ #
    # Public entry points
    # ------------------------------------------------------------------ #
    def run(self, resume: bool = False) -> dict:
        attempt = Attempt(started_at=time.time(), strategy=self.strategy)
        self.attempts.append(attempt)
        # pins present when this attempt began predate it; any drift on them
        # is catch-up work, not a same-attempt mutation.
        self._attempt_start_keys = set(self.pinned)
        try:
            if self.strategy == "pin":
                self._run_pin(attempt)
            else:
                self._run_recheck(attempt)
            # everything still pinned at close time was validated in-attempt
            self.restored_keys.clear()
            attempt.status = "closed"
            attempt.finished_at = time.time()
            attempt.cost = self.client.cost.as_dict()
            return self.snapshot()
        except ClosureError as exc:
            attempt.status = "failed"
            attempt.finished_at = time.time()
            attempt.reason = exc.reason
            attempt.kind = exc.kind
            attempt.cost = self.client.cost.as_dict()
            raise

    def snapshot(self) -> dict:
        return {
            "catalog_id": self.catalog_id,
            "strategy": self.strategy,
            "closed": True,
            "closed_at": time.time(),
            "objects": {k: {"type": p.otype, "id": p.oid, "rev": p.rev,
                            "etag": p.etag, "body": p.body}
                        for k, p in sorted(self.pinned.items())},
            "edges": self.edges,
            "cost": self.client.cost.as_dict(),
        }

    def progress(self) -> dict:
        with self._lock:
            return {"visited": sorted(self.pinned.keys()),
                    "visited_count": len(self.pinned),
                    "current_path": list(self.current_path),
                    "edges": list(self.edges)}

    # ------------------------------------------------------------------ #
    # Fetch with pin cache (also the resume mechanism)
    # ------------------------------------------------------------------ #
    def _fetch(self, otype: str, oid: str,
               allow_drift_refresh: bool = True) -> Pinned:
        """Fetch one object, using the pin cache as a conditional read.

        ``allow_drift_refresh`` (default) refreshes a stale pin in place:
        this is the resume semantics — a previous failed attempt's pin is
        allowed to catch up to the source's current revision.  During the
        final closure pass it is disabled, because a change observed *within
        the same attempt* means the graph moved under us and must fail.
        """
        key = _key(otype, oid)
        cached = self.pinned.get(key)
        if cached is not None:
            # Conditional read: 304 keeps the cache and saves bytes;
            # 200 means the source revised it and we must update the pin.
            resp = self.client.get_retry(
                otype, oid, if_none_match=cached.etag)
            if resp.status == 304:
                return cached
            if resp.status == 404:
                raise ClosureError(
                    f"引用无法闭合：{key} 在采集期间被删除"
                    + ("（来源墓碑）" if resp.tombstone else "（来源不存在）"),
                    "missing", {"key": key, "tombstone": resp.tombstone})
            if resp.status == 200:
                if not allow_drift_refresh:
                    raise ClosureError(
                        f"条件冲突：{key} 在本次遍历期间被来源更新"
                        f"（钉住 rev {cached.rev} / {cached.etag[:8]}，"
                        f"当前 rev {resp.body['_rev']} / "
                        f"{resp.body['_etag'][:8]}），拒绝拼合新旧修订，"
                        "本次不发布，可重试采集",
                        "conflict",
                        {"key": key, "pinned_rev": cached.rev,
                         "current_rev": resp.body["_rev"],
                         "pinned_etag": cached.etag,
                         "current_etag": resp.body["_etag"]})
                fresh = Pinned(otype, oid, resp.body["_rev"],
                               resp.body["_etag"], resp.body)
                self.pinned[key] = fresh
                return fresh
            raise ClosureError(
                f"来源对 {key} 返回异常状态 {resp.status}", "source",
                {"key": key, "status": resp.status})

        resp = self.client.get_retry(otype, oid)
        if resp.status == 404:
            raise ClosureError(
                f"引用无法闭合：{key} 在来源不存在"
                + ("（已删除，来源墓碑）" if resp.tombstone else ""),
                "missing", {"key": key, "tombstone": resp.tombstone})
        if resp.status != 200:
            raise ClosureError(
                f"来源错误：读取 {key} 得到 HTTP {resp.status}"
                f"（已重试 {self.client.cost.retries} 次）",
                "source", {"key": key, "status": resp.status})
        p = Pinned(otype, oid, resp.body["_rev"], resp.body["_etag"],
                   resp.body)
        self.pinned[key] = p
        return p

    # ------------------------------------------------------------------ #
    # Strategy A: recursive pinning
    # ------------------------------------------------------------------ #
    def _run_pin(self, attempt: Attempt) -> None:
        # colored DFS: white unseen, gray on stack, black fully explored
        color: dict[str, str] = {}
        root_key = _key("catalog", self.catalog_id)

        def visit(ref: str) -> None:
            otype, oid = _split_ref(ref)
            key = ref
            with self._lock:
                self.current_path.append(key)
            if color.get(key) == "gray":
                cycle = self._cycle_tail(key)
                raise ClosureError(
                    "检测到循环引用，目录不能闭合：" + " -> ".join(cycle),
                    "cycle", {"cycle": cycle})
            if color.get(key) == "black":
                # shared dependency — or a pin restored from a previous
                # failed attempt: always re-validate conditionally so drift
                # since the last attempt cannot sneak into the snapshot.
                self._fetch(otype, oid)
                return
            color[key] = "gray"
            obj = self._fetch(otype, oid)
            # _fetch may have refreshed a restored pin; color bookkeeping
            # still applies to this node only.
            children = outgoing_refs(otype, obj.body)
            for child in children:
                self.edges.append([key, child])
                visit(child)
            color[key] = "black"
            with self._lock:
                self.current_path.pop()

        try:
            visit(root_key)
        except SourceError as exc:
            raise ClosureError(
                f"来源请求失败：{exc.key or '未知对象'} HTTP {exc.status}"
                f"（已重试 {self.client.cost.retries} 次仍失败，"
                "已获取的引用保留，可断点续采）",
                "source", {"status": exc.status, "key": exc.key})

        # closure pass: every pinned object must still be at the pinned rev.
        self._closure_pass(attempt)

    def _closure_pass(self, attempt: Attempt) -> None:
        """Final whole-graph validation on the pins of *this* attempt.

        Two sweeps:

        1. Catch-up sweep.  Pins restored from a previous failed attempt
           predate this one; their known old drift is refreshed in place.
           Pins created in this traversal must answer 304 here.
        2. Stability sweep.  Every object is unconditionally re-read and its
           current etag compared against the pin held at the end of sweep 1.
           Any mismatch means it changed *within this attempt*: a genuine
           condition conflict, nothing is closed.
        """
        for key in list(self.pinned):
            otype, oid = _split_ref(key)
            try:
                self._fetch(otype, oid,
                            allow_drift_refresh=key in self._attempt_start_keys)
            except SourceError as exc:
                raise ClosureError(
                    f"闭合核对时来源对 {key} 返回 HTTP {exc.status}"
                    f"（已重试 {self.client.cost.retries} 次）",
                    "source", {"key": key, "status": exc.status})
        # sweep 2: conditional stability read with If-None-Match against the
        # pin held at the end of sweep 1.  304 means nobody revised it since
        # sweep 1 (zero body bytes); 200 means it changed *within this
        # attempt* — a genuine conflict that closes nothing.
        for key, pin in list(self.pinned.items()):
            otype, oid = _split_ref(key)
            try:
                resp = self.client.get_retry(otype, oid,
                                             if_none_match=pin.etag)
            except SourceError as exc:
                raise ClosureError(
                    f"闭合核对时来源对 {key} 返回 HTTP {exc.status}"
                    f"（已重试 {self.client.cost.retries} 次）",
                    "source", {"key": key, "status": exc.status})
            if resp.status == 404:
                raise ClosureError(
                    f"闭合核对失败：{key} 在闭合前被删除",
                    "missing", {"key": key, "tombstone": resp.tombstone})
            if resp.status == 200:
                raise ClosureError(
                    f"条件冲突：{key} 在本次遍历期间被来源更新"
                    f"（钉住 rev {pin.rev} / {pin.etag[:8]}，"
                    f"当前 rev {resp.body.get('_rev')} / "
                    f"{str(resp.etag or '')[:8]}），"
                    "拒绝拼合新旧修订，本次不发布，可重试采集",
                    "conflict",
                    {"key": key, "pinned_rev": pin.rev,
                     "current_rev": resp.body.get("_rev"),
                     "pinned_etag": pin.etag,
                     "current_etag": resp.etag})
            if resp.status != 304:
                raise ClosureError(
                    f"闭合核对时来源对 {key} 返回 HTTP {resp.status}",
                    "source", {"key": key, "status": resp.status})

    def _cycle_tail(self, back_edge_key: str) -> list[str]:
        path = list(self.current_path)
        start = path.index(back_edge_key)
        return path[start:] + [back_edge_key]

    # ------------------------------------------------------------------ #
    # Strategy B: re-check whole graph until stable
    # ------------------------------------------------------------------ #
    def _run_recheck(self, attempt: Attempt) -> None:
        round_no = 0
        stable = False
        while round_no < self.max_rounds and not stable:
            round_no += 1
            round_info = {"round": round_no, "changed": [],
                          "missing": [], "cost_start":
                          self.client.cost.requests}
            color: dict[str, str] = {}
            self.edges = []
            self.current_path = []

            def walk(ref: str):
                key = ref
                if color.get(key) in ("gray", "black"):
                    if color.get(key) == "gray":
                        cycle = self._cycle_tail(key)
                        raise ClosureError(
                            "检测到循环引用，目录不能闭合："
                            + " -> ".join(cycle),
                            "cycle", {"cycle": cycle})
                    return
                color[key] = "gray"
                otype, oid = _split_ref(key)
                self.current_path.append(key)
                cached = self.pinned.get(key)
                if cached is None:
                    obj = self._fetch(otype, oid)
                    round_info["changed"].append(key)
                else:
                    resp = self.client.get_retry(
                        otype, oid, if_none_match=cached.etag)
                    if resp.status == 304:
                        obj = cached
                    elif resp.status == 404:
                        round_info["missing"].append(key)
                        raise ClosureError(
                            f"引用无法闭合：{key} 在采集期间被删除",
                            "missing",
                            {"key": key, "tombstone": resp.tombstone})
                    elif resp.status == 200:
                        obj = Pinned(otype, oid, resp.body["_rev"],
                                     resp.body["_etag"], resp.body)
                        self.pinned[key] = obj
                        round_info["changed"].append(key)
                    else:
                        raise ClosureError(
                            f"来源对 {key} 返回 HTTP {resp.status}",
                            "source", {"key": key, "status": resp.status})
                for child in outgoing_refs(otype, obj.body):
                    self.edges.append([key, child])
                    walk(child)
                color[key] = "black"
                self.current_path.pop()

            try:
                walk(_key("catalog", self.catalog_id))
            except SourceError as exc:
                raise ClosureError(
                    f"来源请求失败：{exc.key or '未知对象'} HTTP {exc.status}"
                    "（已获取的引用保留，可断点续采）",
                    "source", {"status": exc.status, "key": exc.key})

            round_info["cost_end"] = self.client.cost.requests
            round_info["requests_this_round"] = (
                round_info["cost_end"] - round_info["cost_start"])
            attempt.rounds.append(round_info)
            stable = not round_info["changed"] and not round_info["missing"]
            if not stable:
                # restart color analysis next round; pins are retained so
                # round N+1 is served mostly by cheap 304s.
                continue

        if not stable:
            changed = attempt.rounds[-1]["changed"]
            raise ClosureError(
                f"反复核对 {self.max_rounds} 轮后仍不稳定，来源持续更新："
                f"最近变化对象 {changed[:5]}，拒绝发布混合修订目录",
                "unstable", {"last_changed": changed,
                             "rounds": round_no})
