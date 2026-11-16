"""Simulated upstream source service.

The source owns four kinds of objects that are updated *independently* with no
cross-object transaction:

    catalog -> products -> dataset -> fields / rules

Every object carries a monotonically increasing revision number ``_rev`` plus an
opaque ETag.  Conditional reads are supported:

    GET with ``If-None-Match: <etag>``   -> 304 when the object is unchanged
    GET with ``If-Match: <etag>``        -> 412 when the object has changed

Deletions are recorded as tombstones (404 with ``x-tombstone: 1``) so a
collector can distinguish "never existed" from "deleted mid-traversal".

The same in-memory state is reachable through a plain Python API (used by the
collector inside the single test process) and through an HTTP handler (used by
the real web page / integration tests).  Every HTTP request is recorded with
its *raw* response so the UI can show exactly what the source answered.
"""

from __future__ import annotations

import json
import threading
import time
import uuid
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse

OBJECT_TYPES = ("catalog", "product", "dataset", "field", "rule")


# --------------------------------------------------------------------------- #
# In-memory source state
# --------------------------------------------------------------------------- #
@dataclass
class SourceObject:
    type: str
    id: str
    body: dict
    rev: int = 1
    etag: str = field(default_factory=lambda: uuid.uuid4().hex)
    deleted: bool = False
    updated_at: float = field(default_factory=time.time)


@dataclass
class RequestRecord:
    seq: int
    method: str
    path: str
    request_headers: dict
    status: int
    response_headers: dict
    body: str  # raw response body exactly as transmitted
    at: float


class SourceState:
    """Thread-safe mutable state of the simulated source."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.objects: dict[str, SourceObject] = {}
        self.records: list[RequestRecord] = []
        self._seq = 0
        # fault injection: type of fault -> configuration
        #   {"fail_oids": ["field/price"], "status": 500, "remaining": 2}
        self.faults: dict = {}
        # next scheduled update: on the Nth GET of a given oid, bump its rev.
        # {"dataset/sales": {"on_get": 1, "bump_to_rev": 2}}
        self.scheduled: dict[str, dict] = {}
        self._get_counts: dict[str, int] = {}

    def key(self, otype: str, oid: str) -> str:
        return f"{otype}/{oid}"

    def put(self, otype: str, oid: str, body: dict,
            if_match: str | None = None) -> tuple[int, dict, dict]:
        """Create or update. Returns (status, response_body, headers).

        Conditional write: If-Match rejects stale writes with 412, mirroring the
        semantics a real source would expose.
        """
        key = self.key(otype, oid)
        with self._lock:
            obj = self.objects.get(key)
            current_etag = obj.etag if obj and not obj.deleted else None
            if if_match is not None and current_etag != if_match:
                return 412, {"error": "precondition failed",
                             "current_etag": current_etag}, {}
            if obj is None:
                obj = SourceObject(otype, oid, body)
                self.objects[key] = obj
                status = 201
            else:
                obj.body = body
                obj.rev += 1
                obj.etag = uuid.uuid4().hex
                obj.deleted = False
                obj.updated_at = time.time()
                status = 200
            return status, self._public(obj), {"ETag": obj.etag}

    def delete(self, otype: str, oid: str,
               if_match: str | None = None) -> tuple[int, dict, dict]:
        key = self.key(otype, oid)
        with self._lock:
            obj = self.objects.get(key)
            if obj is None or obj.deleted:
                return 404, {"error": "not found"}, {}
            if if_match is not None and obj.etag != if_match:
                return 412, {"error": "precondition failed",
                             "current_etag": obj.etag}, {}
            obj.deleted = True
            obj.rev += 1
            obj.etag = uuid.uuid4().hex
            obj.updated_at = time.time()
            return 204, {}, {"ETag": obj.etag}

    def get(self, otype: str, oid: str,
            if_none_match: str | None = None,
            if_match: str | None = None) -> tuple[int, dict, dict]:
        key = self.key(otype, oid)
        with self._lock:
            # scheduled update fires *before* the read is answered
            sched = self.scheduled.get(key)
            if sched is not None:
                self._get_counts[key] = self._get_counts.get(key, 0) + 1
                if self._get_counts[key] >= sched.get("on_get", 1):
                    self._apply_scheduled_update(key, sched)
                    del self.scheduled[key]

            fault = self._check_fault(key)
            if fault is not None:
                return fault, {"error": "injected failure", "key": key}, {}

            obj = self.objects.get(key)
            if obj is None or obj.deleted:
                headers = {}
                if obj is not None:
                    headers["x-tombstone"] = "1"
                    headers["ETag"] = obj.etag
                return 404, {"error": "not found", "key": key}, headers

            if if_match is not None and obj.etag != if_match:
                return 412, {"error": "precondition failed",
                             "current_etag": obj.etag,
                             "current_rev": obj.rev}, {}
            headers = {"ETag": obj.etag, "x-rev": str(obj.rev)}
            if if_none_match is not None and obj.etag == if_none_match:
                return 304, {}, headers
            return 200, self._public(obj), headers

    def _apply_scheduled_update(self, key: str, sched: dict) -> None:
        """Simulate another writer committing a new revision mid-traversal."""
        obj = self.objects.get(key)
        if obj is None or obj.deleted:
            return
        obj.body = json.loads(json.dumps(obj.body))  # copy
        if "patch" in sched:
            for path_key, val in sched["patch"].items():
                obj.body[path_key] = val
        if sched.get("bump_to_rev"):
            obj.rev = sched["bump_to_rev"]
        else:
            obj.rev += 1
        obj.etag = uuid.uuid4().hex
        obj.updated_at = time.time()

    def _check_fault(self, key: str) -> int | None:
        fault = self.faults
        if not fault or key not in fault.get("fail_oids", []):
            return None
        if fault.get("persistent"):
            return fault.get("status", 500)
        if fault.get("remaining", 0) > 0:
            fault["remaining"] -= 1
            return fault.get("status", 500)
        return None

    # -- fault injection control ------------------------------------------- #
    def inject_fault(self, fail_oids: list[str], status: int = 500,
                     times: int = 1, persistent: bool = False) -> None:
        with self._lock:
            # ``times`` = exact number of failing requests before recovery
            self.faults = {"fail_oids": list(fail_oids), "status": status,
                           "remaining": max(1, times),
                           "persistent": persistent}

    def clear_faults(self) -> None:
        with self._lock:
            self.faults = {}

    def schedule_update(self, otype: str, oid: str, on_get: int = 1,
                        patch: dict | None = None,
                        bump_to_rev: int | None = None) -> None:
        with self._lock:
            self.scheduled[self.key(otype, oid)] = {
                "on_get": on_get, "patch": patch or {},
                "bump_to_rev": bump_to_rev}
            self._get_counts[self.key(otype, oid)] = 0

    def clear_scheduled(self) -> None:
        with self._lock:
            self.scheduled.clear()
            self._get_counts.clear()

    def snapshot_state(self) -> dict:
        with self._lock:
            out = {}
            for key, obj in sorted(self.objects.items()):
                out[key] = {"rev": obj.rev, "etag": obj.etag,
                            "deleted": obj.deleted,
                            "body": json.loads(json.dumps(obj.body))}
            return out

    def log(self, method, path, req_headers, status, resp_headers,
            body_text: str) -> RequestRecord:
        with self._lock:
            self._seq += 1
            rec = RequestRecord(self._seq, method, path, dict(req_headers),
                                status, dict(resp_headers), body_text,
                                time.time())
            self.records.append(rec)
            # keep the raw log bounded
            if len(self.records) > 500:
                self.records = self.records[-500:]
            return rec

    def records_for(self, task_id: str | None = None) -> list[dict]:
        with self._lock:
            recs = self.records
            if task_id is not None:
                recs = [r for r in recs
                        if r.request_headers.get("x-task-id") == task_id]
            return [self._record_dict(r) for r in recs]

    def clear_records(self) -> None:
        with self._lock:
            self.records.clear()
            self._seq = 0

    @staticmethod
    def _public(obj: SourceObject) -> dict:
        body = json.loads(json.dumps(obj.body))
        body["_id"] = obj.id
        body["_type"] = obj.type
        body["_rev"] = obj.rev
        body["_etag"] = obj.etag
        return body

    @staticmethod
    def _record_dict(r: RequestRecord) -> dict:
        return {"seq": r.seq, "method": r.method, "path": r.path,
                "request_headers": r.request_headers, "status": r.status,
                "response_headers": r.response_headers, "body": r.body,
                "at": r.at}


# --------------------------------------------------------------------------- #
# HTTP transport wrapper around SourceState
# --------------------------------------------------------------------------- #
def dispatch_source(handler: "BaseHTTPRequestHandler", state: SourceState,
                    method: str) -> None:
    """Route one /source/... request on an *existing* connection handler."""
    parsed = urlparse(handler.path)
    parts = [p for p in parsed.path.split("/") if p]
    req_headers = {k: v for k, v in handler.headers.items()}
    length = int(handler.headers.get("Content-Length", 0) or 0)
    raw = handler.rfile.read(length) if length else b""
    try:
        payload = json.loads(raw) if raw else {}
    except json.JSONDecodeError:
        body = json.dumps({"error": "bad json"}, ensure_ascii=False).encode()
        handler.send_response(400)
        handler.send_header("Content-Type", "application/json")
        handler.send_header("Content-Length", str(len(body)))
        handler.end_headers()
        handler.wfile.write(body)
        state.log(method, parsed.path, req_headers, 400, {}, body.decode())
        return

    status, resp, hdrs = 404, {"error": "not found"}, {}
    if len(parts) == 3 and parts[0] == "source" and parts[1] in OBJECT_TYPES:
        otype, oid = parts[1], parts[2]
        inm = handler.headers.get("If-None-Match")
        im = handler.headers.get("If-Match")
        if method == "GET":
            status, resp, hdrs = state.get(
                otype, oid, if_none_match=inm, if_match=im)
        elif method == "PUT":
            status, resp, hdrs = state.put(otype, oid, payload, if_match=im)
        elif method == "DELETE":
            status, resp, hdrs = state.delete(otype, oid, if_match=im)
    elif len(parts) == 2 and parts[0] == "source" and method == "GET":
        status, resp, hdrs = 200, state.snapshot_state(), {}

    if status == 204:
        body = b""
    else:
        body = json.dumps(resp, ensure_ascii=False).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(body)))
    for k, v in hdrs.items():
        handler.send_header(k, v)
    handler.end_headers()
    if body:
        handler.wfile.write(body)
    state.log(method,
              parsed.path + (("?" + parsed.query) if parsed.query else ""),
              req_headers, status, dict(hdrs), body.decode("utf-8"))


class SourceHandler(BaseHTTPRequestHandler):
    state: SourceState  # injected via factory

    def log_message(self, *args):  # silence stderr noise
        pass

    def do_GET(self):
        dispatch_source(self, self.state, "GET")

    def do_PUT(self):
        dispatch_source(self, self.state, "PUT")

    def do_DELETE(self):
        dispatch_source(self, self.state, "DELETE")


def make_handler(state: SourceState):
    class _Bound(SourceHandler):
        pass
    _Bound.state = state
    return _Bound
