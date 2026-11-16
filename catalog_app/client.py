"""Source client used by the collector.

Two transports share one interface so the same collector logic runs:

* in-process against :class:`SourceState` (fast unit tests, default server);
* over real HTTP via urllib (integration tests / remote source).

Every GET/PUT/DELETE is recorded in the source's raw request log, tagged with
``x-task-id`` so the UI can reconstruct exactly what one collection task saw.
Request cost (counts, 304 saves, bytes, conditional requests, retries) is
accumulated per client instance.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from dataclasses import dataclass, field

from .source import OBJECT_TYPES, SourceState


class SourceError(Exception):
    def __init__(self, status: int, body: str, key: str = ""):
        super().__init__(f"source {status} for {key}: {body[:200]}")
        self.status = status
        self.body = body
        self.key = key


@dataclass
class Cost:
    requests: int = 0
    conditional: int = 0          # requests carrying If-Match / If-None-Match
    not_modified: int = 0         # 304 responses
    precondition_failed: int = 0  # 412 responses
    retries: int = 0
    bytes_received: int = 0
    by_status: dict = field(default_factory=dict)

    def as_dict(self) -> dict:
        return {"requests": self.requests, "conditional": self.conditional,
                "not_modified": self.not_modified,
                "precondition_failed": self.precondition_failed,
                "retries": self.retries,
                "bytes_received": self.bytes_received,
                "by_status": dict(sorted(self.by_status.items()))}


@dataclass
class Response:
    status: int
    body: dict
    etag: str | None
    rev: int | None
    tombstone: bool


class BaseClient:
    def __init__(self, task_id: str = "adhoc"):
        self.task_id = task_id
        self.cost = Cost()

    def get(self, otype: str, oid: str,
            if_none_match: str | None = None,
            if_match: str | None = None) -> Response:
        raise NotImplementedError

    def put(self, otype: str, oid: str, body: dict,
            if_match: str | None = None) -> Response:
        raise NotImplementedError

    def delete(self, otype: str, oid: str,
               if_match: str | None = None) -> Response:
        raise NotImplementedError

    def _record_cost(self, status: int, conditional: bool,
                     body_text: str) -> None:
        self.cost.requests += 1
        if conditional:
            self.cost.conditional += 1
        if status == 304:
            self.cost.not_modified += 1
        if status == 412:
            self.cost.precondition_failed += 1
        self.cost.bytes_received += len(body_text.encode("utf-8"))
        self.cost.by_status[status] = self.cost.by_status.get(status, 0) + 1

    def get_retry(self, otype, oid, *, if_none_match=None, if_match=None,
                  retries: int = 2, retry_on=(429, 500, 502, 503, 504)):
        """GET with bounded retry on transient source failures."""
        attempt = 0
        while True:
            try:
                return self.get(otype, oid, if_none_match=if_none_match,
                                if_match=if_match)
            except SourceError as exc:
                if exc.status not in retry_on or attempt >= retries:
                    raise
                attempt += 1
                self.cost.retries += 1


class LocalClient(BaseClient):
    """Direct in-process transport; still goes through the source's log."""

    def __init__(self, state: SourceState, task_id: str = "adhoc"):
        super().__init__(task_id)
        self.state = state

    def _headers(self):
        return {"x-task-id": self.task_id}

    def get(self, otype, oid, *, if_none_match=None, if_match=None):
        assert otype in OBJECT_TYPES
        path = f"/source/{otype}/{oid}"
        reqh = self._headers()
        if if_none_match:
            reqh["If-None-Match"] = if_none_match
        if if_match:
            reqh["If-Match"] = if_match
        status, body, hdrs = self.state.get(
            otype, oid, if_none_match=if_none_match, if_match=if_match)
        text = json.dumps(body, ensure_ascii=False) if body else ""
        self.state.log("GET", path, reqh, status, hdrs, text)
        self._record_cost(status, bool(if_none_match or if_match), text)
        if status not in (200, 304, 404, 412):
            raise SourceError(status, text, f"{otype}/{oid}")
        return Response(status, body, hdrs.get("ETag"),
                        int(hdrs["x-rev"]) if "x-rev" in hdrs else None,
                        hdrs.get("x-tombstone") == "1")

    def put(self, otype, oid, body, *, if_match=None):
        path = f"/source/{otype}/{oid}"
        reqh = self._headers()
        if if_match:
            reqh["If-Match"] = if_match
        status, resp, hdrs = self.state.put(otype, oid, body,
                                            if_match=if_match)
        text = json.dumps(resp, ensure_ascii=False) if resp else ""
        self.state.log("PUT", path, reqh, status, hdrs, text)
        self._record_cost(status, if_match is not None, text)
        if status not in (200, 201, 412):
            raise SourceError(status, text, f"{otype}/{oid}")
        return Response(status, resp, hdrs.get("ETag"), None, False)

    def delete(self, otype, oid, *, if_match=None):
        path = f"/source/{otype}/{oid}"
        reqh = self._headers()
        if if_match:
            reqh["If-Match"] = if_match
        status, resp, hdrs = self.state.delete(otype, oid, if_match=if_match)
        text = json.dumps(resp, ensure_ascii=False) if resp else ""
        self.state.log("DELETE", path, reqh, status, hdrs, text)
        self._record_cost(status, if_match is not None, text)
        if status not in (204, 404, 412):
            raise SourceError(status, text, f"{otype}/{oid}")
        return Response(status, resp, hdrs.get("ETag"), None, False)


class HttpClient(BaseClient):
    """Real HTTP transport, for integration tests against a running server."""

    def __init__(self, base_url: str, task_id: str = "adhoc", timeout: float = 5):
        super().__init__(task_id)
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def _request(self, method, otype, oid, body=None, if_none_match=None,
                 if_match=None):
        url = f"{self.base_url}/source/{otype}/{oid}"
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("x-task-id", self.task_id)
        if data is not None:
            req.add_header("Content-Type", "application/json")
        if if_none_match:
            req.add_header("If-None-Match", if_none_match)
        if if_match:
            req.add_header("If-Match", if_match)
        conditional = bool(if_none_match or if_match)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as r:
                status = r.status
                text = r.read().decode()
                hdrs = {k: v for k, v in r.headers.items()}
        except urllib.error.HTTPError as exc:
            status = exc.code
            text = exc.read().decode()
            hdrs = {k: v for k, v in exc.headers.items()}
        self._record_cost(status, conditional, text)
        parsed = json.loads(text) if text else {}
        if status not in (200, 201, 204, 304, 404, 412):
            raise SourceError(status, text, f"{otype}/{oid}")
        return Response(status, parsed, hdrs.get("ETag"),
                        int(hdrs["x-rev"]) if hdrs.get("x-rev") else None,
                        hdrs.get("x-tombstone") == "1")

    def get(self, otype, oid, *, if_none_match=None, if_match=None):
        return self._request("GET", otype, oid,
                             if_none_match=if_none_match, if_match=if_match)

    def put(self, otype, oid, body, *, if_match=None):
        return self._request("PUT", otype, oid, body=body, if_match=if_match)

    def delete(self, otype, oid, *, if_match=None):
        return self._request("DELETE", otype, oid, if_match=if_match)
