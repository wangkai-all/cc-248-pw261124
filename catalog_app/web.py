"""Combined HTTP application: control API, collection API and the web page.

One process serves everything on port 8000:

    /api/...        JSON control + collection endpoints (this module)
    /source/...     the simulated upstream source (source.SourceHandler)
    /               single-page console (web/index.html + app.js + styles.css)

The collector runs on a worker thread so long-running re-check rounds do not
block the control plane; task progress is readable while a task is running.
"""

from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from .collector import STRATEGIES
from .seed import fresh_state
from .source import OBJECT_TYPES, dispatch_source
from .store import FenceError, VersionStore

WEB_ROOT = Path(__file__).parent / "web"


class AppState:
    def __init__(self, data_root: str | Path = "data"):
        self.source = fresh_state()
        self.store = VersionStore(data_root, self.source)
        self._workers: dict[str, threading.Thread] = {}

    # -- source control --------------------------------------------------- #
    def source_put(self, otype: str, oid: str, body: dict,
                   if_match: str | None = None):
        return self.source.put(otype, oid, body, if_match=if_match)

    def source_delete(self, otype: str, oid: str,
                      if_match: str | None = None):
        return self.source.delete(otype, oid, if_match=if_match)

    # -- task control ----------------------------------------------------- #
    def start_task(self, catalog_id: str, strategy: str,
                   run_async: bool = True, note: str = "") -> dict:
        record = self.store.create_task(catalog_id, strategy, note=note)
        if run_async:
            t = threading.Thread(
                target=self.store.run_task, args=(record["task_id"],),
                name=f"task-{record['task_id']}", daemon=True)
            self._workers[record["task_id"]] = t
            t.start()
        else:
            record = self.store.run_task(record["task_id"])
        return record

    def resume_task(self, task_id: str) -> dict:
        record = self.store.get_task(task_id)
        if record is None:
            raise KeyError(task_id)
        if record["status"] != "failed":
            raise ValueError("只有 failed 状态的任务可以断点续采")
        t = threading.Thread(
            target=self.store.run_task, args=(task_id, True),
            name=f"resume-{task_id}", daemon=True)
        self._workers[task_id] = t
        t.start()
        return self.store.get_task(task_id)


APP = AppState()


def make_app_handler(app: AppState):
    class AppHandler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        # -- helpers ----------------------------------------------------- #
        def _json(self, status: int, payload):
            body = json.dumps(payload, ensure_ascii=False,
                              default=str).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _read_body(self) -> dict:
            length = int(self.headers.get("Content-Length", 0) or 0)
            raw = self.rfile.read(length) if length else b"{}"
            try:
                return json.loads(raw or b"{}")
            except json.JSONDecodeError:
                self._json(400, {"error": "invalid JSON body"})
                raise

        def _static(self, rel: str):
            rel = rel.lstrip("/") or "index.html"
            # no path traversal
            target = (WEB_ROOT / rel).resolve()
            if WEB_ROOT.resolve() not in target.parents and target != WEB_ROOT.resolve():
                self._json(403, {"error": "forbidden"})
                return
            if not target.is_file():
                self._json(404, {"error": "not found"})
                return
            ctype = {".html": "text/html; charset=utf-8",
                     ".js": "application/javascript; charset=utf-8",
                     ".css": "text/css; charset=utf-8"}.get(
                         target.suffix, "application/octet-stream")
            data = target.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        # -- routing ----------------------------------------------------- #
        def do_GET(self):
            parsed = urlparse(self.path)
            path, qs = parsed.path, parse_qs(parsed.query)
            if path.startswith("/source/"):
                return dispatch_source(self, app.source, "GET")
            if path == "/" or path.startswith("/web/") or path in (
                    "/index.html", "/app.js", "/styles.css"):
                return self._static(path.replace("/web/", "/", 1))
            return self._route_get(path, qs)

        def do_PUT(self):
            if urlparse(self.path).path.startswith("/source/"):
                return dispatch_source(self, app.source, "PUT")
            return self._route_mutate("PUT")

        def do_POST(self):
            return self._route_mutate("POST")

        def do_DELETE(self):
            if urlparse(self.path).path.startswith("/source/"):
                return dispatch_source(self, app.source, "DELETE")
            return self._route_mutate("DELETE")

        # -- GET API ----------------------------------------------------- #
        def _route_get(self, path: str, qs: dict):
            store, source = app.store, app.source
            try:
                if path == "/api/state":
                    return self._json(200, source.snapshot_state())
                if path == "/api/requests":
                    task_id = qs.get("task_id", [None])[0]
                    return self._json(200, source.records_for(task_id))
                if path == "/api/tasks":
                    return self._json(200, store.list_tasks())
                if path.startswith("/api/tasks/"):
                    task_id = path.split("/")[3]
                    rec = store.get_task(task_id)
                    return self._json(200 if rec else 404,
                                      rec or {"error": "task not found"})
                if path == "/api/versions":
                    return self._json(200, {
                        "index": store.index, "versions": store.list_versions()})
                if path.startswith("/api/versions/"):
                    version = path.split("/")[3]
                    doc = store.get_version(version)
                    return self._json(200 if doc else 404,
                                      doc or {"error": "version not found"})
                if path == "/api/meta":
                    return self._json(200, {
                        "strategies": list(STRATEGIES),
                        "object_types": list(OBJECT_TYPES),
                        "time": time.time()})
                return self._json(404, {"error": "unknown endpoint"})
            except Exception as exc:  # pragma: no cover - defensive
                return self._json(500, {"error": str(exc)})

        # -- mutating API ------------------------------------------------ #
        def _route_mutate(self, method: str):
            parsed = urlparse(self.path)
            path = parsed.path
            store, source = app.store, app.source
            try:
                body = self._read_body() if method in ("POST", "PUT") else {}
            except json.JSONDecodeError:
                return

            try:
                # source object control
                if path.startswith("/api/source/") and method in ("PUT",
                                                                  "DELETE"):
                    parts = path.split("/")  # ['', 'api','source',type,id]
                    otype, oid = parts[3], parts[4]
                    if otype not in OBJECT_TYPES:
                        return self._json(400, {"error": "bad type"})
                    im = self.headers.get("If-Match")
                    if method == "PUT":
                        status, resp, hdrs = source.put(
                            otype, oid, body, if_match=im)
                    else:
                        status, resp, hdrs = source.delete(
                            otype, oid, if_match=im)
                    source.log(method, path,
                               {"x-console": "1", **({"If-Match": im} if im
                                                     else {})},
                               status, hdrs,
                               json.dumps(resp, ensure_ascii=False))
                    return self._json(status, resp)

                if path == "/api/faults" and method == "POST":
                    if body.get("clear"):
                        source.clear_faults()
                    else:
                        source.inject_fault(
                            body["fail_oids"],
                            status=int(body.get("status", 500)),
                            times=int(body.get("times", 1)),
                            persistent=bool(body.get("persistent", False)))
                    return self._json(200, {"faults": source.faults})

                if path == "/api/schedule" and method == "POST":
                    if body.get("clear"):
                        source.clear_scheduled()
                        return self._json(200, {"scheduled": {}})
                    otype, oid = body["type"], body["id"]
                    source.schedule_update(
                        otype, oid, on_get=int(body.get("on_get", 1)),
                        patch=body.get("patch"),
                        bump_to_rev=body.get("bump_to_rev"))
                    return self._json(200,
                                      {"scheduled": source.scheduled})

                if path == "/api/requests" and method == "DELETE":
                    source.clear_records()
                    return self._json(200, {"cleared": True})

                # collection tasks
                if path == "/api/tasks" and method == "POST":
                    strategy = body.get("strategy", "pin")
                    if strategy not in STRATEGIES:
                        return self._json(400, {"error": "bad strategy"})
                    rec = app.start_task(
                        body.get("catalog_id", "main"), strategy,
                        run_async=bool(body.get("async", True)),
                        note=body.get("note", ""))
                    return self._json(201, rec)

                if path.startswith("/api/tasks/") and path.endswith(
                        "/resume") and method == "POST":
                    task_id = path.split("/")[3]
                    try:
                        rec = app.resume_task(task_id)
                    except KeyError:
                        return self._json(404, {"error": "task not found"})
                    except ValueError as exc:
                        return self._json(409, {"error": str(exc)})
                    return self._json(202, rec)

                if path.startswith("/api/tasks/") and path.endswith(
                        "/publish") and method == "POST":
                    task_id = path.split("/")[3]
                    try:
                        doc = store.publish(task_id)
                    except KeyError:
                        return self._json(404, {"error": "task not found"})
                    except FenceError as exc:
                        return self._json(409, {"error": str(exc)})
                    except Exception as exc:
                        return self._json(409, {"error": str(exc)})
                    return self._json(201, doc)

                return self._json(404, {"error": "unknown endpoint"})
            except Exception as exc:  # pragma: no cover - defensive
                return self._json(500, {"error": str(exc)})

    return AppHandler


def serve(host: str = "127.0.0.1", port: int = 8000,
          data_root: str | Path = "data"):
    app = AppState(data_root=data_root)
    global APP
    APP = app
    handler = make_app_handler(app)
    httpd = ThreadingHTTPServer((host, port), handler)
    print(f"catalog console: http://{host}:{port}  (data: {data_root})")
    return httpd
