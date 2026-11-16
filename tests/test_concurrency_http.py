"""Concurrency tests: parallel tasks sharing dependencies + real HTTP E2E."""

import json
import shutil
import sys
import tempfile
import threading
import unittest
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from catalog_app.client import HttpClient
from catalog_app.collector import Collector
from catalog_app.seed import fresh_state
from catalog_app.store import FenceError, VersionStore
from catalog_app.web import serve


class ConcurrentPublishTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.state = fresh_state()
        self.store = VersionStore(self.tmp, self.state)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_older_task_finishing_late_cannot_roll_back(self):
        # Two tasks discover the same shared dependency (dataset/sales).
        t1 = self.store.create_task("main", "pin", note="early-seq")
        t2 = self.store.create_task("main", "pin", note="later-seq")
        self.assertLess(t1["seq"], t2["seq"])

        # The newer task closes and publishes first.
        r2 = self.store.run_task(t2["task_id"])
        v2 = self.store.publish(r2["task_id"])

        # Meanwhile the source moved: the older task is now stale. It still
        # closes against its own pins, but its publish must be fenced off.
        self.state.put("field", "amount",
                       {"name": "amount", "dtype": "decimal",
                        "label": "口径已变"})
        r1 = self.store.run_task(t1["task_id"])
        self.assertEqual(r1["status"], "closed")
        with self.assertRaises(FenceError):
            self.store.publish(r1["task_id"])

        # latest pointer is untouched and still points at the newer version
        self.assertEqual(self.store.get_version("latest")["version"],
                         v2["version"])

    def test_parallel_runs_only_one_occupies_latest(self):
        """Two tasks discovering a shared dependency cannot publish
        contradictory catalogs: a task whose seq is already behind the
        published epoch is fenced off, and churn forces real conflicts."""
        results = []
        errors = []

        def worker(catalog_id="main", strategy="pin"):
            rec = self.store.create_task(catalog_id, strategy)
            rec = self.store.run_task(rec["task_id"])
            try:
                if rec["status"] == "closed":
                    doc = self.store.publish(rec["task_id"])
                    results.append((doc["version"], rec["seq"]))
                else:
                    errors.append(rec["error"]["kind"])
            except FenceError as exc:
                errors.append(str(exc))
            except Exception as exc:  # pragma: no cover
                errors.append(repr(exc))

        # conflicting task: isolated churning source -> same-attempt conflict
        from catalog_app.client import LocalClient
        from catalog_app.collector import Collector
        from catalog_app.seed import fresh_state
        import uuid as _uuid

        def conflicting_worker():
            rec = self.store.create_task("main", "pin")
            own_state = fresh_state()
            real_get = own_state.get

            def churning(otype, oid, if_none_match=None, if_match=None):
                if (otype, oid) == ("dataset", "sales"):
                    obj = own_state.objects["dataset/sales"]
                    obj.rev += 1
                    obj.etag = _uuid.uuid4().hex
                return real_get(otype, oid, if_none_match=if_none_match,
                                if_match=if_match)

            own_state.get = churning
            client = LocalClient(own_state, task_id=rec["task_id"])
            coll = Collector(client, "main", "pin")
            try:
                coll.run()
                self.fail("churning source should have prevented closure")
            except Exception as exc:
                errors.append(getattr(exc, "kind", "source"))
            finally:
                own_state.get = real_get

        # stale task: created first so it holds the smallest seq, closes
        # early, but only attempts to publish after newer tasks committed.
        stale_rec = self.store.create_task("main", "pin")
        stale_rec = self.store.run_task(stale_rec["task_id"])
        self.assertEqual(stale_rec["status"], "closed")
        stale_seq = stale_rec["seq"]

        def stale_worker():
            gate.wait(5)
            try:
                self.store.publish(stale_rec["task_id"])
                results.append(("stale", stale_seq))
            except FenceError as exc:
                errors.append(str(exc))

        gate = threading.Event()
        threads = [
            threading.Thread(target=worker),
            threading.Thread(target=conflicting_worker),
            threading.Thread(target=worker),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        gate.set()
        stale = threading.Thread(target=stale_worker)
        stale.start()
        stale.join()

        # the two clean, newer tasks each legitimately publish (monotonic
        # seqs); the churning task conflicts; the stale late task is fenced.
        self.assertEqual(len([r for r in results if r[0] != "stale"]), 2)
        self.assertNotIn("stale", [r[0] for r in results])
        self.assertIn("conflict", errors)
        self.assertTrue(any("围栏" in e for e in errors))
        # latest pointer only ever advanced forward
        versions = self.store.list_versions()
        self.assertEqual([v["seq"] for v in versions],
                         sorted(v["seq"] for v in versions))


class HttpEndToEndTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.mkdtemp()
        cls.httpd = serve("127.0.0.1", 0, cls.tmp)
        cls.port = cls.httpd.server_address[1]
        cls.base = f"http://127.0.0.1:{cls.port}"
        cls.thread = threading.Thread(
            target=cls.httpd.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.httpd.shutdown()
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def _get(self, path, headers=None):
        req = urllib.request.Request(self.base + path, headers=headers or {})
        try:
            with urllib.request.urlopen(req, timeout=5) as r:
                return r.status, dict(r.headers), r.read().decode()
        except urllib.error.HTTPError as exc:
            return exc.code, dict(exc.headers), exc.read().decode()

    def test_conditional_read_over_real_http(self):
        status, headers, body = self._get("/source/dataset/sales")
        self.assertEqual(status, 200)
        etag = headers["ETag"]
        rev = json.loads(body)["_rev"]

        status, _, body = self._get(
            "/source/dataset/sales", {"If-None-Match": etag})
        self.assertEqual(status, 304)
        self.assertEqual(body, "")

        status, body_headers, body = self._get(
            "/source/dataset/sales", {"If-Match": "stale"})
        self.assertEqual(status, 412)
        self.assertEqual(json.loads(body)["current_rev"], rev)

    def test_tombstone_marker_over_http(self):
        self._request("DELETE", "/api/source/field/sku")
        status, headers, _ = self._get("/source/field/sku")
        self.assertEqual(status, 404)
        self.assertEqual(headers.get("x-tombstone"), "1")

    def test_full_collect_publish_flow_via_http_client(self):
        client = HttpClient(self.base, task_id="http-e2e")
        c = Collector(client, "main", "pin")
        snap = c.run()
        self.assertTrue(snap["closed"])
        self.assertGreater(client.cost.not_modified, 0)

        # drive the control API to register + publish a server-side task
        status, _, body = self._request("POST", "/api/tasks",
                                        {"catalog_id": "main",
                                         "strategy": "pin",
                                         "async": False})
        self.assertEqual(status, 201)
        rec = json.loads(body)
        self.assertEqual(rec["status"], "closed")
        status, _, body = self._request(
            "POST", f"/api/tasks/{rec['task_id']}/publish")
        self.assertEqual(status, 201)
        version = json.loads(body)["version"]

        status, _, body = self._get(f"/api/versions/{version}")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["version"], version)

    def test_fault_injection_then_raw_request_log(self):
        self._request("POST", "/api/faults",
                      {"fail_oids": ["rule/qty-limit"], "status": 503,
                       "times": 1})
        status, _, _ = self._get("/source/rule/qty-limit")
        self.assertEqual(status, 503)
        # fault budget exhausted -> second read succeeds
        status, _, _ = self._get("/source/rule/qty-limit")
        self.assertEqual(status, 200)

        status, _, body = self._get("/api/requests")
        records = json.loads(body)
        fault_records = [r for r in records
                         if r["path"] == "/source/rule/qty-limit"]
        self.assertIn(503, [r["status"] for r in fault_records])
        # raw body is preserved verbatim
        raw = next(r for r in fault_records if r["status"] == 503)
        self.assertIn("injected failure", raw["body"])

    def test_web_page_and_assets_served(self):
        for path in ("/", "/app.js", "/styles.css"):
            status, headers, body = self._get(path)
            self.assertEqual(status, 200, path)
            self.assertIn("charset=utf-8", headers.get("Content-Type", ""))
        self.assertIn("产品目录采集控制台",
                      self._get("/")[2])

    def test_conflict_scene_fails_closure_with_reason(self):
        # schedule dataset/sales to change mid-traversal
        self._request("POST", "/api/schedule",
                      {"type": "dataset", "id": "sales", "on_get": 3,
                       "patch": {"name": "并发修订"}})
        status, _, body = self._request("POST", "/api/tasks",
                                        {"catalog_id": "main",
                                         "strategy": "pin", "async": False})
        rec = json.loads(body)
        self.assertEqual(rec["status"], "failed")
        self.assertEqual(rec["error"]["kind"], "conflict")
        # and the task can be resumed after clearing the schedule
        self._request("POST", "/api/schedule", {"clear": True})
        status, _, body = self._request(
            "POST", f"/api/tasks/{rec['task_id']}/resume")
        self.assertEqual(status, 202)
        # wait until the resume actually starts (resume_count bumps) and
        # then until the new attempt reaches a terminal state
        rec2 = rec
        for _ in range(50):
            _, _, body = self._get(f"/api/tasks/{rec['task_id']}")
            rec2 = json.loads(body)
            if rec2["resume_count"] >= 1 and rec2["status"] == "running":
                break
            threading.Event().wait(0.1)
        for _ in range(50):
            _, _, body = self._get(f"/api/tasks/{rec['task_id']}")
            rec2 = json.loads(body)
            if rec2["status"] in ("closed", "failed"):
                break
            threading.Event().wait(0.1)
        self.assertEqual(rec2["status"], "closed")

    def _request(self, method, path, payload=None):
        data = json.dumps(payload).encode() if payload is not None else None
        req = urllib.request.Request(
            self.base + path, data=data, method=method,
            headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=5) as r:
                return r.status, dict(r.headers), r.read().decode()
        except urllib.error.HTTPError as exc:
            return exc.code, dict(exc.headers), exc.read().decode()


if __name__ == "__main__":
    unittest.main(verbosity=2)
