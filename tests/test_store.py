"""Tests for version store persistence and the publish epoch fence."""

import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from catalog_app.seed import fresh_state
from catalog_app.store import FenceError, VersionStore


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.state = fresh_state()
        self.store = VersionStore(self.tmp, self.state)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _closed_task(self, catalog="main", strategy="pin"):
        rec = self.store.create_task(catalog, strategy)
        return self.store.run_task(rec["task_id"])

    def test_task_seq_is_monotonic(self):
        t1 = self.store.create_task("main", "pin")
        t2 = self.store.create_task("main", "recheck")
        self.assertEqual(t2["seq"], t1["seq"] + 1)

    def test_closed_task_publishes_immutable_version(self):
        rec = self._closed_task()
        doc = self.store.publish(rec["task_id"])
        self.assertTrue(doc["version"].startswith("v"))
        # immutable file on disk
        path = Path(self.tmp) / "versions" / f"{doc['version']}.json"
        on_disk = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(on_disk["objects"], doc["objects"])
        latest = self.store.get_version("latest")
        self.assertEqual(latest["version"], doc["version"])

    def test_failed_task_cannot_publish(self):
        rec = self.store.create_task("with-ghost", "pin")
        rec = self.store.run_task(rec["task_id"])
        self.assertEqual(rec["status"], "failed")
        with self.assertRaises(FenceError):
            self.store.publish(rec["task_id"])

    def test_late_old_task_cannot_roll_back_published_version(self):
        # task 1 closes and publishes first
        t1 = self._closed_task()
        v1 = self.store.publish(t1["task_id"])
        self.assertEqual(self.store.index["epoch"], t1["seq"])

        # task 2 also closes against a shared dependency (same source graph),
        # but source moved in between; even if it closes, publishing is fine
        self.state.put("field", "amount",
                       {"name": "amount", "dtype": "decimal",
                        "label": "销售额（口径修订）"})
        t2 = self._closed_task(strategy="recheck")
        v2 = self.store.publish(t2["task_id"])
        self.assertNotEqual(v1["version"], v2["version"])
        self.assertEqual(self.store.index["latest_version"], v2["version"])

        # the late task 1 tries to publish again (replayed/duplicate call)
        with self.assertRaises(FenceError) as cm:
            self.store.publish(t1["task_id"])
        self.assertIn("围栏", str(cm.exception))
        # latest pointer never moved back
        self.assertEqual(self.store.get_version("latest")["version"],
                         v2["version"])

    def test_gap_in_seq_does_not_block_newer_publish(self):
        # task 1 fails (no publish); task 2 must still be allowed to publish
        bad = self.store.create_task("with-ghost", "pin")
        bad = self.store.run_task(bad["task_id"])
        self.assertEqual(bad["status"], "failed")
        good = self._closed_task()
        doc = self.store.publish(good["task_id"])  # seq=2 > epoch=0
        self.assertEqual(doc["seq"], good["seq"])

    def test_failed_task_resumes_and_then_publishes(self):
        self.state.inject_fault(["dataset/sales"], status=500, times=99)
        rec = self.store.create_task("main", "pin")
        rec = self.store.run_task(rec["task_id"])
        self.assertEqual(rec["status"], "failed")
        self.assertTrue(rec["progress"]["visited"])
        # request failure must not have removed any pinned reference
        pins_file = Path(self.tmp) / "tasks" / f"{rec['task_id']}.json"
        saved = json.loads(pins_file.read_text(encoding="utf-8"))
        self.assertGreater(len(saved["progress"]["visited_pins"]), 0)

        self.state.clear_faults()
        rec2 = self.store.run_task(rec["task_id"], resume=True)
        self.assertEqual(rec2["status"], "closed")
        self.assertEqual(rec2["resume_count"], 1)
        doc = self.store.publish(rec2["task_id"])
        self.assertEqual(doc["catalog_id"], "main")

    def test_atomic_writes_leave_no_tmp_files(self):
        self._closed_task()
        leftovers = list(Path(self.tmp).rglob("*.tmp"))
        self.assertEqual(leftovers, [])

    def test_reason_for_non_closure_is_persisted(self):
        self.state.delete("field", "region")
        rec = self.store.create_task("main", "pin")
        rec = self.store.run_task(rec["task_id"])
        self.assertEqual(rec["error"]["kind"], "missing")
        self.assertIn("field/region", rec["error"]["reason"])
        # reload from disk
        again = self.store.get_task(rec["task_id"])
        self.assertEqual(again["error"]["kind"], "missing")

    def test_resume_survives_store_rebuild_from_disk(self):
        # fail a task, then build a brand-new VersionStore over the same
        # directory (simulating a process restart with empty memory)
        self.state.inject_fault(["dataset/inventory"], status=500,
                                times=99, persistent=True)
        rec = self.store.create_task("main", "pin")
        rec = self.store.run_task(rec["task_id"])
        self.assertEqual(rec["status"], "failed")
        self.assertGreater(len(rec["progress"]["visited_pins"]), 0)

        reborn = VersionStore(self.tmp, self.state)
        self.state.clear_faults()
        rec2 = reborn.run_task(rec["task_id"], resume=True)
        self.assertEqual(rec2["status"], "closed")
        self.assertEqual(len(rec2["snapshot"]["objects"]), 12)
        # conditional reads dominated the resumed attempt (cache reuse)
        self.assertGreater(rec2["cost"]["not_modified"], 0)
        doc = reborn.publish(rec2["task_id"])
        self.assertTrue(doc["version"].startswith("v"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
