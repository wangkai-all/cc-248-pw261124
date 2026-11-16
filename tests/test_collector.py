"""Unit tests for collector closure semantics."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from catalog_app.client import LocalClient
from catalog_app.collector import Collector, ClosureError, outgoing_refs
from catalog_app.seed import fresh_state
from catalog_app.source import SourceState


def make(seed=True):
    state = fresh_state() if seed else SourceState()
    return state


class PinStrategyTests(unittest.TestCase):
    def test_clean_catalog_closes_with_pinned_revs(self):
        st = make()
        c = Collector(LocalClient(st, "t"), "main", "pin")
        snap = c.run()
        self.assertTrue(snap["closed"])
        keys = set(snap["objects"])
        # full closure: catalog, 3 products, 2 datasets, 4 fields, 2 rules
        self.assertEqual(len(keys), 12)
        self.assertIn("dataset/sales", keys)
        self.assertIn("rule/region-check", keys)
        # every object carries an immutable rev + etag pin
        for obj in snap["objects"].values():
            self.assertGreaterEqual(obj["rev"], 1)
            self.assertEqual(len(obj["etag"]), 32)

    def test_closure_pass_uses_conditional_reads(self):
        st = make()
        c = Collector(LocalClient(st, "t"), "main", "pin")
        c.run()
        cost = c.client.cost
        # 12 first visits + 12 closure re-validations + shared revisit(s)
        self.assertGreater(cost.conditional, 12)
        self.assertGreater(cost.not_modified, 0)
        self.assertEqual(cost.precondition_failed, 0)

    def test_shared_dependency_visited_once_but_revalidated(self):
        st = make()
        c = Collector(LocalClient(st, "t"), "main", "pin")
        snap = c.run()
        # dataset/sales shared by p100 and p101: only one snapshot entry
        self.assertEqual(
            sum(1 for k in snap["objects"] if k == "dataset/sales"), 1)
        # edges record both references
        sales_edges = [e for e in snap["edges"] if e[1] == "dataset/sales"]
        self.assertEqual(len(sales_edges), 2)

    def test_mid_traversal_update_causes_conflict_not_mixed_snapshot(self):
        st = make()
        old_rev = st.objects["dataset/sales"].rev
        # fire on the closure-pass GET (first 2 GETs are DFS visit + the
        # shared revisit; update lands on the 3rd)
        st.schedule_update("dataset", "sales", on_get=3,
                           patch={"name": "被并发改写的数据集"})
        c = Collector(LocalClient(st, "t"), "main", "pin")
        with self.assertRaises(ClosureError) as cm:
            c.run()
        self.assertEqual(cm.exception.kind, "conflict")
        self.assertIn("条件冲突", cm.exception.reason)
        self.assertEqual(cm.exception.details["pinned_rev"], old_rev)
        self.assertEqual(cm.exception.details["current_rev"], old_rev + 1)
        # source did advance; no snapshot was published (collector only)
        self.assertEqual(st.objects["dataset/sales"].rev, old_rev + 1)
        # partial pins retained for resume
        self.assertIn("dataset/sales", c.pinned)

    def test_deleted_referenced_object_cannot_close(self):
        st = make()
        st.delete("field", "region")
        c = Collector(LocalClient(st, "t"), "main", "pin")
        with self.assertRaises(ClosureError) as cm:
            c.run()
        self.assertEqual(cm.exception.kind, "missing")
        self.assertTrue(cm.exception.details["tombstone"])

    def test_never_existed_reference_cannot_close(self):
        st = make()
        c = Collector(LocalClient(st, "t"), "with-ghost", "pin")
        with self.assertRaises(ClosureError) as cm:
            c.run()
        self.assertEqual(cm.exception.kind, "missing")
        self.assertIn("dataset/missing", cm.exception.reason)
        self.assertFalse(cm.exception.details["tombstone"])

    def test_circular_reference_detected_with_path(self):
        st = make()
        # make the cyclic dataset reachable from the main catalog
        st.put("product", "p100",
               {"name": "销售看板", "dataset": "dataset/cyclic"})
        c = Collector(LocalClient(st, "t"), "main", "pin")
        with self.assertRaises(ClosureError) as cm:
            c.run()
        self.assertEqual(cm.exception.kind, "cycle")
        cycle = cm.exception.details["cycle"]
        self.assertEqual(cycle[0], cycle[-1])
        self.assertIn("dataset/cyclic", cycle)

    def test_transient_failure_keeps_pins_and_resume_closes(self):
        st = make()
        st.inject_fault(["dataset/sales"], status=500, times=10)
        c = Collector(LocalClient(st, "t"), "main", "pin")
        with self.assertRaises(ClosureError) as cm:
            c.run()
        self.assertEqual(cm.exception.kind, "source")
        pinned_before = set(c.pinned)
        self.assertTrue(pinned_before)  # references not deleted by failure
        # source recovers; resume reuses pins via conditional reads
        st.clear_faults()
        snap = c.run(resume=True)
        self.assertTrue(snap["closed"])
        self.assertTrue(pinned_before.issubset(set(snap["objects"])))
        self.assertGreaterEqual(c.client.cost.retries, 1)

    def test_retry_budget_is_bounded(self):
        st = make()
        st.inject_fault(["catalog/main"], status=503, times=99,
                        persistent=True)
        c = Collector(LocalClient(st, "t"), "main", "pin")
        with self.assertRaises(ClosureError):
            c.run()
        # default retries=2 -> at most 3 attempts on the root
        self.assertLessEqual(c.client.cost.requests, 3)


class RecheckStrategyTests(unittest.TestCase):
    def test_stable_graph_converges(self):
        st = make()
        c = Collector(LocalClient(st, "t"), "main", "recheck")
        snap = c.run()
        self.assertTrue(snap["closed"])
        rounds = c.attempts[-1].rounds
        self.assertEqual(rounds[-1]["changed"], [])
        # round 1 fills pins, round 2 all-304 => stable
        self.assertLessEqual(len(rounds), 3)
        self.assertGreater(c.client.cost.not_modified, 0)

    def test_changing_graph_reports_unstable_within_bounded_rounds(self):
        st = make()

        # a source where dataset/sales is revised by another writer on
        # every single read: convergence must be impossible.
        real_get = st.get

        def churning_get(otype, oid, if_none_match=None, if_match=None):
            if (otype, oid) == ("dataset", "sales"):
                # another writer commits a new revision before every read,
                # so conditional reads always see 200, never 304
                obj = st.objects["dataset/sales"]
                obj.rev += 1
                obj.etag = __import__("uuid").uuid4().hex
            return real_get(otype, oid, if_none_match=if_none_match,
                            if_match=if_match)

        st.get = churning_get
        c = Collector(LocalClient(st, "t"), "main", "recheck",
                      max_rounds=4)
        with self.assertRaises(ClosureError) as cm:
            c.run()
        self.assertEqual(cm.exception.kind, "unstable")
        self.assertEqual(len(c.attempts[-1].rounds), 4)
        self.assertIn("不稳定", cm.exception.reason)

    def test_one_round_of_drift_then_stable(self):
        st = make()
        # changes once on round 2's first conditional read, then settles
        st.schedule_update("dataset", "sales", on_get=2,
                           patch={"name": "销售数据集-v2"})
        c = Collector(LocalClient(st, "t"), "main", "recheck")
        snap = c.run()
        self.assertTrue(snap["closed"])
        rounds = c.attempts[-1].rounds
        self.assertGreaterEqual(len(rounds), 3)
        self.assertEqual(snap["objects"]["dataset/sales"]["body"]["name"],
                         "销售数据集-v2")

    def test_recheck_detects_cycle_and_missing(self):
        st = make()
        st.delete("field", "qty")
        c = Collector(LocalClient(st, "t"), "main", "recheck")
        with self.assertRaises(ClosureError) as cm:
            c.run()
        self.assertEqual(cm.exception.kind, "missing")


class ConditionalReadTests(unittest.TestCase):
    def test_if_none_match_returns_304(self):
        st = make()
        etag = st.objects["dataset/sales"].etag
        client = LocalClient(st, "t")
        resp = client.get("dataset", "sales", if_none_match=etag)
        self.assertEqual(resp.status, 304)
        self.assertEqual(client.cost.not_modified, 1)

    def test_if_match_conflict_returns_412(self):
        st = make()
        client = LocalClient(st, "t")
        resp = client.get("dataset", "sales", if_match="stale-etag")
        self.assertEqual(resp.status, 412)
        self.assertEqual(client.cost.precondition_failed, 1)

    def test_conditional_write_rejects_stale_if_match(self):
        st = make()
        status, _, _ = st.put("field", "region",
                              {"name": "x"}, if_match="stale")
        self.assertEqual(status, 412)

    def test_delete_is_tombstone_then_404_with_marker(self):
        st = make()
        status, _, _ = st.delete("rule", "qty-limit")
        self.assertEqual(status, 204)
        status, body, headers = st.get("rule", "qty-limit")
        self.assertEqual(status, 404)
        self.assertEqual(headers["x-tombstone"], "1")


class GraphModelTests(unittest.TestCase):
    def test_outgoing_refs_cover_all_edge_kinds(self):
        self.assertEqual(
            outgoing_refs("catalog", {"products": ["product/a"]}),
            ["product/a"])
        self.assertEqual(
            outgoing_refs("product", {"dataset": "dataset/d"}),
            ["dataset/d"])
        refs = outgoing_refs("dataset", {
            "fields": ["field/f"], "rules": ["rule/r"],
            "depends_on": ["dataset/d2"]})
        self.assertEqual(set(refs), {"field/f", "rule/r", "dataset/d2"})


if __name__ == "__main__":
    unittest.main(verbosity=2)
