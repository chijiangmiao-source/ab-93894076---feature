"""Pending (half-wrap) observations: retention, auto-settlement, conflicts.

Covers
* registering an equidistant frame as a pending observation that does not
  move the window and writes no ordinary receipt;
* identical observation retransmission returning the current state while any
  changed association is rejected;
* an ordinary frame advancing the highest seq settling every pending
  observation in the same SQLite transaction, each exactly once, with
  convergence and cross-link isolation;
* a reserved original receipt id being refused at the ordinary entry;
* restart recovery and concurrent anchoring deriving one final position;
* ordinary frame reception regressing none of the original verdicts.
"""

import os
import tempfile
import threading
import unittest

from app.db import Database, NotAnchorable, ReceiptConflict
from app import window as win

MOD = 1 << 32
HALF = 1 << 31


class TempDB(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Database(os.path.join(self.tmp.name, "test.db"))

    def tearDown(self):
        self.db.close()
        self.tmp.cleanup()

    def make_link(self, name="L"):
        return self.db.create_link(name)["id"]

    def submit(self, link, counter, receipt, payload=None):
        return self.db.submit_frame(
            link, counter, receipt,
            payload if payload is not None else {"v": 1})

    def anchor(self, link, counter, obs_id, receipt, payload=None):
        return self.db.anchor_observation(
            link, counter, obs_id, receipt,
            payload if payload is not None else {"v": 1})

    def receipt_rows(self, receipt_id):
        return self.db.conn.execute(
            "SELECT status, extended FROM receipts WHERE receipt_id = ?",
            (receipt_id,),
        ).fetchall()


class TestAnchorRetention(TempDB):
    def test_only_equidistant_frame_is_anchorable(self):
        lid = self.make_link()
        with self.assertRaises(NotAnchorable):
            self.anchor(lid, HALF, "o1", "r1")          # no frame yet
        self.submit(lid, 0, "base")
        with self.assertRaises(NotAnchorable):
            self.anchor(lid, HALF - 1, "o1", "r1")      # not a tie
        with self.assertRaises(NotAnchorable):
            self.anchor(lid, HALF + 1, "o1", "r1")

    def test_anchor_does_not_move_window_or_write_receipt(self):
        lid = self.make_link()
        self.submit(lid, 0, "base")
        res = self.anchor(lid, HALF, "obs-1", "orig-1")
        self.assertEqual(res["status"], "pending")
        self.assertIsNone(res["verdict"])
        self.assertIsNone(res["extended"])
        self.assertFalse(res["retransmit"])
        # window untouched
        link = self.db.get_link(lid)
        self.assertEqual(link["highest"], 0)
        self.assertEqual(link["bitmap"], 1)
        # no ordinary ledger row for the carried receipt id
        self.assertEqual(self.receipt_rows("orig-1"), [])
        # observation is listed as pending
        obs = link["observations"]
        self.assertEqual(len(obs), 1)
        self.assertEqual(obs[0]["observation_id"], "obs-1")
        self.assertEqual(obs[0]["receipt_id"], "orig-1")
        self.assertEqual(obs[0]["status"], "pending")

    def test_identical_observation_retransmit_returns_current_state(self):
        lid = self.make_link()
        self.submit(lid, 0, "base")
        first = self.anchor(lid, HALF, "obs-1", "orig-1")
        again = self.anchor(lid, HALF, "obs-1", "orig-1")
        self.assertTrue(again["retransmit"])
        self.assertEqual(again["status"], "pending")
        # still exactly one observation row, no ledger side effects
        self.assertEqual(len(self.db.get_link(lid)["observations"]), 1)
        self.assertEqual(self.receipt_rows("orig-1"), [])
        self.assertEqual(first["observation_id"], again["observation_id"])

    def test_observation_id_reuse_with_changed_data_rejected(self):
        lid = self.make_link()
        self.submit(lid, 0, "base")
        self.anchor(lid, HALF, "obs-1", "orig-1", {"v": 1})
        with self.assertRaises(ReceiptConflict):
            self.anchor(lid, HALF, "obs-1", "orig-1", {"v": 2})  # payload
        with self.assertRaises(ReceiptConflict):
            self.anchor(lid, HALF, "obs-1", "orig-2")             # receipt
        l2 = self.make_link("other")
        self.submit(l2, 0, "base2")
        with self.assertRaises(ReceiptConflict):
            self.anchor(l2, HALF, "obs-1", "orig-1")              # link
        # after settlement the observation id is immutable too: re-anchoring it
        # against a different (still-tie) counter is a conflict
        self.submit(lid, 1, "advance")
        with self.assertRaises(ReceiptConflict):
            self.anchor(lid, 0, "obs-1", "orig-1")

    def test_carried_receipt_id_reused_by_another_observation_rejected(self):
        lid = self.make_link()
        self.submit(lid, 0, "base")
        self.anchor(lid, HALF, "obs-1", "orig-1")
        with self.assertRaises(ReceiptConflict):
            self.anchor(lid, HALF, "obs-2", "orig-1")

    def test_unknown_link(self):
        with self.assertRaises(LookupError):
            self.db.anchor_observation("nope", HALF, "o", "r", {})


class TestReservedReceipt(TempDB):
    def test_reserved_receipt_id_refused_at_ordinary_entry(self):
        lid = self.make_link()
        self.submit(lid, 0, "base")
        self.anchor(lid, HALF, "obs-1", "orig-1")
        # even an identical frame may not bypass the observation
        with self.assertRaises(ReceiptConflict):
            self.submit(lid, HALF, "orig-1")
        # window unchanged by the refused ordinary submission
        link = self.db.get_link(lid)
        self.assertEqual(link["highest"], 0)
        self.assertEqual(self.receipt_rows("orig-1"), [])

    def test_anchor_cannot_reserve_an_already_ledgered_receipt(self):
        lid = self.make_link()
        self.submit(lid, 0, "base")
        self.submit(lid, 7, "used-rx")             # highest -> 7
        tie7 = (7 + HALF) & (MOD - 1)
        # a receipt id already used by an ordinary frame cannot be reserved
        with self.assertRaises(ReceiptConflict):
            self.anchor(lid, tie7, "obs-0", "used-rx")
        # a receipt id produced by settling an earlier observation lands on the
        # ledger and cannot be reserved by a new observation either
        self.anchor(lid, tie7, "obs-1", "held-rx")
        self.submit(lid, 8, "advance")             # settles obs-1
        new_tie = (HALF + 7) - HALF                # counter 7: tie from highest
        self.anchor(lid, new_tie, "obs-2", "held-2")
        with self.assertRaises(ReceiptConflict):
            self.anchor(lid, new_tie, "obs-3", "held-rx")

    def test_settled_receipt_replays_at_ordinary_entry(self):
        lid = self.make_link()
        self.submit(lid, 0, "base")
        self.anchor(lid, HALF, "obs-1", "orig-1")
        settling = self.submit(lid, 1, "advance")
        self.assertEqual(settling["highest"], HALF)
        # once settled, the original receipt id is an ordinary stable receipt
        replay = self.submit(lid, HALF, "orig-1")
        self.assertTrue(replay["retransmit"])
        self.assertEqual(replay["status"], "accepted")
        self.assertEqual(replay["extended"], HALF)


class TestAutoSettlement(TempDB):
    def test_advancing_frame_settles_pending_in_same_transaction(self):
        lid = self.make_link()
        self.submit(lid, 0, "base")
        self.anchor(lid, HALF, "obs-1", "orig-1")
        verdict = self.submit(lid, 1, "advance")
        # triggering ordinary frame verdict plus the settled observation
        self.assertEqual(verdict["status"], "accepted")
        self.assertEqual(len(verdict["settled"]), 1)
        settled = verdict["settled"][0]
        self.assertEqual(settled["observation_id"], "obs-1")
        self.assertEqual(settled["verdict"], "accepted")
        self.assertEqual(settled["extended"], HALF)
        # window jumped to the observation's final extended seq
        self.assertEqual(verdict["highest"], HALF)
        # ledger row written exactly once
        rows = self.receipt_rows("orig-1")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["status"], "accepted")
        self.assertEqual(rows[0]["extended"], HALF)
        # observation now reports settled with its final extended seq
        obs = self.db.get_link(lid)["observations"]
        self.assertEqual(obs[0]["status"], "settled")
        self.assertEqual(obs[0]["verdict"], "accepted")
        self.assertEqual(obs[0]["extended"], HALF)

    def test_settled_exactly_once_on_repeated_trigger(self):
        lid = self.make_link()
        self.submit(lid, 0, "base")
        self.anchor(lid, HALF, "obs-1", "orig-1")
        self.submit(lid, 1, "advance")
        # more advancing frames must not re-settle or duplicate the ledger row
        v2 = self.submit(lid, 2, "advance-2")
        self.assertEqual(v2["settled"], [])
        self.assertEqual(len(self.receipt_rows("orig-1")), 1)
        # retransmitting the observation returns its settled state
        again = self.anchor(lid, HALF, "obs-1", "orig-1")
        self.assertTrue(again["retransmit"])
        self.assertEqual(again["status"], "settled")
        self.assertEqual(again["verdict"], "accepted")
        self.assertEqual(again["extended"], HALF)
        self.assertEqual(len(self.receipt_rows("orig-1")), 1)

    def test_multiple_observations_settle_in_stable_id_order(self):
        lid = self.make_link()
        self.submit(lid, 0, "base")
        # two observations of the same ambiguous counter, inserted out of
        # lexical order to prove ordering is by observation id
        self.anchor(lid, HALF, "obs-b", "orig-b")
        self.anchor(lid, HALF, "obs-a", "orig-a")
        verdict = self.submit(lid, 1, "advance")
        ids = [s["observation_id"] for s in verdict["settled"]]
        self.assertEqual(ids, ["obs-a", "obs-b"])
        # first occupies the bit, second is an ordinary duplicate verdict
        verdicts = {s["observation_id"]: s["verdict"] for s in verdict["settled"]}
        self.assertEqual(verdicts["obs-a"], "accepted")
        self.assertEqual(verdicts["obs-b"], "duplicate")
        self.assertEqual(verdict["highest"], HALF)
        # both ledger rows present, each exactly once
        for rid, expected in (("orig-a", "accepted"), ("orig-b", "duplicate")):
            rows = self.receipt_rows(rid)
            self.assertEqual(len(rows), 1, rid)
            self.assertEqual(rows[0]["status"], expected, rid)

    def test_non_advancing_frame_does_not_settle(self):
        lid = self.make_link()
        self.submit(lid, 0, "base")
        self.anchor(lid, HALF, "obs-1", "orig-1")
        # a genuinely duplicate counter 0 (new receipt id): highest unchanged,
        # so the observation stays pending
        v = self.submit(lid, 0, "base-dup")
        self.assertEqual(v["status"], "duplicate")
        self.assertEqual(v["settled"], [])
        obs = self.db.get_link(lid)["observations"]
        self.assertEqual(obs[0]["status"], "pending")

    def test_new_ambiguity_after_settlement_stays_pending(self):
        lid = self.make_link()
        self.submit(lid, 0, "base")
        self.anchor(lid, HALF, "obs-a", "orig-a")
        self.submit(lid, 1, "advance")            # highest -> HALF
        # counter 0 is again exactly half a wrap away from highest HALF
        res = self.anchor(lid, 0, "obs-c", "orig-c")
        self.assertEqual(res["status"], "pending")
        link = self.db.get_link(lid)
        self.assertEqual(link["highest"], HALF)
        pending = [o for o in link["observations"] if o["status"] == "pending"]
        self.assertEqual([o["observation_id"] for o in pending], ["obs-c"])
        # one more ordinary advancement resolves it into epoch 1 (extended MOD)
        verdict = self.submit(lid, HALF + 1, "advance-2")
        self.assertEqual(verdict["highest"], MOD)
        rows = self.receipt_rows("orig-c")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["status"], "accepted")
        self.assertEqual(rows[0]["extended"], MOD)

    def test_other_link_advancement_leaves_pending_untouched(self):
        l1 = self.make_link("one")
        l2 = self.make_link("two")
        self.submit(l1, 0, "l1-base")
        self.submit(l2, 0, "l2-base")
        self.anchor(l2, HALF, "obs-l2", "orig-l2")
        self.submit(l1, 1, "l1-advance")
        obs = self.db.get_link(l2)["observations"]
        self.assertEqual(obs[0]["status"], "pending")
        self.assertEqual(self.receipt_rows("orig-l2"), [])

    def test_settlement_across_wrap_boundary(self):
        lid = self.make_link()
        self.submit(lid, MOD - 5, "base")
        tie_counter = (MOD - 5 + HALF) & (MOD - 1)
        self.anchor(lid, tie_counter, "obs-1", "orig-1")
        verdict = self.submit(lid, MOD - 4, "advance")
        # the observation resolves into epoch 1, half a wrap above the boundary
        expected = MOD + tie_counter             # epoch-1 candidate
        settled = verdict["settled"][0]
        self.assertEqual(settled["verdict"], "accepted")
        self.assertEqual(settled["extended"], expected)
        self.assertEqual(verdict["highest"], expected)
        # the counter now resolves to the settled epoch-1 position and is an
        # ordinary duplicate when resent with a fresh receipt id
        late = self.submit(lid, tie_counter, "late-replay")
        self.assertEqual(late["status"], "duplicate")

    def test_observation_can_only_settle_as_accepted_or_duplicate(self):
        # From a fixed highest a single ordinary frame cannot leap past the
        # half-wrap (counters beyond HALF resolve to the prior epoch and are
        # expired), so once advancement breaks the tie the pending observation
        # always lies ahead of the window: accepted first, duplicate for any
        # further observation of the same ambiguous counter.
        lid = self.make_link()
        self.submit(lid, 0, "base")
        self.submit(lid, HALF + 5, "beyond")       # resolves behind -> expired
        link = self.db.get_link(lid)
        self.assertEqual(link["highest"], 0)      # no advancement, no trigger
        self.anchor(lid, HALF, "obs-1", "orig-1")
        self.anchor(lid, HALF, "obs-2", "orig-2")
        verdict = self.submit(lid, 1, "advance")
        results = {s["observation_id"]: s["verdict"]
                   for s in verdict["settled"]}
        self.assertEqual(results, {"obs-1": "accepted", "obs-2": "duplicate"})
        self.assertEqual(verdict["highest"], HALF)


class TestRestartRecovery(TempDB):
    def test_pending_and_settled_survive_reopen_to_same_position(self):
        lid = self.make_link()
        self.submit(lid, 0, "base")
        self.anchor(lid, HALF, "obs-a", "orig-a")
        self.anchor(lid, HALF, "obs-b", "orig-b")
        self.submit(lid, 1, "advance")            # settles both
        path = self.db.path
        self.db.close()

        db2 = Database(path)
        try:
            link = db2.get_link(lid)
            self.assertEqual(link["highest"], HALF)
            settled = {o["observation_id"]: o for o in link["observations"]}
            self.assertEqual(settled["obs-a"]["extended"], HALF)
            self.assertEqual(settled["obs-b"]["verdict"], "duplicate")
            # the settled receipt still replays its first verdict
            replay = db2.submit_frame(lid, HALF, "orig-a", {"v": 1})
            self.assertEqual(replay["status"], "accepted")
            self.assertTrue(replay["retransmit"])
            self.assertEqual(replay["highest"], HALF)
        finally:
            db2.close()

    def test_pending_survives_reopen_and_settles_after_restart(self):
        lid = self.make_link()
        self.submit(lid, 0, "base")
        self.anchor(lid, HALF, "obs-1", "orig-1")
        path = self.db.path
        self.db.close()

        db2 = Database(path)
        try:
            obs = db2.get_link(lid)["observations"]
            self.assertEqual(obs[0]["status"], "pending")
            verdict = db2.submit_frame(lid, 1, "advance", {"v": 1})
            self.assertEqual(verdict["highest"], HALF)
            self.assertEqual(verdict["settled"][0]["observation_id"], "obs-1")
        finally:
            db2.close()


class TestConcurrentAnchor(unittest.TestCase):
    def test_concurrent_anchor_and_frames_converge_to_one_position(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        db = Database(os.path.join(tmp.name, "c.db"))
        self.addCleanup(db.close)
        lid = db.create_link("c")["id"]
        db.submit_frame(lid, 0, "base", {"v": 1})
        # the observation is registered first; concurrent traffic then mixes
        # identical observation retransmissions with the advancing frame
        db.anchor_observation(lid, HALF, "obs-1", "orig-1", {"v": 1})

        barrier = threading.Barrier(8)

        def reanchor():
            barrier.wait()
            return db.anchor_observation(
                lid, HALF, "obs-1", "orig-1", {"v": 1})

        def advance():
            barrier.wait()
            return db.submit_frame(lid, 1, "advance", {"v": 1})

        threads = [threading.Thread(target=reanchor) for _ in range(7)]
        threads.append(threading.Thread(target=advance))
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Whatever the interleaving, the final position is unique: the
        # observation settles exactly once at extended 2**31.
        link = db.get_link(lid)
        self.assertEqual(link["highest"], HALF)
        rows = db.conn.execute(
            "SELECT COUNT(*) AS n FROM receipts WHERE receipt_id = 'orig-1'"
        ).fetchone()
        self.assertEqual(rows["n"], 1)
        settled = db.conn.execute(
            "SELECT status, verdict, extended FROM observations "
            "WHERE observation_id = 'obs-1'"
        ).fetchone()
        self.assertEqual(settled["status"], "settled")
        self.assertEqual(settled["verdict"], "accepted")
        self.assertEqual(settled["extended"], HALF)


class TestOrdinaryReceptionRegression(TempDB):
    def test_plain_flow_without_observations_unchanged(self):
        lid = self.make_link()
        for c, r in zip((MOD - 1, 0, 1), ("a", "b", "c")):
            v = self.submit(lid, c, r)
            self.assertEqual(v["status"], "accepted")
        self.assertEqual(self.submit(lid, MOD - 1, "a")["status"], "accepted")
        self.assertTrue(self.submit(lid, MOD - 1, "a")["retransmit"])
        self.assertEqual(self.submit(lid, 0, "d")["status"], "duplicate")
        self.assertEqual(self.submit(lid, MOD + 70 & (MOD - 1), "e")["status"],
                         "accepted")
        self.assertEqual(self.submit(lid, MOD - 2, "late")["status"], "expired")
        with self.assertRaises(ReceiptConflict):
            self.submit(lid, 999, "a")           # id reuse, changed counter
        # verdict payload stays backward compatible
        v = self.submit(lid, 1, "c")
        self.assertIn("settled", v)
        self.assertEqual(v["settled"], [])

    def test_half_wrap_without_anchoring_is_still_rejected(self):
        lid = self.make_link()
        self.submit(lid, 0, "base")
        v = self.submit(lid, HALF, "tie")
        self.assertEqual(v["status"], "rejected")
        self.assertEqual(v["highest"], 0)
        self.assertEqual(v["settled"], [])


if __name__ == "__main__":
    unittest.main()
