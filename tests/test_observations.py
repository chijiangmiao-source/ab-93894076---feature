"""Persistence tests for half-wrap pending observations.

Covers the acceptance scenarios:

* half-wrap retention -- a tied frame is parked without moving the window
  or writing a receipt verdict, and survives restarts;
* automatic settlement -- an advancing ordinary frame re-judges all pending
  observations in one transaction, in stable observation-id order, settling
  each determinate one exactly once into a normal verdict + receipt row +
  window update, converging further when the window keeps moving;
* receipt-occupation conflicts -- the retained original receipt id is
  refused at the normal entry and every id reuse with changed data is a
  conflict, while identical retransmission replays the current state;
* ordinary-receive regression -- the normal frame flow is unchanged.
"""

import os
import tempfile
import threading
import unittest

from app.db import (
    Database,
    NotAtHalfSpan,
    ObservationConflict,
    ReceiptConflict,
)

MOD = 1 << 32
HALF = 1 << 31


class ObsDB(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Database(os.path.join(self.tmp.name, "obs.db"))

    def tearDown(self):
        self.db.close()
        self.tmp.cleanup()

    def make_link(self, name="L"):
        return self.db.create_link(name)["id"]

    def frame(self, link, counter, receipt, payload=None):
        return self.db.submit_frame(
            link, counter, receipt,
            payload if payload is not None else {"v": 1})

    def anchor(self, link, counter, obs_id, receipt, payload=None):
        return self.db.anchor_observation(
            link, counter, obs_id, receipt,
            payload if payload is not None else {"v": 1})

    def receipt_count(self, receipt_id):
        return self.db.conn.execute(
            "SELECT COUNT(*) AS n FROM receipts WHERE receipt_id = ?",
            (receipt_id,),
        ).fetchone()["n"]


class TestHalfSpanRetention(ObsDB):
    def test_tied_frame_parks_without_window_or_ledger_change(self):
        lid = self.make_link()
        self.frame(lid, 0, "r0")
        result = self.anchor(lid, HALF, "o1", "park-1")
        self.assertFalse(result["retransmit"])
        self.assertEqual(result["observation"]["status"], "pending")
        self.assertIsNone(result["observation"]["extended"])
        # Window and receipt ledger are untouched.
        link = self.db.get_link(lid)
        self.assertEqual(link["highest"], 0)
        self.assertEqual(link["bitmap"], 1)
        self.assertEqual(self.receipt_count("park-1"), 0)
        self.assertEqual(
            [o["observation_id"] for o in link["observations"]], ["o1"])

    def test_non_half_span_frame_cannot_be_parked(self):
        lid = self.make_link()
        self.frame(lid, 0, "r0")
        with self.assertRaises(NotAtHalfSpan):
            self.anchor(lid, 5, "o1", "park-1")
        # nothing was parked
        self.assertEqual(self.db.get_link(lid)["observations"], [])

    def test_cannot_park_before_any_frame(self):
        lid = self.make_link()
        with self.assertRaises(NotAtHalfSpan):
            self.anchor(lid, HALF, "o1", "park-1")

    def test_identical_observation_retransmit_replays_state(self):
        lid = self.make_link()
        self.frame(lid, 0, "r0")
        self.anchor(lid, HALF, "o1", "park-1")
        again = self.anchor(lid, HALF, "o1", "park-1")
        self.assertTrue(again["retransmit"])
        self.assertEqual(again["observation"]["status"], "pending")
        # exactly one observation row, no window motion
        n = self.db.conn.execute(
            "SELECT COUNT(*) AS n FROM observations").fetchone()["n"]
        self.assertEqual(n, 1)
        self.assertEqual(self.db.get_link(lid)["highest"], 0)

    def test_observation_id_changed_data_is_conflict(self):
        lid = self.make_link()
        self.frame(lid, 0, "r0")
        self.anchor(lid, HALF, "o1", "park-1")
        # change counter / original receipt / payload, one at a time
        with self.assertRaises(ObservationConflict):
            self.anchor(lid, HALF + 1, "o1", "park-1")
        with self.assertRaises(ObservationConflict):
            self.anchor(lid, HALF, "o1", "park-2")
        with self.assertRaises(ObservationConflict):
            self.anchor(lid, HALF, "o1", "park-1", {"v": 2})
        # reusing the observation id on another link is a conflict too
        other = self.make_link("other")
        self.frame(other, 0, "x0")
        with self.assertRaises(ObservationConflict):
            self.anchor(other, HALF, "o1", "whatever")

    def test_original_receipt_id_cannot_be_shared_or_pre_used(self):
        lid = self.make_link()
        self.frame(lid, 0, "r0")
        self.anchor(lid, HALF, "o1", "park-1")
        # a second observation cannot carry the same original receipt
        with self.assertRaises(ObservationConflict):
            self.anchor(lid, HALF, "o2", "park-1")
        # an id already settled through the normal entry cannot be parked
        with self.assertRaises(ReceiptConflict):
            self.anchor(lid, HALF, "o2", "r0")

    def test_retained_receipt_refused_at_normal_entry(self):
        lid = self.make_link()
        self.frame(lid, 0, "r0")
        self.anchor(lid, HALF, "o1", "park-1")
        # same link, any counter/payload -> refused
        with self.assertRaises(ReceiptConflict):
            self.frame(lid, HALF, "park-1")
        with self.assertRaises(ReceiptConflict):
            self.frame(lid, 123, "park-1", {"other": True})
        # globally reserved: refused on another link as well
        other = self.make_link("other")
        self.frame(other, 0, "x0")
        with self.assertRaises(ReceiptConflict):
            self.frame(other, 0, "park-1")
        # refusal never touches the window
        self.assertEqual(self.db.get_link(lid)["highest"], 0)

    def test_observation_remains_pending_while_highest_does_not_advance(self):
        # An ordinary frame that only fills an in-window bit (highest
        # unchanged) must not settle anything.
        lid = self.make_link()
        self.frame(lid, 10, "r10")
        self.anchor(lid, (10 + HALF) & (MOD - 1), "o1", "park-1")
        self.frame(lid, 9, "r9")          # older, inside the window
        self.frame(lid, 10, "r10")        # pure retransmission
        obs = self.db.list_observations(lid)
        self.assertEqual([o["status"] for o in obs], ["pending"])
        self.assertEqual(self.db.get_link(lid)["highest"], 10)


class TestAutomaticSettlement(ObsDB):
    def _park(self, lid, obs_id, receipt, counter=HALF, payload=None):
        return self.anchor(lid, counter, obs_id, receipt, payload)

    def test_advancing_frame_settles_in_same_transaction(self):
        lid = self.make_link()
        self.frame(lid, 0, "r0")
        self._park(lid, "o1", "park-1")
        # Frame HALF-2 breaks the tie; the parked frame settles 2 ahead and
        # the window converges all the way to HALF in the same transaction.
        verdict = self.frame(lid, HALF - 2, "adv")
        self.assertEqual(verdict["highest"], HALF)
        settled = verdict["settled_observations"]
        self.assertEqual([s["observation_id"] for s in settled], ["o1"])
        self.assertEqual(settled[0]["status"], "accepted")
        self.assertEqual(settled[0]["extended"], HALF)
        # exactly-once: one receipt-ledger row carrying the original id
        self.assertEqual(self.receipt_count("park-1"), 1)
        obs = self.db.list_observations(lid)[0]
        self.assertEqual(obs["status"], "accepted")
        self.assertEqual(obs["extended"], HALF)

    def test_settles_only_once_on_repeated_trigger(self):
        lid = self.make_link()
        self.frame(lid, 0, "r0")
        self._park(lid, "o1", "park-1")
        first = self.frame(lid, HALF - 2, "adv")
        self.assertIn("settled_observations", first)
        # identical retransmission of the advancing frame replays its verdict
        # and must not settle anything a second time
        replay = self.frame(lid, HALF - 2, "adv")
        self.assertTrue(replay["retransmit"])
        self.assertNotIn("settled_observations", replay)
        self.assertEqual(self.receipt_count("park-1"), 1)

    def test_stable_order_and_duplicate_verdict_for_same_counter(self):
        lid = self.make_link()
        self.frame(lid, 0, "r0")
        # Three observations of the same ambiguous counter; insert them in
        # id-reversed order so stable ordering is really exercised.
        self._park(lid, "obs-z", "park-z")
        self._park(lid, "obs-a", "park-a")
        self._park(lid, "obs-m", "park-m")
        verdict = self.frame(lid, HALF - 1, "adv")
        settled = verdict["settled_observations"]
        self.assertEqual(
            [s["observation_id"] for s in settled],
            ["obs-a", "obs-m", "obs-z"])
        # all land on the same extended seq: first accepted, rest duplicate
        self.assertEqual([s["status"] for s in settled],
                         ["accepted", "duplicate", "duplicate"])
        self.assertTrue(all(s["extended"] == HALF for s in settled))
        self.assertEqual(verdict["highest"], HALF)
        for rid in ("park-a", "park-m", "park-z"):
            self.assertEqual(self.receipt_count(rid), 1)

    def test_only_accepted_or_duplicate_reachable_never_expired(self):
        # Geometry note: a tied frame's two readings sit exactly HALF below
        # and above the highest at parking time.  The first advancing frame
        # can move at most to (upper reading - 1) before the tie breaks, so
        # the upper reading is always still ahead of the window when the
        # observation is re-judged: a half-span observation can only settle
        # as accepted (or duplicate when the same counter was parked more
        # than once), never expired.  Check both a one-step and a maximal
        # first advance.
        for i, step in enumerate((1, HALF - 1)):
            lid = self.make_link(f"L{i}")
            self.frame(lid, 0, f"r0-{i}")
            self._park(lid, f"o1-{i}", f"park-1-{i}")
            verdict = self.frame(lid, step, f"adv-{i}")
            settled = verdict["settled_observations"]
            self.assertEqual(len(settled), 1, step)
            self.assertIn(settled[0]["status"], ("accepted", "duplicate"))
            self.assertEqual(settled[0]["extended"], HALF)
            self.assertEqual(verdict["highest"], HALF)

    def test_chain_convergence_across_epoch_boundary(self):
        lid = self.make_link()
        self.frame(lid, 0, "r0")
        self._park(lid, "p1", "park-1")
        # settle p1 into epoch 0 at HALF
        v = self.frame(lid, HALF - 2, "adv1")
        self.assertEqual(v["highest"], HALF)
        # now park the new half-span frame around HALF: wire counter 0 ties
        # between 0 and MOD
        self._park(lid, "p2", "park-2", counter=0)
        v = self.frame(lid, HALF + 2, "adv2")   # breaks tie toward MOD
        ids = [s["observation_id"] for s in v["settled_observations"]]
        self.assertEqual(ids, ["p2"])
        p2 = v["settled_observations"][0]
        self.assertEqual(p2["status"], "accepted")
        self.assertEqual(p2["extended"], MOD)
        self.assertEqual(v["highest"], MOD)
        # p1 must not be re-settled
        self.assertEqual(self.receipt_count("park-1"), 1)
        self.assertEqual(self.receipt_count("park-2"), 1)

    def test_settled_observation_retransmit_replays_verdict(self):
        lid = self.make_link()
        self.frame(lid, 0, "r0")
        self._park(lid, "o1", "park-1")
        self.frame(lid, HALF - 2, "adv")
        again = self.anchor(lid, HALF, "o1", "park-1")
        self.assertTrue(again["retransmit"])
        self.assertEqual(again["observation"]["status"], "accepted")
        self.assertEqual(again["observation"]["extended"], HALF)

    def test_settled_receipt_stays_refused_at_normal_entry(self):
        lid = self.make_link()
        self.frame(lid, 0, "r0")
        self._park(lid, "o1", "park-1")
        self.frame(lid, HALF - 2, "adv")
        with self.assertRaises(ReceiptConflict):
            self.frame(lid, HALF, "park-1")

    def test_concurrent_advances_reach_single_final_position(self):
        lid = self.make_link()
        self.frame(lid, 0, "r0")
        # Park the ambiguous frame, then advance from many threads; only the
        # first disambiguating transaction settles the observation, but every
        # interleaving must end at the same highest / same single ledger row.
        self._park(lid, "o1", "park-1")
        errors = []

        def advance(counter, rid):
            try:
                self.frame(lid, counter, rid)
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=advance, args=(HALF - 2, "adv"))]
        threads += [threading.Thread(target=advance, args=(i, f"f{i}"))
                    for i in range(1, 20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(errors, [])
        self.assertEqual(self.db.get_link(lid)["highest"], HALF)
        self.assertEqual(self.receipt_count("park-1"), 1)
        obs = self.db.list_observations(lid)[0]
        self.assertEqual((obs["status"], obs["extended"]),
                         ("accepted", HALF))

    def test_concurrent_identical_anchor_ids_leave_one_row(self):
        lid = self.make_link()
        self.frame(lid, 0, "r0")
        outcomes = []

        def anchor():
            outcomes.append(self.anchor(lid, HALF, "o1", "park-1"))

        threads = [threading.Thread(target=anchor) for _ in range(16)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(len(outcomes), 16)
        self.assertTrue(all(
            o["observation"]["status"] == "pending" for o in outcomes))
        n = self.db.conn.execute(
            "SELECT COUNT(*) AS n FROM observations "
            "WHERE observation_id = 'o1'").fetchone()["n"]
        self.assertEqual(n, 1)

    def test_concurrent_same_id_changed_data_single_winner(self):
        lid = self.make_link()
        self.frame(lid, 0, "r0")
        wins = {"ok": 0, "conflict": 0}
        lock = threading.Lock()

        def anchor(receipt):
            try:
                self.anchor(lid, HALF, "o1", receipt)
                with lock:
                    wins["ok"] += 1
            except ObservationConflict:
                with lock:
                    wins["conflict"] += 1

        # Same global observation id, each thread carrying different
        # associated data (a different original receipt id): exactly one
        # registration wins and all the rest are conflicts.
        receipts = [f"park-{i}" for i in range(12)]
        threads = [threading.Thread(target=anchor, args=(r,)) for r in receipts]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(wins["ok"], 1)
        self.assertEqual(wins["conflict"], len(threads) - 1)


class TestObservationRestart(ObsDB):
    def test_pending_survives_restart_and_settles_identically(self):
        path = self.db.path
        lid = self.make_link()
        self.frame(lid, 0, "r0")
        self.anchor(lid, HALF, "o1", "park-1")
        self.db.close()

        db2 = Database(path)
        try:
            # still pending after reopen, window unchanged
            link = db2.get_link(lid)
            self.assertEqual(link["highest"], 0)
            self.assertEqual(link["observations"][0]["status"], "pending")
            v = db2.submit_frame(lid, HALF - 2, "adv", {"v": 1})
            self.assertEqual(v["highest"], HALF)
            settled = v["settled_observations"]
            self.assertEqual(
                [(s["observation_id"], s["status"], s["extended"])
                 for s in settled],
                [("o1", "accepted", HALF)])
            self.assertEqual(
                db2.conn.execute(
                    "SELECT COUNT(*) AS n FROM receipts "
                    "WHERE receipt_id = 'park-1'").fetchone()["n"],
                1)
        finally:
            db2.close()

    def test_settled_state_survives_restart_without_resettlement(self):
        path = self.db.path
        lid = self.make_link()
        self.frame(lid, 0, "r0")
        self.anchor(lid, HALF, "o1", "park-1")
        self.frame(lid, HALF - 2, "adv")
        self.db.close()

        db2 = Database(path)
        try:
            obs = db2.list_observations(lid)[0]
            self.assertEqual((obs["status"], obs["extended"]),
                             ("accepted", HALF))
            # further ordinary motion does not re-settle it
            v = db2.submit_frame(lid, HALF + 5, "next", {"v": 1})
            self.assertNotIn("settled_observations", v)
            self.assertEqual(
                db2.conn.execute(
                    "SELECT COUNT(*) AS n FROM receipts "
                    "WHERE receipt_id = 'park-1'").fetchone()["n"],
                1)
        finally:
            db2.close()


class TestNormalReceiveRegression(ObsDB):
    """The ordinary entry keeps behaving exactly as before: a half-span
    frame submitted normally (with a fresh receipt id) is still rejected."""

    def test_ordinary_half_span_frame_is_still_rejected(self):
        lid = self.make_link()
        self.frame(lid, 0, "r0")
        v = self.frame(lid, HALF, "direct-half")
        self.assertEqual(v["status"], "rejected")
        self.assertEqual(v["highest"], 0)
        # the ordinary verdict is ledgered and replays
        replay = self.frame(lid, HALF, "direct-half")
        self.assertTrue(replay["retransmit"])
        self.assertEqual(replay["status"], "rejected")

    def test_normal_flow_unaffected_around_observation(self):
        lid = self.make_link()
        for c, r in ((MOD - 1, "a"), (0, "b"), (1, "c")):
            self.assertEqual(self.frame(lid, c, r)["status"], "accepted")
        self.assertEqual(self.db.get_link(lid)["highest"], MOD + 1)
        self.assertEqual(self.frame(lid, 0, "d")["status"], "duplicate")
        # park the frame tied around the current highest: wire
        # (MOD+1+HALF) mod MOD = HALF+1
        tied = (MOD + 1 + HALF) & (MOD - 1)
        self.anchor(lid, tied, "o1", "park-1")
        upper = MOD + 1 + HALF
        # an ordinary frame that does not move the highest (a duplicate of an
        # in-window position) neither settles nor disturbs the observation
        v = self.frame(lid, 0, "dup0")
        self.assertEqual(v["status"], "duplicate")
        self.assertNotIn("settled_observations", v)
        self.assertEqual(
            self.db.list_observations(lid)[0]["status"], "pending")
        # the first frame that advances the highest breaks the tie and the
        # observation settles (accepted, sliding the window to its reading)
        v = self.frame(lid, (upper - 1) & (MOD - 1), "f")
        self.assertEqual(v["highest"], upper)
        settled = v["settled_observations"]
        self.assertEqual(len(settled), 1)
        self.assertEqual(settled[0]["observation_id"], "o1")
        self.assertEqual(settled[0]["status"], "accepted")
        self.assertEqual(settled[0]["extended"], upper)


if __name__ == "__main__":
    unittest.main()
