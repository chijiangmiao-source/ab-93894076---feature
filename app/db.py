"""SQLite persistence: link windows, stable receipts and pending
half-wrap observations.

Every frame arrival is resolved inside one ``BEGIN IMMEDIATE`` transaction
against *both* the sliding-window state and the receipt ledger, so that
concurrent arrivals and process restarts always derive a consistent verdict.

A frame whose counter lands exactly half a wrap away from the current
highest seq is epoch-ambiguous: instead of rejecting it outright the
operator may park it as a *pending observation* (without touching the
window or the receipt ledger).  When an ordinary frame later advances the
highest seq, all pending observations are re-judged inside that same
transaction; the ones that become determinate settle exactly once into a
normal verdict, a receipt-ledger row and a window update.
"""

import hashlib
import json
import sqlite3
import threading
import uuid

from . import window as win

SCHEMA = """
CREATE TABLE IF NOT EXISTS links (
    id         TEXT PRIMARY KEY,
    name       TEXT NOT NULL,
    created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS link_windows (
    link_id  TEXT PRIMARY KEY REFERENCES links(id),
    highest  INTEGER,
    bitmap   INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS receipts (
    receipt_id   TEXT PRIMARY KEY,
    link_id      TEXT NOT NULL REFERENCES links(id),
    counter      INTEGER NOT NULL,
    payload_hash TEXT NOT NULL,
    status       TEXT NOT NULL,
    extended     INTEGER NOT NULL,
    created_at   REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS observations (
    observation_id      TEXT PRIMARY KEY,
    link_id             TEXT NOT NULL REFERENCES links(id),
    original_receipt_id TEXT NOT NULL,
    counter             INTEGER NOT NULL,
    payload_hash        TEXT NOT NULL,
    status              TEXT NOT NULL,
    extended            INTEGER,
    created_at          REAL NOT NULL,
    settled_at          REAL
);
CREATE INDEX IF NOT EXISTS idx_observations_link
    ON observations(link_id, status, observation_id);
-- The original frame receipt id is globally retained by exactly one
-- observation, for the link's whole life (pending or settled); the unique
-- index backs up the in-transaction check even across processes.
CREATE UNIQUE INDEX IF NOT EXISTS idx_observations_receipt
    ON observations(original_receipt_id);
"""


class ReceiptConflict(Exception):
    """A stable receipt id was reused with a different link/counter/payload,
    or is reserved as the original receipt id of a pending observation."""

    def __init__(self, receipt_id, reason="reused with different data"):
        super().__init__(f"{receipt_id}: {reason}")
        self.receipt_id = receipt_id
        self.reason = reason


class ObservationConflict(Exception):
    """An observation id (or its original receipt id) was reused with
    different associated data."""


class NotAtHalfSpan(Exception):
    """The frame is not exactly half a wrap away from the highest seq and
    therefore cannot be registered as a pending observation."""


def canonical_hash(payload):
    """Deterministic hash of an arbitrary JSON payload."""
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


class Database:
    def __init__(self, path):
        self.path = path
        # check_same_thread=False: http.server spawns a thread per request;
        # the lock + BEGIN IMMEDIATE serialise write transactions.
        # isolation_level=None => autocommit unless we open a transaction
        # explicitly, so all BEGIN/COMMIT boundaries below are real.
        self.conn = sqlite3.connect(
            path, check_same_thread=False, isolation_level=None
        )
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.executescript(SCHEMA)
        self._lock = threading.Lock()

    def close(self):
        with self._lock:
            self.conn.close()

    # ---------------------------------------------------------------- links

    def create_link(self, name):
        link_id = uuid.uuid4().hex
        with self._lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                self.conn.execute(
                    "INSERT INTO links (id, name, created_at) "
                    "VALUES (?, ?, strftime('%s','now'))",
                    (link_id, name),
                )
                self.conn.execute(
                    "INSERT INTO link_windows (link_id, highest, bitmap) "
                    "VALUES (?, NULL, 0)",
                    (link_id,),
                )
                self.conn.execute("COMMIT")
            except Exception:
                self.conn.execute("ROLLBACK")
                raise
        return self.get_link(link_id)

    def get_link(self, link_id):
        row = self.conn.execute(
            "SELECT l.id, l.name, w.highest, w.bitmap FROM links l "
            "JOIN link_windows w ON w.link_id = l.id WHERE l.id = ?",
            (link_id,),
        ).fetchone()
        if row is None:
            return None
        state = self._link_state(row)
        state["observations"] = self.list_observations(link_id)
        return state

    def list_links(self):
        rows = self.conn.execute(
            "SELECT l.id, l.name, w.highest, w.bitmap FROM links l "
            "JOIN link_windows w ON w.link_id = l.id ORDER BY l.created_at"
        ).fetchall()
        return [self._link_state(r) for r in rows]

    @staticmethod
    def _link_state(row):
        highest = row["highest"]
        bitmap = row["bitmap"] or 0
        return {
            "id": row["id"],
            "name": row["name"],
            "highest": highest,
            "bitmap": bitmap,
            "recent": win.recent_positions(highest, bitmap),
        }

    # ---------------------------------------------------------------- frames

    @staticmethod
    def _receipt_conflict(prior, link_id, counter, payload_hash):
        return (prior["link_id"] != link_id
                or prior["counter"] != counter
                or prior["payload_hash"] != payload_hash)

    def _settle_pending(self, link_id, highest, bitmap):
        """Re-judge pending observations until no further settlement moves
        the window.  Must run inside an open write transaction.

        Pending items are revisited in stable order (observation id).  After
        each full pass the loop repeats only if a settled frame strictly
        advanced the highest seq, letting a chain of newly disambiguated
        observations keep converging.  Observations whose two candidates are
        still equidistant stay pending.

        Returns ``(highest, bitmap, settled)`` where ``settled`` lists the
        observation rows that changed state, each with its final verdict.
        """
        settled = []
        window_moved = True
        while window_moved:
            window_moved = False
            rows = self.conn.execute(
                "SELECT observation_id, original_receipt_id, counter, "
                "payload_hash FROM observations "
                "WHERE link_id = ? AND status = 'pending' "
                "ORDER BY observation_id",
                (link_id,),
            ).fetchall()
            for obs in rows:
                verdict = win.rejudge(obs["counter"], highest, bitmap)
                if verdict is None:
                    continue  # still epoch-ambiguous; remains pending
                status, extended, new_highest, new_bitmap = verdict
                # Settle exactly once: ledger row + window + observation row
                # all happen in the caller's transaction.
                self.conn.execute(
                    "INSERT INTO receipts (link_id, receipt_id, counter, "
                    "payload_hash, status, extended, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, strftime('%s','now'))",
                    (link_id, obs["original_receipt_id"], obs["counter"],
                     obs["payload_hash"], status, extended),
                )
                self.conn.execute(
                    "UPDATE observations SET status = ?, extended = ?, "
                    "settled_at = strftime('%s','now') "
                    "WHERE observation_id = ?",
                    (status, extended, obs["observation_id"]),
                )
                self.conn.execute(
                    "UPDATE link_windows SET highest = ?, bitmap = ? WHERE link_id = ?",
                    (new_highest, new_bitmap, link_id),
                )
                settled.append({
                    "observation_id": obs["observation_id"],
                    "original_receipt_id": obs["original_receipt_id"],
                    "counter": obs["counter"],
                    "status": status,
                    "extended": extended,
                })
                if new_highest > highest:
                    # The window slid: an item earlier in stable order might
                    # only now be determinate, so another full pass is needed.
                    window_moved = True
                highest, bitmap = new_highest, new_bitmap
        return highest, bitmap, settled

    def submit_frame(self, link_id, counter, receipt_id, payload):
        """Atomically resolve an ordinary frame arrival.

        Returns the verdict dict.  Raises ``LookupError`` for an unknown link
        and ``ReceiptConflict`` when a receipt id is reused with changed
        link/counter/payload -- this includes the original receipt id of a
        pending (or already settled) observation, which is reserved and must
        never enter through the ordinary entry.  Window state and receipt
        record are committed in the same transaction; whenever the frame
        advances the highest seq, pending observations are re-judged and
        settled in that same transaction.
        """
        payload_hash = canonical_hash(payload)
        with self._lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                row = self.conn.execute(
                    "SELECT highest, bitmap FROM link_windows WHERE link_id = ?",
                    (link_id,),
                ).fetchone()
                if row is None:
                    raise LookupError(link_id)

                # An observation permanently retains its original receipt id,
                # which is globally reserved (receipt ids are globally unique):
                # feeding it straight into the normal entry on any link, with
                # any data, and whether the observation is pending or already
                # settled, is rejected -- a parked frame can only be read back
                # through its observation id and can never be judged twice.
                obs_owner = self.conn.execute(
                    "SELECT 1 FROM observations "
                    "WHERE original_receipt_id = ? LIMIT 1",
                    (receipt_id,),
                ).fetchone()
                if obs_owner is not None:
                    raise ReceiptConflict(
                        receipt_id,
                        "receipt id is retained by a registered observation")

                prior = self.conn.execute(
                    "SELECT link_id, counter, payload_hash, status, extended "
                    "FROM receipts WHERE receipt_id = ?",
                    (receipt_id,),
                ).fetchone()

                retransmit = prior is not None
                settled = []
                if retransmit:
                    # Stable receipt ids are global: an identical retransmission
                    # (same link, counter and payload) replays the first verdict;
                    # reusing the id with the link, counter or payload changed is
                    # a conflict.
                    if self._receipt_conflict(prior, link_id, counter, payload_hash):
                        raise ReceiptConflict(receipt_id)
                    status = prior["status"]
                    extended = prior["extended"]
                    highest = row["highest"]
                    bitmap = row["bitmap"] or 0
                else:
                    status, new_highest, new_bitmap = win.decide(
                        counter, row["highest"], row["bitmap"] or 0
                    )
                    extended, _ = win.extend_counter(counter, row["highest"])
                    self.conn.execute(
                        "UPDATE link_windows SET highest = ?, bitmap = ? WHERE link_id = ?",
                        (new_highest, new_bitmap, link_id),
                    )
                    self.conn.execute(
                        "INSERT INTO receipts (link_id, receipt_id, counter, "
                        "payload_hash, status, extended, created_at) "
                        "VALUES (?, ?, ?, ?, ?, ?, strftime('%s','now'))",
                        (link_id, receipt_id, counter, payload_hash,
                         status, extended),
                    )
                    highest, bitmap = new_highest, new_bitmap
                    # An ordinary frame that pushes the highest seq forward may
                    # disambiguate parked half-span frames; converge them all
                    # in this same transaction.
                    if status == "accepted" and (
                            row["highest"] is None
                            or new_highest > row["highest"]):
                        highest, bitmap, settled = self._settle_pending(
                            link_id, highest, bitmap)

                self.conn.execute("COMMIT")
            except Exception:
                self.conn.execute("ROLLBACK")
                raise

        result = {
            "status": status,
            "retransmit": retransmit,
            "counter": counter,
            "extended": extended,
            "highest": highest,
            "recent": win.recent_positions(highest, bitmap),
        }
        if settled:
            result["settled_observations"] = settled
        return result

    # -------------------------------------------------------- observations

    def anchor_observation(self, link_id, counter, observation_id,
                           original_receipt_id, payload):
        """Park a half-span frame as a pending observation.

        The frame is accepted for parking only when its two epoch-adjacent
        candidates are exactly ``2**31`` from the current highest seq.  The
        window and the receipt ledger are left untouched.  Re-registering the
        same observation id replays its current state; reusing either id with
        changed associated data is a conflict.
        """
        payload_hash = canonical_hash(payload)
        with self._lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                row = self.conn.execute(
                    "SELECT highest, bitmap FROM link_windows WHERE link_id = ?",
                    (link_id,),
                ).fetchone()
                if row is None:
                    raise LookupError(link_id)
                highest = row["highest"]

                # Idempotency/conflict on the observation id takes precedence
                # over everything else: an identical retransmission must
                # replay its current (pending or settled) state on any window,
                # and reusing the global id with changed data -- even on
                # another link with no frames yet -- is a conflict.
                existing_obs = self.conn.execute(
                    "SELECT observation_id, link_id, original_receipt_id, "
                    "counter, payload_hash, status, extended FROM observations "
                    "WHERE observation_id = ?",
                    (observation_id,),
                ).fetchone()
                if existing_obs is not None:
                    if (existing_obs["link_id"] != link_id
                            or existing_obs["original_receipt_id"]
                            != original_receipt_id
                            or existing_obs["counter"] != counter
                            or existing_obs["payload_hash"] != payload_hash):
                        raise ObservationConflict(observation_id)
                    # Identical retransmission: replay current state, no change.
                    obs = self._observation_dict(existing_obs)
                    self.conn.execute("COMMIT")
                    return {"observation": obs, "retransmit": True}

                if highest is None:
                    # No ordinary frame yet: epoch is assumed 0, no tie exists.
                    raise NotAtHalfSpan(counter)

                # Identifier conflicts take precedence over the geometric
                # half-span check: both carried ids are globally unique.
                other_obs = self.conn.execute(
                    "SELECT observation_id FROM observations "
                    "WHERE original_receipt_id = ? AND observation_id != ?",
                    (original_receipt_id, observation_id),
                ).fetchone()
                if other_obs is not None:
                    raise ObservationConflict(
                        f"original receipt id {original_receipt_id!r} already "
                        f"carried by observation {other_obs['observation_id']}")
                prior_receipt = self.conn.execute(
                    "SELECT link_id FROM receipts WHERE receipt_id = ?",
                    (original_receipt_id,),
                ).fetchone()
                if prior_receipt is not None:
                    # The original frame already has an ordinary verdict: it
                    # cannot be parked as an observation afterwards.
                    raise ReceiptConflict(
                        original_receipt_id,
                        "receipt id already settled through the normal entry")

                _, ambiguous = win.extend_counter(counter, highest)
                if not ambiguous:
                    raise NotAtHalfSpan(counter)

                self.conn.execute(
                    "INSERT INTO observations (observation_id, link_id, "
                    "original_receipt_id, counter, payload_hash, status, "
                    "extended, created_at) "
                    "VALUES (?, ?, ?, ?, ?, 'pending', NULL, "
                    "strftime('%s','now'))",
                    (observation_id, link_id, original_receipt_id, counter,
                     payload_hash),
                )
                self.conn.execute("COMMIT")
            except Exception:
                self.conn.execute("ROLLBACK")
                raise

        return {
            "observation": {
                "observation_id": observation_id,
                "link_id": link_id,
                "original_receipt_id": original_receipt_id,
                "counter": counter,
                "status": "pending",
                "extended": None,
            },
            "retransmit": False,
        }

    def list_observations(self, link_id):
        rows = self.conn.execute(
            "SELECT observation_id, link_id, original_receipt_id, counter, "
            "status, extended FROM observations "
            "WHERE link_id = ? ORDER BY created_at, observation_id",
            (link_id,),
        ).fetchall()
        return [self._observation_dict(r) for r in rows]

    @staticmethod
    def _observation_dict(row):
        return {
            "observation_id": row["observation_id"],
            "link_id": row["link_id"],
            "original_receipt_id": row["original_receipt_id"],
            "counter": row["counter"],
            "status": row["status"],
            "extended": row["extended"],
        }
