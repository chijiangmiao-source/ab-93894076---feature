"""SQLite persistence: link windows, stable receipts and pending observations.

Every frame arrival is resolved inside one ``BEGIN IMMEDIATE`` transaction
against *both* the sliding-window state and the receipt ledger, so that
concurrent arrivals and process restarts always derive a consistent verdict.

A frame whose wire counter is exactly half a wrap (2**31) away from the
current highest extended seq has two equidistant epoch candidates and cannot
be placed yet.  Instead of rejecting it outright the operator may *anchor* it
as a pending observation: the observation stores its own id together with the
original frame's receipt id, counter and payload, while neither the window nor
the receipt ledger is touched.  As soon as a later ordinary frame advances the
highest seq, every pending observation is re-judged inside that same
transaction (in stable observation-id order, repeatedly until the window
converges) and each determinable one is settled exactly once into an ordinary
verdict, receipt-ledger row and window update.
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
    observation_id TEXT PRIMARY KEY,
    receipt_id     TEXT NOT NULL UNIQUE,
    link_id        TEXT NOT NULL REFERENCES links(id),
    counter        INTEGER NOT NULL,
    payload_hash   TEXT NOT NULL,
    status         TEXT NOT NULL CHECK (status IN ('pending', 'settled')),
    verdict        TEXT,
    extended       INTEGER,
    created_at     REAL NOT NULL,
    settled_at     REAL
);
"""


class ReceiptConflict(Exception):
    """A stable receipt/observation id was reused with changed data, or a
    receipt id reserved by a pending observation was sent to the ordinary
    frame entry."""


class NotAnchorable(Exception):
    """The frame is not exactly half a wrap away from the current highest, so
    it cannot be registered as a pending observation."""


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

    # -------------------------------------------------------- observations

    @staticmethod
    def _observation_state(row):
        return {
            "observation_id": row["observation_id"],
            "receipt_id": row["receipt_id"],
            "counter": row["counter"],
            "status": row["status"],
            "verdict": row["verdict"],
            "extended": row["extended"],
        }

    def list_observations(self, link_id=None):
        """Pending and settled observations.

        Ordering is stable and deterministic: pending observations first
        (oldest anchoring time, then observation id), then settled ones, so a
        restart or a repeated trigger always re-judges them in the same order.
        """
        if link_id is None:
            sql = ("SELECT observation_id, receipt_id, counter, status, "
                   "verdict, extended FROM observations "
                   "ORDER BY status DESC, created_at, observation_id")
            rows = self.conn.execute(sql).fetchall()
        else:
            sql = ("SELECT observation_id, receipt_id, counter, status, "
                   "verdict, extended FROM observations WHERE link_id = ? "
                   "ORDER BY status DESC, created_at, observation_id")
            rows = self.conn.execute(sql, (link_id,)).fetchall()
        return [self._observation_state(r) for r in rows]

    def anchor_observation(self, link_id, counter, observation_id,
                           receipt_id, payload):
        """Register a half-wrap frame as a pending observation.

        Does not move the window and does not write the receipt ledger.
        Raises ``LookupError`` for an unknown link, ``NotAnchorable`` when the
        counter is not exactly half a wrap away from the current highest, and
        ``ReceiptConflict`` when either id is reused with different data or a
        reserved original receipt id is already present elsewhere.
        """
        counter &= win.COUNTER_MAX
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
                current_highest = row["highest"]
                obs = self.conn.execute(
                    "SELECT observation_id, link_id, counter, payload_hash, "
                    "receipt_id, status, verdict, extended FROM observations "
                    "WHERE observation_id = ?",
                    (observation_id,),
                ).fetchone()
                if obs is not None:
                    # Same observation id: an identical retransmission returns
                    # the current state (even once the window has moved past
                    # the original half-wrap distance); any changed association
                    # is a conflict.
                    if (obs["link_id"] != link_id or obs["counter"] != counter
                            or obs["payload_hash"] != payload_hash
                            or obs["receipt_id"] != receipt_id):
                        raise ReceiptConflict(observation_id)
                    result = self._observation_state(obs)
                    result["retransmit"] = True
                else:
                    if current_highest is None:
                        # Without an ordinary frame there is no epoch to be half
                        # a wrap away from; the frame must enter through the
                        # normal entry to initialise the window.
                        raise NotAnchorable(counter)
                    _, ambiguous = win.extend_counter(counter, current_highest)
                    if not ambiguous:
                        raise NotAnchorable(counter)
                    self._assert_ids_free(link_id, counter, payload_hash,
                                          receipt_id, observation_id)
                    self.conn.execute(
                        "INSERT INTO observations (observation_id, receipt_id, "
                        "link_id, counter, payload_hash, status, created_at) "
                        "VALUES (?, ?, ?, ?, ?, 'pending', "
                        "strftime('%s','now'))",
                        (observation_id, receipt_id, link_id, counter,
                         payload_hash),
                    )
                    result = {
                        "observation_id": observation_id,
                        "receipt_id": receipt_id,
                        "counter": counter,
                        "status": "pending",
                        "verdict": None,
                        "extended": None,
                        "retransmit": False,
                    }

                self.conn.execute("COMMIT")
            except Exception:
                self.conn.execute("ROLLBACK")
                raise

        result["highest"] = current_highest
        return result

    def _assert_ids_free(self, link_id, counter, payload_hash,
                         receipt_id, observation_id):
        """Reject id reuse that would associate an id with different data."""
        other_obs = self.conn.execute(
            "SELECT observation_id, link_id, counter, payload_hash FROM "
            "observations WHERE receipt_id = ?",
            (receipt_id,),
        ).fetchone()
        if other_obs is not None and (
                other_obs["observation_id"] != observation_id
                or other_obs["link_id"] != link_id
                or other_obs["counter"] != counter
                or other_obs["payload_hash"] != payload_hash):
            raise ReceiptConflict(receipt_id)
        # A receipt id already settled into (or directly used by) the ordinary
        # ledger can never be (re)reserved by a new observation, even when the
        # associated data is identical: the id already owns a verdict.
        ledger = self.conn.execute(
            "SELECT 1 FROM receipts WHERE receipt_id = ?",
            (receipt_id,),
        ).fetchone()
        if ledger is not None:
            raise ReceiptConflict(receipt_id)

    def _settle_pending(self, link_id, highest, bitmap):
        """Re-judge every pending observation of the link until convergence.

        Must run inside an already-open transaction.  Observations are
        reconsidered in stable observation-id order; the loop repeats whenever
        a settlement advances the window (which may make a previously
        equidistant observation determinable).  Each observation is settled at
        most once: a ledger row is written and its row flips to ``settled``.
        Observations still equidistant stay pending.
        """
        settled = []
        while True:
            rows = self.conn.execute(
                "SELECT observation_id, receipt_id, counter, payload_hash "
                "FROM observations WHERE link_id = ? AND status = 'pending' "
                "ORDER BY observation_id",
                (link_id,),
            ).fetchall()
            progressed = False
            for obs in rows:
                extended, ambiguous = win.extend_counter(
                    obs["counter"], highest
                )
                if ambiguous:
                    continue
                status, new_highest, new_bitmap = win.decide(
                    obs["counter"], highest, bitmap
                )
                # Settling is an ordinary verdict with the original frame's
                # receipt id.
                self.conn.execute(
                    "INSERT INTO receipts (link_id, receipt_id, counter, "
                    "payload_hash, status, extended, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, strftime('%s','now'))",
                    (link_id, obs["receipt_id"], obs["counter"],
                     obs["payload_hash"], status, extended),
                )
                self.conn.execute(
                    "UPDATE observations SET status = 'settled', verdict = ?, "
                    "extended = ?, settled_at = strftime('%s','now') "
                    "WHERE observation_id = ?",
                    (status, extended, obs["observation_id"]),
                )
                settled.append({
                    "observation_id": obs["observation_id"],
                    "receipt_id": obs["receipt_id"],
                    "counter": obs["counter"],
                    "status": "settled",
                    "verdict": status,
                    "extended": extended,
                })
                if new_highest != highest:
                    progressed = True
                highest, bitmap = new_highest, new_bitmap
            if not progressed:
                break
        self.conn.execute(
            "UPDATE link_windows SET highest = ?, bitmap = ? WHERE link_id = ?",
            (highest, bitmap, link_id),
        )
        return highest, bitmap, settled

    # ---------------------------------------------------------------- frames

    def submit_frame(self, link_id, counter, receipt_id, payload):
        """Atomically resolve an ordinary frame arrival.

        Returns the verdict dict.  Raises ``LookupError`` for an unknown link
        and ``ReceiptConflict`` when a receipt id is reused with changed
        link/counter/payload, or when the id is still reserved by a pending
        observation (the original frame may only re-enter through its
        observation).  Window state, the receipt record and any observation
        settlements are committed in the same transaction.
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

                # A receipt id held by a still-pending observation must never
                # be admitted through the ordinary entry - not even with
                # identical data; it is reserved until the observation settles.
                reserved = self.conn.execute(
                    "SELECT 1 FROM observations WHERE receipt_id = ? "
                    "AND status = 'pending'",
                    (receipt_id,),
                ).fetchone()
                if reserved is not None:
                    raise ReceiptConflict(receipt_id)

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
                    if (prior["link_id"] != link_id
                            or prior["counter"] != counter
                            or prior["payload_hash"] != payload_hash):
                        raise ReceiptConflict(receipt_id)
                    status = prior["status"]
                    extended = prior["extended"]
                    highest = row["highest"]
                    bitmap = row["bitmap"] or 0
                else:
                    old_highest = row["highest"]
                    status, new_highest, new_bitmap = win.decide(
                        counter, old_highest, row["bitmap"] or 0
                    )
                    extended, _ = win.extend_counter(counter, old_highest)
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
                    # Once an ordinary frame advances the highest seq, every
                    # pending observation is re-judged in the same transaction
                    # until the window converges.
                    if old_highest is None or new_highest != old_highest:
                        highest, bitmap, settled = self._settle_pending(
                            link_id, highest, bitmap
                        )

                self.conn.execute("COMMIT")
            except Exception:
                self.conn.execute("ROLLBACK")
                raise

        return {
            "status": status,
            "retransmit": retransmit,
            "counter": counter,
            "extended": extended,
            "highest": highest,
            "recent": win.recent_positions(highest, bitmap),
            "settled": settled,
        }
