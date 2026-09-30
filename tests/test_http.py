import json
import os
import tempfile
import threading
import time
import unittest
import urllib.request
import urllib.error

from app.server import Server

MOD = 1 << 32
HALF = 1 << 31


def _request(method, url, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read())


class TestHTTP(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        db_path = os.path.join(cls.tmp.name, "http.db")
        cls.server = Server(("127.0.0.1", 0), db_path, log_file=open(os.devnull, "w"))
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.base = f"http://127.0.0.1:{cls.port}"
        # Wait for readiness.
        for _ in range(50):
            try:
                _request("GET", cls.base + "/healthz")
                break
            except OSError:
                time.sleep(0.05)

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.db.close()
        cls.tmp.cleanup()

    def test_health(self):
        code, body = _request("GET", self.base + "/healthz")
        self.assertEqual(code, 200)
        self.assertEqual(body["status"], "ok")

    def test_index_served(self):
        with urllib.request.urlopen(self.base + "/", timeout=5) as resp:
            self.assertEqual(resp.status, 200)
            self.assertIn("text/html", resp.headers["Content-Type"])
            self.assertIn(b"\xe6\xb7\xb1\xe7\xa9\xba", resp.read())  # 深空

    def test_full_link_and_frame_flow(self):
        code, link = _request("POST", self.base + "/api/links", {"name": "flow"})
        self.assertEqual(code, 201)
        lid = link["id"]

        code, listing = _request("GET", self.base + "/api/links")
        self.assertEqual(code, 200)
        self.assertTrue(any(l["id"] == lid for l in listing["links"]))

        def frame(counter, rid, payload=None):
            return _request("POST", f"{self.base}/api/links/{lid}/frames",
                            {"counter": counter, "receipt_id": rid,
                             "payload": payload if payload is not None else {"v": 1}})

        code, v = frame(MOD - 1, "w1")
        self.assertEqual(code, 200)
        self.assertEqual(v["status"], "accepted")

        code, v = frame(0, "w2")                 # wraps to epoch 1
        self.assertEqual(v["status"], "accepted")
        self.assertEqual(v["extended"], MOD)

        code, v = frame(0, "w2")                 # identical replay
        self.assertEqual(v["status"], "accepted")
        self.assertTrue(v["retransmit"])

        code, v = frame(0, "w2", {"v": 9})       # payload changed
        self.assertEqual(code, 409)

        code, v = frame(0, "w2")                 # ledger unaffected by conflict
        self.assertEqual(code, 200)
        self.assertTrue(v["retransmit"])

        code, v = frame(0, "w2-on-other-link")  # sanity: other id fine here
        self.assertEqual(code, 200)
        self.assertEqual(v["status"], "duplicate")

    def test_unknown_link_404(self):
        code, _ = _request("POST",
                           f"{self.base}/api/links/nope/frames",
                           {"counter": 1, "receipt_id": "x", "payload": {}})
        self.assertEqual(code, 404)

    def test_bad_counter_400(self):
        _, link = _request("POST", self.base + "/api/links", {"name": "b"})
        lid = link["id"]
        for bad in (-1, MOD, "abc", None):
            code, body = _request(
                "POST", f"{self.base}/api/links/{lid}/frames",
                {"counter": bad, "receipt_id": "r", "payload": {}})
            self.assertEqual(code, 400, bad)

    def test_equidistant_counter_rejected(self):
        _, link = _request("POST", self.base + "/api/links", {"name": "tie"})
        lid = link["id"]
        _request("POST", f"{self.base}/api/links/{lid}/frames",
                 {"counter": 0, "receipt_id": "z0", "payload": {}})
        code, v = _request("POST", f"{self.base}/api/links/{lid}/frames",
                           {"counter": HALF, "receipt_id": "zh", "payload": {}})
        self.assertEqual(code, 200)
        self.assertEqual(v["status"], "rejected")

    # -------------------------------------------------------- observations
    # NOTE: the class shares one server/database, and observation/receipt
    # ids are globally unique, so every test uses its own id prefix.

    def _new_link(self, name="obs"):
        _, link = _request("POST", self.base + "/api/links", {"name": name})
        return link["id"]

    def _frame(self, lid, counter, rid, payload=None):
        return _request("POST", f"{self.base}/api/links/{lid}/frames",
                        {"counter": counter, "receipt_id": rid,
                         "payload": payload if payload is not None else {"v": 1}})

    def _anchor(self, lid, counter, obs_id, rid, payload=None):
        return _request("POST", f"{self.base}/api/links/{lid}/observations",
                        {"counter": counter, "observation_id": obs_id,
                         "original_receipt_id": rid,
                         "payload": payload if payload is not None else {"v": 1}})

    def test_half_span_observation_parked_without_window_change(self):
        lid = self._new_link("park")
        self._frame(lid, 0, "park-r0")
        code, r = self._anchor(lid, HALF, "park-o1", "park-receipt-1")
        self.assertEqual(code, 200)
        self.assertEqual(r["observation"]["status"], "pending")
        self.assertIsNone(r["observation"]["extended"])
        self.assertFalse(r["retransmit"])
        _, state = _request("GET", f"{self.base}/api/links/{lid}")
        self.assertEqual(state["highest"], 0)
        self.assertEqual(state["observations"][0]["observation_id"], "park-o1")

    def test_observation_retransmit_and_conflicts(self):
        lid = self._new_link("retr")
        self._frame(lid, 0, "retr-r0")
        self._anchor(lid, HALF, "retr-o1", "retr-park-1")
        # identical retransmission replays current state
        code, r = self._anchor(lid, HALF, "retr-o1", "retr-park-1")
        self.assertEqual(code, 200)
        self.assertTrue(r["retransmit"])
        self.assertEqual(r["observation"]["status"], "pending")
        # any associated-data change -> 409
        for body in (
            {"counter": HALF + 1, "observation_id": "retr-o1",
             "original_receipt_id": "retr-park-1", "payload": {"v": 1}},
            {"counter": HALF, "observation_id": "retr-o1",
             "original_receipt_id": "retr-park-2", "payload": {"v": 1}},
            {"counter": HALF, "observation_id": "retr-o1",
             "original_receipt_id": "retr-park-1", "payload": {"v": 2}},
        ):
            code, _ = _request("POST",
                               f"{self.base}/api/links/{lid}/observations", body)
            self.assertEqual(code, 409)

    def test_observation_non_half_span_422(self):
        lid = self._new_link("notie")
        self._frame(lid, 0, "notie-r0")
        code, body = self._anchor(lid, 5, "notie-o1", "notie-park-1")
        self.assertEqual(code, 422)
        self.assertIn("half", body["error"])

    def test_retained_receipt_refused_at_normal_entry_409(self):
        lid = self._new_link("retain")
        self._frame(lid, 0, "retain-r0")
        self._anchor(lid, HALF, "retain-o1", "retain-park-1")
        code, _ = self._frame(lid, HALF, "retain-park-1")
        self.assertEqual(code, 409)
        # window untouched after the refused normal submission
        _, state = _request("GET", f"{self.base}/api/links/{lid}")
        self.assertEqual(state["highest"], 0)

    def test_advancing_frame_settles_observation(self):
        lid = self._new_link("settle")
        self._frame(lid, 0, "settle-r0")
        self._anchor(lid, HALF, "settle-o1", "settle-park-1")
        code, v = self._frame(lid, HALF - 2, "settle-adv")
        self.assertEqual(code, 200)
        self.assertEqual(v["highest"], HALF)
        settled = v["settled_observations"]
        self.assertEqual(len(settled), 1)
        self.assertEqual(settled[0]["observation_id"], "settle-o1")
        self.assertEqual(settled[0]["status"], "accepted")
        self.assertEqual(settled[0]["extended"], HALF)
        # settled observation is reported with its final extended seq
        _, state = _request("GET", f"{self.base}/api/links/{lid}")
        obs = state["observations"][0]
        self.assertEqual((obs["status"], obs["extended"]),
                         ("accepted", HALF))
        # retained receipt stays refused, and idempotent replay shows state
        self.assertEqual(self._frame(lid, HALF, "settle-park-1")[0], 409)
        code, r = self._anchor(lid, HALF, "settle-o1", "settle-park-1")
        self.assertEqual(code, 200)
        self.assertTrue(r["retransmit"])
        self.assertEqual(r["observation"]["status"], "accepted")

    def test_observation_requires_fields(self):
        lid = self._new_link("fields")
        for body in (
            {"counter": HALF, "observation_id": "fields-o1"},
            {"counter": HALF, "original_receipt_id": "fields-p"},
            {"observation_id": "fields-o1", "original_receipt_id": "fields-p"},
            {"counter": "nope", "observation_id": "fields-o1",
             "original_receipt_id": "fields-p"},
        ):
            code, _ = _request("POST",
                               f"{self.base}/api/links/{lid}/observations", body)
            self.assertEqual(code, 400, body)


if __name__ == "__main__":
    unittest.main()
