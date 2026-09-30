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


class TestObservationHTTP(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        db_path = os.path.join(cls.tmp.name, "obs.db")
        cls.server = Server(("127.0.0.1", 0), db_path, log_file=open(os.devnull, "w"))
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.base = f"http://127.0.0.1:{cls.port}"
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

    def setUp(self):
        import uuid
        _, link = _request("POST", self.base + "/api/links", {"name": "obs"})
        self.lid = link["id"]
        self.token = uuid.uuid4().hex[:8]
        _request("POST", f"{self.base}/api/links/{self.lid}/frames",
                 {"counter": 0, "receipt_id": f"base-{self.token}",
                  "payload": {"v": 1}})

    def anchor(self, counter=HALF, obs_id=None, receipt=None, payload=None):
        obs_id = obs_id or f"obs-{self.token}"
        receipt = receipt or f"orig-{self.token}"
        return _request(
            "POST", f"{self.base}/api/links/{self.lid}/observations",
            {"counter": counter, "observation_id": obs_id,
             "receipt_id": receipt,
             "payload": payload if payload is not None else {"v": 1}})

    def frame(self, counter, rid, payload=None):
        return _request(
            "POST", f"{self.base}/api/links/{self.lid}/frames",
            {"counter": counter, "receipt_id": f"{rid}-{self.token}",
             "payload": payload if payload is not None else {"v": 1}})

    def test_anchor_retain_and_settle(self):
        code, v = self.anchor()
        self.assertEqual(code, 200)
        self.assertEqual(v["status"], "pending")
        self.assertIsNone(v["extended"])

        # identical observation retransmission returns current state
        code, v = self.anchor()
        self.assertEqual(code, 200)
        self.assertTrue(v["retransmit"])
        self.assertEqual(v["status"], "pending")

        # state page lists the pending observation
        _, link = _request("GET", f"{self.base}/api/links/{self.lid}")
        self.assertEqual(link["highest"], 0)
        self.assertEqual(link["observations"][0]["observation_id"],
                         f"obs-{self.token}")
        self.assertEqual(link["observations"][0]["status"], "pending")

        # ordinary advancement settles it in the same transaction
        code, v = self.frame(1, "advance")
        self.assertEqual(code, 200)
        self.assertEqual(len(v["settled"]), 1)
        self.assertEqual(v["settled"][0]["extended"], HALF)
        self.assertEqual(v["highest"], HALF)
        _, link = _request("GET", f"{self.base}/api/links/{self.lid}")
        obs = link["observations"][0]
        self.assertEqual(obs["status"], "settled")
        self.assertEqual(obs["verdict"], "accepted")
        self.assertEqual(obs["extended"], HALF)

    def test_anchor_conflicts(self):
        self.anchor()
        # changed payload on same observation id -> 409
        code, _ = self.anchor(payload={"v": 2})
        self.assertEqual(code, 409)
        # carried receipt id reused by another observation -> 409
        code, _ = self.anchor(obs_id=f"obs-other-{self.token}")
        self.assertEqual(code, 409)
        # reserved original receipt sent to the ordinary entry -> 409
        code, body = self.frame(HALF, "orig")
        self.assertEqual(code, 409)
        self.assertIn("reserved", body["error"])
        # window never moved
        _, link = _request("GET", f"{self.base}/api/links/{self.lid}")
        self.assertEqual(link["highest"], 0)

    def test_anchor_requires_half_wrap(self):
        code, _ = _request(
            "POST", f"{self.base}/api/links/{self.lid}/observations",
            {"counter": HALF - 1, "observation_id": f"x-{self.token}",
             "receipt_id": f"r-{self.token}", "payload": {}})
        self.assertEqual(code, 400)

    def test_anchor_validation_and_unknown_link(self):
        code, _ = _request(
            "POST", f"{self.base}/api/links/{self.lid}/observations",
            {"counter": HALF, "receipt_id": f"r-{self.token}", "payload": {}})
        self.assertEqual(code, 400)             # missing observation_id
        code, _ = _request(
            "POST", f"{self.base}/api/links/nope/observations",
            {"counter": HALF, "observation_id": "o",
             "receipt_id": "r", "payload": {}})
        self.assertEqual(code, 404)


if __name__ == "__main__":
    unittest.main()
