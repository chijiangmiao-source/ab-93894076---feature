#!/usr/bin/env python3
"""End-to-end HTTP smoke test run by the Compose ``verify`` service.

Asserts wrap-around, expiry, duplicate, epoch-tie rejection and receipt
semantics against a fully running server, and exits non-zero on any failure.
"""

import json
import sys
import time
import urllib.error
import urllib.request

MOD = 1 << 32
HALF = 1 << 31


def request(method, url, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read())


def check(cond, msg):
    if not cond:
        raise AssertionError(msg)
    print(f"  ok - {msg}")


def main(base):
    for _ in range(60):
        try:
            code, body = request("GET", base + "/healthz")
            if code == 200 and body.get("status") == "ok":
                break
        except OSError:
            time.sleep(1)
    else:
        raise RuntimeError("server never became healthy")
    print("ok - healthz reports ok")

    _, link = request("POST", base + "/api/links", {"name": "smoke-wrap"})
    lid = link["id"]
    frames_url = f"{base}/api/links/{lid}/frames"

    def frame(counter, rid, payload=None):
        return request("POST", frames_url,
                       {"counter": counter, "receipt_id": rid,
                        "payload": payload if payload is not None else {"v": 1}})

    # --- wrap-around with out-of-order delivery, each accepted once ---
    code, v = frame(MOD - 2, "s-2")
    check(code == 200 and v["status"] == "accepted", f"counter 2^32-2 accepted ({v})")
    _, v = frame(MOD - 1, "s-1")
    check(v["status"] == "accepted" and v["extended"] == MOD - 1, "2^32-1 accepted")
    _, v = frame(1, "s1")  # arrives before 0
    check(v["status"] == "accepted" and v["extended"] == MOD + 1,
          "out-of-order counter 1 placed in epoch 1")
    _, v = frame(0, "s0")
    check(v["status"] == "accepted" and v["extended"] == MOD,
          "counter 0 wraps to 2^32")

    _, state = request("GET", f"{base}/api/links/{lid}")
    check(state["highest"] == MOD + 1, f"highest extended seq is 2^32+1 (got {state['highest']})")
    check(state["recent"][:4] == [MOD + 1, MOD, MOD - 1, MOD - 2],
          "recent positions show all four wrap frames once")

    # --- duplicates: same counter, new receipt id ---
    _, v = frame(0, "s0-newid")
    check(v["status"] == "duplicate", "counter 0 resent is duplicate")

    # --- identical retransmission replays the FIRST verdict ---
    _, v = frame(0, "s0")
    check(v["status"] == "accepted" and v["retransmit"],
          "identical receipt+payload replays original accepted verdict")

    # --- receipt id reuse with changed counter / payload / link -> 409 ---
    code, _ = frame(5, "s0")
    check(code == 409, "receipt id reused with different counter -> 409")
    code, _ = frame(0, "s0", {"v": 2})
    check(code == 409, "receipt id reused with changed payload -> 409")

    _, other = request("POST", base + "/api/links", {"name": "smoke-other"})
    code, _ = request("POST", f"{base}/api/links/{other['id']}/frames",
                      {"counter": 0, "receipt_id": "s0", "payload": {"v": 1}})
    check(code == 409, "receipt id reused on another link -> 409")

    # --- expired: slide forward, old frame can never re-enter ---
    _, v = frame((MOD + 70) & (MOD - 1), "s70")
    check(v["status"] == "accepted", "jump 70 frames into epoch 1 accepted")
    _, v = frame(MOD - 2, "s-2-late")  # distance 72 from highest
    check(v["status"] == "expired", "old epoch-0 frame after wrap is expired")
    _, v = frame(MOD - 2, "s-2-late2")
    check(v["status"] == "expired", "expired frame stays expired")

    # --- epoch tie: counter exactly 2^31 away is rejected ---
    _, tie = request("POST", base + "/api/links", {"name": "smoke-tie"})
    request("POST", f"{base}/api/links/{tie['id']}/frames",
            {"counter": 0, "receipt_id": "t0", "payload": {}})
    code, v = request("POST", f"{base}/api/links/{tie['id']}/frames",
                      {"counter": HALF, "receipt_id": "th", "payload": {}})
    check(code == 200 and v["status"] == "rejected",
          "equidistant epoch candidates (2^31) rejected")

    # --- half-wrap observations: retain, auto-settle, receipt conflicts ---
    _, anchor_link = request("POST", base + "/api/links",
                             {"name": "smoke-anchor"})
    aid = anchor_link["id"]
    a_frames = f"{base}/api/links/{aid}/frames"
    a_obs = f"{base}/api/links/{aid}/observations"

    def observe(counter, obs_id, receipt_id, payload=None):
        return request("POST", a_obs,
                       {"counter": counter, "observation_id": obs_id,
                        "receipt_id": receipt_id,
                        "payload": payload if payload is not None else {"v": 1}})

    def aframe(counter, rid, payload=None):
        return request("POST", a_frames,
                       {"counter": counter, "receipt_id": rid,
                        "payload": payload if payload is not None else {"v": 1}})

    request("POST", a_frames,
            {"counter": 0, "receipt_id": "a-base", "payload": {"v": 1}})

    # 1) half-wrap retention: anchoring moves neither window nor ledger
    code, v = observe(HALF, "a-obs-1", "a-orig-1")
    check(code == 200 and v["status"] == "pending" and v["extended"] is None,
          "half-wrap frame registered as pending observation")
    _, state = request("GET", f"{base}/api/links/{aid}")
    check(state["highest"] == 0 and len(state["observations"]) == 1
          and state["observations"][0]["status"] == "pending",
          "anchoring leaves window untouched and lists the pending observation")

    # identical observation retransmission returns the current state
    code, v = observe(HALF, "a-obs-1", "a-orig-1")
    check(code == 200 and v["retransmit"] and v["status"] == "pending",
          "identical observation retransmission returns current state")
    # any changed association is refused
    code, _ = observe(HALF, "a-obs-1", "a-orig-1", {"v": 2})
    check(code == 409, "observation id reused with changed payload -> 409")
    code, _ = observe(HALF, "a-obs-2", "a-orig-1")
    check(code == 409, "carried receipt id reused by another observation -> 409")

    # 2) receipt occupation conflict: reserved original receipt cannot enter
    #    through the ordinary entry (even with identical data)
    code, body = aframe(HALF, "a-orig-1")
    check(code == 409 and "reserved" in body["error"],
          "reserved original receipt sent to ordinary entry -> 409")

    # 3) auto-settlement inside the advancing frame's transaction
    code, v = aframe(1, "a-advance")
    check(code == 200 and len(v["settled"]) == 1
          and v["settled"][0]["observation_id"] == "a-obs-1"
          and v["settled"][0]["extended"] == HALF
          and v["highest"] == HALF,
          "advancing ordinary frame settles the observation at 2^31 in one tx")
    _, state = request("GET", f"{base}/api/links/{aid}")
    obs = state["observations"][0]
    check(obs["status"] == "settled" and obs["verdict"] == "accepted"
          and obs["extended"] == HALF,
          "page shows settled observation with its final extended seq")
    # settled exactly once: a further trigger settles nothing and the ledger
    # still holds one row (the settled receipt now replays its first verdict)
    code, v = aframe(2, "a-advance-2")
    check(code == 200 and v["settled"] == [],
          "repeated trigger does not re-settle observations")
    code, v = aframe(HALF, "a-orig-1")
    check(code == 200 and v["retransmit"] and v["status"] == "accepted",
          "settled receipt id replays its verdict via the ordinary entry")

    # 4) ordinary reception regression: normal verdicts are unchanged
    _, reg_link = request("POST", base + "/api/links", {"name": "smoke-reg"})
    rid = reg_link["id"]
    r_frames = f"{base}/api/links/{rid}/frames"

    def rframe(counter, r, payload=None):
        return request("POST", r_frames,
                       {"counter": counter, "receipt_id": r,
                        "payload": payload if payload is not None else {"v": 1}})

    _, v = rframe(MOD - 1, "g-1")
    check(v["status"] == "accepted", "regression: pre-wrap frame accepted")
    _, v = rframe(0, "g-0")
    check(v["status"] == "accepted" and v["extended"] == MOD,
          "regression: counter 0 wraps to 2^32")
    _, v = rframe(0, "g-0")
    check(v["retransmit"] and v["status"] == "accepted",
          "regression: identical retransmission replays first verdict")
    _, v = rframe(0, "g-dup")
    check(v["status"] == "duplicate", "regression: duplicate detected")
    code, _ = rframe(MOD - 1, "g-1", {"v": 9})
    check(code == 409, "regression: receipt reuse with changed payload -> 409")

    # --- input validation ---
    code, _ = request("POST", frames_url,
                      {"counter": MOD, "receipt_id": "bad", "payload": {}})
    check(code == 400, "counter >= 2^32 rejected with 400")
    code, _ = request("POST", f"{base}/api/links/deadbeef/frames",
                      {"counter": 1, "receipt_id": "x", "payload": {}})
    check(code == 404, "unknown link -> 404")

    print("\nALL SMOKE CHECKS PASSED")


if __name__ == "__main__":
    base = sys.argv[1] if len(sys.argv) > 1 else "http://web:8080"
    main(base.rstrip("/"))
