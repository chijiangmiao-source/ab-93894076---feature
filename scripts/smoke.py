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
    # ordinary-receive regression: the direct rejection is stable on replay
    code, v = request("POST", f"{base}/api/links/{tie['id']}/frames",
                      {"counter": HALF, "receipt_id": "th", "payload": {}})
    check(v["status"] == "rejected" and v["retransmit"],
          "half-span frame via the normal entry stays rejected on replay")

    # --- half-wrap pending observations ---
    _, obs_link = request("POST", base + "/api/links", {"name": "smoke-obs"})
    ol = obs_link["id"]
    obs_frames = f"{base}/api/links/{ol}/frames"
    obs_url = f"{base}/api/links/{ol}/observations"

    def oframe(counter, rid, payload=None):
        return request("POST", obs_frames,
                       {"counter": counter, "receipt_id": rid,
                        "payload": payload if payload is not None else {"v": 1}})

    def oanchor(counter, oid, rid, payload=None):
        return request("POST", obs_url,
                       {"counter": counter, "observation_id": oid,
                        "original_receipt_id": rid,
                        "payload": payload if payload is not None else {"v": 1}})

    # half-wrap retention: park the tied frame; window and ledger untouched
    code, v = oframe(0, "obs-r0")
    check(code == 200 and v["status"] == "accepted", "obs link base frame accepted")
    code, v = oanchor(HALF, "obs-o1", "obs-park-1")
    check(code == 200 and v["observation"]["status"] == "pending"
          and v["observation"]["extended"] is None and not v["retransmit"],
          "exactly-half-wrap frame registered as pending observation")
    _, state = request("GET", f"{base}/api/links/{ol}")
    check(state["highest"] == 0 and len(state["observations"]) == 1
          and state["observations"][0]["observation_id"] == "obs-o1",
          "parking changes neither the window nor the receipt ledger")

    # identical observation retransmission returns the current state
    code, v = oanchor(HALF, "obs-o1", "obs-park-1")
    check(code == 200 and v["retransmit"]
          and v["observation"]["status"] == "pending",
          "identical observation retransmission replays pending state")

    # any id reused with changed associated data is rejected
    code, _ = oanchor(HALF + 1, "obs-o1", "obs-park-1")
    check(code == 409, "observation id reused with changed counter -> 409")
    code, _ = oanchor(HALF, "obs-o1", "obs-park-2")
    check(code == 409, "observation id reused with changed receipt -> 409")
    code, _ = oanchor(HALF, "obs-o1", "obs-park-1", {"v": 2})
    check(code == 409, "observation id reused with changed payload -> 409")

    # retained original receipt fed to the normal entry is refused
    code, _ = oframe(HALF, "obs-park-1")
    check(code == 409, "retained original receipt at normal entry -> 409")
    _, state = request("GET", f"{base}/api/links/{ol}")
    check(state["highest"] == 0, "refused normal submission leaves window intact")

    # only a genuinely half-span frame may be parked
    code, _ = oanchor(5, "obs-oX", "obs-park-X")
    check(code == 422, "non-half-span frame cannot be anchored (422)")

    # automatic settlement inside the advancing frame's transaction
    code, v = oframe(HALF - 2, "obs-adv")
    settled = v.get("settled_observations", [])
    check(code == 200 and v["highest"] == HALF
          and len(settled) == 1
          and settled[0]["observation_id"] == "obs-o1"
          and settled[0]["status"] == "accepted"
          and settled[0]["extended"] == HALF,
          "advancing ordinary frame auto-settles the observation once in-transaction")
    _, state = request("GET", f"{base}/api/links/{ol}")
    o = state["observations"][0]
    check(o["status"] == "accepted" and o["extended"] == HALF,
          "page/API shows settled observation with its final extended seq")
    # exactly-once: replay the advancing frame settles nothing again
    _, v = oframe(HALF - 2, "obs-adv")
    check(v["retransmit"] and "settled_observations" not in v,
          "repeated trigger never settles an observation twice")
    # retained receipt stays blocked; observation id replays the verdict
    code, _ = oframe(HALF, "obs-park-1")
    check(code == 409, "retained receipt still refused after settlement")
    code, v = oanchor(HALF, "obs-o1", "obs-park-1")
    check(code == 200 and v["retransmit"]
          and v["observation"]["status"] == "accepted"
          and v["observation"]["extended"] == HALF,
          "settled observation retransmission replays final verdict")

    # stable order: several pending observations of the same tied counter
    # settle in observation-id order (first accepted, rest duplicate)
    _, chain = request("POST", base + "/api/links", {"name": "smoke-chain"})
    cl = chain["id"]
    curl = f"{base}/api/links/{cl}/frames"
    curl_obs = f"{base}/api/links/{cl}/observations"

    def cframe(counter, rid):
        return request("POST", curl,
                       {"counter": counter, "receipt_id": rid, "payload": {}})

    def canchor(oid, rid):
        return request("POST", curl_obs,
                       {"counter": HALF, "observation_id": oid,
                        "original_receipt_id": rid, "payload": {}})

    cframe(0, "chain-r0")
    for oid, rid in (("chain-z", "chain-pz"), ("chain-a", "chain-pa"),
                     ("chain-m", "chain-pm")):
        code, _ = canchor(oid, rid)
        check(code == 200, f"parked {oid}")
    code, v = cframe(HALF - 1, "chain-adv")
    settled = v.get("settled_observations", [])
    check(code == 200 and v["highest"] == HALF
          and [s["observation_id"] for s in settled]
          == ["chain-a", "chain-m", "chain-z"]
          and [s["status"] for s in settled]
          == ["accepted", "duplicate", "duplicate"]
          and all(s["extended"] == HALF for s in settled),
          "all pending observations re-judged in stable id order, settled once")

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
