#!/usr/bin/env python3
"""Resilience test: a peer-node outage must not degrade the S3 data plane.

With its peer down, an s3cache node must keep serving PUT/GET/LIST correctly and
*fast* (the freshness barrier waits only on already-applied feed heads, never on a
dead peer) — a peer outage is not a data-plane outage. When the peer comes back it
returns in a fresh feed epoch: the survivor sees a gap, flushes, resyncs its index
from the origin, and cross-node coherence resumes in both directions. Exits 0/1.
"""
import sys
import time

import _s3cache_e2e as h

BUCKET = "resilience-test"
PORT_A, PORT_B = 18071, 18072
GOSSIP_A, GOSSIP_B = 19071, 19072


def start_b():
    return h.start_node("resB", PORT_B, BUCKET, gossip_port=GOSSIP_B,
                        seeds=f"resA=127.0.0.1:{GOSSIP_A}")


def keys(cli):
    return {o["Key"] for o in cli.list_objects_v2(Bucket=BUCKET).get("Contents", [])}


def timed(fn):
    s = time.time()
    fn()
    return (time.time() - s) * 1000.0


def main():
    d = h.direct()
    h.reset_bucket(d, BUCKET)
    a = h.start_node("resA", PORT_A, BUCKET, gossip_port=GOSSIP_A,
                     seeds=f"resB=127.0.0.1:{GOSSIP_B}")
    b = start_b()
    fails = []

    def check(name, ok, detail=""):
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  ({detail})" if detail and not ok else ""))
        if not ok:
            fails.append(name)

    try:
        assert h.wait_port(PORT_A) and h.wait_port(PORT_B), "nodes did not bind"
        time.sleep(1.5)
        pa, pb = h.s3(f"http://127.0.0.1:{PORT_A}"), h.s3(f"http://127.0.0.1:{PORT_B}")

        # Baseline: coherence works with both nodes up.
        pa.put_object(Bucket=BUCKET, Key="base", Body=b"1")
        check("baseline: B sees A's write", h.poll(lambda: "base" in keys(pb)) is not None)

        # Kill the peer; A's data plane must stay correct AND fast.
        print(">>> stopping node B")
        h.stop_nodes(b)
        time.sleep(0.5)

        put_ms = timed(lambda: pa.put_object(Bucket=BUCKET, Key="down", Body=b"payload"))
        get_ms = timed(lambda: pa.get_object(Bucket=BUCKET, Key="down")["Body"].read())
        list_ms = timed(lambda: keys(pa))

        check("peer down: GET returns correct bytes == direct",
              pa.get_object(Bucket=BUCKET, Key="down")["Body"].read()
              == d.get_object(Bucket=BUCKET, Key="down")["Body"].read())
        check("peer down: LIST works", "down" in keys(pa))
        check("peer down: PUT is fast (< 500ms)", put_ms < 500, f"{put_ms:.0f}ms")
        check("peer down: GET is fast (< 500ms)", get_ms < 500, f"{get_ms:.0f}ms")
        check("peer down: LIST is fast (< 500ms)", list_ms < 500, f"{list_ms:.0f}ms")

        # Bring the peer back (fresh feed epoch); coherence must resume both ways.
        print(">>> restarting node B")
        b = start_b()
        assert h.wait_port(PORT_B), "restarted node did not bind"
        time.sleep(2.0)  # rejoin gossip + bootstrap LIST
        pb = h.s3(f"http://127.0.0.1:{PORT_B}")

        pa.put_object(Bucket=BUCKET, Key="recovered", Body=b"back")
        check("after restart: B sees A's new write",
              h.poll(lambda: "recovered" in keys(pb), timeout=15) is not None)
        # B's writes arrive in a new epoch: A takes the gap (flush + origin
        # resync) and then converges — the loud-recovery path, end to end.
        pb.put_object(Bucket=BUCKET, Key="from-b", Body=b"epoch2")
        check("after restart: A sees B's write (new epoch, gap-resync path)",
              h.poll(lambda: "from-b" in keys(pa), timeout=20) is not None)
    except Exception as e:  # noqa: BLE001
        fails.append(f"harness error: {e!r}")
    finally:
        h.stop_nodes(a, b)

    if fails:
        print(f"\nFAILED ({len(fails)}): {fails}")
        return 1
    print("\nALL RESILIENCE CHECKS PASSED (peer outage != data-plane outage)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
