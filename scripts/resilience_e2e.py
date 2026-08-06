#!/usr/bin/env python3
"""Resilience test: a peer-node outage must not degrade the S3 data plane.

With its peer down, an s3cache node must keep serving PUT/GET/LIST *correctly*, and must
degrade in latency only where the coherence lease says it may. Under leases (the default
`strong` mode) a peer death is a step, not a plateau: the FIRST write after it waits out
the dead peer's serve-lease remainder — bounded by the lease duration `D`
(`S3CACHE_LEASE_MS`) and ended by that lease expiring in the writer's own engine — and
every write after it is fast, because a lapsed holder leaves the wait set and a dead node
never re-enters it. That first wait is the guarantee being collected, not a timeout being
suffered: when it returns, the dead peer provably cannot serve what the write invalidated.

Reads stay correct throughout. While the dead peer sits unreaped in the roster the
survivor cannot get its own lease confirmed, so its reads go to the origin — a round trip,
not a barrier, and still fast. That freeze ends at the reap rather than latching: the
survivor's lapse watcher runs the gap remediation (flush, origin re-LIST, affirm) and it
serves locally again, cold. When the peer comes back it returns in a fresh feed epoch:
the survivor sees a gap, stands its lease down, flushes, resyncs its index from the origin
and affirms the catch-up, and cross-node coherence resumes in both directions. Exits 0/1.
"""
import os
import sys
import time

import _s3cache_e2e as h

BUCKET = "resilience-test"
PORT_A, PORT_B = 18071, 18072
GOSSIP_A, GOSSIP_B = 19071, 19072

# The lease duration `D` these nodes actually run with: `h.start_node` passes the ambient
# environment through, so reading the variable the binary reads is the number the binary
# uses. 2000ms is the binary's own default (src/sync.rs `DEFAULT_LEASE_MS`).
LEASE_MS = int(os.environ.get("S3CACHE_LEASE_MS") or 2000)

# Budget for the first write after the peer dies. The wait itself is one lease remainder
# (≤ D); the engine gives it a deadline of D + 1s (src/sync.rs `WRITE_WAIT_SLACK`), past
# which the write ends with no guarantee at all — so D + 1s is the ceiling being asserted
# and the second second is harness slack: the origin PUT round-trip plus scheduling.
FIRST_WRITE_MS = LEASE_MS + 2000

# The budget for anything that waits on nobody: a write whose only lease-holder has lapsed
# out of the wait set, or a read routed straight to the origin. Both are one loopback
# round-trip — milliseconds — so 500ms is loose enough for a busy machine and tight enough
# to catch a re-introduced stall.
FAST_MS = 500

# How long the returning node gets to settle before anything is asserted about it: its
# lease warm-up (one detection window plus two anti-entropy rounds) and its bootstrap LIST.
# Below that it is correct but origin-served, which is exactly what the polls tolerate.
REJOIN_SETTLE_S = LEASE_MS / 1000.0 + 1.0

# The membership timeout the binary actually runs: `max(D, 2s)`, because src/sync.rs
# floors the tuned `dead_timeout_ms` at DEAD_TIMEOUT_FLOOR_MS. Deriving the horizon from
# `D` alone would undershoot the real one on any run with a small `D` — and a budget
# below the true horizon fails the test on timing the binary never promised.
DEAD_TIMEOUT_MS = max(LEASE_MS, 2000)

# The rejoin budgets. Nothing here is a latency claim — they are ceilings on *membership*,
# which is the slow half of a rejoin: the survivor keeps the dead incarnation on its roster
# until the reap horizon (`2 x dead_timeout` past the Dead verdict), and only then does the
# returning node's fresh feed epoch land as a gap — flush, origin re-LIST, affirm. Both
# scale with the tuned timeout, so a fleet tuned differently gets budgets tuned with it.
REAP_S = 2 * (DEAD_TIMEOUT_MS / 1000.0) + 1.0  # 5s at the default D = 2s
REJOIN_S = 3 * REAP_S  # 15s at D = 2s
GAP_RESYNC_S = 4 * REAP_S  # 20s at D = 2s — the above, plus the survivor's origin re-LIST


def start_b():
    return h.start_node("resB", PORT_B, BUCKET, gossip_port=GOSSIP_B,
                        seeds=f"resA=127.0.0.1:{GOSSIP_A}")


def keys(cli):
    return {o["Key"] for o in cli.list_objects_v2(Bucket=BUCKET).get("Contents", [])}


def timed(fn):
    s = time.time()
    fn()
    return (time.time() - s) * 1000.0


def ms(values):
    return "  ".join(f"{v:.0f}ms" for v in values)


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

        # Kill the peer; A's data plane must stay correct, and must slow down in exactly
        # one place: the first write, which owes the dead peer's lease its remainder.
        print(">>> stopping node B")
        h.stop_nodes(b)
        time.sleep(0.5)

        # B died holding a serve-lease A had adopted, so this write cannot be declared
        # coherent until that lease expires in A's engine — up to D, on B's own clock
        # rather than on A's patience.
        first_put_ms = timed(lambda: pa.put_object(Bucket=BUCKET, Key="down", Body=b"payload"))
        # And only that write pays it: a lapsed holder leaves the wait set permanently, so
        # A now waits on nothing at all.
        later_put_ms = [timed(lambda i=i: pa.put_object(Bucket=BUCKET, Key=f"down-{i}", Body=b"payload"))
                        for i in range(3)]
        get_ms = timed(lambda: pa.get_object(Bucket=BUCKET, Key="down")["Body"].read())
        list_ms = timed(lambda: keys(pa))

        check("peer down: GET returns correct bytes == direct",
              pa.get_object(Bucket=BUCKET, Key="down")["Body"].read()
              == d.get_object(Bucket=BUCKET, Key="down")["Body"].read())
        check("peer down: LIST works", "down" in keys(pa))
        check("peer down: later writes landed", {f"down-{i}" for i in range(3)} <= keys(pa))
        check(f"peer down: 1st PUT waits out the dead lease and no longer (< {FIRST_WRITE_MS}ms)",
              first_put_ms < FIRST_WRITE_MS, f"{first_put_ms:.0f}ms")
        check(f"peer down: later PUTs are fast (< {FAST_MS}ms — dead peer left the wait set)",
              max(later_put_ms) < FAST_MS, ms(later_put_ms))
        # Reads never join that wait. During the roster freeze they are origin-served
        # (A cannot get its own lease confirmed while B is unreaped) — correct, and one
        # round trip rather than a barrier.
        check(f"peer down: GET is fast (< {FAST_MS}ms)", get_ms < FAST_MS, f"{get_ms:.0f}ms")
        check(f"peer down: LIST is fast (< {FAST_MS}ms)", list_ms < FAST_MS, f"{list_ms:.0f}ms")

        # Bring the peer back (fresh feed epoch); coherence must resume both ways.
        print(">>> restarting node B")
        b = start_b()
        assert h.wait_port(PORT_B), "restarted node did not bind"
        time.sleep(REJOIN_SETTLE_S)  # lease warm-up + bootstrap LIST
        pb = h.s3(f"http://127.0.0.1:{PORT_B}")

        pa.put_object(Bucket=BUCKET, Key="recovered", Body=b"back")
        check("after restart: B sees A's new write",
              h.poll(lambda: "recovered" in keys(pb), timeout=REJOIN_S) is not None)
        # B's writes arrive in a new epoch: A takes the gap (lease stands down, flush +
        # origin resync, affirm) and then converges — the loud-recovery path, end to end.
        pb.put_object(Bucket=BUCKET, Key="from-b", Body=b"epoch2")
        check("after restart: A sees B's write (new epoch, gap-resync path)",
              h.poll(lambda: "from-b" in keys(pa), timeout=GAP_RESYNC_S) is not None)
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
