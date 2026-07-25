#!/usr/bin/env python3
"""End-to-end cross-node coherence test for s3cache.

Launches two real s3cache nodes (A and B) in front of one shared S3 origin (MinIO) and
gossiping over loopback UDP, then proves a write on one node is
seen by the other:

  1. PUT via A  -> B's index-served LIST shows the key   (index coherence)
  2. GET via B caches it; overwrite via A -> GET via B returns the NEW body
     (cross-node hot-cache invalidation — the anti-stale-read guarantee; this cannot be
     explained by origin passthrough, only by the feed invalidating B's copy)
  3. DELETE via A -> B's LIST loses the key
  4. reverse direction: PUT via B -> A's LIST shows it

The main checks use PLAIN clients — no tokens, no headers: in the default strong
mode a write returns only after every peer applied its invalidation, so the no-poll
assertions are guaranteed for clients that don't know s3cache exists. The forged-token
check at the end exercises the token machinery's origin fallback.

Assumes MinIO is reachable (see scripts/coherence-e2e.sh). Exits 0/1.
"""
import os
import shutil
import sys
import time

import _s3cache_e2e as h

BUCKET = "coherence-test"
PORT_A, PORT_B = 18031, 18032

# The most recent write's session token; tokenized clients echo it on reads.
TOKEN = {"v": None}


def tokenized(endpoint):
    """An S3 client that echoes the latest write token on every request."""
    cli = h.s3(endpoint)

    def inject(request, **_kw):
        if TOKEN["v"]:
            request.headers["x-s3cache-read-token"] = TOKEN["v"]

    cli.meta.events.register("before-send.s3", inject)
    return cli


def keys(cli):
    return {o["Key"] for o in cli.list_objects_v2(Bucket=BUCKET).get("Contents", [])}


def body(cli, key):
    return cli.get_object(Bucket=BUCKET, Key=key)["Body"].read()


def main():
    d = h.direct()
    h.reset_bucket(d, BUCKET)
    node_a = h.start_node("nodeA", PORT_A, BUCKET, gossip_port=19031,
                          seeds="nodeB=127.0.0.1:19032")
    node_b = h.start_node("nodeB", PORT_B, BUCKET, gossip_port=19032,
                          seeds="nodeA=127.0.0.1:19031")
    failures = []
    try:
        assert h.wait_port(PORT_A) and h.wait_port(PORT_B), "nodes did not bind"
        time.sleep(1.5)  # let each node's (empty-bucket) index sync complete
        a, b = h.s3(f"http://127.0.0.1:{PORT_A}"), h.s3(f"http://127.0.0.1:{PORT_B}")

        def check(name, ok):
            print(f"  [{'PASS' if ok else 'FAIL'}] {name}")
            if not ok:
                failures.append(name)

        # Reads echo each write's session token, so every cross-node read must reflect
        # the peer's write *immediately* — asserted with NO poll (the token barrier does
        # the waiting).
        resp = a.put_object(Bucket=BUCKET, Key="k1", Body=b"v1")
        check("PUT response carries a write token",
              bool(resp["ResponseMetadata"]["HTTPHeaders"].get("x-s3cache-write-token")))
        check("PUT via A -> LIST via B sees k1 (strong: write-ack, no poll)", "k1" in keys(b))

        check("GET via B returns v1", body(b, "k1") == b"v1")  # primes B's hot copy
        a.put_object(Bucket=BUCKET, Key="k1", Body=b"v2-overwritten")
        check("overwrite via A -> GET via B returns v2 (strong: write-ack, no stale hot)",
              body(b, "k1") == b"v2-overwritten")

        a.delete_object(Bucket=BUCKET, Key="k1")
        check("DELETE via A -> LIST via B loses k1 (strong: write-ack, no poll)", "k1" not in keys(b))

        b.put_object(Bucket=BUCKET, Key="k2", Body=b"from-b")
        check("PUT via B -> LIST via A sees k2 (strong: write-ack, no poll)", "k2" in keys(a))

        # An unsatisfiable token must route the read to the origin: slower
        # (barrier timeout) but correct — a token read is never downgraded.
        at = tokenized(f"http://127.0.0.1:{PORT_A}")
        TOKEN["v"] = "ghost:999:999"
        t0 = time.time()
        check("forged token: GET still returns correct bytes (origin fallback)",
              body(at, "k2") == b"from-b")
        check("forged token: bounded latency (< 4s)", time.time() - t0 < 4)
        TOKEN["v"] = None

        # With the disk (warm) tier on, a peer's overwrite must invalidate the *disk* copy
        # too, not just hot — else a hot-evicted-but-disk-cached object goes stale.
        dc, dd_dir = f"/tmp/s3cache-diskC-{os.getpid()}", f"/tmp/s3cache-diskD-{os.getpid()}"
        for d in (dc, dd_dir):
            shutil.rmtree(d, ignore_errors=True)
        c_node = h.start_node("nodeC", 18033, BUCKET, gossip_port=19033,
                              seeds="nodeD=127.0.0.1:19034",
                              extra={"S3CACHE_DISK_CACHE": dc, "S3CACHE_DISK_CACHE_BYTES": "104857600"})
        d_node = h.start_node("nodeD", 18034, BUCKET, gossip_port=19034,
                              seeds="nodeC=127.0.0.1:19033",
                              extra={"S3CACHE_DISK_CACHE": dd_dir, "S3CACHE_DISK_CACHE_BYTES": "104857600"})
        node_c, node_d = c_node, d_node
        try:
            assert h.wait_port(18033) and h.wait_port(18034)
            time.sleep(1.5)
            c, dd = h.s3("http://127.0.0.1:18033"), h.s3("http://127.0.0.1:18034")
            c.put_object(Bucket=BUCKET, Key="w", Body=b"one")
            check("disk tier: D LIST sees C's write (strong: write-ack, no poll)", "w" in keys(dd))
            _ = body(dd, "w")  # D reads it -> now in D's hot AND disk tiers
            c.put_object(Bucket=BUCKET, Key="w", Body=b"two")  # C overwrites
            check("disk tier: D GET reflects C's overwrite (hot+disk invalidated, no poll)",
                  body(dd, "w") == b"two")
        finally:
            h.stop_nodes(node_c, node_d)
            for d in (dc, dd_dir):
                shutil.rmtree(d, ignore_errors=True)
    except Exception as e:  # noqa: BLE001
        failures.append(f"harness error: {e!r}")
    finally:
        h.stop_nodes(node_a, node_b)

    if failures:
        print(f"\nFAILED ({len(failures)}): {failures}")
        print("node logs: /tmp/s3cache-nodeA.log /tmp/s3cache-nodeB.log")
        return 1
    print("\nALL CROSS-NODE COHERENCE CHECKS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
