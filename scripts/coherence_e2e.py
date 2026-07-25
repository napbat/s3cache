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

Every write captures its `x-s3cache-write-token` response header and every read echoes
it as `x-s3cache-read-token`, so the no-poll assertions are *guaranteed* strict
read-after-write (the token barrier), not gossip-timing luck.

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


def write(cli, key, data):
    """PUT and capture the write's session token for subsequent reads."""
    resp = cli.put_object(Bucket=BUCKET, Key=key, Body=data)
    TOKEN["v"] = resp["ResponseMetadata"]["HTTPHeaders"].get("x-s3cache-write-token")
    return TOKEN["v"]


def delete(cli, key):
    resp = cli.delete_object(Bucket=BUCKET, Key=key)
    TOKEN["v"] = resp["ResponseMetadata"]["HTTPHeaders"].get("x-s3cache-write-token")


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
        a, b = tokenized(f"http://127.0.0.1:{PORT_A}"), tokenized(f"http://127.0.0.1:{PORT_B}")

        def check(name, ok):
            print(f"  [{'PASS' if ok else 'FAIL'}] {name}")
            if not ok:
                failures.append(name)

        # Reads echo each write's session token, so every cross-node read must reflect
        # the peer's write *immediately* — asserted with NO poll (the token barrier does
        # the waiting).
        token = write(a, "k1", b"v1")
        check("PUT response carries a write token", bool(token))
        check("PUT via A -> LIST via B sees k1 (token barrier, no poll)", "k1" in keys(b))

        check("GET via B returns v1", body(b, "k1") == b"v1")  # primes B's hot copy
        write(a, "k1", b"v2-overwritten")
        check("overwrite via A -> GET via B returns v2 (token barrier, no stale hot)",
              body(b, "k1") == b"v2-overwritten")

        delete(a, "k1")
        check("DELETE via A -> LIST via B loses k1 (token barrier, no poll)", "k1" not in keys(b))

        write(b, "k2", b"from-b")
        check("PUT via B -> LIST via A sees k2 (token barrier, no poll)", "k2" in keys(a))

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
            c, dd = tokenized("http://127.0.0.1:18033"), tokenized("http://127.0.0.1:18034")
            write(c, "w", b"one")
            check("disk tier: D LIST sees C's write (token barrier, no poll)", "w" in keys(dd))
            _ = body(dd, "w")  # D reads it -> now in D's hot AND disk tiers
            write(c, "w", b"two")  # C overwrites
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
