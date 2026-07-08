#!/usr/bin/env python3
"""End-to-end cross-node coherence test for s3cache.

Launches two real s3cache nodes (A and B) in front of one shared S3 origin (MinIO) and
one shared Valkey, with the index commit log enabled, then proves a write on one node is
seen by the other:

  1. PUT via A  -> B's index-served LIST shows the key   (index coherence)
  2. GET via B caches it; overwrite via A -> GET via B returns the NEW body
     (cross-node hot-cache invalidation — the anti-stale-read guarantee; this cannot be
     explained by origin passthrough, only by the log invalidating B's copy)
  3. DELETE via A -> B's LIST loses the key
  4. reverse direction: PUT via B -> A's LIST shows it

Assumes MinIO and Valkey are reachable (see scripts/coherence-e2e.sh). Exits 0/1.
"""
import sys
import time

import _s3cache_e2e as h

BUCKET = "coherence-test"
PORT_A, PORT_B = 18031, 18032


def keys(cli):
    return {o["Key"] for o in cli.list_objects_v2(Bucket=BUCKET).get("Contents", [])}


def body(cli, key):
    return cli.get_object(Bucket=BUCKET, Key=key)["Body"].read()


def main():
    d = h.direct()
    h.reset_bucket(d, BUCKET)
    node_a = h.start_node("nodeA", PORT_A, BUCKET)
    node_b = h.start_node("nodeB", PORT_B, BUCKET)
    failures = []
    try:
        assert h.wait_port(PORT_A) and h.wait_port(PORT_B), "nodes did not bind"
        time.sleep(1.5)  # let each node's (empty-bucket) index sync complete
        a, b = h.s3(f"http://127.0.0.1:{PORT_A}"), h.s3(f"http://127.0.0.1:{PORT_B}")

        def check(name, ok):
            print(f"  [{'PASS' if ok else 'FAIL'}] {name}")
            if not ok:
                failures.append(name)

        a.put_object(Bucket=BUCKET, Key="k1", Body=b"v1")
        check("PUT via A -> LIST via B sees k1", h.poll(lambda: "k1" in keys(b)) is not None)

        check("GET via B returns v1", body(b, "k1") == b"v1")
        a.put_object(Bucket=BUCKET, Key="k1", Body=b"v2-overwritten")
        check("overwrite via A -> GET via B returns v2 (no stale hot read)",
              h.poll(lambda: body(b, "k1") == b"v2-overwritten") is not None)

        a.delete_object(Bucket=BUCKET, Key="k1")
        check("DELETE via A -> LIST via B loses k1", h.poll(lambda: "k1" not in keys(b)) is not None)

        b.put_object(Bucket=BUCKET, Key="k2", Body=b"from-b")
        check("PUT via B -> LIST via A sees k2", h.poll(lambda: "k2" in keys(a)) is not None)
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
