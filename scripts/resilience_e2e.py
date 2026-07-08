#!/usr/bin/env python3
"""Resilience test: a Valkey (cache) outage must not degrade the S3 data plane.

With Valkey down, s3cache must keep serving PUT/GET/LIST correctly and *fast* (from the
origin) — a cache outage is not a data-plane outage — and cross-node coherence must
resume once Valkey recovers.

Controls the Valkey container itself, so it only runs when VALKEY_CONTAINER (and RUNTIME,
default `podman`) are set; otherwise it skips. Exits 0/1.
"""
import os
import subprocess
import sys
import time

import _s3cache_e2e as h

BUCKET = "resilience-test"
PORT_A, PORT_B = 18071, 18072
RUNTIME = os.environ.get("RUNTIME", "podman")
CONTAINER = os.environ.get("VALKEY_CONTAINER")


def valkey(action):
    subprocess.run([RUNTIME, action, CONTAINER], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)


def keys(cli):
    return {o["Key"] for o in cli.list_objects_v2(Bucket=BUCKET).get("Contents", [])}


def timed(fn):
    s = time.time()
    fn()
    return (time.time() - s) * 1000.0


def main():
    if not CONTAINER:
        print("skip resilience_e2e: set VALKEY_CONTAINER (and RUNTIME) to run")
        return 0

    d = h.direct()
    h.reset_bucket(d, BUCKET)
    a = h.start_node("resA", PORT_A, BUCKET)
    b = h.start_node("resB", PORT_B, BUCKET)
    fails = []

    def check(name, ok, detail=""):
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  ({detail})" if detail and not ok else ""))
        if not ok:
            fails.append(name)

    try:
        assert h.wait_port(PORT_A) and h.wait_port(PORT_B), "nodes did not bind"
        time.sleep(1.5)
        pa, pb = h.s3(f"http://127.0.0.1:{PORT_A}"), h.s3(f"http://127.0.0.1:{PORT_B}")

        # Baseline: coherence works while Valkey is up.
        pa.put_object(Bucket=BUCKET, Key="base", Body=b"1")
        check("baseline: B sees A's write (Valkey up)", h.poll(lambda: "base" in keys(pb)) is not None)

        # Kill Valkey; the data plane must stay correct AND fast.
        print(">>> stopping Valkey")
        valkey("stop")
        time.sleep(0.5)

        put_ms = timed(lambda: pa.put_object(Bucket=BUCKET, Key="down", Body=b"payload"))
        get_ms = timed(lambda: pa.get_object(Bucket=BUCKET, Key="down")["Body"].read())
        list_ms = timed(lambda: keys(pa))

        check("Valkey down: GET returns correct bytes == direct",
              pa.get_object(Bucket=BUCKET, Key="down")["Body"].read()
              == d.get_object(Bucket=BUCKET, Key="down")["Body"].read())
        check("Valkey down: LIST works", "down" in keys(pa))
        check("Valkey down: PUT is fast, not the 2s timeout (< 500ms)", put_ms < 500, f"{put_ms:.0f}ms")
        check("Valkey down: GET is fast (< 500ms)", get_ms < 500, f"{get_ms:.0f}ms")
        check("Valkey down: LIST is fast (< 500ms)", list_ms < 500, f"{list_ms:.0f}ms")

        # Bring Valkey back; coherence must resume.
        print(">>> starting Valkey")
        valkey("start")
        time.sleep(2.0)  # reconnect
        pa.put_object(Bucket=BUCKET, Key="recovered", Body=b"back")
        check("after recovery: B sees A's new write again",
              h.poll(lambda: "recovered" in keys(pb), timeout=15) is not None)
    except Exception as e:  # noqa: BLE001
        fails.append(f"harness error: {e!r}")
    finally:
        h.stop_nodes(a, b)
        valkey("start")  # leave it running for any following steps

    if fails:
        print(f"\nFAILED ({len(fails)}): {fails}")
        return 1
    print("\nALL RESILIENCE CHECKS PASSED (cache outage != data-plane outage)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
