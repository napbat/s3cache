#!/usr/bin/env python3
"""End-to-end cross-node coherence test for s3cache.

Launches two real s3cache servers (node A and node B) in front of one shared S3
origin (MinIO) and one shared Valkey, with the index commit log enabled, then drives
them with boto3 to prove that a write on one node is seen by the other:

  1. PUT via A  -> B's index-served LIST shows the key   (index coherence)
  2. GET via B caches it; overwrite via A -> GET via B returns the NEW body
     (cross-node hot-cache invalidation — the anti-stale-read guarantee; this one
     cannot be explained by MinIO passthrough, only by the log invalidating B's copy)
  3. DELETE via A -> B's LIST loses the key
  4. reverse direction: PUT via B -> A's LIST shows it

Assumes MinIO and Valkey are reachable (see scripts/coherence-e2e.sh, which starts
them). Exits 0 on success, 1 on any failed assertion.
"""
import os
import socket
import subprocess
import sys
import time

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

BIN = os.environ.get("S3CACHE_BIN", "target/release/s3cache")
MINIO = os.environ.get("MINIO_ENDPOINT", "http://127.0.0.1:9000")
VALKEY = os.environ.get("VALKEY_URL", "redis://127.0.0.1:6379")
KEY = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
SECRET = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")
REGION = os.environ.get("AWS_REGION", "us-east-1")
BUCKET = os.environ.get("BUCKET", "coherence-test")
PORT_A, PORT_B = 18031, 18032

_boto_cfg = Config(s3={"addressing_style": "path"}, signature_version="s3v4",
                   retries={"max_attempts": 1})


def s3(endpoint):
    return boto3.client("s3", endpoint_url=endpoint, aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET, region_name=REGION, config=_boto_cfg)


def wait_port(port, timeout=15):
    deadline = time.time() + timeout
    while time.time() < deadline:
        with socket.socket() as s:
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(0.1)
    return False


def start_node(name, port):
    env = dict(os.environ,
               S3CACHE_LISTEN=f"127.0.0.1:{port}",
               S3CACHE_UPSTREAM_ENDPOINT=MINIO,
               S3CACHE_VALKEY_URL=VALKEY,
               S3CACHE_MODE="hot+warm",
               S3CACHE_INDEX_LOG="true",
               S3CACHE_BUCKETS=BUCKET,
               S3CACHE_STATS_SECS="2",
               AWS_ACCESS_KEY_ID=KEY, AWS_SECRET_ACCESS_KEY=SECRET, AWS_REGION=REGION,
               HOSTNAME=name, RUST_LOG="info")
    logf = open(f"/tmp/s3cache-{name}.log", "w")
    return subprocess.Popen([BIN], env=env, stdout=logf, stderr=subprocess.STDOUT)


def poll(fn, timeout=10):
    """Retry fn() until it returns truthy or timeout; swallow transient S3 errors."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            v = fn()
            if v:
                return v
        except ClientError:
            pass
        time.sleep(0.2)
    return None


def list_keys(cli):
    resp = cli.list_objects_v2(Bucket=BUCKET)
    return {o["Key"] for o in resp.get("Contents", [])}


def body(cli, key):
    return cli.get_object(Bucket=BUCKET, Key=key)["Body"].read()


def main():
    # Bucket must exist before the nodes start so each syncs (and index-serves) it.
    admin = s3(MINIO)
    try:
        admin.create_bucket(Bucket=BUCKET)
    except ClientError as e:
        if e.response["Error"]["Code"] not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            raise
    for k in list_keys(admin):
        admin.delete_object(Bucket=BUCKET, Key=k)

    node_a = start_node("nodeA", PORT_A)
    node_b = start_node("nodeB", PORT_B)
    failures = []
    try:
        assert wait_port(PORT_A) and wait_port(PORT_B), "nodes did not bind"
        time.sleep(1.5)  # let each node's (empty-bucket) index sync complete
        a, b = s3(f"http://127.0.0.1:{PORT_A}"), s3(f"http://127.0.0.1:{PORT_B}")

        def check(name, ok):
            print(f"  [{'PASS' if ok else 'FAIL'}] {name}")
            if not ok:
                failures.append(name)

        # 1. write on A is seen by B's index.
        a.put_object(Bucket=BUCKET, Key="k1", Body=b"v1")
        check("PUT via A -> LIST via B sees k1",
              poll(lambda: "k1" in list_keys(b)) is not None)

        # 2. B caches k1, A overwrites, B must NOT serve the stale body.
        check("GET via B returns v1", body(b, "k1") == b"v1")
        a.put_object(Bucket=BUCKET, Key="k1", Body=b"v2-overwritten")
        check("overwrite via A -> GET via B returns v2 (no stale hot read)",
              poll(lambda: body(b, "k1") == b"v2-overwritten") is not None)

        # 3. delete on A is seen by B.
        a.delete_object(Bucket=BUCKET, Key="k1")
        check("DELETE via A -> LIST via B loses k1",
              poll(lambda: "k1" not in list_keys(b)) is not None)

        # 4. reverse direction: write on B is seen by A.
        b.put_object(Bucket=BUCKET, Key="k2", Body=b"from-b")
        check("PUT via B -> LIST via A sees k2",
              poll(lambda: "k2" in list_keys(a)) is not None)

        time.sleep(2.5)  # let a post-traffic stats line print for the log-metric check
    except Exception as e:  # noqa: BLE001 - surface any harness error as a failure
        failures.append(f"harness error: {e}")
    finally:
        for p in (node_a, node_b):
            p.terminate()
        for p in (node_a, node_b):
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()

    if failures:
        print(f"\nFAILED ({len(failures)}): {failures}")
        print("node logs: /tmp/s3cache-nodeA.log /tmp/s3cache-nodeB.log")
        return 1
    print("\nALL CROSS-NODE COHERENCE CHECKS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
