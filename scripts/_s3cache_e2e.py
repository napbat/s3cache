"""Shared helpers for the s3cache end-to-end tests.

Config from env, boto3 S3 clients, and launching real s3cache nodes in front of a
shared MinIO (the S3 origin), coherent over the gossip write feed. Imported by
coherence_e2e.py, parity_e2e.py and resilience_e2e.py.
"""
import os

# botocore >= 1.36 asks for response checksum validation on every GetObject
# (`x-amz-checksum-mode: ENABLED`), and s3cache sends a checksum-mode GET straight to the
# origin on purpose — the checksum is the origin's to compute, and a locally served answer
# would have to invent one. Correct, and it also means such a read never touches the body
# cache: with the botocore default, NO boto3 read in any of these tests is cache-eligible,
# so every claim about caching (a hit, a fill, a peer's copy going stale) passes for the
# wrong reason. It belongs here, in the one module every test imports before it builds a
# client, rather than in whichever test last noticed. Older botocore has no such default
# and ignores it.
os.environ.setdefault("AWS_RESPONSE_CHECKSUM_VALIDATION", "when_required")

import socket  # noqa: E402  (the line above has to run before boto3 builds a client)
import subprocess  # noqa: E402
import time  # noqa: E402
import urllib.request  # noqa: E402

import boto3  # noqa: E402
from botocore.client import Config  # noqa: E402
from botocore.exceptions import ClientError  # noqa: E402

BIN = os.environ.get("S3CACHE_BIN", "target/release/s3cache")
MINIO = os.environ.get("MINIO_ENDPOINT", "http://127.0.0.1:9000")
KEY = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
SECRET = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")
REGION = os.environ.get("AWS_REGION", "us-east-1")

_CFG = Config(s3={"addressing_style": "path"}, signature_version="s3v4",
              retries={"max_attempts": 1})


def s3(endpoint):
    """An S3 client pointed at `endpoint` (a proxy node or MinIO itself)."""
    return boto3.client("s3", endpoint_url=endpoint, aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET, region_name=REGION, config=_CFG)


def direct():
    """A client that talks straight to the S3 origin (the ground truth to match)."""
    return s3(MINIO)


def wait_port(port, timeout=15):
    deadline = time.time() + timeout
    while time.time() < deadline:
        with socket.socket() as sk:
            if sk.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(0.1)
    return False


def start_node(name, port, bucket, gossip_port=None, seeds="", extra=None):
    """Launch one s3cache node. `gossip_port` binds the write feed (UDP);
    `seeds` is the comma-separated `id=host:port` peer list."""
    env = dict(os.environ,
               S3CACHE_LISTEN=f"127.0.0.1:{port}",
               S3CACHE_UPSTREAM_ENDPOINT=MINIO,
               S3CACHE_BUCKETS=bucket,
               S3CACHE_STATS_SECS="3600",
               AWS_ACCESS_KEY_ID=KEY, AWS_SECRET_ACCESS_KEY=SECRET, AWS_REGION=REGION,
               HOSTNAME=name, RUST_LOG="warn")
    if gossip_port:
        env.update(S3CACHE_GOSSIP_BIND=f"127.0.0.1:{gossip_port}",
                   S3CACHE_GOSSIP_ADVERTISE=f"127.0.0.1:{gossip_port}",
                   S3CACHE_GOSSIP_SEEDS=seeds)
    if extra:
        env.update(extra)
    logf = open(f"/tmp/s3cache-{name}.log", "w")
    return subprocess.Popen([BIN], env=env, stdout=logf, stderr=subprocess.STDOUT)


def stop_nodes(*procs):
    for p in procs:
        p.terminate()
    for p in procs:
        try:
            p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            p.kill()


def reset_bucket(cli, bucket):
    """Create `bucket` if needed and empty it (incl. any in-progress multipart uploads)."""
    try:
        cli.create_bucket(Bucket=bucket)
    except ClientError as e:
        if e.response["Error"]["Code"] not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            raise
    for o in cli.list_objects_v2(Bucket=bucket).get("Contents", []):
        cli.delete_object(Bucket=bucket, Key=o["Key"])
    for u in cli.list_multipart_uploads(Bucket=bucket).get("Uploads", []):
        cli.abort_multipart_upload(Bucket=bucket, Key=u["Key"], UploadId=u["UploadId"])


def counters(port):
    """One node's Prometheus counters as {name: int} (the `s3cache_` prefix stripped),
    scraped the way Prometheus would. Needs `S3CACHE_METRICS_LISTEN` on that node.

    Which cache path answered a read is invisible from the S3 API — a correct answer is
    correct either way — so a test that asserts on caching at all has to ask here."""
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as resp:
        text = resp.read().decode()
    out = {}
    for line in text.splitlines():
        if line.startswith("s3cache_"):
            name, _, value = line.partition(" ")
            out[name[len("s3cache_"):]] = int(value)
    return out


def poll(fn, timeout=10):
    """Retry fn() until truthy or timeout; swallow transient S3 errors."""
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


def err_code(e):
    """The S3 error code / HTTP status from a boto3 ClientError, for parity comparison."""
    if isinstance(e, ClientError):
        return (e.response["Error"].get("Code"),
                e.response["ResponseMetadata"].get("HTTPStatusCode"))
    return (type(e).__name__, None)
