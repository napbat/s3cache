"""Shared helpers for the s3cache end-to-end tests.

Config from env, boto3 S3 clients, and launching real s3cache nodes in front of a shared
MinIO (the S3 origin) and Valkey. Imported by coherence_e2e.py and parity_e2e.py.
"""
import os
import socket
import subprocess
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


def start_node(name, port, bucket, index_log=True, extra=None):
    env = dict(os.environ,
               S3CACHE_LISTEN=f"127.0.0.1:{port}",
               S3CACHE_UPSTREAM_ENDPOINT=MINIO,
               S3CACHE_VALKEY_URL=VALKEY,
               S3CACHE_INDEX_LOG="true" if index_log else "false",
               S3CACHE_BUCKETS=bucket,
               S3CACHE_STATS_SECS="3600",
               AWS_ACCESS_KEY_ID=KEY, AWS_SECRET_ACCESS_KEY=SECRET, AWS_REGION=REGION,
               HOSTNAME=name, RUST_LOG="warn")
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
