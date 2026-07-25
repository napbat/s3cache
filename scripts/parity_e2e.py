#!/usr/bin/env python3
"""Differential parity test: s3cache must behave the same as talking to S3 directly.

For each operation we perform it through a cache node and directly against the origin
(MinIO) and assert the client-visible result matches — bytes, headers, metadata, LIST
contents/pagination, ranged reads, conditional (OCC) writes/reads, multipart, copy — and
finally that a write on one node is seen identically on another (coherence).

Assumes MinIO + Valkey are reachable (see scripts/coherence-e2e.sh). Exits 0/1.
"""
import sys
import time

import _s3cache_e2e as h
from botocore.exceptions import ClientError

BUCKET = "parity-test"
PORT_P, PORT_Q = 18041, 18042


class Checks:
    def __init__(self):
        self.failed = []

    def ok(self, name, cond, detail=""):
        print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f"  ({detail})" if detail and not cond else ""))
        if not cond:
            self.failed.append(f"{name} {detail}".strip())

    def eq(self, name, a, b):
        self.ok(name, a == b, f"proxy={a!r} direct={b!r}")


def get_fields(cli, key, **kw):
    r = cli.get_object(Bucket=BUCKET, Key=key, **kw)
    return {
        "body": r["Body"].read(),
        "etag": r["ETag"],
        "ct": r.get("ContentType"),
        "len": r["ContentLength"],
        "meta": r.get("Metadata", {}),
        "status": r["ResponseMetadata"]["HTTPStatusCode"],
        "content_range": r.get("ContentRange"),
    }


def head_fields(cli, key):
    r = cli.head_object(Bucket=BUCKET, Key=key)
    return {"etag": r["ETag"], "ct": r.get("ContentType"), "len": r["ContentLength"], "meta": r.get("Metadata", {})}


def list_norm(cli, **kw):
    """Normalize a (possibly paginated) ListObjectsV2 into a comparable snapshot."""
    keys, prefixes, pages = [], [], 0
    token = None
    while True:
        args = dict(Bucket=BUCKET, **kw)
        if token:
            args["ContinuationToken"] = token
        r = cli.list_objects_v2(**args)
        pages += 1
        keys += [(o["Key"], o["Size"]) for o in r.get("Contents", [])]
        prefixes += [p["Prefix"] for p in r.get("CommonPrefixes", [])]
        if r.get("IsTruncated") and r.get("NextContinuationToken"):
            token = r["NextContinuationToken"]
        else:
            break
    return {"keys": keys, "prefixes": sorted(prefixes), "pages": pages}


def main():
    d = h.direct()
    h.reset_bucket(d, BUCKET)
    p_node = h.start_node("parityP", PORT_P, BUCKET, gossip_port=19035,
                          seeds="parityQ=127.0.0.1:19036")
    q_node = h.start_node("parityQ", PORT_Q, BUCKET, gossip_port=19036,
                          seeds="parityP=127.0.0.1:19035")
    c = Checks()
    try:
        assert h.wait_port(PORT_P) and h.wait_port(PORT_Q), "nodes did not bind"
        time.sleep(1.5)  # let the (empty-bucket) index sync finish -> nodes index-serve
        p, q = h.s3(f"http://127.0.0.1:{PORT_P}"), h.s3(f"http://127.0.0.1:{PORT_Q}")

        # --- PUT + GET: bytes, headers, metadata identical to direct -----------------
        p.put_object(Bucket=BUCKET, Key="obj", Body=b"hello world", ContentType="text/plain",
                     Metadata={"foo": "bar", "n": "42"})
        pf, df = get_fields(p, "obj"), get_fields(d, "obj")
        c.eq("GET body == direct", pf["body"], df["body"])
        c.eq("GET etag == direct", pf["etag"], df["etag"])
        c.eq("GET content-type == direct", pf["ct"], df["ct"])
        c.eq("GET content-length == direct", pf["len"], df["len"])
        c.eq("GET user-metadata == direct", pf["meta"], df["meta"])
        c.eq("GET cached (2nd hit) body == direct", get_fields(p, "obj")["body"], df["body"])

        # --- HEAD parity -------------------------------------------------------------
        c.eq("HEAD == direct", head_fields(p, "obj"), head_fields(d, "obj"))

        # --- ranged GET parity -------------------------------------------------------
        for rng in ("bytes=2-5", "bytes=6-", "bytes=-4"):
            pr, dr = get_fields(p, "obj", Range=rng), get_fields(d, "obj", Range=rng)
            c.eq(f"range {rng}: status", pr["status"], dr["status"])
            c.eq(f"range {rng}: body", pr["body"], dr["body"])
            c.eq(f"range {rng}: content-range", pr["content_range"], dr["content_range"])
        # range start past EOF must error the same way (416).
        c.eq("range past EOF: error code",
             range_err(p, "obj", "bytes=100-200"), range_err(d, "obj", "bytes=100-200"))

        # --- LIST parity: plain, prefix, delimiter, pagination -----------------------
        for i in range(7):
            p.put_object(Bucket=BUCKET, Key=f"list/a/{i}", Body=b"x" * i)
            p.put_object(Bucket=BUCKET, Key=f"list/b/{i}", Body=b"y")
        c.eq("LIST all == direct", list_norm(p)["keys"], list_norm(d)["keys"])
        c.eq("LIST prefix == direct", list_norm(p, Prefix="list/a/")["keys"], list_norm(d, Prefix="list/a/")["keys"])
        c.eq("LIST delimiter common-prefixes == direct",
             list_norm(p, Prefix="list/", Delimiter="/")["prefixes"],
             list_norm(d, Prefix="list/", Delimiter="/")["prefixes"])
        pl, dl = list_norm(p, MaxKeys=3), list_norm(d, MaxKeys=3)
        c.eq("LIST paginated keys == direct", pl["keys"], dl["keys"])
        c.ok("LIST paginated across multiple pages", pl["pages"] >= 2, f"pages={pl['pages']}")
        c.eq("LIST delimiter+paginated common-prefixes == direct",
             list_norm(p, Prefix="list/", Delimiter="/", MaxKeys=1)["prefixes"],
             list_norm(d, Prefix="list/", Delimiter="/", MaxKeys=1)["prefixes"])
        c.eq("LIST StartAfter == direct",
             list_norm(p, StartAfter="list/a/3")["keys"], list_norm(d, StartAfter="list/a/3")["keys"])

        # --- conditional PUT (OCC) parity: If-None-Match / If-Match ------------------
        c.eq("PUT If-None-Match=* twice -> 2nd fails same as direct",
             cond_put_ifnonematch(p, "occ-proxy"), cond_put_ifnonematch(d, "occ-direct"))
        c.eq("PUT If-Match parity (stale etag rejected, current accepted)",
             cond_put_ifmatch(p, "occm-proxy"), cond_put_ifmatch(d, "occm-direct"))
        # cond_put_* wrote the *-direct keys straight to the origin (behind the proxy),
        # which the proxy — the sole writer by design — never sees. Remove them so the
        # later full-bucket LIST comparison isn't polluted by keys only the origin knows.
        for k in ("occ-direct", "occm-direct"):
            d.delete_object(Bucket=BUCKET, Key=k)

        # --- checksum parity: ChecksumMode=ENABLED must match, even after caching ----
        p.put_object(Bucket=BUCKET, Key="ck", Body=b"checksummed", ChecksumAlgorithm="SHA256")
        _ = get_fields(p, "ck")  # prime the plain cache entry
        c.eq("GET ChecksumMode after caching == direct",
             p.get_object(Bucket=BUCKET, Key="ck", ChecksumMode="ENABLED").get("ChecksumSHA256"),
             d.get_object(Bucket=BUCKET, Key="ck", ChecksumMode="ENABLED").get("ChecksumSHA256"))

        # --- conditional GET parity: If-None-Match -> 304 ---------------------------
        etag = d.head_object(Bucket=BUCKET, Key="obj")["ETag"]
        c.eq("GET If-None-Match(current) -> 304 like direct",
             cond_get_status(p, "obj", etag), cond_get_status(d, "obj", etag))
        c.eq("GET If-None-Match(stale) -> 200 like direct",
             cond_get_status(p, "obj", '"deadbeef"'), cond_get_status(d, "obj", '"deadbeef"'))

        # --- DELETE parity: gone from GET and LIST ----------------------------------
        p.delete_object(Bucket=BUCKET, Key="obj")
        c.eq("GET after DELETE -> same error as direct", get_err(p, "obj"), get_err(d, "obj"))
        c.ok("LIST after DELETE drops key", ("obj", 11) not in list_norm(p)["keys"])

        # --- multipart parity --------------------------------------------------------
        part = b"m" * (5 * 1024 * 1024)
        mp_put(p, "multi", [part, b"tail"])
        c.eq("multipart GET body == direct", get_fields(p, "multi")["body"], get_fields(d, "multi")["body"])
        c.ok("multipart LIST size correct", ("multi", len(part) + 4) in list_norm(p)["keys"])

        # --- copy parity -------------------------------------------------------------
        p.copy_object(Bucket=BUCKET, Key="multi-copy", CopySource={"Bucket": BUCKET, "Key": "multi"})
        c.eq("copy GET body == direct", get_fields(p, "multi-copy")["body"], get_fields(d, "multi-copy")["body"])
        c.ok("copy LIST present", ("multi-copy", len(part) + 4) in list_norm(p)["keys"])

        # --- edge cases: zero-byte, oversize (streamed), odd keys, batch delete, 404 -
        p.put_object(Bucket=BUCKET, Key="empty", Body=b"")
        c.eq("zero-byte GET == direct", get_fields(p, "empty")["body"], get_fields(d, "empty")["body"])
        c.eq("zero-byte cached GET stays empty", get_fields(p, "empty")["body"], b"")
        c.ok("zero-byte listed with size 0", ("empty", 0) in list_norm(p)["keys"])

        big = b"Z" * (9 * 1024 * 1024)  # larger than the default 8 MiB per-object cap
        p.put_object(Bucket=BUCKET, Key="big", Body=big)
        c.eq("oversize GET (streamed) == direct", get_fields(p, "big")["body"], get_fields(d, "big")["body"])
        c.eq("oversize ranged GET == direct",
             get_fields(p, "big", Range="bytes=1000-2000")["body"],
             get_fields(d, "big", Range="bytes=1000-2000")["body"])

        for k in ["a key with spaces.txt", "uni/тест/文件.bin", "weird+=&chars.dat"]:
            p.put_object(Bucket=BUCKET, Key=k, Body=k.encode())
            c.eq(f"odd-key GET {k!r} == direct", get_fields(p, k)["body"], get_fields(d, k)["body"])
        c.eq("LIST incl. odd keys (ordered) == direct", list_norm(p)["keys"], list_norm(d)["keys"])

        p.put_object(Bucket=BUCKET, Key="del1", Body=b"1")
        p.put_object(Bucket=BUCKET, Key="del2", Body=b"2")
        p.delete_objects(Bucket=BUCKET, Delete={"Objects": [{"Key": "del1"}, {"Key": "del2"}]})
        listed = {k for k, _ in list_norm(p)["keys"]}
        c.ok("batch delete drops keys from LIST", "del1" not in listed and "del2" not in listed)
        c.eq("batch delete -> GET 404 like direct", get_err(p, "del1"), get_err(d, "del1"))

        c.eq("HEAD missing key == direct error", head_err(p, "nope"), head_err(d, "nope"))

        pe = p.put_object(Bucket=BUCKET, Key="petag", Body=b"etagcheck")["ETag"]
        c.eq("PUT ETag == origin ETag", pe, d.head_object(Bucket=BUCKET, Key="petag")["ETag"])

        # --- cross-node parity: write on P, read on Q, must equal direct -------------
        p.put_object(Bucket=BUCKET, Key="xn", Body=b"v1", ContentType="text/plain")
        c.ok("cross-node: Q LIST sees P's write",
             h.poll(lambda: ("xn", 2) in list_norm(q)["keys"]) is not None)
        c.eq("cross-node: Q GET == direct", get_fields(q, "xn")["body"], get_fields(d, "xn")["body"])
        # overwrite on P; Q must not serve the stale cached body.
        _ = get_fields(q, "xn")  # prime Q's cache
        p.put_object(Bucket=BUCKET, Key="xn", Body=b"v2-new")
        c.ok("cross-node: Q GET reflects P's overwrite (no stale)",
             h.poll(lambda: get_fields(q, "xn")["body"] == b"v2-new") is not None)
    except Exception as e:  # noqa: BLE001
        c.failed.append(f"harness error: {e!r}")
    finally:
        h.stop_nodes(p_node, q_node)

    if c.failed:
        print(f"\nFAILED ({len(c.failed)}):")
        for f in c.failed:
            print(f"  - {f}")
        print("node logs: /tmp/s3cache-parityP.log /tmp/s3cache-parityQ.log")
        return 1
    print("\nALL PARITY CHECKS PASSED (cache behaves like direct S3)")
    return 0


def range_err(cli, key, rng):
    try:
        cli.get_object(Bucket=BUCKET, Key=key, Range=rng)
        return "no-error"
    except ClientError as e:
        return h.err_code(e)


def get_err(cli, key):
    try:
        cli.get_object(Bucket=BUCKET, Key=key)
        return "no-error"
    except ClientError as e:
        return h.err_code(e)


def head_err(cli, key):
    try:
        cli.head_object(Bucket=BUCKET, Key=key)
        return "no-error"
    except ClientError as e:
        return h.err_code(e)


def cond_put_ifnonematch(cli, key):
    """Return (first_status, second_error): create-if-absent twice."""
    r1 = cli.put_object(Bucket=BUCKET, Key=key, Body=b"one", IfNoneMatch="*")
    first = r1["ResponseMetadata"]["HTTPStatusCode"]
    try:
        cli.put_object(Bucket=BUCKET, Key=key, Body=b"two", IfNoneMatch="*")
        return (first, "no-error")
    except ClientError as e:
        return (first, h.err_code(e))


def cond_put_ifmatch(cli, key):
    """Return (stale_rejected_code, current_accepted_status)."""
    etag = cli.put_object(Bucket=BUCKET, Key=key, Body=b"v1")["ETag"]
    try:
        cli.put_object(Bucket=BUCKET, Key=key, Body=b"bad", IfMatch='"00000000000000000000000000000000"')
        stale = "no-error"
    except ClientError as e:
        stale = h.err_code(e)
    ok = cli.put_object(Bucket=BUCKET, Key=key, Body=b"v2", IfMatch=etag)["ResponseMetadata"]["HTTPStatusCode"]
    return (stale, ok)


def cond_get_status(cli, key, etag):
    try:
        r = cli.get_object(Bucket=BUCKET, Key=key, IfNoneMatch=etag)
        return r["ResponseMetadata"]["HTTPStatusCode"]
    except ClientError as e:
        return h.err_code(e)[1]  # 304


def mp_put(cli, key, parts):
    up = cli.create_multipart_upload(Bucket=BUCKET, Key=key)["UploadId"]
    done = []
    for i, part in enumerate(parts, 1):
        etag = cli.upload_part(Bucket=BUCKET, Key=key, UploadId=up, PartNumber=i, Body=part)["ETag"]
        done.append({"ETag": etag, "PartNumber": i})
    cli.complete_multipart_upload(Bucket=BUCKET, Key=key, UploadId=up, MultipartUpload={"Parts": done})


if __name__ == "__main__":
    sys.exit(main())
