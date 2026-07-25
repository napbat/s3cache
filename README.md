# s3cache

A transparent, **fully S3-compatible caching proxy**. Point any S3 client at it
instead of the real endpoint; it forwards to an upstream S3 (e.g. Cloudflare R2)
while cutting request costs for chatty clients.

It is a *service that speaks S3* — not a client library. Set the client's S3
endpoint to this proxy; no client code changes.

## What it does

- **LIST from an in-memory key index.** Because every request funnels through the
  proxy, it sees every write and maintains an authoritative key index — so
  `ListObjectsV2` is answered locally with **no upstream LIST call**. LISTs are the
  expensive S3 tier (R2 Class A), and clients that poll/list constantly (e.g. a
  log-structured store allocating slots) dominate the bill; this removes them.
- **Layered GET/HEAD object cache — hot / warm / cold.** Cacheable reads (no range / part
  / conditional headers) of objects up to `S3CACHE_MAX_OBJECT_BYTES` are served from a
  **hot** node-local in-memory LRU (`S3CACHE_CACHE_BYTES`) in front of an optional **warm**
  node-local *disk* cache (`S3CACHE_DISK_CACHE`), falling through to **cold** — the S3
  origin — on a miss. Always layered, no mode to pick. Ranged reads slice the cached whole
  object; HEAD is served from the same cache; larger objects stream straight through. See
  [Cache tiers](#cache-tiers).
- **Write-through + invalidation.** `PutObject` / `DeleteObject` / multipart /
  `CopyObject` forward to the upstream (which stays the authority for conditional/OCC
  writes — identical semantics), then update the index **and invalidate the object
  cache** for that key, so reads are never stale.
- **Full passthrough for everything else** — all 98 S3 operations are implemented
  (generated from the `s3s` S3 trait), so arbitrary S3 clients work, not just one.

Correctness rests on one property: the proxy is the *only* writer to the bucket, so
its index can't go stale. A restart just re-syncs the index (one LIST) and refills
on demand — correctness never depends on the cache surviving.

## Cache tiers

The body cache is always layered — there is no mode to select:

```
hot (in-memory, small)  ->  warm (node-local disk, large, optional)  ->  cold (S3 origin)
```

- **hot** — an in-memory LRU (`S3CACHE_CACHE_BYTES`), always on.
- **warm** — an optional node-local **disk** cache under `S3CACHE_DISK_CACHE`
  (`S3CACHE_DISK_CACHE_BYTES`). It's *inclusive* (every object written to hot is also
  written to disk) and **survives restarts** — a fresh pod re-indexes the on-disk files
  and comes up warm instead of stampeding the origin. Size it larger than hot to help.
  All disk ops are best-effort: an I/O error is a miss, never a data-plane failure.
- **cold** — the S3 origin.

Both tiers are node-local; cross-node coherence is handled separately (below): a peer's
write invalidates the local hot *and* disk copies, and strict reads barrier on feed heads.

## Cross-node coherence (gossip write feed)

By default each replica keeps its own LIST index, so running more than one would let
their indexes drift. Setting **`S3CACHE_GOSSIP_BIND`** (with `S3CACHE_GOSSIP_SEEDS` as
comma-separated `id=host:port` pairs) turns on the **gossip write feed** — groupnet's
consistency layer, no broker, no consensus service, no extra infrastructure:

- every durable write publishes one compact event (`put`/`del` + bucket/key/size/ts)
  into this node's feed, pushed to live peers at network latency;
- every replica runs an apply loop that folds peers' events into its own LIST index
  (per-key last-writer-wins; deletes win timestamp ties via tombstones) and drops the
  key from its local hot *and* disk copies.

The fast path stays in local memory (LIST is served from RAM; nothing remote is on the
read path). Loss is **detected, never silent**: a peer that falls behind the feed's ring,
or a peer restart, surfaces as a *gap* — the node flushes its local tiers and resyncs its
index from the origin, which is the authority the index caches. Only seeds need static
addressing; every other peer resolves through gossiped advertisements.

With the feed on, **multiple replicas are safe** — this lifts the historical
single-replica constraint with zero extra services.

## Consistency

Clients should never have to know s3cache exists. In the default mode they don't:

**`strong` (default).** Indistinguishable from talking to one S3 node, for *every*
client, no headers, no cooperation:

- **Writes wait for cluster-wide invalidation.** A write returns only after the origin
  acked it AND every currently-alive peer acknowledged applying its invalidation (an
  in-cluster ack round, ~2 gossip hops, riding behind the origin round-trip already
  paid). So `PUT via A; GET via B` is deterministically fresh — B applied the
  invalidation before A's PUT even returned.
- **Unhealthy nodes bench themselves.** A node whose membership view is not fully
  alive may be the partitioned one — writers can't reach it — so it serves
  cache-eligible reads via the origin until the view heals: slower, never stale. (This
  is also what bounds the writer's ack wait: a dying peer is excluded once SWIM marks
  it suspect.)
- **Honesty footnote:** the residual window is an *asymmetric* partition inside the
  SWIM probe window combined with an ack timeout (logged + counted, ~2s) — bounded and
  loud, never silent. The absolute arbiter for conflicting writers remains the origin:
  conditional `If-Match`/`If-None-Match` writes pass through untouched (OCC — no lost
  updates, regardless of node), and the index heals from the origin (gap resync,
  startup bootstrap). Cross-writer index races resolve by timestamp, deletes winning
  ties.

**`bounded` (`S3CACHE_CONSISTENCY=bounded`, set uniformly).** For clusters too large to
pay a per-write ack round: writes return on the origin ack, reads are fresh within ~one
push hop (the freshness barrier), and no ack-ledger traffic flows. Session tokens then
offer per-client strictness: every write response carries
`x-s3cache-write-token: <writer>:<epoch>:<seq>`; echoing it on a read as
`x-s3cache-read-token` barriers on that specific write, and an unverifiable token
routes the read to the origin — never silently downgraded. (Tokens work in `strong`
mode too; they're just redundant there.)
Requests the cache cannot reproduce faithfully — a specific `versionId`,
`ChecksumMode`, or SSE-C — bypass the cache and are served by the origin.

### Client cookbook: tokens and cross-node OCC

**Getting / using the session token** (opt-in; standard SDKs work fine without it):

```python
# boto3: capture each write's token, echo it on reads.
TOKEN = {"v": None}
def inject(request, **_kw):
    if TOKEN["v"]:
        request.headers["x-s3cache-read-token"] = TOKEN["v"]
cli.meta.events.register("before-send.s3", inject)

resp = cli.put_object(Bucket=b, Key=k, Body=data)
TOKEN["v"] = resp["ResponseMetadata"]["HTTPHeaders"]["x-s3cache-write-token"]
# every following read via ANY node now reflects that write, or is served
# by the origin — never stale, never silently downgraded
```

Zero-client-change alternative: session affinity at the load balancer — a client
pinned to one node reads its own writes locally by construction.

**Cross-node OCC (read-modify-write)** needs no token — conditional operations
bypass the cache and the *origin* arbitrates, so no update is ever lost:

1. `GET key` (any node) → body + ETag.
2. `PUT If-Match: <etag>` with the new value. `200` ⇒ you won (keep the write
   token for your session's reads).
3. `412` ⇒ someone else won: re-read fresh and retry. `GET If-None-Match:
   <your-stale-etag>` is guaranteed fresh in one shot (conditional GETs pass
   through to the origin); a plain re-read is fresh within ~1 RTT.
4. Create-only: `PUT If-None-Match: *` — exactly one creator across all nodes.

### Testing coherence and parity

- **Unit / protocol tests** (`cargo test`): fully in-process — real groupnet nodes over
  an in-memory transport, no external services. They cover peer-write index folding +
  hot invalidation, out-of-order LWW convergence (tombstones, delete-wins-ties,
  no-resurrection), the freshness barrier, and the flush path.
- **End-to-end** (`scripts/coherence-e2e.sh`): spins up MinIO and runs boto3
  harnesses against **real s3cache nodes** gossiping over loopback UDP. Needs
  podman/docker and `python3` + `boto3`.
  - `parity_e2e.py` — **differential parity**: every operation through the cache returns
    the same as talking to S3 directly (GET bytes/headers/metadata, HEAD, ranged GET, LIST
    with prefix/delimiter/pagination/`StartAfter`, conditional PUT & GET / OCC, DELETE,
    multipart, copy), plus cross-node reads.
  - `coherence_e2e.py` — a write on one node is seen by another (LIST, no-stale GET,
    DELETE, both directions).
  - `resilience_e2e.py` — a **peer outage is not a data-plane outage**: with its peer
    down, a node's PUT/GET/LIST stay correct and fast (the barrier never waits on a dead
    peer), and coherence resumes both ways after the peer restarts — including the
    fresh-epoch gap path (flush + origin resync), exercised end to end.

## Config (env)

| Var | Default | Meaning |
|---|---|---|
| `S3CACHE_LISTEN` | `0.0.0.0:8014` | S3 API listen address |
| `S3CACHE_UPSTREAM_ENDPOINT` | (required) | Upstream S3 endpoint URL (e.g. R2) |
| `S3CACHE_BUCKETS` | (empty) | Comma-separated buckets to index eagerly at startup |
| `S3CACHE_CACHE_BYTES` | `268435456` (256 MB) | Hot (in-memory) tier capacity (bytes) |
| `S3CACHE_MAX_OBJECT_BYTES` | `8388608` (8 MB) | Per-object cache cap; bigger objects stream through |
| `S3CACHE_DISK_CACHE` | (empty) | Directory for the warm disk tier; unset = no disk tier |
| `S3CACHE_DISK_CACHE_BYTES` | `10737418240` (10 GB) | Warm disk tier capacity (bytes) |
| `S3CACHE_GOSSIP_BIND` | (empty) | UDP bind for the gossip write feed (e.g. `0.0.0.0:7946`); unset = single-node, no coherence layer |
| `S3CACHE_GOSSIP_SEEDS` | (empty) | Comma-separated seed peers as `id=host:port`; only seeds need static addressing |
| `S3CACHE_GOSSIP_ADVERTISE` | bind addr | Address peers should dial back (set under NAT/container networking) |
| `S3CACHE_CONSISTENCY` | `strong` | `strong`: writes wait for cluster-wide invalidation, unhealthy nodes serve via origin. `bounded`: ~one-hop freshness, no per-write ack round (large clusters; set uniformly) |
| `S3CACHE_STATS_SECS` | `60` | Stats log interval (seconds) |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_REGION` | — | Upstream creds (R2: region `auto`) |

Clients authenticate to the proxy with the same key; the proxy re-signs to the
upstream.
