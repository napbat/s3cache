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
- **Tiered GET/HEAD object cache — hot / warm / cold.** Cacheable reads (no range / part
  / conditional headers) of objects up to `S3CACHE_MAX_OBJECT_BYTES` are served from a
  **hot** node-local LRU (`S3CACHE_CACHE_BYTES` total) and/or a **warm** shared Valkey
  tier, falling through to **cold** — the S3 origin — on a miss. Ranged reads slice the
  cached whole object; HEAD is served from the same cache. `S3CACHE_MODE` picks the
  tiers; larger objects stream straight through. See [Cache modes](#cache-modes).
- **Write-through + invalidation.** `PutObject` / `DeleteObject` / multipart /
  `CopyObject` forward to the upstream (which stays the authority for conditional/OCC
  writes — identical semantics), then update the index **and invalidate the object
  cache** for that key, so reads are never stale.
- **Full passthrough for everything else** — all 98 S3 operations are implemented
  (generated from the `s3s` S3 trait), so arbitrary S3 clients work, not just one.

Correctness rests on one property: the proxy is the *only* writer to the bucket, so
its index can't go stale. A restart just re-syncs the index (one LIST) and refills
on demand — correctness never depends on the cache surviving.

## Cache modes

`S3CACHE_MODE` selects which tiers sit in front of the cold S3 origin:

| Mode | Tiers | Use it when |
|---|---|---|
| `off` | none | You only want LIST-from-index; bodies pass straight through. |
| `hot` (default) | node-local heap | Single replica; lowest latency. |
| `warm` | shared Valkey | Multiple replicas that must agree — no node-local copy to drift. |
| `hot+warm` | heap → Valkey → origin | A fast local cache backed by a shared one. |

The **warm** tier is a Valkey/Redis instance shared by every replica, so a body cached
or invalidated on one node is visible to all of them — there is no peer-to-peer chatter,
Valkey is the rendezvous. Warm operations are best-effort: if Valkey is slow or down, the
request falls through to the origin, so a cache outage never becomes a data-plane outage.

## Cross-node coherence (index commit log)

By default each replica keeps its own LIST index, so running more than one would let
their indexes drift. Setting **`S3CACHE_INDEX_LOG=true`** (with `S3CACHE_VALKEY_URL`)
turns on a shared, ordered **commit log** — a Valkey Stream — that fixes this:

- every write appends one compact event (`put`/`del` + bucket/key/size) to the stream;
- every replica runs a background consumer that tails the stream and applies peers'
  events to its own local index and drops the key from its local hot cache.

So the fast path stays in local memory (LIST is still served from RAM, no per-request
Valkey round-trip) while writes on any node reach all nodes. The log is **replayable** —
each node tracks its position and resumes after a reconnect, so it can't miss an event
the way fire-and-forget pub/sub can. On startup a node captures the stream tail, does its
one full LIST bootstrap, then replays from that point (re-applying is idempotent), so no
write slips through the gap. The stream is capped (`S3CACHE_INDEX_LOG_MAXLEN`, approximate
`MAXLEN` trimming). Appends are best-effort with a timeout: if Valkey is down a write
still succeeds and peers re-converge on their next restart/bootstrap.

With the index log on, **multiple replicas are safe** — this is the mechanism that lifts
the historical single-replica constraint. It works with any body-cache mode (e.g. `hot`
bodies + a coherent shared index).

Roadmap: **OCC** (atomic read-modify-write) on top of the log — the stream is already the
linearization order, so a conditional write becomes "append if the version is unchanged."
The same log is the durability journal for future write-back coalescing.

## Config (env)

| Var | Default | Meaning |
|---|---|---|
| `S3CACHE_LISTEN` | `0.0.0.0:8014` | S3 API listen address |
| `S3CACHE_UPSTREAM_ENDPOINT` | (required) | Upstream S3 endpoint URL (e.g. R2) |
| `S3CACHE_BUCKETS` | (empty) | Comma-separated buckets to index eagerly at startup |
| `S3CACHE_MODE` | `hot` | Cache tiers: `off` / `hot` / `warm` / `hot+warm` (see [Cache modes](#cache-modes)) |
| `S3CACHE_CACHE_BYTES` | `268435456` (256 MB) | Hot-tier capacity (bytes) |
| `S3CACHE_MAX_OBJECT_BYTES` | `8388608` (8 MB) | Per-object cache cap; bigger objects stream through |
| `S3CACHE_VALKEY_URL` | (required for warm/log) | Valkey/Redis URL, e.g. `redis://valkey:6379`; shared by the warm tier and the index log |
| `S3CACHE_VALKEY_POOL` | `4` | Valkey connections per replica |
| `S3CACHE_WARM_TTL_SECS` | `0` | Warm entry TTL in seconds; `0` = keep until invalidated/evicted |
| `S3CACHE_INDEX_LOG` | `false` | Share the LIST index across replicas via a Valkey commit log (see [above](#cross-node-coherence-index-commit-log)) |
| `S3CACHE_INDEX_LOG_MAXLEN` | `1000000` | Approximate max entries kept in the log stream |
| `S3CACHE_INDEX_LOG_STREAM` | `s3cache:index:log` | Valkey stream key for the commit log |
| `S3CACHE_STATS_SECS` | `60` | Stats log interval (seconds) |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_REGION` | — | Upstream creds (R2: region `auto`) |

Clients authenticate to the proxy with the same key; the proxy re-signs to the
upstream.
