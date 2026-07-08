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
write invalidates the local hot *and* disk copies, and reads barrier on the commit log.

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

## Consistency

s3cache is **always strongly consistent** — indistinguishable from talking to S3
directly, including across nodes. There is no eventual/relaxed mode: an object store that
can return stale reads is a footgun, so it isn't offered.

- **Read-after-write, same node and across nodes.** A read served from node-local state —
  a `LIST` from the index, or a `GET`/`HEAD` of a hot body copy — first *barriers* on the
  commit log: it reads the stream tail and waits for this node's consumer to catch up
  before answering, so a peer's just-completed write is never read stale. A write completes
  only after its log event is appended, so any write ordered before the read is waited for.
  The shared `warm` tier is synchronously invalidated on write and is never stale, so it
  needs no barrier. Cost: one `XREVRANGE` + a usually-zero wait per cache-served read;
  Valkey stays off the query path (the index/body stay local, only the fence is remote).
  The barrier no-ops if Valkey is unreachable (see [resilience](#testing-coherence-and-parity)).
- **Conditional writes / OCC** are correct by construction — the origin is the authority,
  so a conditional `If-Match`/`If-None-Match` write is arbitrated at the origin and can
  never lose an update. Requests the cache cannot reproduce faithfully — a specific
  `versionId`, `ChecksumMode`, or SSE-C — bypass the cache and are served by the origin.

Single-node deployments (and any without the index log) are strongly consistent inherently
— the proxy is the sole writer.

### Testing coherence and parity

- **Unit / protocol tests** (`cargo test`): the coherence tests against a live Valkey run
  only when `S3CACHE_TEST_VALKEY_URL` is set, e.g.
  `S3CACHE_TEST_VALKEY_URL=redis://127.0.0.1:6379 cargo test`. They cover peer-write hot
  invalidation, the startup bootstrap/replay window, resume-from-position, ordering,
  multi-bucket convergence, and `MAXLEN` trimming.
- **End-to-end** (`scripts/coherence-e2e.sh`): spins up MinIO + Valkey and runs two boto3
  harnesses against **real s3cache nodes**. Needs podman/docker and `python3` + `boto3`.
  - `parity_e2e.py` — **differential parity**: every operation through the cache returns
    the same as talking to S3 directly (GET bytes/headers/metadata, HEAD, ranged GET, LIST
    with prefix/delimiter/pagination/`StartAfter`, conditional PUT & GET / OCC, DELETE,
    multipart, copy), plus cross-node reads.
  - `coherence_e2e.py` — a write on one node is seen by another (LIST, no-stale GET,
    DELETE, both directions).
  - `resilience_e2e.py` — a **Valkey outage is not a data-plane outage**: with Valkey
    down, PUT/GET/LIST stay correct and fast (served from the origin, no timeout stalls),
    and cross-node coherence resumes once Valkey recovers.

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
| `S3CACHE_VALKEY_URL` | (required for log) | Valkey/Redis URL for the index commit log, e.g. `redis://valkey:6379` |
| `S3CACHE_VALKEY_POOL` | `4` | Valkey connections per replica |
| `S3CACHE_INDEX_LOG` | `false` | Share the LIST index across replicas via a Valkey commit log (see [above](#cross-node-coherence-index-commit-log)) |
| `S3CACHE_INDEX_LOG_MAXLEN` | `1000000` | Approximate max entries kept in the log stream |
| `S3CACHE_INDEX_LOG_STREAM` | `s3cache:index:log` | Valkey stream key for the commit log |
| `S3CACHE_STATS_SECS` | `60` | Stats log interval (seconds) |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_REGION` | — | Upstream creds (R2: region `auto`) |

Clients authenticate to the proxy with the same key; the proxy re-signs to the
upstream.
