# s3cache

A transparent, **fully S3-compatible caching proxy**. Point any S3 client at it
instead of the real endpoint; it forwards to an upstream S3 (e.g. Cloudflare R2)
while cutting request costs for chatty clients.

It is a *service that speaks S3* — not a client library. Set the client's S3
endpoint to this proxy; no client code changes.

> ## ⚠️ DO NOT SHIP THIS TREE — it does not build anywhere but one workstation
>
> `Cargo.toml` carries a `[patch."https://github.com/napbat/groupnet"]` section pointing
> at a **local** `../groupnet` checkout. The lease tier this branch is built on
> (`consistency-leases`, groupnet milestones M0–M6) is not pushed to GitHub yet, so
> **every non-local build is structurally broken**: CI, the Dockerfile image build, and
> anyone cloning this repo resolve `../groupnet` and find nothing.
>
> Release order, no steps skipped:
>
> 1. push groupnet `main` to `github.com/napbat/groupnet`;
> 2. delete the `[patch]` section from `Cargo.toml`;
> 3. `cargo update -p groupnet`;
> 4. run the whole gate — `cargo build`, `cargo test`, `cargo clippy --all-targets`,
>    `cargo fmt --check`, `helm lint`, `helm template`.
>
> Until step 2 lands, treat this branch as unmergeable and unreleasable.

## What it does

- **LIST and HEAD from an in-memory key index.** Because every request funnels through
  the proxy, it sees every write and maintains an authoritative key index — so
  `ListObjectsV2` is answered locally with **no upstream LIST call**. LISTs are the
  expensive S3 tier (R2 Class A), and clients that poll/list constantly (e.g. a
  log-structured store allocating slots) dominate the bill; this removes them. The same
  index answers `HeadObject` for a synced bucket — an immediate 404 for a key it does not
  hold, and the full record for one it does — so a per-key existence probe (the class-B
  volume driver) costs no upstream call either, cached body or not. An entry only answers
  a HEAD once it is *faithful*: a bootstrap LIST row or a peer's gossiped write proves the
  key exists and carries what LIST reports, but not the `Content-Type` or `x-amz-meta-*`
  a HEAD does, so the first HEAD of such a key is forwarded once and its answer completes
  the entry in place (`index_backfills`). Nothing local is ever served that would differ
  from the origin — see [Testing coherence and parity](#testing-coherence-and-parity).
- **Layered GET/HEAD object cache — hot / warm / cold.** Cacheable reads (no range / part
  / conditional headers) of objects up to `S3CACHE_MAX_OBJECT_BYTES` are served from a
  **hot** node-local in-memory LRU (`S3CACHE_CACHE_BYTES`) in front of an optional **warm**
  node-local *disk* cache (`S3CACHE_DISK_CACHE`), falling through to **cold** — the S3
  origin — on a miss. Always layered, no mode to pick. Ranged reads slice the cached whole
  object; HEAD is served from the same cache; larger objects stream straight through. See
  [Cache tiers](#cache-tiers).
- **Write-through + invalidation, and fill-on-write.** `PutObject` / `DeleteObject` /
  multipart / `CopyObject` forward to the upstream (which stays the authority for
  conditional/OCC writes — identical semantics), then update the index **and invalidate
  the object cache** for that key, so reads are never stale. A `PutObject` goes one
  further: its body is already in hand, so the writing node **keeps it** (`write_fill`)
  instead of leaving the object's first read to be a guaranteed origin GET. Peers are
  still invalidated over the write feed — only the writer, which knows the new bytes, is
  refilled. The fill is taken only when the write knows exactly what a read of it will
  report: a `Content-Type` (without one the origin invents one), an `ETag` on the write
  response, a body within `S3CACHE_MAX_OBJECT_BYTES`, and none of the request forms whose
  stored object or response headers a kept copy could not reproduce (SSE-C, an append, a
  named storage class, object lock, tagging, a website redirect, `Expires`). Everything
  else — and every refused write — behaves exactly as before.
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

**`strong` (default) — lease-backed.** Indistinguishable from talking to one S3 node,
for *every* client, no headers, no cooperation. The mechanism is a **coherence lease**
(Gray–Cheriton freshness leases, groupnet's T3 tier), and it replaces the view-stability
heuristic earlier releases used:

- **A node may serve locally only while it holds a lease.** Every node continuously
  renews a *serve-lease* (one small gossip entry every `D/3`), and every other node
  grants the renewals it has adopted. A node answers a LIST from its index, a GET from
  its cache, or an authoritative 404 **only** while a lease confirmed by every peer
  covers this instant. It is one mechanism for all three questions, not three
  heuristics — and `false` covers everything: booting, warming up, lapsed, awaiting a
  resync, a peer gone silent, a partition. Every one of them sends the read to the
  origin: slower, never stale (`unhealthy_bypasses`).
- **Writes end at an ack *or* at a lapse.** A write returns only after the origin acked
  it AND every lease-holder either applied the invalidation (the fast path — one
  in-cluster ack round, ~2 gossip hops, behind the origin round-trip already paid) or
  had its serve-lease **expire in the writer's own engine** (the slow path — bounded by
  `D`, counted as `write_lease_lapses`). So `PUT via A; GET via B` is deterministically
  fresh, and a peer that stops answering costs one write up to `D` **once**, not an ack
  timeout on every write forever.
- **That second path is the whole point.** The old mode ended an unacked write in a
  *degradation*: the writer proceeded and correctness then depended on the stale peer
  *learning* it should stand down — exactly what an asymmetrically-partitioned peer
  cannot do. A lapse is a guarantee instead: the straggler's own clock has closed its
  serve window, and a lapsed node serves nothing cached until it re-synchronizes from
  the origin and affirms it.
- **A gap stands the lease down first.** A write-feed gap (ring overflow, a peer
  restart) is proof this node missed invalidations, so it drops its right to serve
  *before* the remediation runs — and only the resync that actually ran may hand it
  back. A booting node likewise serves nothing local until its configured buckets have
  warmed and the lease shell's own warm-up window has passed.
- **A lapse with no gap behind it gets the same remediation.** Not every way of losing
  the licence arrives as an event: a peer scaled in, lost for good, or restarted while
  the write feed was quiet freezes this node's confirmation with no gap to notice. The
  lapse latches, so a watcher on the lease runs exactly the gap's remediation — flush,
  origin re-LIST, affirm — and the node returns to service the moment its lease can be
  confirmed again (`lease_lapse_resyncs`). Without it a lapse would be permanent
  origin-serving for a node whose surviving peers are perfectly healthy.

**What it rests on, stated as failure modes** (groupnet's `consistency::lease` honesty
box is the long form):

- **Bounded clock *rate* skew, not bounded connectivity.** A reader computes its window
  on its own monotonic clock and its granters expire on theirs. A reader whose clock
  runs slow relative to a granter's by more than the rate margin (`max(D/100, 5ms)`)
  over one lease duration can believe it holds a lease the granter already expired. It
  is an assumption about *rates* — a few hundred ppm on any healthy host — never about
  wall-clock steps, which cannot affect it.
- **The fail-slow reader is the one shape no lease bounds.** A node that keeps
  *renewing* while it stops *applying* — a stuck apply loop, a partition that carries
  gossip but not writes — offers neither an ack nor a lapse, so writes behind it run to
  their own deadline (`D + 1s`) and end with **no** guarantee, counted as
  `ack_timeouts` and logged naming the node. Raising the deadline cannot help; the
  remedy is operational — stop that pod. The log line names it.
- **One unresponsive pod freezes every reader, briefly.** Confirmation is a min over
  the whole roster and only a *reap* removes a member, so a pod that stops granting
  freezes every other reader's lease cluster-wide: reads stay correct and all of them
  go to the origin. s3cache tunes `dead_timeout` down to `D` for exactly this, turning
  the untuned ~19s of cluster-wide origin-serving into ≈3s. The freeze *ends* at the
  reap rather than latching: each frozen reader watches its own lapse and remediates it
  (above), so it comes back — cold, having flushed, but in service. The price is the
  other end of the same horizon: a partition outliving ~4s lands on the write-feed
  **gap** path (flush + origin re-LIST) instead of reconciling — loud, correct, and the
  standing remedy a cache wants.
- **First write after a pod dies costs up to `D`.** That is the lapse being waited out,
  once. It shows up as `write_lease_lapses`, not `ack_timeouts`, because the guarantee
  held. A *planned* stop does not pay it: on `SIGTERM` (a rollout, a scale-in) the node
  retracts its serve-lease before it drains, so there is no lapse to wait out. That is
  the write side only — the reader-side freeze above is unchanged either way, because
  the departing node's capability advertisement lives in every roster until the reap.
- The absolute arbiter for conflicting writers remains the origin: conditional
  `If-Match`/`If-None-Match` writes pass through untouched (OCC — no lost updates,
  regardless of node), and the index heals from the origin (gap resync, startup
  bootstrap). Cross-writer index races resolve by timestamp, deletes winning ties.

**`strong-acks` (`S3CACHE_CONSISTENCY=strong-acks`) — deprecated, one release only.**
The pre-lease *coherence mechanism*, kept so a deployment can roll the lease tier
through the fleet in stages: writes wait on every alive peer that participates in the
ack tier, an ack timeout ends in a degradation rather than a guarantee, and the
read-side licence is the old view-stability heuristic (a fully-alive membership view,
unchanged for the failure detector's whole window).

It is a rollback lever for that mechanism, **not a time machine**. Three things changed
underneath every mode, and this one gets them too:

- **Membership is tuned differently, in every mode.** `dead_timeout` is pulled from
  groupnet's 10s default down to `max(D, 2s)` uniformly — a mixed fleet must have one
  membership timing, not two — which puts the reap horizon at ~4s instead of ~20s. A
  partition outliving that lands on the write-feed **gap** path (flush + origin re-LIST)
  rather than reconciling through a digest catch-up: loud, correct, and not what this
  mode did before.
- **The 404 trust window is computed per call, not fixed.** It was a constant 650ms off
  groupnet's *default* probe timings. It is now `detection_window_ms` read off the
  **effective** config and the membership actually being swept (floored at two members)
  — a correction of a window that was too short, and a larger number: 700ms for a pair,
  growing with the cluster.
- **The ack wait skips peers that declare themselves bounded.** A peer advertising
  `s3cache:bounded` is no longer waited out per write. A peer that advertises *nothing*
  still is — absence is not non-participation — so a pre-upgrade fleet is unaffected.

It is removed in the next release. Nothing new should choose it, and a fleet running it
is running the brittleness the lease tier exists to remove. Mixed fleets are safe in
both directions — a leased writer waits for `strong-acks` and pre-upgrade peers through
the ordinary ack round, since they publish no lease for it to wait on or expire.

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

Nothing in this section is required. In the default `strong` mode there is no client
cooperation to opt into — every client is strict automatically, tokens are redundant.
Tokens matter only if you switch to `bounded`, and even there an unaware client is
fresh within ~one push hop and never *silently* stale; the token upgrades one client
to read-its-own-writes strictness without paying strong's per-write ack round.

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
- **Integration tests** (`cargo test`, needs a Docker daemon): `tests/e2e.rs` and
  `tests/coherence.rs` run the real `CachingProxy` against a **real MinIO origin**
  (testcontainers) reached through a transparent request counter, so every claim is
  asserted twice — what the client saw, and what the origin was asked for. LIST/HEAD
  from the index cost the origin nothing; a GET misses once; over-cap objects bypass;
  ranges slice the cached body; conditional writes (`If-None-Match: *`, `If-Match`)
  keep the origin's 412 semantics and leave the index and cache consistent with the
  outcome. `tests/coherence.rs` does the same with two nodes gossiping over loopback
  UDP: a write on A is in B's index by the time it returns, an overwrite on A drops B's
  cached body, a delete on A makes B's HEAD a local 404, and a contested
  create-if-absent is arbitrated by the origin.
- **Differential tests** (`tests/differential.rs`, same Docker origin): every row asks
  one question twice — once through the proxy, once straight at MinIO — and asserts a
  client could not tell which answered, over the status, the body and the headers it
  branches on (`ETag`, `Content-Length`, `Content-Range`, `Last-Modified`,
  `Content-Type`, `Accept-Ranges`, `x-amz-meta-*`) plus the whole `ListObjectsV2`
  envelope. It is the referee for anything the proxy chooses to answer locally: the
  conditional-request matrix, the range matrix in every serving state, the LIST matrix
  walked to exhaustion, `response-*` overrides, `max-keys=0`, and a partially-refused
  batch delete.
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
| `S3CACHE_CONSISTENCY` | `strong` | `strong`: lease-backed — a node serves locally only while it holds a serve-lease, and a write ends at an ack or at a lapse. `strong-acks`: the pre-lease ack-only mechanism, **deprecated**, removed next release. `bounded`: ~one-hop freshness, no per-write ack round (large clusters; set uniformly) |
| `S3CACHE_LEASE_MS` | `2000` | Coherence-lease duration `D` (`strong` only): the writer's worst-case stall on a silent peer, each reader's window between confirmations, and the fleet's membership `dead_timeout`. Renewal traffic is one small entry per node per `D/3` |
| `S3CACHE_STATS_SECS` | `60` | Stats log interval (seconds) |
| `S3CACHE_METRICS_LISTEN` | (empty) | Listen address for the Prometheus text endpoint (e.g. `0.0.0.0:9090`); unset = no endpoint |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_REGION` | — | Upstream creds (R2: region `auto`) |

Clients authenticate to the proxy with the same key; the proxy re-signs to the
upstream.

## Metrics

Every counter is logged as one `s3cache stats:` line each `S3CACHE_STATS_SECS`, and —
when `S3CACHE_METRICS_LISTEN` is set — served as Prometheus text at `GET /metrics` on
that address, `s3cache_`-prefixed (`metrics.enabled` in the chart). Both are generated
from one declaration, so a counter cannot exist in one and not the other.

What they attribute: LIST (`list_from_index` vs `list_passthrough`), GET
(`get_hit` / `get_miss` / `get_bypass`, `range_*`), HEAD (`head_hit` from a cached body,
`head_index` and `head_404` from the key index, `head_miss` forwarded upstream), the
writes folded into the index by operation (`writes_indexed_put` / `_copy` / `_multipart`,
each a separately billed upstream class-A call, plus `_observed` for keys learned on the
read path), `write_fill` (writes whose body was kept rather than dropped — each one an
origin GET the object's first read no longer costs), `index_backfills` (index entries
completed from a forwarded answer — see below), the warm tier (`warm_hit` / `warm_miss` / `warm_error`, with `warm_rejects` for
objects the per-object cap declined, kept apart so `warm_error` stays alertable) and the
gossip write feed (`feed_*`, `ack_timeouts`, `write_lease_lapses`, `lease_lapse_resyncs`,
`unhealthy_bypasses`).

The last four are the coherence tier's, and the split between the first two is the one
worth wiring an alert around: **`write_lease_lapses` is the guarantee working** — a peer
stopped acknowledging, its serve-lease expired, and the write completed knowing that peer
can serve nothing cached until it re-synchronizes. Sustained movement means a pod is
unresponsive and each such write cost up to one lease duration, but no read was ever
stale. **`ack_timeouts` is the absence of the guarantee**: peers still live and still
behind when the wait's deadline passed (in `strong`, the fail-slow reader — renewing but
not applying — plus any un-leased peer that did not ack). Alert on that one.
`unhealthy_bypasses` counts reads sent to the origin because this node held no licence to
answer them locally; it is expected to be non-zero at every startup and after every gap.
`lease_lapse_resyncs` is the read side of the same story: this node's *own* lease lapsed
with no gap to explain it (a peer stopped granting), so it flushed, re-LISTed and affirmed
its way back into service. One per peer death is normal; sustained movement is a flapping
peer, and each one leaves this node cold.
