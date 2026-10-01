# Index startup

## Serving while cold

Pod readiness and index completion are separate states.
`GET /ready` is served once the S3 listener is bound. It succeeds while this
node's index is incomplete unless a live peer may hold an index, so requests can
reach the origin from a cold fleet (see the README's *Readiness and rollouts*).
`GET /index-ready` separately reports initial index and coherence readiness.

In gossip mode, Groupnet owns one initial origin scan per node and keeps the local
serving gate closed until that scan and its recovery affirmation complete.
Positive GETs still forward to the origin while this happens; they do not wait
for LIST and may fill a body for later index validation. This increases origin
GETs and local-hit latency during a cold join compared with the prior early
lease affirmation. A body from a prior process still requires validation, and
each later local read must pass the current coherence read barrier.
Connected nodes currently scan independently. Fleet-wide single-builder
bootstrap and peer index transfer remain a separate follow-up; skipping another
node's scan without a transferred, validated index would leave it origin-only.
Recovery retries individual failed scans within a finite total budget. If that
budget expires during a prolonged origin outage, the default node remains
origin-routed until `CachingProxy::restart_coherence` explicitly starts a new
episode (or a new feed gap does). Opting in with
`S3CACHE_RECOVERY_REARM=true` (Helm `recovery.rearm`) lets Groupnet start a new
full episode after a capped delay: 5 seconds initially, doubling to at most
60 seconds between exhausted episodes. Failed scans inside one episode still
retry at the ordinary finite poll interval, so this is not per-request
exponential backoff. The rearm timer does not grant local read authority.
An incomplete index cannot prove that a key is absent or that a LIST is complete.
Those requests continue to use the origin.

The former s3cache lapse/retry state-machine tests now live at the protocol
boundary in Groupnet's deterministic volatile-recovery core and simulator:
generation supersession, lapse and vanished-peer proofs, frontier barriers,
bounded retries, and terminal failure. S3cache keeps black-box assertions at
its own boundary: cold positive GETs and warm bodies use origin fallback, a
timed-out scan cannot publish a delayed page, a prolonged outage needs a new
episode, malformed advertised feed data fails observation, and the default
path creates no origin control objects.

A feed gap during boot supersedes that scan and requires a fresh guarded scan
before local serving. Retained volatile feed history alone is not a certified
index baseline for a new process.

## Parallel scan

Discovery samples directory prefixes to choose key-range boundaries.
The boundaries are performance hints. The final ranges cover the entire keyspace,
including keys outside the sampled prefixes.
Each range follows its own continuation tokens.
All ranges belong to one rebuild generation.
Every range must finish before that generation can publish a complete index.

Concurrency defaults to the process's available CPU parallelism, capped at 64.
`S3CACHE_INDEX_SCAN_CONCURRENCY` overrides it. A value of 1 selects a serial scan.
The default discovery budget is 16 additional LIST requests per rebuild.
`S3CACHE_INDEX_SCAN_DISCOVERY_REQUESTS` overrides that budget.
Each extra range can also read one page beyond its upper boundary.
No new periodic scan is added.

## Writes during a scan

Each rebuild records the keys changed by definitive writes, deletes, or exact
reconciliation results. LIST pages cannot replace those keys.
The first such mutation also supersedes a previously scanned row, regardless of
the relative origin and proxy timestamps. An unresolved mutation keeps its key
unavailable to local reads.

A newer rebuild generation rejects pages from the previous generation. Each
origin LIST page and the final complete-index flag also require the current
Groupnet publication permit, so a delayed page cannot publish after a gap or
recovery timeout.
After completion, each local LIST still passes the coherence read barrier.
The barrier and lease govern whether the node can serve its index.
A feed gap requires recovery.

This protects scan results from overwriting concurrent feed changes.
It preserves the existing timestamp ordering between definitive writers.
It does not introduce a new global write-ordering protocol.
All object mutations must pass through the coherent proxy fleet.
Direct origin writes do not publish invalidations to that fleet.

## Local timing probe

Run the ignored probe locally:

```text
cargo test --locked -j 2 --lib scan_latency_probe -- --ignored --nocapture
```

The measured fixture had 32,768 keys across 1,024 shard-like prefixes.
Its HTTP origin added 40 milliseconds to each LIST request.

| Scan | Elapsed | Origin LIST requests |
| --- | ---: | ---: |
| Serial | 1,883 ms | 33 |
| Eight workers | 998 ms | 45 |

This run was 1.89 times faster and used 12 additional LIST requests.
These are local measurements with injected latency. They are not an R2 forecast.
Bucket layout, CPU capacity, origin latency, and throttling affect the result.

## Peer snapshots: proposed follow-up

Peer index transfer is not implemented by this change.
A peer snapshot could replace most origin LIST requests during a rolling restart.
It needs a consistent snapshot and the per-writer feed positions covered by that snapshot.
The receiver must replay later events before it serves the transferred index.
A gap in retained events must cause an origin rebuild.

The snapshot must carry its format, bucket identity, and generation.
The transfer must have size and integrity checks.
Transfer completion alone cannot establish freshness.
The receiving node still needs a valid coherence lease and a caught-up write feed.
