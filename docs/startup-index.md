# Index startup

## Serving while cold

Listener readiness and index completion are separate states.
`GET /ready` becomes successful when the S3 listener is bound.
Requests can then reach the origin while the index is incomplete.
`GET /index-ready` separately reports initial index and coherence readiness.

A body fetched under a valid coherence lease can serve later reads before the
full index scan finishes. A body from a prior process still requires validation.
Each later read must pass the current coherence read barrier.
An incomplete index cannot prove that a key is absent or that a LIST is complete.
Those requests continue to use the origin.

A feed gap during boot cancels the early lease affirmation.
The node then waits for the full recovery scan before serving local data.
An ordinary rolling join with retained feed history can use early cache hits.

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

A newer rebuild generation rejects pages from the previous generation.
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
