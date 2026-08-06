# AGENTS.md

Guidance for AI agents working in this repository.

## What this is

`s3cache` is a transparent, S3-compatible caching proxy. It binds an S3 API, forwards
to an upstream S3 (e.g. Cloudflare R2), serves LIST from an in-memory key index, and
serves GET/HEAD from a tiered object-body cache: **hot** (in-memory heap) → **warm**
(node-local disk, optional) → **cold** (the S3 origin). Writes go through to the
upstream. Cross-node coherence rides a gossip write feed (`groupnet`).

## ⚠️ DO NOT SHIP THIS TREE — it builds on exactly one workstation

`Cargo.toml` carries a `[patch."https://github.com/napbat/groupnet"]` pointing at a
**local** `../groupnet` checkout, because the coherence-lease tier this branch is built
on (`consistency-leases`) is not pushed to GitHub yet. CI, the Dockerfile image build,
and any fresh clone all resolve `../groupnet`, find nothing, and fail. Release order:
push groupnet `main`, delete the `[patch]` section, `cargo update -p groupnet`, then run
the whole gate below. Do not merge, tag, or build an image before that.

## Agent workflow: Fable orchestrator, Opus 5 implementer

Work in this repo uses a two-tier agent setup:

- **Orchestrator — Claude Fable 5** (`claude-fable-5`): the main session. Plans the
  work, splits it into well-scoped implementation briefs, reviews results, and owns
  the final summary to the user. The orchestrator does not grind through large
  mechanical edits itself when a brief can be delegated.
- **Implementation agent — Claude Opus 5** (`claude-opus-5`): defined in
  `.claude/agents/implementer.md` (`model: opus` resolves to the current Opus,
  Claude Opus 5). Receives a complete brief up front (files, constraints, acceptance
  criteria), implements, and must build + test before reporting back.

Rules of engagement:

- Give the implementer the **whole task spec in one brief** — files to touch, the
  crate/chart conventions below, and the verification commands. Don't drip-feed.
- The implementer reports facts (test output, clippy status), not intentions.
- The orchestrator verifies independently (`cargo test`, `helm template`) before
  declaring work done.

## Project layout

Follows the standard [Cargo project layout](https://doc.rust-lang.org/cargo/guide/project-layout.html):

```
Cargo.toml
src/
  lib.rs        # library crate: public modules (cache, config, index, metrics, sync, tier)
  main.rs       # thin binary: wires Config::from_env() into the lib, runs the server
  config.rs     # all S3CACHE_* env parsing (pure, unit-tested seams)
  cache.rs      # the S3 proxy: LIST-from-index + tiered body cache + write-through
  index.rs      # in-memory LIST key index
  tier.rs       # hot (moka heap) / warm (mmap disk) tiered object-body cache
  sync.rs       # cross-node coherence: gossip write feed (groupnet)
  metrics.rs    # counters + periodic stats logging
tests/          # integration tests (link against the library crate)
deploy/helm/s3cache/   # Helm chart
scripts/
Dockerfile
```

Integration tests live in `tests/` and exercise the public library surface:
`tier_cache.rs` (hot→warm rollover and warm-tier restart survival), `e2e.rs` and
`coherence.rs` (the whole proxy against a real MinIO origin, single- and dual-node),
`differential.rs` (every row asked twice — through the proxy and straight at MinIO —
asserting a client couldn't tell which answered; the referee for anything served
locally), `metrics_endpoint.rs` (the Prometheus listener), with the shared
origin/counter harness in `common/mod.rs` and the differential comparator in
`common/diff.rs`.

## Build, test, lint

```sh
cargo build
cargo test                          # unit + integration tests (needs a Docker daemon, see below)
cargo clippy --all-targets          # clippy::pedantic is DENIED (Cargo.toml) — must be clean
cargo fmt --check
helm lint deploy/helm/s3cache       # after chart changes
helm template deploy/helm/s3cache --set upstream.endpoint=https://example.com   # render check
```

`tests/e2e.rs` and `tests/coherence.rs` start **MinIO** through testcontainers (one
container per test, torn down on drop), so `cargo test` needs a reachable Docker
daemon. The proxy reaches MinIO through a transparent counting forwarder in
`tests/common/mod.rs`, which is what lets a test assert that an answer cost the origin
*nothing*. The image (`minio/minio:latest`) is pulled once.

## Conventions

- **Line endings: LF everywhere**, enforced by `.gitattributes`. Never commit CRLF.
- Rust edition 2024; `clippy::pedantic` at deny level — public items need `# Errors` /
  `# Panics` doc sections where applicable.
- Comments state constraints the code can't show; match the existing density and voice.
- The chart's env-var names are the contract with `src/main.rs` (`S3CACHE_*`). Change
  them in both places or not at all.
- Commit messages: short imperative subject, matching existing history
  (`chore: …`, `fleet: …`).

## Runtime configuration (env vars read by the binary)

| Env var | Default | Meaning |
|---|---|---|
| `S3CACHE_LISTEN` | `0.0.0.0:8014` | Bind address for the S3 API |
| `S3CACHE_UPSTREAM_ENDPOINT` | — (required) | Upstream S3/R2 endpoint URL |
| `S3CACHE_BUCKETS` | empty | Comma-separated buckets to index eagerly |
| `S3CACHE_CACHE_BYTES` | `268435456` (256 MiB) | Hot (in-memory) tier capacity; the rollover point into the warm tier |
| `S3CACHE_MAX_OBJECT_BYTES` | `8388608` (8 MiB) | Per-object cap; larger objects stream through uncached |
| `S3CACHE_DISK_CACHE` | empty (disabled) | Directory for the warm (disk) tier |
| `S3CACHE_DISK_CACHE_BYTES` | `10737418240` (10 GiB) | Warm tier byte budget |
| `S3CACHE_GOSSIP_BIND` / `_ADVERTISE` / `_SEEDS` | empty (disabled) | Gossip write feed for cross-node coherence |
| `S3CACHE_CONSISTENCY` | `strong` | `strong` (lease-backed), `strong-acks` (the pre-lease mechanism — **deprecated**, removed next release), or `bounded` |
| `S3CACHE_LEASE_MS` | `2000` | Coherence-lease duration `D` (`strong` only) — also the fleet's membership `dead_timeout` |
| `S3CACHE_STATS_SECS` | `60` | Stats log interval |
| `S3CACHE_METRICS_LISTEN` | empty (disabled) | Bind address for the Prometheus text endpoint (`GET /metrics`) |

## Helm chart knobs (deploy/helm/s3cache/values.yaml)

- `memoryCache.bytes` — hot in-memory cache size (`S3CACHE_CACHE_BYTES`).
- `memoryCache.maxObjectBytes` — per-object cache cap (`S3CACHE_MAX_OBJECT_BYTES`).
- `diskCache.enabled` / `diskCache.path` / `diskCache.bytes` — the warm disk tier
  that hot-tier evictions roll over into. Back it with
  `diskCache.volumeClaimTemplate` (per-ordinal PVCs that survive deploys — wins over
  `diskCache.volume` when set) or a plain `diskCache.volume` pod-volume spec.
- `replicaCount` — the one scaling knob; every pod is a gossip cluster member.
- `upstream.endpoint` (required) / `upstream.buckets`.
- `metrics.enabled` / `metrics.port` — the Prometheus text endpoint
  (`S3CACHE_METRICS_LISTEN`) on a named `metrics` container port.
- `gossip.consistency` / `gossip.leaseMs` — the coherence mode
  (`S3CACHE_CONSISTENCY`) and the lease duration `D` (`S3CACHE_LEASE_MS`; empty renders
  no env var, so the binary's own 2000ms default applies).

## Consistency, in one paragraph

The default `strong` mode is **lease-backed**: a node may answer a LIST, a GET, or an
authoritative 404 from its own state only while it holds an unexpired *serve-lease* its
peers granted (groupnet's `consistency::lease`, tier T3), and a write ends when every
lease-holder has either applied the invalidation (one ack round) or had its lease lapse
in the writer's engine (bounded by `D`, on the straggler's own clock). That replaces the
old view-stability heuristic (`Stability`/`settled()`), which survives only for
`strong-acks` — deprecated, removed next release. `strong-acks` pins the **mechanism**
and nothing else: `dead_timeout` is retuned to `max(D, 2s)` in every mode, the 404 trust
window is computed per call, and declared-bounded peers are skipped by every ack wait —
so do not describe it as pinning the previous release's behaviour (the README's
`strong-acks` section states the three differences). The named failure modes
(clock-*rate* skew, the fail-slow reader, the unreaped-granter freeze, the first write
after a death) are in the README's Consistency section and in groupnet's own honesty box;
do not restate them loosely, and do not add a mode without answering
`Consistency::{acks, leases, capabilities}` explicitly — they are exhaustive on purpose.

Losing the read-side licence is a **latch**, so every way of losing it needs a way back,
and the two ways are deliberately priced differently. A write-feed gap is proof that
specific events were missed: the apply loop's `sync::remediate` stands the licence down,
**distrusts** every cached body (the trust generation moves; nothing is dropped) and
re-LISTs the index, so each copy proves itself against that index on its next read or is
evicted then. A lapse with no gap behind it is *not* that proof, so `sync::watch_lapses`
(strong only) runs a staged recovery on the same `ResyncGate` generation — every granter
re-grants (per granter, *not* off the roster-wide min), settle, no-peer-vanished, barrier
on every advertised feed head, no-peer-vanished again — and keeps the cache whole when the
barrier proves it (`lapse_barrier_retains`), falling back to `remediate` whenever a stage
cannot get its proof (`lapse_barrier_fallbacks` / `lease_lapse_resyncs`).
`LocalCache::flush` survives only as an escape hatch; no remediation path calls it. The
correctness argument for the barrier is in `src/sync.rs`'s module docs — it has two
checked hinges and one named residual; read it before touching the stages. A planned stop calls `WriteSync::leave` from the binary's signal
path so peers do not wait out a lease of a pod that is leaving on purpose.
