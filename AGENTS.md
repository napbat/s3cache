# AGENTS.md

Guidance for AI agents working in this repository.

## What this is

`s3cache` is a transparent, S3-compatible caching proxy. It binds an S3 API, forwards
to an upstream S3 (e.g. Cloudflare R2), serves LIST from an in-memory key index, and
serves GET/HEAD from a tiered object-body cache: **hot** (in-memory heap) → **warm**
(node-local disk, optional) → **cold** (the S3 origin). Writes go through to the
upstream. Cross-node coherence rides a gossip write feed (`groupnet`).

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
- The orchestrator independently runs the applicable locked core, chart, and
  container gates below before declaring work done.

## Project layout

Follows the standard [Cargo project layout](https://doc.rust-lang.org/cargo/guide/project-layout.html):

```
Cargo.toml      # workspace manifest: shared package metadata, dependencies, and lints
Cargo.lock      # committed; dependency-resolving Cargo gates use --locked
src/
  lib.rs        # crate link point: module docs and declarations only
  main.rs       # thin binary: wires Config::from_env() into the lib, runs the server
  config.rs     # S3 API/cache/metrics env parsing (pure, unit-tested seams)
  cache/
    mod.rs      # cache link point: module docs and declarations only
    proxy.rs    # cache types and core tier/index helpers
    service.rs  # the S3 service implementation and write-through paths
    tests.rs    # cache unit tests
  index.rs      # in-memory LIST key index
  tier.rs       # hot (moka heap) / warm (mmap disk) tiered object-body cache
  sync/
    mod.rs       # sync link point: module docs and declarations only
    coherence.rs # gossip write-feed and coherence core
    config.rs    # gossip env/config parsing and node construction
    recovery.rs  # staged lease-lapse recovery and its correctness argument
    wire.rs      # write-event and session-token codec
    tests.rs     # coherence unit tests
  metrics.rs    # counters + periodic stats logging
tests/          # integration tests (link against the library crate)
deploy/helm/s3cache/   # Helm chart
scripts/
Dockerfile
.dockerignore   # image context allowlist: manifests, lockfile, and src only
.github/workflows/build.yml   # locked Rust/docs/chart gate, image push, Fleet bump
```

Integration tests live in `tests/` and exercise the public library surface:
`tier_cache.rs` (hot→warm rollover and warm-tier restart survival), `e2e.rs` and
`coherence.rs` (the whole proxy against a real MinIO origin, single- and dual-node),
`differential.rs` (every row asked twice — through the proxy and straight at MinIO —
asserting a client couldn't tell which answered; the referee for anything served
locally), `metrics_endpoint.rs` (the Prometheus listener), with the shared
origin/counter harness in `common/mod.rs` and the differential comparator in
`common/diff.rs`.

## Repository and module invariants

- `src/lib.rs`, `src/cache/mod.rs`, and `src/sync/mod.rs` are link points: keep only
  module documentation and declarations there, never substantive implementation.
- Link modules do not provide facade re-exports. Public code names the owning modules
  directly: `cache::proxy`, `sync::coherence`, and `sync::config`.
- Extracted modules declare explicit `crate::...` dependencies rather than collecting
  `use super::...` import bags. Keep production files near the current roughly
  1,000-line scale and split them by responsibility before they become monoliths.
- Declare every direct package dependency once in `[workspace.dependencies]`; package
  dependency tables use `workspace = true`, and package metadata and lints are
  workspace-inherited. Commit `Cargo.lock`, and use `--locked` in every build gate.
- `groupnet` comes from its GitHub `main` branch and is pinned to the resolved revision
  by `Cargo.lock`. Local `[patch]` or path overrides are forbidden in shippable changes.
  A deliberate `cargo update -p groupnet` must be followed by the full locked gate and
  a review that every groupnet lock entry has the intended Git source and revision.
- `.dockerignore` is an allowlist. Any new Docker build input must be added there
  deliberately.

## Verification gates

Run the core locked gate after Rust or dependency changes and before a release:

```sh
cargo metadata --locked --no-deps --format-version 1
cargo fmt --all --check
cargo build --locked --workspace --all-targets --all-features
cargo clippy --locked --workspace --all-targets --all-features
cargo test --locked --workspace --all-features
RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --all-features --no-deps
```

After chart changes, run both chart checks with the required example upstream:

```sh
helm lint deploy/helm/s3cache --set upstream.endpoint=https://example.com
helm template deploy/helm/s3cache --set upstream.endpoint=https://example.com
```

After dependency, Dockerfile/`.dockerignore`, or release changes, verify the container:

```sh
docker build --tag s3cache:check .
```

`tests/e2e.rs` and `tests/coherence.rs` start **MinIO** through testcontainers (one
container per test, torn down on drop), so the locked workspace test command needs a
reachable Docker daemon. The proxy reaches MinIO through a transparent counting
forwarder in `tests/common/mod.rs`, which is what lets a test assert that an answer cost
the origin *nothing*. The image (`minio/minio:latest`) is pulled once. Chart and
container checks are conditional on the changes above; an unrelated documentation-only
edit does not require an image build.

## Conventions

- **Line endings: LF everywhere**, enforced by `.gitattributes`. Never commit CRLF.
- Rust edition 2024; `clippy::pedantic` and Rust's `unused_imports` are denied
  workspace-wide — public items need `# Errors` / `# Panics` doc sections where
  applicable.
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
specific events were missed: the apply loop's `sync::recovery::remediate` stands the
licence down,
**distrusts** every cached body (the trust generation moves; nothing is dropped) and
re-LISTs the index, so each copy proves itself against that index on its next read or is
evicted then. A lapse with no gap behind it is *not* that proof, so
`sync::recovery::watch_lapses`
(strong only) runs a staged recovery on the same `ResyncGate` generation — every granter
re-grants (per granter, *not* off the roster-wide min), settle, no-peer-vanished, barrier
on every advertised feed head, no-peer-vanished again — and keeps the cache whole when
the barrier proves it (`lapse_barrier_retains`), falling back to `remediate` whenever a
stage cannot get its proof (`lapse_barrier_fallbacks` / `lease_lapse_resyncs`).
`LocalCache::flush` survives only as an escape hatch; no remediation path calls it. The
correctness argument for the barrier is in `src/sync/recovery.rs`'s module docs — it has
two checked hinges and one named residual; read it before touching the stages. A planned
stop calls `WriteSync::leave` from the binary's signal path so peers do not wait out a
lease of a pod that is leaving on purpose.
