//! Cross-node coherence over groupnet's consistency layer — the whole of it:
//! no raft, no shared broker. Each node publishes its durable writes as typed
//! write events into a per-node [`groupnet::consistency::WriteFeed`]; every
//! peer's apply loop folds them into the LIST index (per-key last-writer-wins
//! with delete-wins-ties tombstones — see [`crate::index`]) and drops the
//! stale body-cache copies. Events reach live peers at network latency (the
//! engine pushes deltas eagerly).
//!
//! Honest semantics, in one paragraph: each node's events arrive in its own
//! write order; there is no cross-writer total order, so concurrent writes to
//! one key through different nodes resolve by timestamp (deletes win ties)
//! and the origin — which serves conditional (OCC) writes untouched — stays
//! the authority the index is a cache of. Provably-missed events (ring
//! overflow, a peer restart) surface as a gap: every local body is *distrusted*
//! and the LIST index resyncs from the origin, so a copy is served again only
//! once that index has proved it current. The strict-LIST barrier
//! (`coherence::WriteSync::await_fresh`) waits until every peer's
//! currently-advertised feed head has been applied locally — freshness
//! bounded by one push/gossip hop and failing closed to the origin on timeout.
//!
//! Write responses surface the feed token in `x-s3cache-write-token`; a client
//! can echo it as `x-s3cache-read-token` to make a later read wait until that
//! specific write has been applied locally.
//!
//! In `strong` mode the read side is licensed by a **coherence lease** rather
//! than by a heuristic: this node may answer from local state only while it
//! holds an unexpired serve-lease its peers granted, and a write ends when
//! every lease-holder has either applied it or had its lease lapse. See
//! [`Consistency::Strong`](coherence::Consistency::Strong) and groupnet's
//! `consistency::lease` honesty box.
//! Losing that licence is a latch, so every way of losing it needs a way back:
//! a gap has the apply loop, and a lapse with no gap behind it — a peer scaled
//! in, lost, or restarted quietly — has the staged recovery. Its full barrier
//! correctness argument lives beside the implementation in `recovery`.

/// Consistency modes and the gossip write-feed coherence engine.
pub mod coherence;
/// Gossip environment/config parsing and node construction.
pub mod config;
mod recovery;
pub(crate) mod wire;

#[cfg(test)]
mod tests;
