//! Cache-effectiveness counters and the task that periodically logs them.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tracing::info;

/// Cache-effectiveness counters. The `warm_*` counters cover the disk (warm) tier, the
/// `feed_*` counters the gossip write feed, and the rest the index and hot path. Fields
/// are bumped directly by the proxy; the tier and feed use the helper methods.
#[derive(Default)]
pub(crate) struct Metrics {
    pub(crate) list_from_index: AtomicU64,
    pub(crate) list_passthrough: AtomicU64,
    pub(crate) writes_indexed: AtomicU64,
    pub(crate) get_hit: AtomicU64,
    pub(crate) get_miss: AtomicU64,
    pub(crate) get_bypass: AtomicU64,
    pub(crate) range_hit: AtomicU64,
    pub(crate) range_promote: AtomicU64,
    pub(crate) range_promote_reject: AtomicU64,
    warm_hit: AtomicU64,
    warm_miss: AtomicU64,
    warm_error: AtomicU64,
    feed_published: AtomicU64,
    feed_applied: AtomicU64,
    feed_gaps: AtomicU64,
    ack_timeouts: AtomicU64,
    unhealthy_bypasses: AtomicU64,
}

impl Metrics {
    /// Record a warm-tier hit — the object was served from the node-local disk tier.
    pub(crate) fn warm_hit(&self) {
        self.warm_hit.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a warm-tier miss — the key was absent on disk.
    pub(crate) fn warm_miss(&self) {
        self.warm_miss.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a warm-tier error/timeout/decode failure (all handled as a miss/drop).
    pub(crate) fn warm_error(&self) {
        self.warm_error.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a write advertised to peers over the gossip write feed.
    pub(crate) fn feed_published(&self) {
        self.feed_published.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a peer's write applied from the feed (index + invalidation).
    pub(crate) fn feed_applied(&self) {
        self.feed_applied.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a feed gap (missed writes): local tiers flushed, index resynced.
    pub(crate) fn feed_gap(&self) {
        self.feed_gaps.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a write held past the ack window by an unresponsive peer.
    pub(crate) fn ack_timeout(&self) {
        self.ack_timeouts.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a cache-served read routed to the origin because this node's
    /// membership view was not fully alive.
    pub(crate) fn unhealthy_bypass(&self) {
        self.unhealthy_bypasses.fetch_add(1, Ordering::Relaxed);
    }
}

/// Periodically log the cache-effectiveness counters (LISTs served from the index
/// vs forwarded, writes indexed).
pub(crate) fn spawn_stats(metrics: Arc<Metrics>, interval_secs: u64) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(interval_secs.max(1)));
        loop {
            tick.tick().await;
            info!(
                "s3cache stats: list_from_index={} list_passthrough={} writes_indexed={} \
                 get_hit={} get_miss={} get_bypass={} range_hit={} range_promote={} range_promote_reject={} \
                 warm_hit={} warm_miss={} warm_error={} feed_published={} feed_applied={} \
                 feed_gaps={} ack_timeouts={} unhealthy_bypasses={}",
                metrics.list_from_index.load(Ordering::Relaxed),
                metrics.list_passthrough.load(Ordering::Relaxed),
                metrics.writes_indexed.load(Ordering::Relaxed),
                metrics.get_hit.load(Ordering::Relaxed),
                metrics.get_miss.load(Ordering::Relaxed),
                metrics.get_bypass.load(Ordering::Relaxed),
                metrics.range_hit.load(Ordering::Relaxed),
                metrics.range_promote.load(Ordering::Relaxed),
                metrics.range_promote_reject.load(Ordering::Relaxed),
                metrics.warm_hit.load(Ordering::Relaxed),
                metrics.warm_miss.load(Ordering::Relaxed),
                metrics.warm_error.load(Ordering::Relaxed),
                metrics.feed_published.load(Ordering::Relaxed),
                metrics.feed_applied.load(Ordering::Relaxed),
                metrics.feed_gaps.load(Ordering::Relaxed),
                metrics.ack_timeouts.load(Ordering::Relaxed),
                metrics.unhealthy_bypasses.load(Ordering::Relaxed),
            );
        }
    });
}
