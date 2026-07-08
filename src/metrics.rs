//! Cache-effectiveness counters and the task that periodically logs them.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tracing::info;

/// Cache-effectiveness counters. The `warm_*` counters cover the shared Valkey tier, the
/// `log_*` counters the commit log, and the rest the index and hot path. Fields are
/// bumped directly by the proxy; the tier and log use the helper methods.
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
    log_appended: AtomicU64,
    log_applied: AtomicU64,
    log_error: AtomicU64,
}

impl Metrics {
    /// Record a warm-tier (Valkey) hit — the object was served from the shared cache.
    pub(crate) fn warm_hit(&self) {
        self.warm_hit.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a warm-tier miss — the key was absent in Valkey.
    pub(crate) fn warm_miss(&self) {
        self.warm_miss.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a warm-tier error/timeout/decode failure (all handled as a miss/drop).
    pub(crate) fn warm_error(&self) {
        self.warm_error.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an index-log event appended for peers (a local write).
    pub(crate) fn log_appended(&self) {
        self.log_appended.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an index-log event consumed from the stream (own or a peer's).
    pub(crate) fn log_applied(&self) {
        self.log_applied.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an index-log append/read error or timeout.
    pub(crate) fn log_error(&self) {
        self.log_error.fetch_add(1, Ordering::Relaxed);
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
                 warm_hit={} warm_miss={} warm_error={} log_appended={} log_applied={} log_error={}",
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
                metrics.log_appended.load(Ordering::Relaxed),
                metrics.log_applied.load(Ordering::Relaxed),
                metrics.log_error.load(Ordering::Relaxed),
            );
        }
    });
}
