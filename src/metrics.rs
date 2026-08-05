//! Cache-effectiveness counters and the task that periodically logs them.

use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tracing::info;

/// Declares the counter set once: the fields, a bump method per counter, and the stats
/// line — which is generated in declaration order, so a counter can never be added
/// without being logged. The `warm_*` counters cover the disk (warm) tier, the `feed_*`
/// counters the gossip write feed, and the rest the index and hot path.
macro_rules! counters {
    ($( $(#[$doc:meta])* $field:ident => $bump:ident ),+ $(,)?) => {
        /// Cache-effectiveness counters, shared by the proxy, the tiers, the write feed
        /// and the stats task. Every counter is bumped through its own method, so the
        /// atomics stay an implementation detail.
        #[derive(Default)]
        pub struct Metrics {
            $($field: AtomicU64,)+
        }

        impl Metrics {
            $(
                $(#[$doc])*
                pub(crate) fn $bump(&self) {
                    self.$field.fetch_add(1, Ordering::Relaxed);
                }
            )+

            /// The counters as one `name=value …` line.
            fn stats_line(&self) -> String {
                let mut line = String::new();
                $(
                    let _ = write!(
                        line,
                        concat!(" ", stringify!($field), "={}"),
                        self.$field.load(Ordering::Relaxed),
                    );
                )+
                line
            }
        }
    };
}

counters! {
    /// Record a LIST answered from the in-memory key index.
    list_from_index => list_from_index,
    /// Record a LIST forwarded to the upstream (bucket not yet synced, or not local).
    list_passthrough => list_passthrough,
    /// Record a write folded into the LIST index.
    writes_indexed => write_indexed,
    /// Record a GET/HEAD served from a cached body.
    get_hit => get_hit,
    /// Record a GET that missed and was cached on the way back.
    get_miss => get_miss,
    /// Record a GET streamed straight through, uncached.
    get_bypass => get_bypass,
    /// Record a ranged GET served by slicing a cached body.
    range_hit => range_hit,
    /// Record a whole-object promotion driven by a ranged GET.
    range_promote => range_promote,
    /// Record a refused/failed range promotion (the range streams through instead).
    range_promote_reject => range_promote_reject,
    /// Record a warm-tier hit — the object was served from the node-local disk tier.
    warm_hit => warm_hit,
    /// Record a warm-tier miss — the key was absent on disk.
    warm_miss => warm_miss,
    /// Record a warm-tier error/timeout/decode failure (all handled as a miss/drop).
    warm_error => warm_error,
    /// Record a write advertised to peers over the gossip write feed.
    feed_published => feed_published,
    /// Record a peer's write applied from the feed (index + invalidation).
    feed_applied => feed_applied,
    /// Record a feed gap (missed writes): local tiers flushed, index resynced.
    feed_gaps => feed_gap,
    /// Record a write held past the ack window by an unresponsive peer.
    ack_timeouts => ack_timeout,
    /// Record a cache-served read routed to the origin because this node's
    /// membership view was not fully alive.
    unhealthy_bypasses => unhealthy_bypass,
}

/// Periodically log the cache-effectiveness counters (LISTs served from the index
/// vs forwarded, writes indexed).
pub fn spawn_stats(metrics: Arc<Metrics>, interval_secs: u64) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(interval_secs.max(1)));
        loop {
            tick.tick().await;
            info!("s3cache stats:{}", metrics.stats_line());
        }
    });
}

#[cfg(test)]
mod tests {
    use super::Metrics;

    #[test]
    fn the_stats_line_reports_every_counter() {
        let metrics = Metrics::default();
        metrics.get_hit();
        metrics.get_hit();
        metrics.warm_hit();
        let line = metrics.stats_line();
        assert!(
            line.starts_with(" list_from_index=0 list_passthrough=0"),
            "{line}"
        );
        assert!(line.contains(" get_hit=2 "), "{line}");
        assert!(line.contains(" warm_hit=1 "), "{line}");
        assert!(line.ends_with(" unhealthy_bypasses=0"), "{line}");
    }
}
