//! Cache-effectiveness counters, the task that periodically logs them, and the optional
//! Prometheus text endpoint that exposes them for scraping.

use std::convert::Infallible;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use bytes::Bytes;
use http::{Method, Response, StatusCode, header};
use http_body_util::Full;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use tierstore_mmap::MmapDiskStats;
use tokio::net::TcpListener;
use tracing::info;

use crate::index::{IndexStats, KeyIndex};

/// Declares the event-counter set once: the fields, a bump method per counter, the stats
/// line, and the Prometheus exposition — all generated in declaration order. Warm
/// residency gauges sourced from Tierstore follow the generated event counters.
macro_rules! counters {
    ($( $(#[$doc:meta])* $field:ident => $bump:ident ),+ $(,)?) => {
        /// Cache-effectiveness counters, shared by the proxy, the tiers, the write feed
        /// and the stats task. Every counter is bumped through its own method, so the
        /// atomics stay an implementation detail.
        #[derive(Default)]
        pub struct Metrics {
            $($field: AtomicU64,)+
            warm_entries: AtomicU64,
            warm_mapped_entries: AtomicU64,
            warm_disk_bytes: AtomicU64,
            warm_disk_budget_bytes: AtomicU64,
            warm_evictions: AtomicU64,
            warm_evicted_bytes: AtomicU64,
            index: RwLock<Option<Arc<KeyIndex>>>,
        }

        impl Metrics {
            $(
                $(#[$doc])*
                pub(crate) fn $bump(&self) {
                    self.$field.fetch_add(1, Ordering::Relaxed);
                }
            )+

            /// Refresh the mmap tier's non-faulting residency gauges and
            /// lifetime eviction totals.
            pub(crate) fn observe_warm(&self, stats: MmapDiskStats) {
                self.warm_entries.store(
                    u64::try_from(stats.entries).unwrap_or(u64::MAX),
                    Ordering::Relaxed,
                );
                self.warm_mapped_entries.store(
                    u64::try_from(stats.mapped_entries).unwrap_or(u64::MAX),
                    Ordering::Relaxed,
                );
                self.warm_disk_bytes.store(stats.disk_bytes, Ordering::Relaxed);
                self.warm_disk_budget_bytes.store(
                    stats.budget_bytes.unwrap_or(0),
                    Ordering::Relaxed,
                );
                self.warm_evictions.store(stats.evictions, Ordering::Relaxed);
                self.warm_evicted_bytes.store(stats.evicted_bytes, Ordering::Relaxed);
            }

            /// Attach the index whose current footprint this metric set exports.
            pub(crate) fn register_index(&self, index: Arc<KeyIndex>) {
                *self.index.write().unwrap() = Some(index);
            }

            fn index_stats(&self) -> IndexStats {
                self.index
                    .read()
                    .unwrap()
                    .as_deref()
                    .map_or_else(IndexStats::default, KeyIndex::stats)
            }

            /// The counters as one `name=value …` line.
            fn stats_line(&self) -> String {
                let mut line = String::new();
                let index = self.index_stats();
                $(
                    let _ = write!(
                        line,
                        concat!(" ", stringify!($field), "={}"),
                        self.$field.load(Ordering::Relaxed),
                    );
                )+
                let _ = write!(
                    line,
                    " index_objects={} index_logical_bytes={} warm_entries={} \
                     warm_mapped_entries={} warm_disk_bytes={} \
                     warm_disk_budget_bytes={} warm_evictions={} warm_evicted_bytes={}",
                    index.objects,
                    index.logical_bytes,
                    self.warm_entries.load(Ordering::Relaxed),
                    self.warm_mapped_entries.load(Ordering::Relaxed),
                    self.warm_disk_bytes.load(Ordering::Relaxed),
                    self.warm_disk_budget_bytes.load(Ordering::Relaxed),
                    self.warm_evictions.load(Ordering::Relaxed),
                    self.warm_evicted_bytes.load(Ordering::Relaxed),
                );
                line
            }

            /// The counters in Prometheus text exposition format (v0.0.4): a `# TYPE`
            /// line plus a sample per counter, `s3cache_`-prefixed.
            #[must_use]
            pub fn prometheus_text(&self) -> String {
                let mut out = String::new();
                let index = self.index_stats();
                $(
                    let _ = writeln!(
                        out,
                        concat!(
                            "# TYPE s3cache_", stringify!($field), " counter\n",
                            "s3cache_", stringify!($field), " {}",
                        ),
                        self.$field.load(Ordering::Relaxed),
                    );
                )+
                let _ = writeln!(
                    out,
                    "# TYPE s3cache_index_objects gauge\ns3cache_index_objects {}\
                     \n# TYPE s3cache_index_logical_bytes gauge\ns3cache_index_logical_bytes {}\
                     \n# TYPE s3cache_warm_entries gauge\ns3cache_warm_entries {}\
                     \n# TYPE s3cache_warm_mapped_entries gauge\ns3cache_warm_mapped_entries {}\
                     \n# TYPE s3cache_warm_disk_bytes gauge\ns3cache_warm_disk_bytes {}\
                     \n# TYPE s3cache_warm_disk_budget_bytes gauge\ns3cache_warm_disk_budget_bytes {}\
                     \n# TYPE s3cache_warm_evictions counter\ns3cache_warm_evictions {}\
                     \n# TYPE s3cache_warm_evicted_bytes counter\ns3cache_warm_evicted_bytes {}",
                    index.objects,
                    index.logical_bytes,
                    self.warm_entries.load(Ordering::Relaxed),
                    self.warm_mapped_entries.load(Ordering::Relaxed),
                    self.warm_disk_bytes.load(Ordering::Relaxed),
                    self.warm_disk_budget_bytes.load(Ordering::Relaxed),
                    self.warm_evictions.load(Ordering::Relaxed),
                    self.warm_evicted_bytes.load(Ordering::Relaxed),
                );
                out
            }
        }
    };
}

counters! {
    /// Record a LIST answered from the in-memory key index.
    list_from_index => list_from_index,
    /// Record a LIST forwarded to the upstream (bucket not yet synced, or not local).
    list_passthrough => list_passthrough,
    /// Record a `PutObject` folded into the LIST index.
    writes_indexed_put => write_indexed_put,
    /// Record a `CopyObject` folded into the LIST index.
    writes_indexed_copy => write_indexed_copy,
    /// Record a copy whose ETag-matched source index row avoided a metadata HEAD.
    copy_head_avoided => copy_head_avoided,
    /// Record a copy that required a metadata HEAD because its source was unproved.
    copy_head_fallbacks => copy_head_fallback,
    /// Record a create-only copy conflict whose existing destination was folded
    /// into the local index from one authoritative HEAD.
    copy_conflict_reconciled => copy_conflict_reconciled,
    /// Record a create-only copy conflict whose destination remained unobservable
    /// after the bounded authoritative metadata probe.
    copy_conflict_reconcile_misses => copy_conflict_reconcile_miss,
    /// Record a `CompleteMultipartUpload` folded into the LIST index.
    writes_indexed_multipart => write_indexed_multipart,
    /// Record a key folded into the LIST index from a read-path observation
    /// (not a write of ours — the origin proved the key's size).
    writes_indexed_observed => write_indexed_observed,
    /// Record a `PutObject` whose body was kept in the tiers rather than dropped —
    /// one origin GET the object's first read no longer costs.
    write_fill => write_fill,
    /// Record a GET served from a cached body.
    get_hit => get_hit,
    /// Record a GET that missed and was cached on the way back.
    get_miss => get_miss,
    /// Record a GET streamed straight through, uncached.
    get_bypass => get_bypass,
    /// Record a HEAD served from a cached body.
    head_hit => head_hit,
    /// Record a HEAD answered from the LIST index (no upstream call).
    head_index => head_index,
    /// Record a HEAD answered 404 from the LIST index (no upstream call).
    head_404 => head_404,
    /// Record a HEAD forwarded to the upstream.
    head_miss => head_miss,
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
    /// Record an object the warm tier declined because its encoding exceeds the
    /// per-object cap — policy, not failure, and kept out of `warm_error` so that
    /// counter stays alertable.
    warm_rejects => warm_reject,
    /// Record a **suspect** cached body — one filled under an older trust generation, or
    /// decoded off the warm tier after a restart — that proved itself current against
    /// the LIST index instead of being refetched: same `ETag`, and an indexed mtime no
    /// newer than the copy. One index lookup where the alternative was an origin GET.
    body_revalidations => body_revalidation,
    /// Record a suspect cached body the index contradicted — a different `ETag`, no
    /// `ETag` on one side to compare, or a key the synced index no longer holds (the
    /// DELETE this node missed) — so the copy was dropped and the read went to the
    /// origin. This is a warm tier that outlived its coherence being made to pay, which
    /// is what it is for; sustained movement outside a restart means writes are being
    /// missed.
    body_revalidation_evictions => body_revalidation_eviction,
    /// Record a write advertised to peers over the gossip write feed.
    feed_published => feed_published,
    /// Record a peer's write applied from the feed (index + invalidation).
    feed_applied => feed_applied,
    /// Record a feed gap (missed writes): every local body distrusted, index resynced.
    feed_gaps => feed_gap,
    /// Record a write that ended with **no** coherence guarantee: peers still live and
    /// still behind when the wait's deadline passed. In `strong` that is the lease
    /// tier's one unbounded shape — a holder renewing but not applying, whose remedy is
    /// operational rather than a longer deadline — plus any transitional (unleased) peer
    /// that did not ack; in `strong-acks` it is an unresponsive peer. Replicated index
    /// misses already fail through to the origin in every mode. Alertable in every mode.
    ack_timeouts => ack_timeout,
    /// Record a write that completed on a **lease lapse** rather than an acknowledgement:
    /// a peer stopped acking and its serve-lease expired here, so it serves nothing
    /// cached until it re-synchronizes. The guarantee held — this is the slow path
    /// working, not failing, and it is deliberately not `ack_timeouts`. Sustained
    /// movement means a peer is unresponsive, and each such write cost up to one lease
    /// duration.
    write_lease_lapses => write_lease_lapse,
    /// Record a resync this node ran because **its own** serve-lease lapsed with no
    /// write-feed gap to explain it — a peer stopped granting (scale-in, a crash, a
    /// partition that healed without overflowing the ring) — and the staged barrier could
    /// not prove its cache instead, so the lapse watcher fell back to the gap
    /// remediation: every body distrusted, index re-LISTed from the origin, licence
    /// re-affirmed. Each one is a stretch of origin-serving that ends at the reap
    /// horizon, plus a cache that has to buy itself back one key at a time; sustained
    /// movement means a peer is flapping.
    ///
    /// The **expensive** arm of a lapse, and the same set `lapse_barrier_fallbacks`
    /// counts — the two move together, and the pair with `lapse_barrier_retains` is what
    /// says how often the cheap arm is winning.
    lease_lapse_resyncs => lease_lapse_resync,
    /// Record a serve-lease lapse the staged barrier answered by **retaining** the body
    /// cache: the peers that were alive when the lapse landed were still there after the
    /// lease re-confirmed, and their advertised feed heads had all been applied locally,
    /// so every write of the lapse era had already evicted exactly the keys it touched.
    /// Nothing was distrusted, nothing was re-LISTed, and every untouched body kept its
    /// proof — the cheap arm, and the one a healthy fleet should live on.
    lapse_barrier_retains => lapse_barrier_retain,
    /// Record a lapse the staged barrier could **not** answer, so the node fell back to
    /// the full remediation (which is what `lease_lapse_resyncs` counts): a peer that was
    /// alive when the lapse landed vanished from membership before the barrier ran (its
    /// feed frame went with it), a peer's head never arrived, or the lease never
    /// re-confirmed inside the deadline. Fail-closed by construction — every one of these
    /// is "the proof was unavailable", never "the proof failed".
    lapse_barrier_fallbacks => lapse_barrier_fallback,
    /// Record a cache-served read routed to the origin because this node held no licence
    /// to serve it locally: no valid coherence lease (`strong` — booting, warming up,
    /// lapsed, awaiting a resync affirmation, or a granter gone silent), or a membership
    /// view that was not fully alive (`strong-acks`).
    unhealthy_bypasses => unhealthy_bypass,
    /// Record a skeletal index entry completed from an origin response (the one
    /// forwarded HEAD that makes every later HEAD of that key local *and* faithful).
    index_backfills => index_backfill,
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

/// Bind `listen` and serve the counters at `GET /metrics` (anything else is a 404) until
/// the process ends. The 60s stats line can't be graphed or alerted on; this is the same
/// numbers in a form Prometheus can scrape. Off unless `S3CACHE_METRICS_LISTEN` is set.
/// Returns the bound address, which is the requested one unless port 0 asked the OS to
/// choose.
///
/// # Errors
///
/// The bind error, so a misconfigured address fails loudly at startup rather than
/// silently leaving the fleet unscraped.
pub async fn spawn_exporter(
    metrics: Arc<Metrics>,
    listen: &str,
) -> std::io::Result<std::net::SocketAddr> {
    let listener = TcpListener::bind(listen).await?;
    let bound = listener.local_addr()?;
    info!("metrics exporter on {bound} (GET /metrics)");
    tokio::spawn(async move {
        let http = ConnBuilder::new(TokioExecutor::new());
        loop {
            let socket = match listener.accept().await {
                Ok((socket, _)) => socket,
                Err(err) => {
                    tracing::error!("metrics accept error: {err}");
                    continue;
                }
            };
            let metrics = Arc::clone(&metrics);
            // Owned: the connection future outlives this iteration's borrow of the
            // builder, exactly as the S3 listener's does.
            let conn = http
                .serve_connection(
                    TokioIo::new(socket),
                    service_fn(move |req| {
                        let metrics = Arc::clone(&metrics);
                        async move {
                            Ok::<_, Infallible>(scrape(&metrics, req.method(), req.uri().path()))
                        }
                    }),
                )
                .into_owned();
            tokio::spawn(async move {
                let _ = conn.await;
            });
        }
    });
    Ok(bound)
}

/// One scrape: the exposition for `GET /metrics`, a 404 for anything else.
fn scrape(metrics: &Metrics, method: &Method, path: &str) -> Response<Full<Bytes>> {
    if method == Method::GET && path == "/metrics" {
        Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "text/plain; version=0.0.4")
            .body(Full::new(Bytes::from(metrics.prometheus_text())))
            .unwrap_or_else(|_| Response::new(Full::default()))
    } else {
        Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Full::default())
            .unwrap_or_else(|_| Response::new(Full::default()))
    }
}

#[cfg(test)]
mod tests {
    use super::{Metrics, scrape};
    use crate::index::{KeyIndex, ObjEntry, apply_put, standard_class};
    use http::{Method, StatusCode};
    use std::sync::Arc;
    use std::time::SystemTime;
    use tierstore_mmap::MmapDiskStats;

    #[test]
    fn the_stats_line_reports_every_counter() {
        let metrics = Metrics::default();
        metrics.get_hit();
        metrics.get_hit();
        metrics.warm_hit();
        metrics.head_index();
        let line = metrics.stats_line();
        assert!(
            line.starts_with(" list_from_index=0 list_passthrough=0"),
            "{line}"
        );
        assert!(line.contains(" get_hit=2 "), "{line}");
        assert!(line.contains(" warm_hit=1 "), "{line}");
        assert!(line.contains(" head_index=1 "), "{line}");
        assert!(line.contains(" index_backfills=0 "), "{line}");
        assert!(line.ends_with(" warm_evicted_bytes=0"), "{line}");
    }

    /// The exposition is generated from the same declaration as the stats line, so
    /// every counter in one must appear — with the same value — in the other.
    #[test]
    fn the_exposition_covers_exactly_the_stats_line() {
        let metrics = Metrics::default();
        metrics.head_miss();
        metrics.write_indexed_multipart();
        let text = metrics.prometheus_text();
        let mut counted = 0;
        for entry in metrics.stats_line().split_whitespace() {
            let (name, value) = entry.split_once('=').expect("name=value");
            let kind = if matches!(
                name,
                "index_objects"
                    | "index_logical_bytes"
                    | "warm_entries"
                    | "warm_mapped_entries"
                    | "warm_disk_bytes"
                    | "warm_disk_budget_bytes"
            ) {
                "gauge"
            } else {
                "counter"
            };
            assert!(
                text.contains(&format!("# TYPE s3cache_{name} {kind}\n")),
                "{name} has no TYPE line"
            );
            assert!(
                text.contains(&format!("\ns3cache_{name} {value}\n")),
                "{name} is not exposed as {value}"
            );
            counted += 1;
        }
        assert_eq!(
            text.lines().count(),
            counted * 2,
            "the exposition holds a TYPE + sample line per counter and nothing else"
        );
    }

    #[test]
    fn warm_residency_and_evictions_are_exported_from_one_snapshot() {
        let metrics = Metrics::default();
        metrics.observe_warm(MmapDiskStats {
            entries: 17,
            mapped_entries: 5,
            disk_bytes: 4096,
            budget_bytes: Some(8192),
            evictions: 3,
            evicted_bytes: 768,
        });

        let text = metrics.prometheus_text();
        for sample in [
            "s3cache_warm_entries 17",
            "s3cache_warm_mapped_entries 5",
            "s3cache_warm_disk_bytes 4096",
            "s3cache_warm_disk_budget_bytes 8192",
            "s3cache_warm_evictions 3",
            "s3cache_warm_evicted_bytes 768",
        ] {
            assert!(text.contains(sample), "missing {sample}: {text}");
        }
    }

    #[test]
    fn index_footprint_is_rendered_as_prometheus_gauges() {
        let metrics = Metrics::default();
        let index = Arc::new(KeyIndex::default());
        metrics.register_index(Arc::clone(&index));
        apply_put(
            &index,
            "bucket",
            "key",
            ObjEntry {
                size: Some(4096),
                last_modified: SystemTime::now(),
                etag: None,
                storage_class: standard_class(),
                content_type: None,
                meta: None,
            },
        );

        let text = metrics.prometheus_text();
        assert!(
            text.contains("# TYPE s3cache_index_objects gauge\ns3cache_index_objects 1\n"),
            "{text}"
        );
        assert!(
            text.contains(
                "# TYPE s3cache_index_logical_bytes gauge\ns3cache_index_logical_bytes 4096\n"
            ),
            "{text}"
        );
    }

    #[test]
    fn only_get_metrics_is_served() {
        let metrics = Metrics::default();
        let ok = scrape(&metrics, &Method::GET, "/metrics");
        assert_eq!(ok.status(), StatusCode::OK);
        assert_eq!(
            ok.headers()
                .get(http::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok()),
            Some("text/plain; version=0.0.4")
        );
        for (method, path) in [
            (Method::GET, "/"),
            (Method::GET, "/metrics/"),
            (Method::POST, "/metrics"),
        ] {
            assert_eq!(
                scrape(&metrics, &method, path).status(),
                StatusCode::NOT_FOUND,
                "{method} {path}"
            );
        }
    }
}
