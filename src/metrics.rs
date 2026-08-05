//! Cache-effectiveness counters, the task that periodically logs them, and the optional
//! Prometheus text endpoint that exposes them for scraping.

use std::convert::Infallible;
use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use http::{Method, Response, StatusCode, header};
use http_body_util::Full;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use tokio::net::TcpListener;
use tracing::info;

/// Declares the counter set once: the fields, a bump method per counter, the stats
/// line, and the Prometheus exposition — all generated in declaration order, so a
/// counter can never be added without being logged *and* scrapeable. The `warm_*`
/// counters cover the disk (warm) tier, the `feed_*` counters the gossip write feed, and
/// the rest the index and hot path.
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

            /// The counters in Prometheus text exposition format (v0.0.4): a `# TYPE`
            /// line plus a sample per counter, `s3cache_`-prefixed.
            #[must_use]
            pub fn prometheus_text(&self) -> String {
                let mut out = String::new();
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
    /// Record a `CompleteMultipartUpload` folded into the LIST index.
    writes_indexed_multipart => write_indexed_multipart,
    /// Record a key folded into the LIST index from a read-path observation
    /// (not a write of ours — the origin proved the key's size).
    writes_indexed_observed => write_indexed_observed,
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
    use http::{Method, StatusCode};

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
        assert!(line.ends_with(" index_backfills=0"), "{line}");
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
            assert!(
                text.contains(&format!("# TYPE s3cache_{name} counter\n")),
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
