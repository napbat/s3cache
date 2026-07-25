//! Transparent, S3-compatible caching proxy. Binds an S3 API, forwards to an upstream
//! S3 (e.g. R2), and layers LIST-from-index + a hot/warm/cold body cache on top. This is
//! the entry point: it wires config, the upstream client, the cache tiers, and the HTTP
//! server. The proxy lives in `cache`, with the LIST index in `index`, the object-body
//! tiers in `tier`, cross-node coherence (the gossip write feed) in `sync`, and counters
//! in `metrics`.

mod cache;
mod index;
mod metrics;
mod sync;
mod tier;

use std::error::Error;
use std::sync::Arc;

use aws_credential_types::provider::ProvideCredentials;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use s3s::auth::SimpleAuth;
use s3s::service::S3ServiceBuilder;
use tokio::net::TcpListener;
use tracing::info;

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_owned())
}

/// Wire the gossip apply loop into the proxy: peers' events fold into the
/// LIST index and invalidate local copies; a gap flushes and resyncs.
fn start_coherence(
    cp: &cache::CachingProxy,
    write_sync: Option<&Arc<sync::WriteSync>>,
    buckets: &[String],
    metrics: &Arc<metrics::Metrics>,
) {
    if let Some(write_sync) = write_sync {
        write_sync.start_apply(
            cp.local_cache(),
            cp.index_state(),
            cp.gap_resync_handle(buckets.to_vec()),
            metrics.clone(),
        );
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    raise_fd_limit();

    let listen = env_or("S3CACHE_LISTEN", "0.0.0.0:8014");
    let endpoint = std::env::var("S3CACHE_UPSTREAM_ENDPOINT")
        .expect("S3CACHE_UPSTREAM_ENDPOINT is required (the upstream S3/R2 endpoint URL)");

    // Upstream S3 client (R2). Creds + region come from the standard AWS env vars
    // (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_REGION). Path-style for R2.
    let sdk_conf = aws_config::from_env().endpoint_url(&endpoint).load().await;
    let client = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::config::Builder::from(&sdk_conf)
            .force_path_style(true)
            .build(),
    );
    let proxy = s3s_aws::Proxy::from(client.clone());
    // Object-cache sizing: total capacity + per-object cap (bigger objects, e.g. segment
    // blobs, stream straight through and aren't cached).
    let cache_bytes: u64 = env_or("S3CACHE_CACHE_BYTES", "268435456")
        .parse()
        .unwrap_or(268_435_456);
    let max_obj_bytes: usize = env_or("S3CACHE_MAX_OBJECT_BYTES", "8388608")
        .parse()
        .unwrap_or(8_388_608);

    // Object-body cache: hot (node-local heap) in front of an optional node-local disk
    // tier (warm), in front of the S3 origin (cold). Always layered — no mode to pick.
    let metrics = Arc::new(metrics::Metrics::default());

    // Optional node-local disk (warm) tier: inclusive, size-limited, survives restarts so
    // a fresh pod comes up warm instead of stampeding the origin. Set S3CACHE_DISK_CACHE
    // to a directory (typically a mounted volume) to enable it.
    let disk_path = env_or("S3CACHE_DISK_CACHE", "");
    let disk = if disk_path.is_empty() {
        None
    } else {
        let disk_bytes: u64 = env_or("S3CACHE_DISK_CACHE_BYTES", "10737418240")
            .parse()
            .unwrap_or(10_737_418_240);
        info!("disk (warm) tier at `{disk_path}`, up to {disk_bytes} bytes");
        Some(tier::open_warm(
            std::path::PathBuf::from(&disk_path),
            disk_bytes,
            max_obj_bytes,
        )?)
    };

    let node_name = env_or("HOSTNAME", "s3cache");

    // Cross-node coherence: the gossip write feed (see `sync`). Peers' writes fold into
    // the LIST index and invalidate local body copies at network latency; strict reads
    // barrier on feed heads. Set S3CACHE_GOSSIP_BIND (and S3CACHE_GOSSIP_SEEDS as
    // comma-separated id=host:port pairs) to enable; single-node needs none of it.
    let write_sync = sync::from_env(&node_name).await.map(Arc::new);
    info!("gossip coherence (write feed): {}", write_sync.is_some());

    let cfg = cache::CacheConfig {
        cache_bytes,
        max_obj_bytes,
    };
    let cp = cache::CachingProxy::new(
        proxy,
        client,
        cfg,
        disk,
        write_sync.clone(),
        metrics.clone(),
    );

    // Warm the LIST index for the configured buckets in the BACKGROUND — don't block the
    // port on a full pre-sync. The proxy serves immediately; LISTs pass through to the
    // upstream (always correct) until a bucket's index is complete, then flip to
    // index-served. Keeps startup instant + independent of bucket size. A bucket that
    // fails to sync just stays in passthrough (safe).
    let buckets: Vec<String> = env_or("S3CACHE_BUCKETS", "")
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect();
    start_coherence(&cp, write_sync.as_ref(), &buckets, &metrics);
    cp.spawn_background_sync(buckets);
    let stats_secs: u64 = env_or("S3CACHE_STATS_SECS", "60").parse().unwrap_or(60);
    metrics::spawn_stats(cp.metrics(), stats_secs);

    let service = {
        let mut b = S3ServiceBuilder::new(cp);
        // Authenticate inbound requests with the same key the upstream uses (in-cluster
        // clients sign with these creds; the proxy re-signs to the upstream).
        if let Some(cp) = sdk_conf.credentials_provider() {
            let cred = cp.provide_credentials().await?;
            b.set_auth(SimpleAuth::from_single(
                cred.access_key_id(),
                cred.secret_access_key(),
            ));
        }
        b.build()
    };

    let listener = TcpListener::bind(&listen).await?;
    let http_server = ConnBuilder::new(TokioExecutor::new());
    let graceful = hyper_util::server::graceful::GracefulShutdown::new();
    let mut ctrl_c = std::pin::pin!(tokio::signal::ctrl_c());

    info!("s3cache listening on {listen}, upstream {endpoint}");

    loop {
        let (socket, _) = tokio::select! {
            res = listener.accept() => match res {
                Ok(conn) => conn,
                Err(err) => { tracing::error!("accept error: {err}"); continue; }
            },
            _ = ctrl_c.as_mut() => break,
        };
        let conn = http_server.serve_connection(TokioIo::new(socket), service.clone());
        let conn = graceful.watch(conn.into_owned());
        tokio::spawn(async move {
            let _ = conn.await;
        });
    }

    tokio::select! {
        () = graceful.shutdown() => info!("graceful shutdown complete"),
        () = tokio::time::sleep(std::time::Duration::from_secs(10)) => info!("shutdown timed out"),
    }
    Ok(())
}

/// Raise `RLIMIT_NOFILE`'s soft limit to the hard cap. The proxy holds one socket per
/// inbound connection plus its upstream pool; a chatty client fleet can keep thousands
/// of keepalive connections open, so the distro-default 1024 soft limit exhausts and
/// `accept()` starts failing with EMFILE.
fn raise_fd_limit() {
    #[cfg(unix)]
    match rlimit::Resource::NOFILE.get() {
        Ok((soft, hard)) if soft < hard => match rlimit::Resource::NOFILE.set(hard, hard) {
            Ok(()) => info!("raised open-file soft limit {soft} -> {hard}"),
            Err(err) => tracing::warn!("could not raise open-file soft limit: {err}"),
        },
        Ok(_) => {}
        Err(err) => tracing::warn!("could not read the open-file limit: {err}"),
    }
}
