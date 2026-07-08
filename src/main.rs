//! Transparent, S3-compatible caching proxy. Binds an S3 API, forwards to an upstream
//! S3 (e.g. R2), and layers LIST-from-index + a hot/warm/cold body cache on top via the
//! `cache` and `tier` modules. This is the entry point: it wires config, the upstream
//! client, the cache tiers, and the HTTP server.

mod cache;
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
    let cache_bytes: u64 = env_or("S3CACHE_CACHE_BYTES", "268435456").parse().unwrap_or(268_435_456);
    let max_obj_bytes: usize = env_or("S3CACHE_MAX_OBJECT_BYTES", "8388608").parse().unwrap_or(8_388_608);

    // Cache tiers: hot (node-local heap), warm (shared Valkey), cold (the S3 origin).
    // S3CACHE_MODE selects which sit in front of the origin; a warm-only mode skips the
    // node-local copy so every node stays coherent. S3CACHE_INDEX_LOG additionally shares
    // the LIST index across nodes via a Valkey Streams commit log (multi-replica safe).
    let mode = tier::CacheMode::parse(&env_or("S3CACHE_MODE", "hot"));
    let index_log_on = matches!(env_or("S3CACHE_INDEX_LOG", "false").trim().to_ascii_lowercase().as_str(), "true" | "1" | "on" | "yes");
    let metrics = Arc::new(cache::Metrics::default());

    // One Valkey pool (appends + warm ops), shared by the warm object cache and the
    // index commit log; the log additionally gets a dedicated connection for its
    // blocking reads so they can't stall an append.
    let valkey_url = (mode.warm() || index_log_on)
        .then(|| std::env::var("S3CACHE_VALKEY_URL").expect("S3CACHE_VALKEY_URL is required when the warm tier or the index log is enabled"));
    let valkey = if let Some(url) = &valkey_url {
        let pool_size: usize = env_or("S3CACHE_VALKEY_POOL", "4").parse().unwrap_or(4);
        let pool = tier::connect_valkey(url, pool_size)?;
        info!("Valkey pool of {pool_size} (connecting in background)");
        Some(pool)
    } else {
        None
    };

    let warm = valkey.clone().filter(|_| mode.warm()).map(|pool| {
        let ttl_secs: u64 = env_or("S3CACHE_WARM_TTL_SECS", "0").parse().unwrap_or(0);
        info!("warm object tier enabled");
        tier::WarmCache::new(pool, max_obj_bytes, (ttl_secs > 0).then_some(ttl_secs), metrics.clone())
    });

    let index_log = if index_log_on {
        let pool = valkey.clone().expect("valkey pool present when the index log is enabled");
        let read = tier::connect_valkey_client(valkey_url.as_deref().unwrap())?;
        let stream = env_or("S3CACHE_INDEX_LOG_STREAM", "s3cache:index:log");
        let maxlen: u64 = env_or("S3CACHE_INDEX_LOG_MAXLEN", "1000000").parse().unwrap_or(1_000_000);
        let node = env_or("HOSTNAME", "s3cache");
        info!("index log enabled: stream `{stream}` maxlen ~{maxlen} node `{node}`");
        Some(cache::IndexLog::new(pool, read, stream, maxlen, node, metrics.clone()))
    } else {
        None
    };

    // Strong cross-node LIST: index-served LIST waits for the commit log to catch up
    // before answering (needs the index log; no effect single-node, which is already
    // strict). Trades a little LIST latency for read-after-write across nodes.
    let strict_list = matches!(env_or("S3CACHE_STRICT_LIST", "false").trim().to_ascii_lowercase().as_str(), "true" | "1" | "on" | "yes");
    info!("cache mode: {mode:?}, index log: {index_log_on}, strict LIST: {strict_list}");
    let cfg = cache::CacheConfig { mode, cache_bytes, max_obj_bytes, strict_list };
    let cp = cache::CachingProxy::new(proxy, client, cfg, warm, index_log, metrics.clone());

    // Start tailing the shared commit log BEFORE the bootstrap LIST, so the replay window
    // begins at-or-before the LIST's snapshot and no peer write slips through the gap.
    cp.start_index_log().await;

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
    cp.spawn_background_sync(buckets);
    let stats_secs: u64 = env_or("S3CACHE_STATS_SECS", "60").parse().unwrap_or(60);
    cache::spawn_stats(cp.metrics(), stats_secs);

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
