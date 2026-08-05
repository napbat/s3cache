//! The `s3cache` binary: the entry point that turns the `S3CACHE_*` environment into a
//! running proxy — the upstream client, the cache tiers, gossip coherence, and the HTTP
//! server. Everything it assembles lives in the `s3cache` library crate; this file is the
//! wiring and the process-level concerns (logging, fd limits, graceful shutdown).

use std::error::Error;
use std::sync::Arc;

use aws_credential_types::provider::ProvideCredentials;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use s3cache::config::Config;
use s3cache::{cache, metrics, sync, tier};
use s3s::auth::SimpleAuth;
use s3s::service::S3ServiceBuilder;
use tokio::net::TcpListener;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    raise_fd_limit();

    let cfg = Config::from_env();

    // Upstream S3 client (R2). Creds + region come from the standard AWS env vars
    // (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_REGION). Path-style for R2.
    let sdk_conf = aws_config::from_env()
        .endpoint_url(&cfg.endpoint)
        .load()
        .await;
    let client = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::config::Builder::from(&sdk_conf)
            .force_path_style(true)
            .build(),
    );
    let proxy = s3s_aws::Proxy::from(client.clone());

    // Optional node-local disk (warm) tier: inclusive, size-limited, survives restarts so
    // a fresh pod comes up warm instead of stampeding the origin. Set S3CACHE_DISK_CACHE
    // to a directory (typically a mounted volume) to enable it.
    let disk = match &cfg.disk_path {
        Some(path) => {
            let (dir, bytes) = (path.display(), cfg.disk_bytes);
            info!("disk (warm) tier at `{dir}`, up to {bytes} bytes");
            Some(tier::open_warm(
                path.clone(),
                cfg.disk_bytes,
                cfg.cache.max_obj_bytes,
            )?)
        }
        None => None,
    };

    // Cross-node coherence: the gossip write feed (see `sync`). Peers' writes fold into
    // the LIST index and invalidate local body copies at network latency; strict reads
    // barrier on feed heads. Set S3CACHE_GOSSIP_BIND (and S3CACHE_GOSSIP_SEEDS as
    // comma-separated id=host:port pairs) to enable; single-node needs none of it.
    let write_sync = sync::from_env(&cfg.node_name).await.map(Arc::new);
    info!("gossip coherence (write feed): {}", write_sync.is_some());

    // Object-body cache: hot (node-local heap) in front of the optional disk tier (warm),
    // in front of the S3 origin (cold). Always layered — no mode to pick. The counters
    // are shared by the tiers, the feed, and the stats task.
    let cp = cache::CachingProxy::new(
        proxy,
        client,
        cfg.cache,
        disk,
        write_sync,
        Arc::new(metrics::Metrics::default()),
    );
    cp.start_coherence(&cfg.buckets);
    // Warm the LIST index for the configured buckets in the BACKGROUND — don't block the
    // port on a full pre-sync. The proxy serves immediately; LISTs pass through to the
    // upstream (always correct) until a bucket's index is complete, then flip to
    // index-served. Keeps startup instant + independent of bucket size. A bucket that
    // fails to sync just stays in passthrough (safe).
    cp.spawn_background_sync(cfg.buckets);
    metrics::spawn_stats(cp.metrics(), cfg.stats_secs);

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

    let listener = TcpListener::bind(&cfg.listen).await?;
    let http_server = ConnBuilder::new(TokioExecutor::new());
    let graceful = hyper_util::server::graceful::GracefulShutdown::new();
    let mut ctrl_c = std::pin::pin!(tokio::signal::ctrl_c());

    let (listen, endpoint) = (&cfg.listen, &cfg.endpoint);
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
