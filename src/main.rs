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

    // One counter set for the whole process: the tiers, the write feed, the proxy and
    // the stats task all report into it.
    let counters = Arc::new(metrics::Metrics::default());

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
                Arc::clone(&counters),
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
    // Kept for the shutdown path: a planned stop retracts this node's serve-lease
    // instead of letting peers wait it out (see `WriteSync::leave`).
    let leaving = write_sync.clone();

    // Object-body cache: hot (node-local heap) in front of the optional disk tier (warm),
    // in front of the S3 origin (cold). Always layered — no mode to pick.
    let cp = cache::CachingProxy::new(proxy, client, cfg.cache, disk, write_sync, counters);
    cp.start_coherence(&cfg.buckets);
    // Warm the LIST index for the configured buckets in the BACKGROUND — don't block the
    // port on a full pre-sync. The proxy serves immediately; LISTs pass through to the
    // upstream (always correct) until a bucket's index is complete, then flip to
    // index-served. Keeps startup instant + independent of bucket size. A bucket that
    // fails to sync just stays in passthrough (safe).
    cp.spawn_background_sync(cfg.buckets);
    metrics::spawn_stats(cp.metrics(), cfg.stats_secs);
    // Optional Prometheus text endpoint on its own port, so the counters can be graphed
    // and alerted on instead of diffed out of the stats line by hand. Off by default;
    // a bad S3CACHE_METRICS_LISTEN fails startup rather than leaving a silent blind spot.
    if let Some(listen) = &cfg.metrics_listen {
        metrics::spawn_exporter(cp.metrics(), listen).await?;
    }

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
    let mut stopping = std::pin::pin!(stop_signal());

    let (listen, endpoint) = (&cfg.listen, &cfg.endpoint);
    info!("s3cache listening on {listen}, upstream {endpoint}");

    loop {
        let (socket, _) = tokio::select! {
            res = listener.accept() => match res {
                Ok(conn) => conn,
                Err(err) => { tracing::error!("accept error: {err}"); continue; }
            },
            () = stopping.as_mut() => break,
        };
        let conn = http_server.serve_connection(TokioIo::new(socket), service.clone());
        let conn = graceful.watch(conn.into_owned());
        tokio::spawn(async move {
            let _ = conn.await;
        });
    }

    // Announce the departure before draining, not after: the retraction is one gossip
    // entry and the drain can take seconds, and every one of them is a second a peer's
    // next write might spend waiting out a lease this node has already stopped using.
    if let Some(sync) = &leaving {
        sync.leave();
    }

    tokio::select! {
        () = graceful.shutdown() => info!("graceful shutdown complete"),
        () = tokio::time::sleep(std::time::Duration::from_secs(10)) => info!("shutdown timed out"),
    }
    Ok(())
}

/// Resolves on the first signal that means "stop": `SIGTERM` or `ctrl_c`.
///
/// `SIGTERM` is the one that matters in production and the one this process would
/// otherwise not hear at all — it is what a `StatefulSet` rollout, a scale-in, and
/// `kubectl delete pod` all send, and Kubernetes follows it with `SIGKILL` at the end of
/// the grace period. Catching it is what turns a planned stop into a *planned* stop:
/// connections drain, and the coherence lease is retracted rather than left for peers to
/// wait out (see [`sync::WriteSync::leave`]).
///
/// A platform without `SIGTERM` (a developer's Windows workstation) keeps `ctrl_c`,
/// which is the only stop it can send.
async fn stop_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        // A failed registration is not a reason to run unstoppable: fall back to
        // `ctrl_c` alone, loudly, so the missing half is visible in the log.
        match signal(SignalKind::terminate()) {
            Ok(mut sigterm) => {
                tokio::select! {
                    _ = sigterm.recv() => info!("SIGTERM received; shutting down"),
                    _ = tokio::signal::ctrl_c() => info!("interrupted; shutting down"),
                }
            }
            Err(err) => {
                tracing::warn!("cannot listen for SIGTERM ({err}); ctrl_c only");
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
        info!("interrupted; shutting down");
    }
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
