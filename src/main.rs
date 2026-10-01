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
use tracing::{info, warn};

const RECOVERY_REARM_INITIAL_MS: u64 = 5_000;
const RECOVERY_REARM_MAX_MS: u64 = 60_000;

fn configure_recovery_rearm(
    proxy: cache::proxy::CachingProxy,
    enabled: bool,
) -> cache::proxy::CachingProxy {
    if !enabled {
        return proxy;
    }
    proxy
        .with_recovery_rearm(groupnet::consistency::volatile_recovery::RecoveryRearm {
            initial_ms: RECOVERY_REARM_INITIAL_MS,
            max_ms: RECOVERY_REARM_MAX_MS,
        })
        .expect("fixed automatic recovery rearm policy is valid")
}

/// Start gossip coherence with peer index bootstrap when the deployment supplies
/// its peer book; otherwise the guarded origin recovery path.
async fn start_coherence(
    cp: cache::proxy::CachingProxy,
    cfg: &Config,
    region: &str,
) -> cache::proxy::CachingProxy {
    let fleet = sync::fleet::config::FleetConfig::parse(
        std::env::var("S3CACHE_FLEET_BIND").ok(),
        std::env::var("S3CACHE_FLEET_ADVERTISE").ok(),
        std::env::var("S3CACHE_FLEET_PEERS").ok(),
        std::env::var("S3CACHE_FLEET_ORIGIN_ID").ok(),
        &cfg.endpoint,
        region,
        &cfg.node_name,
        &cfg.buckets,
    );
    match fleet {
        Ok(Some(fleet)) => {
            let cp = cp.with_fleet_config(fleet);
            cp.start_fleet_coherence(&cfg.buckets).await;
            cp
        }
        Ok(None) => {
            cp.start_coherence(&cfg.buckets);
            cp
        }
        Err(error) => {
            warn!(
                ?error,
                "fleet configuration refused; using guarded origin recovery"
            );
            cp.start_coherence(&cfg.buckets);
            cp
        }
    }
}

/// Optional Prometheus text endpoint on its own port, so the counters can be graphed
/// and alerted on instead of diffed out of the stats line by hand. Off by default;
/// a bad `S3CACHE_METRICS_LISTEN` fails startup rather than leaving a silent blind spot.
async fn spawn_readiness(
    cp: &cache::proxy::CachingProxy,
    cfg: &Config,
) -> Result<Arc<metrics::StartupReady>, Box<dyn Error + Send + Sync + 'static>> {
    let readiness = Arc::new(metrics::StartupReady::default());
    if let Some(listen) = &cfg.metrics_listen {
        let probe = cp.clone();
        let buckets = cfg.buckets.clone();
        let latch = Arc::clone(&readiness);
        tokio::spawn(async move {
            loop {
                if probe.initially_ready(&buckets) {
                    latch.mark_index_ready();
                    info!(
                        "initial index ready for {} configured buckets",
                        buckets.len()
                    );
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            }
        });
        metrics::spawn_exporter(cp.metrics(), Arc::clone(&readiness), listen).await?;
    }
    Ok(readiness)
}

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
    let proxy = s3s_aws::Proxy::builder(client.clone()).build();

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
    let write_sync = sync::config::from_env(&cfg.node_name).await.map(Arc::new);
    info!("gossip coherence (write feed): {}", write_sync.is_some());
    // Kept for the shutdown path: a planned stop retracts this node's serve-lease
    // instead of letting peers wait it out (see `WriteSync::leave`).
    let leaving = write_sync.clone();

    // Object-body cache: hot (node-local heap) in front of the optional disk tier (warm),
    // in front of the S3 origin (cold). Always layered — no mode to pick.
    let cp = cache::proxy::CachingProxy::new(proxy, client, cfg.cache, disk, write_sync, counters)
        .with_index_scan(cfg.index_scan);
    let cp = configure_recovery_rearm(cp, cfg.recovery_rearm);
    let cp = start_coherence(
        cp,
        &cfg,
        sdk_conf.region().map_or("", |region| region.as_ref()),
    )
    .await;
    // Warm the LIST index for the configured buckets in the BACKGROUND — don't block the
    // port on a full pre-sync. The proxy serves immediately; LISTs pass through to the
    // upstream (always correct) until a bucket's index is complete, then flip to
    // index-served. Keeps startup instant + independent of bucket size. A bucket that
    // fails to sync just stays in passthrough (safe).
    cp.spawn_background_sync(cfg.buckets.clone());
    metrics::spawn_stats(cp.metrics(), cfg.stats_secs);
    let readiness = spawn_readiness(&cp, &cfg).await?;
    // Kept for the shutdown path too: a drained stop seals the write feed through it.
    let stopping_proxy = cp.clone();

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
    // Origin forwarding is available before the index is complete. Do not remove
    // every cold pod from the Service during overlapping rollouts or a cold start.
    readiness.mark_ready();
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

    let drained = tokio::select! {
        () = graceful.shutdown() => {
            info!("graceful shutdown complete");
            true
        }
        () = tokio::time::sleep(DRAIN_WAIT) => {
            info!("shutdown timed out");
            false
        }
    };
    // Seal only a drained stop. A request still running could publish after the
    // seal, and the seal would promise peers a tail this life did not end at; the
    // restart then stays an ordinary gap, which is always safe.
    if drained {
        stopping_proxy.seal_writes(SEAL_WAIT).await;
    } else {
        warn!("requests were still running at the drain deadline; not sealing the write feed");
    }
    Ok(())
}

/// How long a planned stop lets in-flight requests finish.
const DRAIN_WAIT: std::time::Duration = std::time::Duration::from_secs(10);

/// How long a drained stop waits for PUT tails and for peers to acknowledge the feed
/// seal. With [`DRAIN_WAIT`] it must fit inside the pod's termination grace period
/// (the Helm chart's `terminationGracePeriodSeconds`).
const SEAL_WAIT: std::time::Duration = std::time::Duration::from_secs(5);

/// Resolves on the first signal that means "stop": `SIGTERM` or `ctrl_c`.
///
/// `SIGTERM` is the one that matters in production and the one this process would
/// otherwise not hear at all — it is what a `StatefulSet` rollout, a scale-in, and
/// `kubectl delete pod` all send, and Kubernetes follows it with `SIGKILL` at the end of
/// the grace period. Catching it is what turns a planned stop into a *planned* stop:
/// connections drain, and the coherence lease is retracted rather than left for peers to
/// wait out (see [`sync::coherence::WriteSync::leave`]).
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
