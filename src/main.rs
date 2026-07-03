//! Transparent S3 caching proxy. Milestone 1: passthrough to the upstream S3 (R2).
//! Caching (LIST-from-index, GET LRU, write-through index updates) is layered on top
//! of the upstream `s3s_aws::Proxy` in the `cache` module.

mod cache;

use std::error::Error;

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
    // Object-cache sizing: total LRU capacity + per-object cap (bigger objects, e.g.
    // segment blobs, stream straight through and aren't cached).
    let cache_bytes: u64 = env_or("S3CACHE_CACHE_BYTES", "268435456").parse().unwrap_or(268_435_456);
    let max_obj_bytes: usize = env_or("S3CACHE_MAX_OBJECT_BYTES", "8388608").parse().unwrap_or(8_388_608);
    let cp = cache::CachingProxy::new(proxy, client, cache_bytes, max_obj_bytes);

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
