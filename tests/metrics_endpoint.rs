//! The Prometheus endpoint as a scraper actually meets it: a real listener, real HTTP,
//! real exposition text. The unit tests cover the routing decision; this covers the
//! wiring around it — that `S3CACHE_METRICS_LISTEN` produces a port a scraper can talk
//! to, and that what comes back parses as the format Prometheus expects.

use std::net::SocketAddr;
use std::sync::Arc;

use http::{Request, StatusCode};
use http_body_util::{BodyExt, Empty};
use hyper_util::rt::TokioIo;
use s3cache::metrics::{Metrics, spawn_exporter};
use tokio::net::TcpStream;

/// One scrape over the wire.
async fn scrape(addr: SocketAddr, path: &str) -> (StatusCode, String) {
    let stream = TcpStream::connect(addr).await.expect("connect to exporter");
    let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .expect("http handshake");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    let req = Request::builder()
        .uri(path)
        .header("host", addr.to_string())
        .body(Empty::<bytes::Bytes>::new())
        .expect("a well-formed request");
    let resp = sender.send_request(req).await.expect("a response");
    let status = resp.status();
    let body = resp.into_body().collect().await.expect("a body").to_bytes();
    (status, String::from_utf8(body.to_vec()).expect("utf-8"))
}

#[tokio::test]
async fn the_exporter_serves_the_counters_as_prometheus_text() {
    let metrics = Arc::new(Metrics::default());
    let addr = spawn_exporter(Arc::clone(&metrics), "127.0.0.1:0")
        .await
        .expect("the exporter binds");

    let (status, body) = scrape(addr, "/metrics").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body.contains("# TYPE s3cache_head_index counter\ns3cache_head_index 0\n"),
        "{body}"
    );

    // Every line is either a TYPE declaration or a `name value` sample, and every
    // counter appears exactly once as each — the shape a scraper requires.
    let (types, samples): (Vec<_>, Vec<_>) = body.lines().partition(|line| line.starts_with("# "));
    assert_eq!(types.len(), samples.len(), "{body}");
    for sample in &samples {
        let (name, value) = sample.split_once(' ').expect("name value");
        assert!(name.starts_with("s3cache_"), "{name}");
        assert!(value.parse::<u64>().is_ok(), "{sample}");
    }

    assert_eq!(scrape(addr, "/").await.0, StatusCode::NOT_FOUND);
}
