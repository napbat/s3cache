//! An ignored local probe for scan latency and origin LIST request counts.

use std::collections::BTreeSet;
use std::convert::Infallible;
use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

use super::ScanConfig;
use crate::index::{KeyIndex, sync_bucket_into_with_config};

fn decode(input: &str) -> String {
    let mut output = Vec::new();
    let mut bytes = input.as_bytes().iter().copied();
    while let Some(byte) = bytes.next() {
        if byte == b'%' {
            let high = bytes.next().and_then(|value| (value as char).to_digit(16));
            let low = bytes.next().and_then(|value| (value as char).to_digit(16));
            if let (Some(high), Some(low)) = (high, low) {
                output.push(u8::try_from(high * 16 + low).unwrap_or_default());
            }
        } else if byte == b'+' {
            output.push(b' ');
        } else {
            output.push(byte);
        }
    }
    String::from_utf8(output).expect("fixture query is UTF-8")
}

fn query(req: &Request<Incoming>, name: &str) -> Option<String> {
    req.uri().query()?.split('&').find_map(|pair| {
        let (key, value) = pair.split_once('=')?;
        (key == name).then(|| decode(value))
    })
}

fn list_xml(keys: &[String], req: &Request<Incoming>) -> String {
    let prefix = query(req, "prefix").unwrap_or_default();
    let cursor = query(req, "continuation-token")
        .or_else(|| query(req, "start-after"))
        .unwrap_or_default();
    let delimiter = query(req, "delimiter");
    let max = query(req, "max-keys")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(1000);
    let entries: Vec<(String, bool)> = if let Some(separator) = delimiter.as_deref() {
        let mut entries = BTreeSet::new();
        for key in keys.iter().filter(|key| key.starts_with(&prefix)) {
            let entry = match key[prefix.len()..].find(separator) {
                Some(at) => (
                    prefix.clone() + &key[prefix.len()..prefix.len() + at + separator.len()],
                    true,
                ),
                None => (key.clone(), false),
            };
            if entry.0 > cursor {
                entries.insert(entry);
            }
        }
        entries.into_iter().take(max + 1).collect()
    } else {
        let first = keys.partition_point(|key| key <= &cursor);
        keys[first..]
            .iter()
            .filter(|key| key.starts_with(&prefix))
            .take(max + 1)
            .map(|key| (key.clone(), false))
            .collect()
    };
    let truncated = entries.len() > max;
    let page: Vec<_> = entries.into_iter().take(max).collect();
    let mut xml = format!(
        "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>probe</Name><Prefix>{prefix}</Prefix><KeyCount>{}</KeyCount><MaxKeys>{max}</MaxKeys><IsTruncated>{truncated}</IsTruncated>",
        page.len()
    );
    for (key, common) in &page {
        if *common {
            write!(
                xml,
                "<CommonPrefixes><Prefix>{key}</Prefix></CommonPrefixes>"
            )
            .expect("writing to String cannot fail");
        } else {
            write!(xml, "<Contents><Key>{key}</Key><LastModified>2024-01-01T00:00:00.000Z</LastModified><ETag>\"x\"</ETag><Size>1</Size><StorageClass>STANDARD</StorageClass></Contents>")
                .expect("writing to String cannot fail");
        }
    }
    if truncated {
        let last = &page.last().expect("a truncated page has a key").0;
        write!(xml, "<NextContinuationToken>{last}</NextContinuationToken>")
            .expect("writing to String cannot fail");
    }
    xml.push_str("</ListBucketResult>");
    xml
}

/// Run with `cargo test --locked --lib scan_latency_probe -- --ignored --nocapture`.
/// The 40 ms LIST delay is injected. It is not a forecast for R2 latency.
#[tokio::test]
#[ignore = "local timing probe"]
async fn scan_latency_probe() {
    let keys: Arc<Vec<String>> = Arc::new(
        (0..1024)
            .flat_map(|shard| {
                (0..32).map(move |object| format!("ns/tenant/s/{shard:04x}/seg/{object:04x}"))
            })
            .collect(),
    );
    let requests = Arc::new(AtomicUsize::new(0));
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind probe");
    let address = listener.local_addr().expect("probe address");
    let server = tokio::spawn({
        let keys = Arc::clone(&keys);
        let requests = Arc::clone(&requests);
        async move {
            loop {
                let (socket, _) = listener.accept().await.expect("accept probe request");
                let keys = Arc::clone(&keys);
                let requests = Arc::clone(&requests);
                tokio::spawn(async move {
                    let service = service_fn(move |req: Request<Incoming>| {
                        let keys = Arc::clone(&keys);
                        let requests = Arc::clone(&requests);
                        async move {
                            requests.fetch_add(1, Ordering::Relaxed);
                            tokio::time::sleep(Duration::from_millis(40)).await;
                            let xml = list_xml(&keys, &req);
                            Ok::<_, Infallible>(Response::new(Full::new(Bytes::from(xml))))
                        }
                    });
                    let _ = http1::Builder::new()
                        .serve_connection(TokioIo::new(socket), service)
                        .await;
                });
            }
        }
    });
    let client = client_for(address);
    for (name, plan) in [
        (
            "serial",
            ScanConfig {
                workers: 1,
                discovery_budget: 0,
            },
        ),
        (
            "parallel_8",
            ScanConfig {
                workers: 8,
                discovery_budget: 16,
            },
        ),
    ] {
        let before = requests.load(Ordering::Relaxed);
        let index = KeyIndex::default();
        let start = Instant::now();
        let found = sync_bucket_into_with_config(&client, &index, "probe", plan)
            .await
            .expect("full scan");
        let elapsed = start.elapsed();
        assert_eq!(found, keys.len());
        let guard = index.read().expect("index lock");
        assert_eq!(guard.get("probe").expect("bucket").keys.len(), keys.len());
        println!(
            "{name}: keys={found} list_requests={} elapsed_ms={}",
            requests.load(Ordering::Relaxed) - before,
            elapsed.as_millis()
        );
    }
    server.abort();
}

fn client_for(address: std::net::SocketAddr) -> aws_sdk_s3::Client {
    let config = aws_sdk_s3::config::Builder::new()
        .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
        .region(aws_sdk_s3::config::Region::new("us-east-1"))
        .credentials_provider(aws_sdk_s3::config::Credentials::new(
            "probe",
            "probe",
            None,
            None,
            "scan-probe",
        ))
        .endpoint_url(format!("http://{address}"))
        .force_path_style(true)
        .build();
    aws_sdk_s3::Client::from_conf(config)
}

#[tokio::test]
async fn truncated_page_without_token_stays_unsynced_and_retry_discards_it() {
    let faulty = Arc::new(AtomicBool::new(true));
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind fixture");
    let address = listener.local_addr().expect("fixture address");
    let server = tokio::spawn({
        let faulty = Arc::clone(&faulty);
        async move {
            loop {
                let (socket, _) = listener.accept().await.expect("accept fixture request");
                let faulty = Arc::clone(&faulty);
                tokio::spawn(async move {
                    let service = service_fn(move |_req: Request<Incoming>| {
                        let bad = faulty.load(Ordering::Relaxed);
                        async move {
                            let key = if bad { "stale" } else { "current" };
                            let xml = format!(
                                "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>probe</Name><IsTruncated>{bad}</IsTruncated><Contents><Key>{key}</Key><LastModified>2024-01-01T00:00:00.000Z</LastModified><ETag>\"x\"</ETag><Size>1</Size><StorageClass>STANDARD</StorageClass></Contents></ListBucketResult>"
                            );
                            Ok::<_, Infallible>(Response::new(Full::new(Bytes::from(xml))))
                        }
                    });
                    let _ = http1::Builder::new()
                        .serve_connection(TokioIo::new(socket), service)
                        .await;
                });
            }
        }
    });
    let client = client_for(address);
    let index = KeyIndex::default();
    let serial = ScanConfig {
        workers: 1,
        discovery_budget: 0,
    };
    assert!(
        sync_bucket_into_with_config(&client, &index, "probe", serial)
            .await
            .is_err()
    );
    assert!(!index.read().expect("index lock")["probe"].synced);

    faulty.store(false, Ordering::Relaxed);
    assert_eq!(
        sync_bucket_into_with_config(&client, &index, "probe", serial)
            .await
            .expect("retry full scan"),
        1
    );
    let state = index.read().expect("index lock");
    assert!(state["probe"].synced);
    assert!(state["probe"].keys.contains_key("current"));
    assert!(!state["probe"].keys.contains_key("stale"));
    server.abort();
}
