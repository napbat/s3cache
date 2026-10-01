//! Exercise the binary's readiness boundary while its origin LIST is blocked.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;

struct Process(Child);

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn free_address() -> SocketAddr {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

async fn status(address: SocketAddr, path: &str) -> Option<http::StatusCode> {
    let socket = TcpStream::connect(address).await.ok()?;
    let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(socket))
        .await
        .ok()?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let response = sender
        .send_request(
            http::Request::builder()
                .uri(path)
                .header("host", address.to_string())
                .body(Full::new(Bytes::new()))
                .unwrap(),
        )
        .await
        .ok()?;
    let code = response.status();
    response.into_body().collect().await.ok()?;
    Some(code)
}

async fn wait_ready(address: SocketAddr, path: &str) {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if status(address, path).await == Some(http::StatusCode::OK) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{path} did not become ready"));
}

#[tokio::test]
async fn cold_binary_accepts_requests_before_the_index_finishes() {
    let origin = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin_address = origin.local_addr().unwrap();
    let release = Arc::new(Semaphore::new(0));
    let entered = Arc::new(Semaphore::new(0));
    let origin_task = {
        let release = Arc::clone(&release);
        let entered = Arc::clone(&entered);
        tokio::spawn(async move {
            loop {
                let (socket, _) = origin.accept().await.unwrap();
                let release = Arc::clone(&release);
                let entered = Arc::clone(&entered);
                tokio::spawn(async move {
                    let service = service_fn(move |_| {
                        let release = Arc::clone(&release);
                        let entered = Arc::clone(&entered);
                        async move {
                            entered.add_permits(1);
                            let _permit = release.acquire().await.unwrap();
                            Ok::<_, Infallible>(http::Response::new(Full::new(Bytes::from_static(
                                b"<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>test</Name><KeyCount>0</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated></ListBucketResult>",
                            ))))
                        }
                    });
                    let _ = Builder::new(TokioExecutor::new())
                        .serve_connection(TokioIo::new(socket), service)
                        .await;
                });
            }
        })
    };

    let s3_address = free_address();
    let metrics_address = free_address();
    let _process = Process(
        Command::new(env!("CARGO_BIN_EXE_s3cache"))
            .env(
                "S3CACHE_UPSTREAM_ENDPOINT",
                format!("http://{origin_address}"),
            )
            .env("S3CACHE_LISTEN", s3_address.to_string())
            .env("S3CACHE_METRICS_LISTEN", metrics_address.to_string())
            .env("S3CACHE_BUCKETS", "test")
            .env("S3CACHE_INDEX_SCAN_CONCURRENCY", "1")
            .env("S3CACHE_DISK_CACHE", "")
            .env("S3CACHE_GOSSIP_BIND", "")
            .env("AWS_ACCESS_KEY_ID", "test")
            .env("AWS_SECRET_ACCESS_KEY", "test")
            .env("AWS_REGION", "us-east-1")
            .env("AWS_EC2_METADATA_DISABLED", "true")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    tokio::time::timeout(Duration::from_secs(15), entered.acquire())
        .await
        .expect("the index starts its origin LIST")
        .unwrap()
        .forget();
    wait_ready(metrics_address, "/ready").await;
    assert!(TcpStream::connect(s3_address).await.is_ok());
    assert_eq!(
        status(metrics_address, "/index-ready").await,
        Some(http::StatusCode::SERVICE_UNAVAILABLE),
        "listener readiness must not claim the blocked index is complete"
    );
    release.add_permits(1);
    wait_ready(metrics_address, "/index-ready").await;
    origin_task.abort();
}
