//! Shared end-to-end test infrastructure: a **real S3 origin** — `MinIO` in a container
//! (testcontainers) — reached through a transparent request counter, with a real
//! [`CachingProxy`] in front of it.
//!
//! Two things have to be true at once for these tests to mean anything:
//!
//! * **The origin must be real.** A hand-written in-memory upstream would only ever
//!   agree with whatever the proxy already does. Conditional writes (`If-None-Match` /
//!   `If-Match`, the CAS the callers build their optimistic concurrency on), `ETag`
//!   derivation, error codes and read-after-write are the upstream's semantics, and the
//!   only honest way to test that the proxy preserves them is to put the real thing
//!   behind it.
//! * **Origin traffic must be countable.** "Served from cache" is a claim until the
//!   origin can testify that nothing reached it. So the proxy talks to `MinIO` through
//!   [`counting_proxy`] — a byte-faithful forwarder that classifies each request
//!   (list/get/head/put/copy/delete) and forwards it verbatim, signature and all.
//!   Nothing is rewritten; it only counts.
//!
//! Requests are driven by calling the `s3s::S3` trait methods on the proxy directly with
//! constructed [`S3Request`]s: the proxy's own decisions (index vs cache vs origin) are
//! what is under test, and the inbound HTTP hop would only re-test signing and parsing
//! that `s3s` already covers. The *outbound* hop is real all the way to `MinIO`.

#![allow(dead_code)] // each test binary drives a different subset of the harness

pub mod diff;

use bytes::Bytes;
use http::{Extensions, HeaderMap, Method, Request, Response, StatusCode, Uri};
use http_body_util::{BodyExt, Full, combinators::BoxBody};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use s3cache::cache::{CacheConfig, CachingProxy};
use s3cache::metrics::Metrics;
use s3cache::sync::{Consistency, SyncConfig, WriteSync};
use s3cache::tier::buffer_body;
use s3s::dto::{
    DeleteObjectInput, ETag, ETagCondition, GetObjectInput, GetObjectOutput, HeadObjectInput,
    HeadObjectOutput, ListObjectsV2Input, PutObjectInput, Range, StreamingBlob,
};
use s3s::{S3, S3Request, S3Result};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use testcontainers::core::wait::HttpWaitStrategy;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::net::{TcpListener, TcpStream};

/// The `MinIO` image the repo's shell-driven e2e suite already uses, so both suites test
/// the same origin. Pulled once; every run after that starts from the local image.
const MINIO_IMAGE: (&str, &str) = ("minio/minio", "latest");
/// `MinIO`'s S3 port inside the container.
const MINIO_PORT: u16 = 9000;
/// `MinIO`'s root credentials, which are also the S3 keys the client signs with.
const ACCESS_KEY: &str = "minioadmin";
const SECRET_KEY: &str = "minioadmin";

/// Room for anything a test caches, so the hot tier is never the thing under test.
const HOT_BYTES: u64 = 32 * 1024 * 1024;

/// Polls `$cond` (an expression that may `.await`) until it holds or the deadline
/// passes, then panics naming `$what`. Deadline-polling, never a bare sleep: the
/// property under test is "this becomes true", not "this takes exactly N ms".
#[macro_export]
macro_rules! eventually {
    ($what:expr, $cond:expr) => {{
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        loop {
            if $cond {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for: {}",
                $what
            );
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    }};
}

/// One counter per S3 operation class: what actually reached the origin.
#[derive(Default)]
pub struct Ops {
    list: AtomicU64,
    get: AtomicU64,
    head: AtomicU64,
    put: AtomicU64,
    delete: AtomicU64,
    copy: AtomicU64,
    other: AtomicU64,
}

macro_rules! op_readers {
    ($($field:ident),+ $(,)?) => {
        impl Ops {
            $(
                /// How many requests of this class have reached the origin.
                #[must_use]
                pub fn $field(&self) -> u64 {
                    self.$field.load(Ordering::Relaxed)
                }
            )+
        }
    };
}
op_readers!(list, get, head, put, delete, copy, other);

impl Ops {
    /// Classify one forwarded request the way S3 bills it, and count it. Path-style
    /// addressing (what the proxy uses) makes `/bucket` a bucket operation and
    /// `/bucket/key` an object one; a copy is a PUT carrying a copy source.
    fn record(&self, method: &Method, uri: &Uri, headers: &HeaderMap) {
        let path = uri.path().trim_start_matches('/');
        let on_key = path.split_once('/').is_some_and(|(_, key)| !key.is_empty());
        let counter = match (method, on_key) {
            (&Method::GET, false) => &self.list,
            (&Method::GET, true) => &self.get,
            (&Method::HEAD, true) => &self.head,
            (&Method::PUT, true) if headers.contains_key("x-amz-copy-source") => &self.copy,
            (&Method::PUT, true) => &self.put,
            (&Method::DELETE, true) => &self.delete,
            _ => &self.other,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }
}

/// A real S3 origin for one test: a `MinIO` container, a bucket of its own, and the
/// counted endpoint the proxy under test talks to.
///
/// Held by the test; dropping it removes the container.
pub struct Origin {
    _container: ContainerAsync<GenericImage>,
    /// Straight to `MinIO`, bypassing the counter — for seeding fixtures and for reading
    /// back what the origin really holds, neither of which is traffic under test.
    direct: aws_sdk_s3::Client,
    /// What the proxy under test is pointed at: the counting forwarder.
    counted_endpoint: String,
    bucket: String,
    pub ops: Arc<Ops>,
}

impl Origin {
    /// Start `MinIO`, create this test's bucket, and put a request counter in front.
    /// `bucket` is the test's own name so parallel tests never share state.
    pub async fn start(bucket: &str) -> Arc<Self> {
        let container = GenericImage::new(MINIO_IMAGE.0, MINIO_IMAGE.1)
            .with_exposed_port(MINIO_PORT.tcp())
            .with_wait_for(WaitFor::http(
                HttpWaitStrategy::new("/minio/health/live")
                    .with_port(MINIO_PORT.tcp())
                    .with_expected_status_code(200_u16),
            ))
            .with_env_var("MINIO_ROOT_USER", ACCESS_KEY)
            .with_env_var("MINIO_ROOT_PASSWORD", SECRET_KEY)
            .with_cmd(["server", "/data"])
            .start()
            .await
            .expect("MinIO starts (is the docker daemon up?)");
        let port = container
            .get_host_port_ipv4(MINIO_PORT.tcp())
            .await
            .expect("MinIO's mapped port");
        let minio: SocketAddr = ([127, 0, 0, 1], port).into();

        let direct = client_for(&format!("http://{minio}"));
        direct
            .create_bucket()
            .bucket(bucket)
            .send()
            .await
            .expect("create the test bucket");

        let ops = Arc::new(Ops::default());
        let counted = counting_proxy(minio, Arc::clone(&ops)).await;
        Arc::new(Self {
            _container: container,
            direct,
            counted_endpoint: format!("http://{counted}"),
            bucket: bucket.to_owned(),
            ops,
        })
    }

    /// This test's bucket.
    #[must_use]
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// A client for the proxy under test: everything it sends is counted.
    #[must_use]
    pub fn counted_client(&self) -> aws_sdk_s3::Client {
        client_for(&self.counted_endpoint)
    }

    /// The origin as a **plain `s3s` route** — the reference leg of a differential row.
    ///
    /// It is the same `s3s_aws::Proxy` translation the cache itself uses for passthrough,
    /// so every artefact of that layer (DTO mapping, error conversion) cancels out and a
    /// difference between the two legs can only come from a decision the cache made. It
    /// rides the uncounted client, so the reference leg never moves the counters that
    /// judge the proxy leg.
    #[must_use]
    pub fn direct_route(&self) -> s3s_aws::Proxy {
        s3s_aws::Proxy::from(self.direct.clone())
    }

    /// Create another bucket in this origin — for tests that need a second keyspace
    /// (a copy's destination, say) without a second container. `object_lock` also turns
    /// on versioning, which is what lets a test make one key of a batch delete fail.
    pub async fn make_bucket(&self, bucket: &str, object_lock: bool) {
        self.direct
            .create_bucket()
            .bucket(bucket)
            .set_object_lock_enabled_for_bucket(object_lock.then_some(true))
            .send()
            .await
            .expect("create an extra test bucket");
    }

    /// The origin's own S3 client, uncounted — for the few assertions that need the raw
    /// HTTP answer rather than the `s3s` view of it (an error's response headers, which
    /// `s3s-aws` does not carry over).
    #[must_use]
    pub fn client(&self) -> aws_sdk_s3::Client {
        self.direct.clone()
    }

    /// Seed an object straight into the origin, so a test's fixtures never move the
    /// counters it is about to assert on.
    pub async fn seed(&self, key: &str, body: &[u8]) {
        self.direct
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(body.to_vec().into())
            .send()
            .await
            .expect("seed the origin");
    }

    /// Seed an object carrying the response metadata a client would set on it — the
    /// fields a bare [`Origin::seed`] leaves at the origin's defaults.
    pub async fn seed_rich(
        &self,
        key: &str,
        body: &[u8],
        content_type: &str,
        metadata: &[(&str, &str)],
    ) {
        self.direct
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(body.to_vec().into())
            .content_type(content_type)
            .set_metadata(Some(
                metadata
                    .iter()
                    .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                    .collect(),
            ))
            .send()
            .await
            .expect("seed the origin");
    }

    /// What the origin durably holds for a key — the check that a write-through landed.
    pub async fn stored(&self, key: &str) -> Option<Bytes> {
        let out = self
            .direct
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .ok()?;
        Some(out.body.collect().await.expect("origin body").into_bytes())
    }

    /// The origin's `ETag` for a key, unquoted, for comparing with what the proxy reports.
    pub async fn etag(&self, key: &str) -> Option<String> {
        let out = self
            .direct
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .ok()?;
        Some(out.e_tag()?.trim_matches('"').to_owned())
    }
}

/// A transparent forwarder to `upstream` that counts what passes through it.
///
/// Everything is forwarded verbatim — method, path, query, headers (`Host` and
/// `Authorization` included) and body bytes — so the origin verifies exactly the
/// signature the client produced. Only hop-by-hop headers are dropped, as any
/// intermediary must. Returns the address to point a client at.
async fn counting_proxy(upstream: SocketAddr, ops: Arc<Ops>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind the counting proxy");
    let addr = listener.local_addr().expect("counter address");
    tokio::spawn(async move {
        let http = ConnBuilder::new(TokioExecutor::new());
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                continue;
            };
            let ops = Arc::clone(&ops);
            let conn = http
                .serve_connection(
                    TokioIo::new(socket),
                    service_fn(move |req: Request<Incoming>| {
                        let ops = Arc::clone(&ops);
                        async move {
                            ops.record(req.method(), req.uri(), req.headers());
                            Ok::<_, std::convert::Infallible>(forward(upstream, req).await)
                        }
                    }),
                )
                .into_owned();
            tokio::spawn(async move {
                let _ = conn.await;
            });
        }
    });
    addr
}

/// Hop-by-hop headers, which belong to one connection and must not be relayed. Note
/// `content-length` is *not* here: it is the client's framing of a signed request and is
/// passed through untouched.
const HOP_BY_HOP: [&str; 7] = [
    "connection",
    "keep-alive",
    "proxy-connection",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

async fn forward(
    upstream: SocketAddr,
    req: Request<Incoming>,
) -> Response<BoxBody<Bytes, std::io::Error>> {
    match relay(upstream, req).await {
        Ok(resp) => resp.map(|body| body.map_err(std::io::Error::other).boxed()),
        Err(err) => Response::builder()
            .status(StatusCode::BAD_GATEWAY)
            .body(
                Full::new(Bytes::from(format!("counting proxy: {err}")))
                    .map_err(|never| match never {})
                    .boxed(),
            )
            .expect("a 502 is well-formed"),
    }
}

async fn relay(
    upstream: SocketAddr,
    req: Request<Incoming>,
) -> Result<Response<Incoming>, Box<dyn std::error::Error + Send + Sync>> {
    let (mut parts, body) = req.into_parts();
    for name in HOP_BY_HOP {
        parts.headers.remove(name);
    }
    let stream = TcpStream::connect(upstream).await?;
    let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream)).await?;
    // The connection has to keep running while the response body streams back.
    tokio::spawn(async move {
        let _ = conn.await;
    });
    Ok(sender
        .send_request(Request::from_parts(parts, body))
        .await?)
}

/// An `aws_sdk_s3` client for `endpoint` — the same shape `main.rs` builds (path style,
/// static creds), without the ambient AWS environment.
#[must_use]
fn client_for(endpoint: &str) -> aws_sdk_s3::Client {
    let conf = aws_sdk_s3::config::Builder::new()
        .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
        .region(aws_sdk_s3::config::Region::new("us-east-1"))
        .credentials_provider(aws_sdk_s3::config::Credentials::new(
            ACCESS_KEY,
            SECRET_KEY,
            None,
            None,
            "s3cache-tests",
        ))
        .endpoint_url(endpoint)
        .force_path_style(true)
        .build();
    aws_sdk_s3::Client::from_conf(conf)
}

/// A proxy over `client`, wired the way `main.rs` wires one: hot tier only, no warm
/// disk, `sync` for cross-node coherence when a test wants it.
#[must_use]
pub fn proxy_over(
    client: &aws_sdk_s3::Client,
    max_obj_bytes: usize,
    sync: Option<Arc<WriteSync>>,
) -> CachingProxy {
    CachingProxy::new(
        s3s_aws::Proxy::from(client.clone()),
        client.clone(),
        CacheConfig {
            cache_bytes: HOT_BYTES,
            max_obj_bytes,
        },
        None,
        sync,
        Arc::new(Metrics::default()),
    )
}

/// A proxy in front of `origin`, indexing its bucket in the background — the
/// single-node stack. Build it *after* seeding, so the warm-up sync sees the fixtures
/// (the proxy is the only writer in production; a test that seeds behind its back is
/// racing its own setup, not testing anything).
#[must_use]
pub fn node(origin: &Origin, max_obj_bytes: usize) -> CachingProxy {
    let proxy = proxy_over(&origin.counted_client(), max_obj_bytes, None);
    proxy.spawn_background_sync(vec![origin.bucket().to_owned()]);
    proxy
}

/// A pair of gossiping [`WriteSync`]s on loopback UDP, each seeded with the other, in
/// strong mode. Ports are grabbed from the OS and released a moment before the
/// transports claim them; a lost race just means another attempt.
pub async fn gossip_pair(a: &str, b: &str) -> (Arc<WriteSync>, Arc<WriteSync>) {
    for _ in 0..10 {
        let (port_a, port_b) = (free_udp_port(), free_udp_port());
        let config = |me: &str, my_port: u16, peer: &str, peer_port: u16| SyncConfig {
            bind: format!("127.0.0.1:{my_port}"),
            advertise: Some(format!("127.0.0.1:{my_port}")),
            seeds: vec![(peer.to_owned(), format!("127.0.0.1:{peer_port}"))],
            node_id: me.to_owned(),
            consistency: Consistency::Strong,
        };
        let (Some(sync_a), Some(sync_b)) = (
            WriteSync::new(config(a, port_a, b, port_b)).await,
            WriteSync::new(config(b, port_b, a, port_a)).await,
        ) else {
            continue;
        };
        return (Arc::new(sync_a), Arc::new(sync_b));
    }
    panic!("could not bind a pair of loopback gossip ports");
}

/// A port the OS just handed out and nobody holds any more.
fn free_udp_port() -> u16 {
    let socket = std::net::UdpSocket::bind("127.0.0.1:0").expect("a free UDP port");
    socket.local_addr().expect("bound address").port()
}

/// A request carrying nothing but its input — no signing, region or session state, the
/// shape the S3 service hands the proxy for an ordinary request.
pub fn request<T>(input: T) -> S3Request<T> {
    S3Request {
        input,
        method: Method::GET,
        uri: Uri::default(),
        headers: HeaderMap::new(),
        extensions: Extensions::new(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

pub fn body_blob(bytes: Bytes) -> StreamingBlob {
    StreamingBlob::wrap(futures::stream::once(async move {
        Ok::<Bytes, std::io::Error>(bytes)
    }))
}

/// The keys a LIST through the proxy reports, in order.
pub async fn list(proxy: &CachingProxy, bucket: &str) -> Vec<String> {
    let out = proxy
        .list_objects_v2(request(ListObjectsV2Input {
            bucket: bucket.to_owned(),
            ..Default::default()
        }))
        .await
        .expect("list succeeds");
    out.output
        .contents
        .into_iter()
        .flatten()
        .filter_map(|object| object.key)
        .collect()
}

/// What a LIST through the proxy reports for one key: its size and `ETag`.
pub async fn list_entry(
    proxy: &CachingProxy,
    bucket: &str,
    key: &str,
) -> Option<(i64, Option<String>)> {
    let out = proxy
        .list_objects_v2(request(ListObjectsV2Input {
            bucket: bucket.to_owned(),
            ..Default::default()
        }))
        .await
        .expect("list succeeds");
    out.output
        .contents
        .into_iter()
        .flatten()
        .find(|object| object.key.as_deref() == Some(key))
        .map(|object| {
            (
                object.size.unwrap_or(-1),
                object.e_tag.map(s3s::dto::ETag::into_value),
            )
        })
}

/// A whole-object GET through the proxy, as bytes.
pub async fn get(proxy: &CachingProxy, bucket: &str, key: &str) -> Bytes {
    let out = get_output(proxy, bucket, key, None)
        .await
        .expect("get succeeds");
    read_body(out).await
}

/// A ranged GET through the proxy: the sliced bytes and the `Content-Range` header.
pub async fn get_range(
    proxy: &CachingProxy,
    bucket: &str,
    key: &str,
    first: u64,
    last: u64,
) -> (Bytes, Option<String>) {
    let out = get_output(proxy, bucket, key, Some((first, last)))
        .await
        .expect("ranged get succeeds");
    let range = out.content_range.clone();
    (read_body(out).await, range)
}

/// A conditional GET: `If-None-Match` against `etag`, which a fresh object answers with
/// `NotModified`.
pub async fn get_if_none_match(
    proxy: &CachingProxy,
    bucket: &str,
    key: &str,
    etag: &str,
) -> S3Result<GetObjectOutput> {
    let input = GetObjectInput {
        bucket: bucket.to_owned(),
        key: key.to_owned(),
        if_none_match: Some(ETagCondition::ETag(ETag::Strong(etag.to_owned()))),
        ..Default::default()
    };
    Ok(proxy.get_object(request(input)).await?.output)
}

async fn get_output(
    proxy: &CachingProxy,
    bucket: &str,
    key: &str,
    range: Option<(u64, u64)>,
) -> S3Result<GetObjectOutput> {
    let input = GetObjectInput {
        bucket: bucket.to_owned(),
        key: key.to_owned(),
        range: range.map(|(first, last)| Range::Int {
            first,
            last: Some(last),
        }),
        ..Default::default()
    };
    Ok(proxy.get_object(request(input)).await?.output)
}

async fn read_body(mut out: GetObjectOutput) -> Bytes {
    let body = out.body.take().expect("a GET carries a body");
    buffer_body(body, usize::MAX).await.expect("readable body")
}

/// A HEAD through the proxy.
pub async fn head(proxy: &CachingProxy, bucket: &str, key: &str) -> S3Result<HeadObjectOutput> {
    let input = HeadObjectInput {
        bucket: bucket.to_owned(),
        key: key.to_owned(),
        ..Default::default()
    };
    Ok(proxy.head_object(request(input)).await?.output)
}

/// A conditional HEAD: `If-None-Match` against `etag`, which an unchanged object
/// answers with `NotModified` — a judgement only the origin can make.
pub async fn head_if_none_match(
    proxy: &CachingProxy,
    bucket: &str,
    key: &str,
    etag: &str,
) -> S3Result<HeadObjectOutput> {
    let input = HeadObjectInput {
        bucket: bucket.to_owned(),
        key: key.to_owned(),
        if_none_match: Some(ETagCondition::ETag(ETag::Strong(etag.to_owned()))),
        ..Default::default()
    };
    Ok(proxy.head_object(request(input)).await?.output)
}

/// A write-through PUT, optionally conditional (the CAS clients build OCC on).
pub async fn put_conditional(
    proxy: &CachingProxy,
    bucket: &str,
    key: &str,
    body: &[u8],
    if_none_match: Option<ETagCondition>,
    if_match: Option<ETagCondition>,
) -> S3Result<()> {
    let bytes = Bytes::copy_from_slice(body);
    let input = PutObjectInput {
        bucket: bucket.to_owned(),
        key: key.to_owned(),
        content_length: Some(i64::try_from(bytes.len()).unwrap_or(i64::MAX)),
        body: Some(body_blob(bytes)),
        if_none_match,
        if_match,
        ..Default::default()
    };
    proxy.put_object(request(input)).await?;
    Ok(())
}

/// A write-through PUT.
pub async fn put(proxy: &CachingProxy, bucket: &str, key: &str, body: &[u8]) {
    put_conditional(proxy, bucket, key, body, None, None)
        .await
        .expect("put succeeds");
}

/// A write-through DELETE.
pub async fn delete(proxy: &CachingProxy, bucket: &str, key: &str) {
    let input = DeleteObjectInput {
        bucket: bucket.to_owned(),
        key: key.to_owned(),
        ..Default::default()
    };
    proxy
        .delete_object(request(input))
        .await
        .expect("delete succeeds");
}

/// Block until `bucket`'s index has finished its background warm-up on `proxy`.
///
/// The readiness signal is the flip itself: a LIST that leaves the origin's LIST counter
/// untouched was served from the index, which is only possible once the bucket is
/// synced. Until then each probe passes through (and is counted), so tests take their
/// counter baselines *after* this returns.
pub async fn wait_for_index(proxy: &CachingProxy, origin: &Origin, bucket: &str) {
    eventually!("the bucket index to finish syncing", {
        let before = origin.ops.list();
        let _ = list(proxy, bucket).await;
        origin.ops.list() == before
    });
}
