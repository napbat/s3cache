//! Object-body cache tiers: **hot** (node-local heap) in front of **warm** (shared
//! Valkey) in front of **cold** (the S3 origin, always the fallthrough).
//!
//! [`CacheMode`] selects which tiers are active. Reads check hot then warm — a warm hit
//! backfills hot — and writes/invalidations fan out to whichever tiers are on, so each
//! layer stays a strict cache of the one beneath it. The warm tier is shared by every
//! node, which is what lets writes on one node be seen by the others (see the crate
//! roadmap for the commit-log that makes that coherent for OCC).

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use fred::prelude::*;
use futures::StreamExt;
use s3s::dto::{ETag, GetObjectOutput, HeadObjectOutput, Metadata, StreamingBlob, Timestamp};
use serde::{Deserialize, Serialize};

use crate::cache::Metrics;

/// A warm-tier operation may never stall the data path: if Valkey is slow or gone, the
/// op is abandoned after this and treated as a miss (reads) or a drop (writes).
const WARM_OP_TIMEOUT: Duration = Duration::from_secs(2);

/// Which cache tiers sit in front of the cold S3 origin (the origin is always present).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CacheMode {
    /// No caching: every request passes straight through to the origin.
    Off,
    /// Node-local heap only — fastest, but each node's copy is private and can drift.
    Hot,
    /// Shared Valkey only — no node-local copy, so every node sees one coherent view.
    Warm,
    /// Node-local heap in front of shared Valkey.
    HotWarm,
}

impl CacheMode {
    /// Parse `S3CACHE_MODE`; unknown values fall back to `Hot` (the historical default).
    #[must_use]
    pub fn parse(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().replace(['_', ' '], "").as_str() {
            "off" | "none" | "passthrough" => Self::Off,
            "hot" | "heap" | "memory" => Self::Hot,
            "warm" | "valkey" | "redis" => Self::Warm,
            "hotwarm" | "warmhot" | "hot+warm" | "tiered" | "all" => Self::HotWarm,
            other => {
                tracing::warn!("unknown S3CACHE_MODE `{other}`, defaulting to `hot`");
                Self::Hot
            }
        }
    }

    /// Whether the node-local heap tier is active.
    #[must_use]
    pub fn hot(self) -> bool {
        matches!(self, Self::Hot | Self::HotWarm)
    }

    /// Whether the shared Valkey tier is active.
    #[must_use]
    pub fn warm(self) -> bool {
        matches!(self, Self::Warm | Self::HotWarm)
    }
}

/// A cached object body plus the response metadata needed to reconstruct a GET/HEAD.
/// `Serialize`/`Deserialize` so the warm tier can round-trip it through Valkey.
#[derive(Serialize, Deserialize)]
pub struct CachedObject {
    body: Bytes,
    content_length: Option<i64>,
    content_type: Option<String>,
    e_tag: Option<ETag>,
    last_modified: Option<Timestamp>,
    cache_control: Option<String>,
    content_encoding: Option<String>,
    content_language: Option<String>,
    content_disposition: Option<String>,
    accept_ranges: Option<String>,
    metadata: Option<Metadata>,
}

impl CachedObject {
    /// Capture a GET response's metadata alongside its (already-buffered) body.
    pub fn from_get(out: &GetObjectOutput, body: Bytes) -> Self {
        Self {
            body,
            content_length: out.content_length,
            content_type: out.content_type.clone(),
            e_tag: out.e_tag.clone(),
            last_modified: out.last_modified.clone(),
            cache_control: out.cache_control.clone(),
            content_encoding: out.content_encoding.clone(),
            content_language: out.content_language.clone(),
            content_disposition: out.content_disposition.clone(),
            accept_ranges: out.accept_ranges.clone(),
            metadata: out.metadata.clone(),
        }
    }

    fn body_blob(&self) -> StreamingBlob {
        let b = self.body.clone();
        StreamingBlob::wrap(futures::stream::once(async move { Ok::<Bytes, std::io::Error>(b) }))
    }

    /// Reconstruct a full-body GET response from the cached copy.
    pub fn to_get(&self) -> GetObjectOutput {
        GetObjectOutput {
            body: Some(self.body_blob()),
            content_length: self.content_length,
            content_type: self.content_type.clone(),
            e_tag: self.e_tag.clone(),
            last_modified: self.last_modified.clone(),
            cache_control: self.cache_control.clone(),
            content_encoding: self.content_encoding.clone(),
            content_language: self.content_language.clone(),
            content_disposition: self.content_disposition.clone(),
            accept_ranges: self.accept_ranges.clone(),
            metadata: self.metadata.clone(),
            ..Default::default()
        }
    }

    /// A 206-shaped GET for an inclusive byte range sliced out of the cached body
    /// (clamped at EOF). `None` when the range start is past the object.
    pub fn to_get_range(&self, first: u64, last: Option<u64>) -> Option<GetObjectOutput> {
        let total = self.body.len() as u64;
        if first >= total {
            return None;
        }
        let last_incl = last.map_or(total - 1, |l| l.min(total - 1));
        let slice = self.body.slice(usize::try_from(first).ok()?..=usize::try_from(last_incl).ok()?);
        let len = slice.len();
        Some(GetObjectOutput {
            body: Some(StreamingBlob::wrap(futures::stream::once(async move { Ok::<Bytes, std::io::Error>(slice) }))),
            content_length: Some(i64::try_from(len).unwrap_or(i64::MAX)),
            content_range: Some(format!("bytes {first}-{last_incl}/{total}")),
            content_type: self.content_type.clone(),
            e_tag: self.e_tag.clone(),
            last_modified: self.last_modified.clone(),
            cache_control: self.cache_control.clone(),
            content_encoding: self.content_encoding.clone(),
            content_language: self.content_language.clone(),
            content_disposition: self.content_disposition.clone(),
            accept_ranges: self.accept_ranges.clone(),
            metadata: self.metadata.clone(),
            ..Default::default()
        })
    }

    /// Reconstruct a HEAD response from the cached metadata.
    pub fn to_head(&self) -> HeadObjectOutput {
        HeadObjectOutput {
            content_length: self.content_length,
            content_type: self.content_type.clone(),
            e_tag: self.e_tag.clone(),
            last_modified: self.last_modified.clone(),
            cache_control: self.cache_control.clone(),
            content_encoding: self.content_encoding.clone(),
            content_language: self.content_language.clone(),
            content_disposition: self.content_disposition.clone(),
            accept_ranges: self.accept_ranges.clone(),
            metadata: self.metadata.clone(),
            ..Default::default()
        }
    }
}

/// Drain a streamed body into memory, bailing (`None`) past `cap` bytes or on error.
pub async fn buffer_body(blob: StreamingBlob, cap: usize) -> Option<Bytes> {
    let mut blob = std::pin::pin!(blob);
    let mut buf: Vec<u8> = Vec::new();
    while let Some(chunk) = blob.next().await {
        let b = chunk.ok()?;
        if buf.len() + b.len() > cap {
            return None;
        }
        buf.extend_from_slice(&b);
    }
    Some(Bytes::from(buf))
}

/// Shared object-body cache backed by Valkey/Redis (the warm tier).
///
/// Every operation is best-effort: a Valkey error, a decode failure, or a timeout is
/// treated as a miss (reads) or silently dropped (writes), so a cache outage never
/// becomes a data-plane outage. Startup never blocks on Valkey — the pool connects and
/// reconnects in the background, and requests made before it is ready just miss.
pub struct WarmCache {
    pool: Pool,
    /// Objects whose serialized form exceeds this are not stored (mirrors the hot cap).
    max_obj_bytes: usize,
    /// Optional TTL in seconds; `None` keeps warm entries until invalidated or evicted.
    ttl_secs: Option<i64>,
    metrics: Arc<Metrics>,
}

/// Build a Valkey connection pool from `url` and start connecting in the background.
/// Shared by the warm object cache and the index commit log, so both use one pool.
/// Errors only on a bad URL or pool size — never for the server being down, which
/// self-heals via the reconnect policy.
pub fn connect_valkey(url: &str, pool_size: usize) -> anyhow::Result<Pool> {
    let config = Config::from_url(url)?;
    let mut builder = Builder::from_config(config);
    // Reconnect forever with exponential backoff so a Valkey blip self-heals.
    builder.set_policy(ReconnectPolicy::new_exponential(0, 100, 30_000, 2));
    let pool = builder.build_pool(pool_size.max(1))?;
    pool.connect();
    Ok(pool)
}

/// A single dedicated Valkey connection (also connecting in the background). Used for the
/// index-log consumer's blocking `XREAD`: a blocking read monopolizes its connection, so
/// it must not share the pool with the write-path appends (they would stall behind it).
pub fn connect_valkey_client(url: &str) -> anyhow::Result<Client> {
    let config = Config::from_url(url)?;
    let mut builder = Builder::from_config(config);
    builder.set_policy(ReconnectPolicy::new_exponential(0, 100, 30_000, 2));
    let client = builder.build()?;
    client.connect();
    Ok(client)
}

impl WarmCache {
    /// Wrap an existing (already-connecting) Valkey pool as the warm object cache.
    #[must_use]
    pub fn new(pool: Pool, max_obj_bytes: usize, ttl_secs: Option<u64>, metrics: Arc<Metrics>) -> Self {
        Self {
            pool,
            max_obj_bytes,
            ttl_secs: ttl_secs.map(|s| i64::try_from(s).unwrap_or(i64::MAX)),
            metrics,
        }
    }

    fn rkey(bucket: &str, key: &str) -> String {
        format!("s3cache:obj:{bucket}:{key}")
    }

    async fn get(&self, bucket: &str, key: &str) -> Option<Arc<CachedObject>> {
        let rk = Self::rkey(bucket, key);
        match tokio::time::timeout(WARM_OP_TIMEOUT, self.pool.get::<Option<Bytes>, _>(rk)).await {
            Ok(Ok(Some(bytes))) => match bincode::deserialize::<CachedObject>(&bytes) {
                Ok(obj) => {
                    self.metrics.warm_hit();
                    Some(Arc::new(obj))
                }
                Err(e) => {
                    tracing::debug!("warm decode failed for {bucket}/{key}: {e}");
                    self.metrics.warm_error();
                    None
                }
            },
            Ok(Ok(None)) => {
                self.metrics.warm_miss();
                None
            }
            Ok(Err(e)) => {
                tracing::debug!("warm get failed for {bucket}/{key}: {e}");
                self.metrics.warm_error();
                None
            }
            Err(_) => {
                tracing::debug!("warm get timed out for {bucket}/{key}");
                self.metrics.warm_error();
                None
            }
        }
    }

    async fn put(&self, bucket: &str, key: &str, obj: &CachedObject) {
        let bytes = match bincode::serialize(obj) {
            Ok(b) if b.len() <= self.max_obj_bytes => b,
            Ok(_) => return, // oversize: leave it to stream through, don't fill Valkey
            Err(e) => {
                tracing::debug!("warm encode failed for {bucket}/{key}: {e}");
                return;
            }
        };
        let rk = Self::rkey(bucket, key);
        let expire = self.ttl_secs.map(Expiration::EX);
        let set = self.pool.set::<(), _, _>(rk, bytes, expire, None, false);
        match tokio::time::timeout(WARM_OP_TIMEOUT, set).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                tracing::debug!("warm set failed for {bucket}/{key}: {e}");
                self.metrics.warm_error();
            }
            Err(_) => {
                tracing::debug!("warm set timed out for {bucket}/{key}");
                self.metrics.warm_error();
            }
        }
    }

    async fn invalidate(&self, bucket: &str, key: &str) {
        let rk = Self::rkey(bucket, key);
        let del = self.pool.del::<(), _>(rk);
        match tokio::time::timeout(WARM_OP_TIMEOUT, del).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                tracing::debug!("warm del failed for {bucket}/{key}: {e}");
                self.metrics.warm_error();
            }
            Err(_) => {
                tracing::debug!("warm del timed out for {bucket}/{key}");
                self.metrics.warm_error();
            }
        }
    }
}

/// The node-local heap LRU: `(bucket, key) -> object`. Cheap to clone (an `Arc`
/// inside), so a handle can be shared with the commit-log consumer for invalidation.
pub type HotCache = moka::future::Cache<(String, String), Arc<CachedObject>>;

/// The active object-body tiers in front of the cold S3 origin: an optional node-local
/// heap LRU (hot) and an optional shared Valkey store (warm). Both absent (`Off` mode)
/// makes every method a no-op and reads always miss, i.e. straight passthrough.
pub struct TieredCache {
    hot: Option<HotCache>,
    warm: Option<WarmCache>,
}

impl TieredCache {
    /// Assemble the tiers `mode` calls for. The hot LRU is weighted by body bytes up to
    /// `cache_bytes`; `warm` is attached only when the mode enables it.
    #[must_use]
    pub fn new(mode: CacheMode, cache_bytes: u64, warm: Option<WarmCache>) -> Self {
        let hot = mode.hot().then(|| {
            moka::future::Cache::builder()
                .max_capacity(cache_bytes)
                .weigher(|_k, v: &Arc<CachedObject>| u32::try_from(v.body.len()).unwrap_or(u32::MAX))
                .build()
        });
        Self {
            hot,
            warm: if mode.warm() { warm } else { None },
        }
    }

    /// Whether any tier is active. `false` means every request is a straight passthrough.
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        self.hot.is_some() || self.warm.is_some()
    }

    /// A clone of the hot-tier handle (if hot is active) for the commit-log consumer to
    /// invalidate on peers' writes. Warm is shared, so the log only touches the hot copy.
    #[must_use]
    pub fn hot_handle(&self) -> Option<HotCache> {
        self.hot.clone()
    }

    /// Look up a whole cached object: hot first, then warm (a warm hit backfills hot).
    pub async fn get(&self, key: &(String, String)) -> Option<Arc<CachedObject>> {
        if let Some(hot) = &self.hot
            && let Some(obj) = hot.get(key).await
        {
            return Some(obj);
        }
        if let Some(warm) = &self.warm
            && let Some(obj) = warm.get(&key.0, &key.1).await
        {
            if let Some(hot) = &self.hot {
                hot.insert(key.clone(), obj.clone()).await;
            }
            return Some(obj);
        }
        None
    }

    /// Store an object into every active tier (warm first, so a warm error still leaves
    /// hot populated for this node).
    pub async fn insert(&self, key: (String, String), obj: Arc<CachedObject>) {
        if let Some(warm) = &self.warm {
            warm.put(&key.0, &key.1, &obj).await;
        }
        if let Some(hot) = &self.hot {
            hot.insert(key, obj).await;
        }
    }

    /// Drop an object from every active tier.
    pub async fn invalidate(&self, key: &(String, String)) {
        if let Some(hot) = &self.hot {
            hot.invalidate(key).await;
        }
        if let Some(warm) = &self.warm {
            warm.invalidate(&key.0, &key.1).await;
        }
    }

    /// Get `key`, or run `origin` to fetch it and populate the tiers. When hot is active
    /// the fetch is singleflighted (moka's `try_get_with`), so concurrent callers share
    /// one origin round-trip; a warm hit short-circuits the fetch entirely.
    pub async fn get_or_fetch<Fut>(
        &self,
        key: &(String, String),
        origin: Fut,
    ) -> Result<Arc<CachedObject>, String>
    where
        Fut: Future<Output = Result<Arc<CachedObject>, String>> + Send,
    {
        let load = async {
            if let Some(warm) = &self.warm
                && let Some(obj) = warm.get(&key.0, &key.1).await
            {
                return Ok(obj);
            }
            let obj = origin.await?;
            if let Some(warm) = &self.warm {
                warm.put(&key.0, &key.1, &obj).await;
            }
            Ok(obj)
        };
        match &self.hot {
            Some(hot) => hot
                .try_get_with(key.clone(), load)
                .await
                .map_err(|e: Arc<String>| e.to_string()),
            None => load.await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{buffer_body, connect_valkey, CacheMode, CachedObject, WarmCache};
    use crate::cache::Metrics;
    use bytes::Bytes;
    use s3s::dto::{ETag, GetObjectOutput, Timestamp};
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::{Duration, UNIX_EPOCH};

    fn sample() -> CachedObject {
        let out = GetObjectOutput {
            content_length: Some(5),
            content_type: Some("text/plain".to_owned()),
            e_tag: Some(ETag::Strong("\"deadbeef\"".to_owned())),
            // Whole-second timestamp: the DateTime format round-trips it exactly.
            last_modified: Some(Timestamp::from(UNIX_EPOCH + Duration::from_secs(1_700_000_000))),
            metadata: Some(HashMap::from([("k".to_owned(), "v".to_owned())])),
            ..Default::default()
        };
        CachedObject::from_get(&out, Bytes::from_static(b"hello"))
    }

    #[test]
    fn mode_parse() {
        assert_eq!(CacheMode::parse("off"), CacheMode::Off);
        assert_eq!(CacheMode::parse("HOT"), CacheMode::Hot);
        assert_eq!(CacheMode::parse(" warm "), CacheMode::Warm);
        assert_eq!(CacheMode::parse("hot+warm"), CacheMode::HotWarm);
        assert_eq!(CacheMode::parse("tiered"), CacheMode::HotWarm);
        assert_eq!(CacheMode::parse("nonsense"), CacheMode::Hot); // default
        assert!(CacheMode::HotWarm.hot() && CacheMode::HotWarm.warm());
        assert!(CacheMode::Warm.warm() && !CacheMode::Warm.hot());
    }

    #[test]
    fn cached_object_bincode_roundtrip() {
        let obj = sample();
        let bytes = bincode::serialize(&obj).unwrap();
        let back: CachedObject = bincode::deserialize(&bytes).unwrap();
        assert_eq!(obj.body, back.body);
        assert_eq!(obj.content_length, back.content_length);
        assert_eq!(obj.content_type, back.content_type);
        assert_eq!(obj.e_tag, back.e_tag);
        assert_eq!(obj.last_modified, back.last_modified);
        assert_eq!(obj.metadata, back.metadata);
    }

    #[tokio::test]
    async fn buffer_body_respects_cap() {
        let blob = s3s::dto::StreamingBlob::wrap(futures::stream::once(async {
            Ok::<Bytes, std::io::Error>(Bytes::from_static(b"0123456789"))
        }));
        assert!(buffer_body(blob, 4).await.is_none()); // over cap -> None
    }

    // Live round-trip against a real Valkey. Skipped unless S3CACHE_TEST_VALKEY_URL is
    // set (e.g. `redis://127.0.0.1:6379`), so CI without a server still passes.
    #[tokio::test]
    async fn warm_roundtrip() {
        let Ok(url) = std::env::var("S3CACHE_TEST_VALKEY_URL") else {
            eprintln!("skip warm_roundtrip: set S3CACHE_TEST_VALKEY_URL to run");
            return;
        };
        let metrics = Arc::new(Metrics::default());
        let pool = connect_valkey(&url, 2).unwrap();
        let warm = WarmCache::new(pool, 8 * 1024 * 1024, None, metrics);
        tokio::time::sleep(Duration::from_millis(300)).await; // let the pool connect

        let obj = sample();
        warm.put("bucket", "key", &obj).await;
        let got = warm.get("bucket", "key").await.expect("warm should hit after put");
        assert_eq!(got.body, obj.body);
        assert_eq!(got.e_tag, obj.e_tag);
        assert_eq!(got.metadata, obj.metadata);
        assert_eq!(got.last_modified, obj.last_modified);

        warm.invalidate("bucket", "key").await;
        assert!(warm.get("bucket", "key").await.is_none(), "invalidate should remove it");
    }
}
