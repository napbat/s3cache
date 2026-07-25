//! Layered object-body cache: **hot** (node-local heap) in front of **warm** (a node-local
//! on-disk cache) in front of **cold** (the S3 origin). Always layered — there is no mode
//! to pick. Built on `tierstore`: the hot tier is a byte-weighted moka cache (sharded,
//! `TinyLFU` admission, via `tierstore-moka`), the warm
//! tier is a byte-budgeted, restart-surviving mmap-disk store (values served as
//! kernel-evictable mapped bytes) reached through a codec (bincode of `(key, object)`)
//! and a blocking-I/O offload pool. Warm is inclusive (fills write hot *and* disk) and
//! best-effort by policy: a disk error or oversize rejection never blocks the hot fill or
//! the data plane. Origin fetches are singleflighted probe-then-gate, so only misses
//! contend and concurrent callers share one round-trip. Cross-node coherence is separate
//! (see `coherence`): a peer's write invalidates the local hot *and* disk copies, and
//! reads barrier on the log.

use std::future::Future;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt;
use s3s::dto::{ETag, GetObjectOutput, HeadObjectOutput, Metadata, StreamingBlob, Timestamp};
use serde::{Deserialize, Serialize};
use tierstore::{CodecTier, KeyStatus, OffloadTier, SingleFlight};
use tierstore_mmap::MmapDiskTier;
use tierstore_moka::MokaTier;

use crate::metrics::Metrics;

/// `(bucket, key)` — the cache's addressing unit.
pub(crate) type CacheKey = (String, String);

/// A cached object body plus the response metadata needed to reconstruct a GET/HEAD.
/// `Serialize`/`Deserialize` so the warm disk tier can round-trip it.
#[derive(Serialize, Deserialize)]
pub(crate) struct CachedObject {
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
    pub(crate) fn from_get(out: &GetObjectOutput, body: Bytes) -> Self {
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
        StreamingBlob::wrap(futures::stream::once(async move {
            Ok::<Bytes, std::io::Error>(b)
        }))
    }

    /// Reconstruct a full-body GET response from the cached copy.
    pub(crate) fn to_get(&self) -> GetObjectOutput {
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
    pub(crate) fn to_get_range(&self, first: u64, last: Option<u64>) -> Option<GetObjectOutput> {
        let total = self.body.len() as u64;
        if first >= total {
            return None;
        }
        let last_incl = last.map_or(total - 1, |l| l.min(total - 1));
        let slice = self
            .body
            .slice(usize::try_from(first).ok()?..=usize::try_from(last_incl).ok()?);
        let len = slice.len();
        Some(GetObjectOutput {
            body: Some(StreamingBlob::wrap(futures::stream::once(async move {
                Ok::<Bytes, std::io::Error>(slice)
            }))),
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
    pub(crate) fn to_head(&self) -> HeadObjectOutput {
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
pub(crate) async fn buffer_body(blob: StreamingBlob, cap: usize) -> Option<Bytes> {
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

/// Blake3 of `bucket\0key` — a fixed-length, path-safe, collision-free warm-tier key.
fn warm_key(bucket: &str, key: &str) -> String {
    let mut h = blake3::Hasher::new();
    h.update(bucket.as_bytes());
    h.update(&[0]);
    h.update(key.as_bytes());
    h.finalize().to_hex().to_string()
}

/// The warm tier: typed objects over a byte-budgeted, restart-surviving mmap-disk store,
/// with the blocking file I/O on a dedicated worker pool. The codec embeds the cache key
/// in the encoding, so entries the disk tier evicts decode back fully typed.
pub(crate) type WarmTier = OffloadTier<CodecTier<Arc<MmapDiskTier>, CacheKey, Arc<CachedObject>>>;

/// A warm tier plus a maintenance handle to its underlying disk store (for
/// [`LocalCache::flush`] — the tier stack has no whole-cache clear).
pub(crate) type WarmPair = (WarmTier, Arc<MmapDiskTier>);

/// Worker threads for the warm tier's blocking file I/O.
const WARM_IO_THREADS: usize = 4;

/// Open (creating if needed) the warm disk tier under `dir`, byte-bounded to `disk_bytes`;
/// files already present are re-indexed so the cache survives restarts. Objects whose
/// encoding exceeds `max_obj_bytes` are rejected by the codec — under the cache's
/// best-effort write policy that skips the disk fill without failing the hot one.
pub(crate) fn open_warm(
    dir: PathBuf,
    disk_bytes: u64,
    max_obj_bytes: usize,
) -> anyhow::Result<WarmPair> {
    let budget = NonZeroU64::new(disk_bytes.max(1)).unwrap_or(NonZeroU64::MIN);
    let disk = Arc::new(MmapDiskTier::open_bounded(dir, budget)?);
    let codec = CodecTier::new(
        Arc::clone(&disk),
        |key: &CacheKey| warm_key(&key.0, &key.1),
        move |key: &CacheKey, obj: &Arc<CachedObject>| {
            let bytes =
                bincode::serialize(&(key, obj.as_ref())).map_err(tierstore::BoxError::from)?;
            if bytes.len() > max_obj_bytes {
                return Err("encoded object exceeds S3CACHE_MAX_OBJECT_BYTES".into());
            }
            Ok(Bytes::from(bytes))
        },
        |bytes: Bytes| {
            let (key, obj): (CacheKey, CachedObject) =
                bincode::deserialize(&bytes).map_err(tierstore::BoxError::from)?;
            Ok((key, Arc::new(obj)))
        },
    );
    let warm = OffloadTier::new(
        codec,
        NonZeroUsize::new(WARM_IO_THREADS).unwrap_or(NonZeroUsize::MIN),
    );
    Ok((warm, disk))
}

/// The shared cache core: the tierstore hierarchy plus fill gates and metrics.
struct Core {
    cache: tierstore::TieredCache<CacheKey, Arc<CachedObject>>,
    /// The hot tier, kept for whole-cache flushes (moka's `invalidate_all`).
    hot: Arc<MokaTier<CacheKey, Arc<CachedObject>>>,
    /// The warm disk store, kept for whole-cache flushes (`clear`).
    warm_disk: Option<Arc<MmapDiskTier>>,
    fills: SingleFlight<CacheKey>,
    metrics: Arc<Metrics>,
    has_warm: bool,
}

impl Core {
    /// One routed lookup with tier provenance for the warm metrics: a hit below the hot
    /// tier is a warm hit (and is promoted into hot by policy); an incomplete report means
    /// a tier failed and was routed around.
    async fn lookup(&self, key: &CacheKey) -> Option<Arc<CachedObject>> {
        let (status, failures) = self.cache.router().read_one(key).await.ok()?;
        if !failures.is_empty() {
            self.metrics.warm_error();
        }
        if let KeyStatus::Hit { tier, value } = status {
            if tier > 0 {
                self.metrics.warm_hit();
            }
            Some(value)
        } else {
            if self.has_warm {
                self.metrics.warm_miss();
            }
            None
        }
    }

    /// Best-effort fill of hot and (inclusively) warm: a disk rejection or I/O error is
    /// skipped by policy, never surfaced to the data plane.
    async fn insert(&self, key: CacheKey, obj: Arc<CachedObject>) {
        if self.cache.put(key, obj).await.is_err() {
            self.metrics.warm_error();
        }
    }

    /// Drop an object from every local tier. A tier that fails to delete is counted; its
    /// copy lingers only until eviction and is never authoritative.
    async fn invalidate(&self, key: &CacheKey) {
        if self.cache.invalidate(key).await.is_err() {
            self.metrics.warm_error();
        }
    }

    /// Drop EVERY local copy — the coarse remediation when an unknown set of
    /// entries may be stale (a write-feed gap): the hot tier empties
    /// immediately, the disk store unlinks its files on a blocking worker.
    async fn flush(&self) {
        self.hot.inner().invalidate_all();
        if let Some(disk) = &self.warm_disk {
            let disk = Arc::clone(disk);
            let cleared = tokio::task::spawn_blocking(move || disk.clear()).await;
            if !matches!(cleared, Ok(Ok(()))) {
                self.metrics.warm_error();
            }
        }
    }
}

/// The layered object-body cache handle held by the proxy.
pub(crate) struct TieredCache {
    core: Arc<Core>,
}

impl TieredCache {
    /// Build the cache: a hot LRU weighted by body bytes up to `cache_bytes`, plus the
    /// optional warm disk tier. Fill singleflight is handled here (probe-then-gate), so
    /// the tierstore-level gate is disabled.
    #[must_use]
    pub(crate) fn new(cache_bytes: u64, warm: Option<WarmPair>, metrics: Arc<Metrics>) -> Self {
        let hot = Arc::new(MokaTier::bounded_weighted(
            cache_bytes,
            |_key: &CacheKey, obj: &Arc<CachedObject>| {
                u32::try_from(obj.body.len()).unwrap_or(u32::MAX)
            },
        ));
        let has_warm = warm.is_some();
        let (warm_tier, warm_disk) = match warm {
            Some((tier, disk)) => (Some(tier), Some(disk)),
            None => (None, None),
        };
        let builder = tierstore::TieredCache::builder()
            .tier(Arc::clone(&hot))
            .single_flight(false);
        let cache = match warm_tier {
            Some(warm) => builder.tier(warm),
            None => builder,
        }
        .build();
        Self {
            core: Arc::new(Core {
                cache,
                hot,
                warm_disk,
                fills: SingleFlight::new(),
                metrics,
                has_warm,
            }),
        }
    }

    /// A handle the commit-log consumer uses to invalidate this node's local copies.
    #[must_use]
    pub(crate) fn local(&self) -> LocalCache {
        LocalCache {
            core: Arc::clone(&self.core),
        }
    }

    /// Look up a whole cached object: hot, then warm disk (a disk hit promotes to hot).
    pub(crate) async fn get(&self, key: &CacheKey) -> Option<Arc<CachedObject>> {
        self.core.lookup(key).await
    }

    /// Store into hot and (inclusively) the warm disk tier, best-effort.
    pub(crate) async fn insert(&self, key: CacheKey, obj: Arc<CachedObject>) {
        self.core.insert(key, obj).await;
    }

    /// Drop an object from every local tier.
    pub(crate) async fn invalidate(&self, key: &CacheKey) {
        self.core.invalidate(key).await;
    }

    /// Get `key`, or run `origin` to fetch it and populate the tiers. Kept local (rather
    /// than `tierstore::TieredCache::get_or_load`) because the probes here feed the
    /// per-request warm metrics via tier provenance. Probe-then-gate
    /// singleflight: hot hits never touch the gate; concurrent misses for one key share a
    /// single origin round-trip (followers re-probe under the gate and reuse the leader's
    /// fill). Errors are not cached.
    pub(crate) async fn get_or_fetch<Fut>(
        &self,
        key: &CacheKey,
        origin: Fut,
    ) -> Result<Arc<CachedObject>, String>
    where
        Fut: Future<Output = Result<Arc<CachedObject>, String>> + Send,
    {
        if let Some(obj) = self.core.lookup(key).await {
            return Ok(obj);
        }
        let gate = self.core.fills.acquire(key.clone()).await;
        if let Some(obj) = self.core.lookup(key).await {
            drop(gate);
            return Ok(obj);
        }
        let result = origin.await;
        if let Ok(obj) = &result {
            self.core.insert(key.clone(), obj.clone()).await;
        }
        drop(gate);
        result
    }
}

/// A cloneable handle to this node's local tiers for the commit-log consumer to
/// invalidate on a peer's write.
#[derive(Clone)]
pub(crate) struct LocalCache {
    core: Arc<Core>,
}

impl LocalCache {
    /// Drop a key from every node-local tier so a peer's overwrite is never read stale.
    pub(crate) async fn invalidate(&self, key: &CacheKey) {
        self.core.invalidate(key).await;
    }

    /// Drop every node-local copy (see [`Core::flush`]) — used when peers'
    /// writes were provably missed and the stale subset is unknowable.
    pub(crate) async fn flush(&self) {
        self.core.flush().await;
    }
}

#[cfg(test)]
mod tests {
    use super::{CachedObject, TieredCache, buffer_body, open_warm};
    use crate::metrics::Metrics;
    use bytes::Bytes;
    use s3s::dto::{ETag, GetObjectOutput, Timestamp};
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, UNIX_EPOCH};
    use tierstore::{TierRead, TierWrite};

    static DIR_SEQ: AtomicU64 = AtomicU64::new(0);

    fn temp_dir() -> std::path::PathBuf {
        let n = DIR_SEQ.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("s3cache-disk-{}-{n}", std::process::id()))
    }

    fn sample() -> CachedObject {
        let out = GetObjectOutput {
            content_length: Some(5),
            content_type: Some("text/plain".to_owned()),
            e_tag: Some(ETag::Strong("\"deadbeef\"".to_owned())),
            last_modified: Some(Timestamp::from(
                UNIX_EPOCH + Duration::from_secs(1_700_000_000),
            )),
            metadata: Some(HashMap::from([("k".to_owned(), "v".to_owned())])),
            ..Default::default()
        };
        CachedObject::from_get(&out, Bytes::from_static(b"hello"))
    }

    fn ck(bucket: &str, key: &str) -> super::CacheKey {
        (bucket.to_owned(), key.to_owned())
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

    #[tokio::test]
    async fn warm_tier_roundtrip_and_restart_recovery() {
        let dir = temp_dir();
        let _ = std::fs::remove_dir_all(&dir);
        let cap = 10 * 1024 * 1024;

        let (warm, _disk) = open_warm(dir.clone(), cap, 8 * 1024 * 1024).unwrap();
        let obj = Arc::new(sample());
        warm.put(ck("b", "k"), obj.clone()).await.unwrap();
        assert_eq!(
            warm.get(&ck("b", "k")).await.unwrap().expect("hit").body,
            obj.body
        );
        assert!(warm.get(&ck("b", "missing")).await.unwrap().is_none());

        // A fresh tier over the same dir re-indexes the file — survives a restart.
        let (warm2, _disk2) = open_warm(dir.clone(), cap, 8 * 1024 * 1024).unwrap();
        assert!(
            warm2.get(&ck("b", "k")).await.unwrap().is_some(),
            "warm tier survives restart"
        );

        warm2.delete(&ck("b", "k")).await.unwrap();
        assert!(
            warm2.get(&ck("b", "k")).await.unwrap().is_none(),
            "invalidate deletes the entry"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn layered_cache_fills_and_invalidates() {
        let dir = temp_dir();
        let _ = std::fs::remove_dir_all(&dir);
        let warm = open_warm(dir.clone(), 10 * 1024 * 1024, 8 * 1024 * 1024).unwrap();
        let cache = TieredCache::new(1024 * 1024, Some(warm), Arc::new(Metrics::default()));

        let key = ck("b", "k");
        let obj = Arc::new(sample());
        cache.insert(key.clone(), obj.clone()).await;
        assert_eq!(cache.get(&key).await.expect("hit").body, obj.body);

        // Origin is only consulted once per key; the second get_or_fetch hits locally.
        let fetched = cache
            .get_or_fetch(&ck("b", "k2"), async { Ok(Arc::new(sample())) })
            .await
            .expect("fetch");
        assert_eq!(fetched.body, obj.body);
        let served = cache
            .get_or_fetch(&ck("b", "k2"), async {
                Err("origin must not be called".to_owned())
            })
            .await
            .expect("served from cache");
        assert_eq!(served.body, obj.body);

        cache.invalidate(&key).await;
        assert!(
            cache.get(&key).await.is_none(),
            "invalidate drops all local copies"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }
}
