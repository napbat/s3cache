//! Layered object-body cache: **hot** (node-local heap) in front of **warm** (a node-local
//! on-disk cache) in front of **cold** (the S3 origin). Always layered — there is no mode
//! to pick. The warm disk tier is inclusive (every object written to hot is also written
//! to disk) and survives process restarts, so a fresh pod comes up warm instead of
//! stampeding the origin. Cross-node coherence is separate (see `coherence`): a peer's
//! write invalidates the local hot *and* disk copies, and reads barrier on the log.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt;
use moka::notification::RemovalCause;
use s3s::dto::{ETag, GetObjectOutput, HeadObjectOutput, Metadata, StreamingBlob, Timestamp};
use serde::{Deserialize, Serialize};

use crate::metrics::Metrics;

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
        StreamingBlob::wrap(futures::stream::once(async move { Ok::<Bytes, std::io::Error>(b) }))
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

/// The node-local heap LRU: `(bucket, key) -> object`. Cheap to clone (an `Arc` inside).
pub(crate) type HotCache = moka::future::Cache<(String, String), Arc<CachedObject>>;

/// Node-local on-disk warm tier: an inclusive, size-limited cache under a directory that
/// survives restarts. An in-memory LRU index (weighted by file bytes, bounded by the disk
/// budget) tracks what's on disk; when an entry leaves the index — evicted for size or
/// invalidated — its file is deleted. Best-effort: any I/O error is a miss/drop, never a
/// data-plane failure.
#[derive(Clone)]
pub(crate) struct DiskCache {
    dir: PathBuf,
    index: moka::future::Cache<String, u64>,
    max_obj_bytes: usize,
    seq: Arc<AtomicU64>,
    metrics: Arc<Metrics>,
}

impl DiskCache {
    /// Open (creating if needed) a disk cache under `dir`, bounded to `disk_bytes`, and
    /// re-index any files already present so the cache survives restarts.
    pub(crate) async fn open(dir: PathBuf, disk_bytes: u64, max_obj_bytes: usize, metrics: Arc<Metrics>) -> anyhow::Result<Self> {
        tokio::fs::create_dir_all(&dir).await?;
        let evict_dir = dir.clone();
        let index = moka::future::Cache::builder()
            .max_capacity(disk_bytes)
            .weigher(|_h: &String, size: &u64| u32::try_from(*size).unwrap_or(u32::MAX))
            .eviction_listener(move |h: Arc<String>, _size, cause| {
                // `Replaced` = re-inserted at the same path (the put already wrote the new
                // file), so keep it; size eviction and explicit invalidation delete.
                if cause != RemovalCause::Replaced {
                    let _ = std::fs::remove_file(Self::path_in(&evict_dir, &h));
                }
            })
            .build();
        let cache = Self { dir, index, max_obj_bytes, seq: Arc::new(AtomicU64::new(0)), metrics };
        cache.reindex().await;
        Ok(cache)
    }

    /// Blake3 of `bucket\0key` — a fixed-length, path-safe, collision-free filename.
    fn hash(bucket: &str, key: &str) -> String {
        let mut h = blake3::Hasher::new();
        h.update(bucket.as_bytes());
        h.update(&[0]);
        h.update(key.as_bytes());
        h.finalize().to_hex().to_string()
    }

    fn path_in(dir: &Path, h: &str) -> PathBuf {
        dir.join(&h[..2]).join(h) // shard by the first two hex chars
    }

    fn path(&self, h: &str) -> PathBuf {
        Self::path_in(&self.dir, h)
    }

    /// Populate the index from files already on disk (restart recovery).
    async fn reindex(&self) {
        let Ok(mut shards) = tokio::fs::read_dir(&self.dir).await else { return };
        while let Ok(Some(shard)) = shards.next_entry().await {
            if !shard.file_type().await.is_ok_and(|t| t.is_dir()) {
                continue;
            }
            let Ok(mut files) = tokio::fs::read_dir(shard.path()).await else { continue };
            while let Ok(Some(f)) = files.next_entry().await {
                // Cache files are pure hex (no dot); temp files are `<hash>.<n>.tmp`.
                if let Ok(meta) = f.metadata().await
                    && let Some(name) = f.file_name().to_str()
                    && !name.contains('.')
                {
                    self.index.insert(name.to_owned(), meta.len()).await;
                }
            }
        }
    }

    async fn get(&self, bucket: &str, key: &str) -> Option<Arc<CachedObject>> {
        let h = Self::hash(bucket, key);
        self.index.get(&h).await?; // touch LRU recency; miss if not indexed
        let Ok(bytes) = tokio::fs::read(self.path(&h)).await else {
            self.index.invalidate(&h).await; // indexed but file gone
            self.metrics.warm_miss();
            return None;
        };
        match bincode::deserialize::<CachedObject>(&bytes) {
            Ok(obj) => {
                self.metrics.warm_hit();
                Some(Arc::new(obj))
            }
            Err(e) => {
                tracing::debug!("disk decode failed for {bucket}/{key}: {e}");
                self.index.invalidate(&h).await;
                self.metrics.warm_error();
                None
            }
        }
    }

    async fn put(&self, bucket: &str, key: &str, obj: &CachedObject) {
        let bytes = match bincode::serialize(obj) {
            Ok(b) if b.len() <= self.max_obj_bytes => b,
            Ok(_) => return, // oversize: leave it to stream through, don't fill the disk
            Err(e) => {
                tracing::debug!("disk encode failed for {bucket}/{key}: {e}");
                return;
            }
        };
        let h = Self::hash(bucket, key);
        if let Err(e) = self.write_atomic(&self.path(&h), &bytes).await {
            tracing::debug!("disk write failed for {bucket}/{key}: {e}");
            self.metrics.warm_error();
            return;
        }
        self.index.insert(h, bytes.len() as u64).await;
    }

    async fn invalidate(&self, bucket: &str, key: &str) {
        // Removing from the index fires the listener, which deletes the file; also remove
        // directly in case the key was never indexed on this node (a stray file).
        let h = Self::hash(bucket, key);
        self.index.invalidate(&h).await;
        let _ = tokio::fs::remove_file(self.path(&h)).await;
    }

    /// Write to a unique temp file then rename, so a crash never leaves a partial file and
    /// concurrent writers don't clobber each other's temp.
    async fn write_atomic(&self, path: &Path, bytes: &[u8]) -> std::io::Result<()> {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let n = self.seq.fetch_add(1, Ordering::Relaxed);
        let tmp = path.with_extension(format!("{n}.tmp"));
        tokio::fs::write(&tmp, bytes).await?;
        tokio::fs::rename(&tmp, path).await
    }
}

/// A cloneable handle to this node's local tiers (hot + optional disk) for the commit-log
/// consumer to invalidate on a peer's write.
#[derive(Clone)]
pub(crate) struct LocalCache {
    hot: HotCache,
    warm: Option<DiskCache>,
}

impl LocalCache {
    #[must_use]
    pub(crate) fn new(hot: HotCache, warm: Option<DiskCache>) -> Self {
        Self { hot, warm }
    }

    /// Drop a key from every node-local tier so a peer's overwrite is never read stale.
    pub(crate) async fn invalidate(&self, key: &(String, String)) {
        self.hot.invalidate(key).await;
        if let Some(warm) = &self.warm {
            warm.invalidate(&key.0, &key.1).await;
        }
    }
}

/// The layered object-body cache: an always-present hot heap LRU and an optional node-local
/// disk tier, in front of the cold S3 origin.
pub(crate) struct TieredCache {
    hot: HotCache,
    warm: Option<DiskCache>,
}

impl TieredCache {
    /// Build the cache: a hot LRU weighted by body bytes up to `cache_bytes`, plus the
    /// optional disk tier.
    #[must_use]
    pub(crate) fn new(cache_bytes: u64, warm: Option<DiskCache>) -> Self {
        let hot = moka::future::Cache::builder()
            .max_capacity(cache_bytes)
            .weigher(|_k, v: &Arc<CachedObject>| u32::try_from(v.body.len()).unwrap_or(u32::MAX))
            .build();
        Self { hot, warm }
    }

    /// A handle the commit-log consumer uses to invalidate this node's local copies.
    #[must_use]
    pub(crate) fn local(&self) -> LocalCache {
        LocalCache::new(self.hot.clone(), self.warm.clone())
    }

    /// Look up a whole cached object: hot, then warm disk (a disk hit backfills hot).
    pub(crate) async fn get(&self, key: &(String, String)) -> Option<Arc<CachedObject>> {
        if let Some(obj) = self.hot.get(key).await {
            return Some(obj);
        }
        if let Some(warm) = &self.warm
            && let Some(obj) = warm.get(&key.0, &key.1).await
        {
            self.hot.insert(key.clone(), obj.clone()).await;
            return Some(obj);
        }
        None
    }

    /// Store into hot and (inclusively) the warm disk tier.
    pub(crate) async fn insert(&self, key: (String, String), obj: Arc<CachedObject>) {
        if let Some(warm) = &self.warm {
            warm.put(&key.0, &key.1, &obj).await;
        }
        self.hot.insert(key, obj).await;
    }

    /// Drop an object from every local tier.
    pub(crate) async fn invalidate(&self, key: &(String, String)) {
        self.hot.invalidate(key).await;
        if let Some(warm) = &self.warm {
            warm.invalidate(&key.0, &key.1).await;
        }
    }

    /// Get `key`, or run `origin` to fetch it and populate the tiers. The fetch is
    /// singleflighted (moka's `try_get_with`), so concurrent callers share one origin
    /// round-trip; a disk hit short-circuits the fetch entirely.
    pub(crate) async fn get_or_fetch<Fut>(
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
        self.hot.try_get_with(key.clone(), load).await.map_err(|e: Arc<String>| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::{buffer_body, CachedObject, DiskCache};
    use crate::metrics::Metrics;
    use bytes::Bytes;
    use s3s::dto::{ETag, GetObjectOutput, Timestamp};
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::{Duration, UNIX_EPOCH};

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
            last_modified: Some(Timestamp::from(UNIX_EPOCH + Duration::from_secs(1_700_000_000))),
            metadata: Some(HashMap::from([("k".to_owned(), "v".to_owned())])),
            ..Default::default()
        };
        CachedObject::from_get(&out, Bytes::from_static(b"hello"))
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
    async fn disk_cache_roundtrip_and_restart_recovery() {
        let dir = temp_dir();
        let _ = std::fs::remove_dir_all(&dir);
        let m = Arc::new(Metrics::default());
        let cap = 10 * 1024 * 1024;

        let disk = DiskCache::open(dir.clone(), cap, 8 * 1024 * 1024, m.clone()).await.unwrap();
        let obj = sample();
        disk.put("b", "k", &obj).await;
        assert_eq!(disk.get("b", "k").await.expect("hit").body, obj.body);
        assert!(disk.get("b", "missing").await.is_none());

        // A fresh cache over the same dir re-indexes the file — survives a restart.
        let disk2 = DiskCache::open(dir.clone(), cap, 8 * 1024 * 1024, m).await.unwrap();
        assert!(disk2.get("b", "k").await.is_some(), "disk cache survives restart");

        disk2.invalidate("b", "k").await;
        assert!(disk2.get("b", "k").await.is_none(), "invalidate deletes the entry");

        let _ = std::fs::remove_dir_all(&dir);
    }
}
