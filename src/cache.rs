//! Caching layer over the upstream `s3s_aws::Proxy`.
//!
//! The win: because every client S3 request funnels through this proxy, it sees
//! every write — so it can answer **LIST** entirely from an in-memory key index
//! (LISTs are R2 Class-A ops, the expensive tier, and chatty clients issue them
//! constantly), and it maintains that index from writes. Reads/writes are otherwise
//! forwarded (write-through): the upstream stays the authority for conditional
//! (OCC) writes, so correctness is unchanged.
//!
//! Correctness rests on a single property: this proxy is the *only* path to the
//! bucket. The LIST index warms up lazily — the proxy serves immediately and a
//! background task does the one full LIST per bucket; until a bucket's index is
//! complete, LISTs for it pass straight through to the upstream (always correct), then
//! flip to index-served ([`CachingProxy::is_synced`] gates this). Once warm it's kept
//! current by observed writes. The GET/HEAD body cache is separately lazy (populate on
//! miss, invalidate on write).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::Bound;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use s3s::dto::{ListObjectsV2Input, ListObjectsV2Output, Object, CommonPrefix, PutObjectInput, PutObjectOutput, DeleteObjectInput, DeleteObjectOutput, DeleteObjectsInput, DeleteObjectsOutput, CompleteMultipartUploadInput, CompleteMultipartUploadOutput, CopyObjectInput, CopyObjectOutput, GetObjectInput, GetObjectOutput, HeadObjectInput, HeadObjectOutput, StreamingBlob, Metadata, ETag, Timestamp};
use s3s::{S3Request, S3Response, S3Result};
use tracing::info;

struct ObjEntry {
    size: i64,
    last_modified: SystemTime,
}

/// A cached object body + the response metadata needed to reconstruct a GET/HEAD.
struct CachedObject {
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
    fn from_get(out: &GetObjectOutput, body: Bytes) -> Self {
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

    fn to_get(&self) -> GetObjectOutput {
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

    /// A 206-shaped GET for an inclusive byte range sliced out of the cached
    /// body (clamped at EOF). `None` when the range start is past the object.
    fn to_get_range(&self, first: u64, last: Option<u64>) -> Option<GetObjectOutput> {
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

    fn to_head(&self) -> HeadObjectOutput {
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

/// Drain a streamed body into memory, bailing (None) past `cap` bytes or on error.
async fn buffer_body(blob: StreamingBlob, cap: usize) -> Option<Bytes> {
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

#[derive(Default)]
struct BucketState {
    synced: bool,
    keys: BTreeMap<String, ObjEntry>,
}

#[derive(Default)]
pub struct Metrics {
    list_from_index: AtomicU64,
    list_passthrough: AtomicU64,
    writes_indexed: AtomicU64,
    get_hit: AtomicU64,
    get_miss: AtomicU64,
    get_bypass: AtomicU64,
    range_hit: AtomicU64,
    range_promote: AtomicU64,
}

pub struct CachingProxy {
    inner: s3s_aws::Proxy,
    /// Direct client used only for the background full LIST warm-up sync.
    client: aws_sdk_s3::Client,
    /// The LIST index, `Arc` so the background warm-up task shares it with the serving
    /// proxy (the service takes the proxy by value, so the sync can't borrow `self`).
    state: Arc<RwLock<HashMap<String, BucketState>>>,
    /// Object-body LRU (weighted by bytes). Key = (bucket, key).
    obj_cache: moka::future::Cache<(String, String), Arc<CachedObject>>,
    /// Objects larger than this are never cached (segments stream straight through).
    max_obj_bytes: usize,
    metrics: Arc<Metrics>,
}

impl CachingProxy {
    pub fn new(
        inner: s3s_aws::Proxy,
        client: aws_sdk_s3::Client,
        cache_bytes: u64,
        max_obj_bytes: usize,
    ) -> Self {
        let obj_cache = moka::future::Cache::builder()
            .max_capacity(cache_bytes)
            .weigher(|_k, v: &Arc<CachedObject>| u32::try_from(v.body.len()).unwrap_or(u32::MAX))
            .build();
        Self {
            inner,
            client,
            state: Arc::new(RwLock::new(HashMap::new())),
            obj_cache,
            max_obj_bytes,
            metrics: Arc::new(Metrics::default()),
        }
    }

    pub fn metrics(&self) -> Arc<Metrics> {
        self.metrics.clone()
    }

    /// Warm the LIST index in the background instead of pre-loading it before serving.
    /// The proxy binds + serves immediately; a `LIST` for a bucket whose index isn't
    /// complete yet passes straight through to the upstream ([`is_synced`] gates it), so
    /// results are always correct during warm-up. Once a bucket's full sync finishes it
    /// flips to index-served. This keeps startup instant and independent of bucket size,
    /// rather than blocking the port on a full pre-sync (which grows with the object count).
    ///
    /// [`is_synced`]: Self::is_synced
    pub fn spawn_background_sync(&self, buckets: Vec<String>) {
        let client = self.client.clone();
        let state = self.state.clone();
        tokio::spawn(async move {
            for bucket in buckets {
                match sync_bucket_into(&client, &state, &bucket).await {
                    Ok(n) => info!("warmed LIST index for `{bucket}`: {n} keys"),
                    Err(e) => {
                        tracing::warn!("background sync of `{bucket}` failed (staying passthrough): {e}");
                    }
                }
            }
        });
    }

    fn index_insert(&self, bucket: &str, key: &str, size: i64) {
        let mut g = self.state.write().unwrap();
        g.entry(bucket.to_owned())
            .or_default()
            .keys
            .insert(key.to_owned(), ObjEntry { size, last_modified: SystemTime::now() });
        self.metrics.writes_indexed.fetch_add(1, Ordering::Relaxed);
    }

    fn index_remove(&self, bucket: &str, key: &str) {
        if let Some(b) = self.state.write().unwrap().get_mut(bucket) {
            b.keys.remove(key);
        }
    }

    /// The indexed size of a key, if this proxy has seen it (write-through or
    /// LIST warm-up). Drives the range-promotion decision without a HEAD.
    fn index_size(&self, bucket: &str, key: &str) -> Option<i64> {
        self.state.read().unwrap().get(bucket).and_then(|b| b.keys.get(key)).map(|e| e.size)
    }

    fn is_synced(&self, bucket: &str) -> bool {
        self.state.read().unwrap().get(bucket).is_some_and(|b| b.synced)
    }

    /// Build a `ListObjectsV2` response from the index (prefix / delimiter / max-keys /
    /// continuation), matching S3 semantics closely enough for `opendal`-style clients.
    fn list_from_index(&self, inp: &ListObjectsV2Input) -> ListObjectsV2Output {
        let bucket = inp.bucket.as_str();
        let prefix = inp.prefix.clone().unwrap_or_default();
        let delim = inp.delimiter.clone();
        let max = usize::try_from(inp.max_keys.unwrap_or(1000).clamp(1, 1000)).unwrap_or(1000);
        // v2 continuation is opaque — we use the last returned key. start_after is the
        // cold-start equivalent.
        let after = inp.continuation_token.clone().or_else(|| inp.start_after.clone());

        let mut contents: Vec<Object> = Vec::new();
        let mut common: BTreeSet<String> = BTreeSet::new();
        let mut truncated = false;
        let mut next_token = None;

        let g = self.state.read().unwrap();
        let keys = g.get(bucket).map(|b| &b.keys);

        if let Some(keys) = keys {
            let lower = match &after {
                Some(a) => Bound::Excluded(a.clone()),
                None => Bound::Unbounded,
            };
            for (key, entry) in keys.range((lower, Bound::Unbounded)) {
                if !key.starts_with(&prefix) {
                    if key.as_str() > prefix.as_str() {
                        break; // sorted: past the prefix block
                    }
                    continue;
                }
                let count = contents.len() + common.len();
                if let Some(d) = &delim {
                    let rest = &key[prefix.len()..];
                    if let Some(idx) = rest.find(d.as_str()) {
                        let cp = format!("{prefix}{}", &rest[..idx + d.len()]);
                        if !common.contains(&cp) {
                            if count >= max {
                                truncated = true;
                                next_token = Some(key.clone());
                                break;
                            }
                            common.insert(cp);
                        }
                        continue;
                    }
                }
                if count >= max {
                    truncated = true;
                    next_token = Some(key.clone());
                    break;
                }
                contents.push(Object {
                    key: Some(key.clone()),
                    size: Some(entry.size),
                    last_modified: Some(Timestamp::from(entry.last_modified)),
                    ..Default::default()
                });
            }
        }

        let key_count = i32::try_from(contents.len() + common.len()).unwrap_or(i32::MAX);
        ListObjectsV2Output {
            name: Some(bucket.to_owned()),
            prefix: Some(prefix),
            max_keys: Some(i32::try_from(max).unwrap_or(1000)),
            key_count: Some(key_count),
            is_truncated: Some(truncated),
            continuation_token: inp.continuation_token.clone(),
            next_continuation_token: next_token,
            contents: (!contents.is_empty()).then_some(contents),
            common_prefixes: (!common.is_empty())
                .then(|| common.into_iter().map(|p| CommonPrefix { prefix: Some(p) }).collect()),
            delimiter: delim,
            start_after: inp.start_after.clone(),
            ..Default::default()
        }
    }

}

/// Full paginated LIST of a bucket into `state`, then mark it synced. Merges (never
/// clears) so a write that raced the sync isn't lost. Free-standing (takes the client +
/// shared index) so the background warm-up task can run it without borrowing the proxy,
/// which the S3 service owns by value.
async fn sync_bucket_into(
    client: &aws_sdk_s3::Client,
    state: &RwLock<HashMap<String, BucketState>>,
    bucket: &str,
) -> anyhow::Result<usize> {
    let mut token: Option<String> = None;
    let mut found = 0usize;
    loop {
        let mut req = client.list_objects_v2().bucket(bucket).max_keys(1000);
        if let Some(t) = &token {
            req = req.continuation_token(t);
        }
        let resp = req.send().await?;
        for obj in resp.contents() {
            if let Some(key) = obj.key() {
                let last_modified = obj
                    .last_modified()
                    .and_then(|d| u64::try_from(d.secs()).ok())
                    .map_or_else(SystemTime::now, |s| UNIX_EPOCH + Duration::from_secs(s));
                let entry = ObjEntry { size: obj.size().unwrap_or(0), last_modified };
                let mut g = state.write().unwrap();
                g.entry(bucket.to_owned()).or_default().keys.insert(key.to_owned(), entry);
                found += 1;
            }
        }
        if resp.is_truncated().unwrap_or(false) {
            token = resp.next_continuation_token().map(str::to_owned);
            if token.is_none() {
                break;
            }
        } else {
            break;
        }
    }
    state.write().unwrap().entry(bucket.to_owned()).or_default().synced = true;
    info!("synced bucket `{bucket}` into index: {found} keys");
    Ok(found)
}

/// Periodically log the cache-effectiveness counters (LISTs served from the index
/// vs forwarded, writes indexed).
pub fn spawn_stats(metrics: Arc<Metrics>, interval_secs: u64) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(interval_secs.max(1)));
        loop {
            tick.tick().await;
            info!(
                "s3cache stats: list_from_index={} list_passthrough={} writes_indexed={} \
                 get_hit={} get_miss={} get_bypass={} range_hit={} range_promote={}",
                metrics.list_from_index.load(Ordering::Relaxed),
                metrics.list_passthrough.load(Ordering::Relaxed),
                metrics.writes_indexed.load(Ordering::Relaxed),
                metrics.get_hit.load(Ordering::Relaxed),
                metrics.get_miss.load(Ordering::Relaxed),
                metrics.get_bypass.load(Ordering::Relaxed),
                metrics.range_hit.load(Ordering::Relaxed),
                metrics.range_promote.load(Ordering::Relaxed),
            );
        }
    });
}

#[async_trait]
impl s3s::S3 for CachingProxy {
    // LIST served from the index when the bucket is synced; else passthrough.
    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        if self.is_synced(req.input.bucket.as_str()) {
            self.metrics.list_from_index.fetch_add(1, Ordering::Relaxed);
            let out = self.list_from_index(&req.input);
            return Ok(S3Response::new(out));
        }
        self.metrics.list_passthrough.fetch_add(1, Ordering::Relaxed);
        self.inner.list_objects_v2(req).await
    }

    // Writes: forward (write-through), then update the index from the result.
    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        let bucket = req.input.bucket.clone();
        let key = req.input.key.clone();
        let size = req.input.content_length.unwrap_or(0);
        let resp = self.inner.put_object(req).await?;
        self.index_insert(&bucket, &key, size);
        self.obj_cache.invalidate(&(bucket, key)).await;
        Ok(resp)
    }

    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        let bucket = req.input.bucket.clone();
        let key = req.input.key.clone();
        let resp = self.inner.delete_object(req).await?;
        self.index_remove(&bucket, &key);
        self.obj_cache.invalidate(&(bucket, key)).await;
        Ok(resp)
    }

    async fn delete_objects(
        &self,
        req: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        let bucket = req.input.bucket.clone();
        let keys: Vec<String> = req.input.delete.objects.iter().map(|o| o.key.clone()).collect();
        let resp = self.inner.delete_objects(req).await?;
        for k in keys {
            self.index_remove(&bucket, &k);
            self.obj_cache.invalidate(&(bucket.clone(), k)).await;
        }
        Ok(resp)
    }

    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        let bucket = req.input.bucket.clone();
        let key = req.input.key.clone();
        let resp = self.inner.complete_multipart_upload(req).await?;
        self.index_insert(&bucket, &key, 0);
        self.obj_cache.invalidate(&(bucket, key)).await;
        Ok(resp)
    }

    async fn copy_object(
        &self,
        req: S3Request<CopyObjectInput>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        let bucket = req.input.bucket.clone();
        let key = req.input.key.clone();
        let resp = self.inner.copy_object(req).await?;
        self.index_insert(&bucket, &key, 0);
        self.obj_cache.invalidate(&(bucket, key)).await;
        Ok(resp)
    }

    // GET: cacheable (no part/conditional) small objects served from the LRU;
    // miss buffers the body + caches it. RANGED reads of cacheable-size objects
    // are served by slicing the cached whole object — promoted (whole-object
    // fetch, singleflighted by moka's try_get_with) on first touch. docres reads
    // payload packs and lance data via ranged GETs; streaming every range
    // through to R2 made each search hit a full upstream round trip. Oversized/
    // unknown-size ranges and conditional requests still stream through.
    async fn get_object(
        &self,
        mut req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let unconditional = req.input.part_number.is_none()
            && req.input.if_match.is_none()
            && req.input.if_none_match.is_none()
            && req.input.if_modified_since.is_none()
            && req.input.if_unmodified_since.is_none();
        let cacheable = unconditional && req.input.range.is_none();
        let ckey = (req.input.bucket.clone(), req.input.key.clone());
        let int_range = match (unconditional, req.input.range) {
            (true, Some(s3s::dto::Range::Int { first, last })) => Some((first, last)),
            _ => None,
        };
        if let Some((first, last)) = int_range {
            // Cached whole object → serve the slice locally.
            if let Some(obj) = self.obj_cache.get(&ckey).await {
                if let Some(out) = obj.to_get_range(first, last) {
                    self.metrics.range_hit.fetch_add(1, Ordering::Relaxed);
                    return Ok(S3Response::new(out));
                }
            }
            // Promote when the index says the whole object fits the cache: one
            // upstream GET (deduped across concurrent ranges by try_get_with),
            // then every range — this one included — is a local slice.
            let small = self
                .index_size(&ckey.0, &ckey.1)
                .is_some_and(|sz| sz >= 0 && usize::try_from(sz).unwrap_or(usize::MAX) <= self.max_obj_bytes);
            if small {
                req.input.range = None; // fetch the whole object in the loader
                let cap = self.max_obj_bytes;
                let inner = &self.inner;
                let fetched = self
                    .obj_cache
                    .try_get_with(ckey.clone(), async move {
                        let mut resp = inner.get_object(req).await.map_err(|e| e.to_string())?;
                        let body = match resp.output.body.take() {
                            Some(b) => buffer_body(b, cap)
                                .await
                                .ok_or_else(|| "s3cache: range-promote buffer overflow".to_owned())?,
                            None => Bytes::new(),
                        };
                        Ok::<_, String>(Arc::new(CachedObject::from_get(&resp.output, body)))
                    })
                    .await;
                match fetched {
                    Ok(obj) => {
                        self.metrics.range_promote.fetch_add(1, Ordering::Relaxed);
                        return obj
                            .to_get_range(first, last)
                            .map(|out| Ok(S3Response::new(out)))
                            .unwrap_or_else(|| Err(s3s::s3_error!(InvalidRange, "range start past end of object")));
                    }
                    Err(e) => return Err(s3s::s3_error!(InternalError, "s3cache: range promote failed: {e}")),
                }
            }
            // Big or not-yet-indexed: stream the range through as before.
            self.metrics.get_bypass.fetch_add(1, Ordering::Relaxed);
            return self.inner.get_object(req).await;
        }
        if cacheable {
            if let Some(obj) = self.obj_cache.get(&ckey).await {
                self.metrics.get_hit.fetch_add(1, Ordering::Relaxed);
                return Ok(S3Response::new(obj.to_get()));
            }
        }
        let mut resp = self.inner.get_object(req).await?;
        let len = resp.output.content_length.unwrap_or(-1);
        let small = len >= 0 && usize::try_from(len).unwrap_or(usize::MAX) <= self.max_obj_bytes;
        if cacheable && small {
            if let Some(body) = resp.output.body.take() {
                match buffer_body(body, self.max_obj_bytes).await {
                    Some(bytes) => {
                        self.obj_cache
                            .insert(ckey, Arc::new(CachedObject::from_get(&resp.output, bytes.clone())))
                            .await;
                        self.metrics.get_miss.fetch_add(1, Ordering::Relaxed);
                        resp.output.body = Some(StreamingBlob::wrap(futures::stream::once(
                            async move { Ok::<Bytes, std::io::Error>(bytes) },
                        )));
                    }
                    None => return Err(s3s::s3_error!(InternalError, "s3cache: failed to buffer body")),
                }
            }
        } else {
            self.metrics.get_bypass.fetch_add(1, Ordering::Relaxed);
        }
        Ok(resp)
    }

    // HEAD served from the object cache when the body is already cached.
    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        if req.input.range.is_none() && req.input.part_number.is_none() {
            let ckey = (req.input.bucket.clone(), req.input.key.clone());
            if let Some(obj) = self.obj_cache.get(&ckey).await {
                self.metrics.get_hit.fetch_add(1, Ordering::Relaxed);
                return Ok(S3Response::new(obj.to_head()));
            }
        }
        self.inner.head_object(req).await
    }

    // Full S3 passthrough: every other op forwards to the upstream so any S3
    // client works (not just docres). Generated from the s3s S3 trait.
    async fn abort_multipart_upload(&self, req: S3Request<s3s::dto::AbortMultipartUploadInput>) -> S3Result<S3Response<s3s::dto::AbortMultipartUploadOutput>> {
        self.inner.abort_multipart_upload(req).await
    }
    async fn create_bucket(&self, req: S3Request<s3s::dto::CreateBucketInput>) -> S3Result<S3Response<s3s::dto::CreateBucketOutput>> {
        self.inner.create_bucket(req).await
    }
    async fn create_bucket_metadata_table_configuration(&self, req: S3Request<s3s::dto::CreateBucketMetadataTableConfigurationInput>) -> S3Result<S3Response<s3s::dto::CreateBucketMetadataTableConfigurationOutput>> {
        self.inner.create_bucket_metadata_table_configuration(req).await
    }
    async fn create_multipart_upload(&self, req: S3Request<s3s::dto::CreateMultipartUploadInput>) -> S3Result<S3Response<s3s::dto::CreateMultipartUploadOutput>> {
        self.inner.create_multipart_upload(req).await
    }
    async fn create_session(&self, req: S3Request<s3s::dto::CreateSessionInput>) -> S3Result<S3Response<s3s::dto::CreateSessionOutput>> {
        self.inner.create_session(req).await
    }
    async fn delete_bucket(&self, req: S3Request<s3s::dto::DeleteBucketInput>) -> S3Result<S3Response<s3s::dto::DeleteBucketOutput>> {
        self.inner.delete_bucket(req).await
    }
    async fn delete_bucket_analytics_configuration(&self, req: S3Request<s3s::dto::DeleteBucketAnalyticsConfigurationInput>) -> S3Result<S3Response<s3s::dto::DeleteBucketAnalyticsConfigurationOutput>> {
        self.inner.delete_bucket_analytics_configuration(req).await
    }
    async fn delete_bucket_cors(&self, req: S3Request<s3s::dto::DeleteBucketCorsInput>) -> S3Result<S3Response<s3s::dto::DeleteBucketCorsOutput>> {
        self.inner.delete_bucket_cors(req).await
    }
    async fn delete_bucket_encryption(&self, req: S3Request<s3s::dto::DeleteBucketEncryptionInput>) -> S3Result<S3Response<s3s::dto::DeleteBucketEncryptionOutput>> {
        self.inner.delete_bucket_encryption(req).await
    }
    async fn delete_bucket_intelligent_tiering_configuration(&self, req: S3Request<s3s::dto::DeleteBucketIntelligentTieringConfigurationInput>) -> S3Result<S3Response<s3s::dto::DeleteBucketIntelligentTieringConfigurationOutput>> {
        self.inner.delete_bucket_intelligent_tiering_configuration(req).await
    }
    async fn delete_bucket_inventory_configuration(&self, req: S3Request<s3s::dto::DeleteBucketInventoryConfigurationInput>) -> S3Result<S3Response<s3s::dto::DeleteBucketInventoryConfigurationOutput>> {
        self.inner.delete_bucket_inventory_configuration(req).await
    }
    async fn delete_bucket_lifecycle(&self, req: S3Request<s3s::dto::DeleteBucketLifecycleInput>) -> S3Result<S3Response<s3s::dto::DeleteBucketLifecycleOutput>> {
        self.inner.delete_bucket_lifecycle(req).await
    }
    async fn delete_bucket_metadata_table_configuration(&self, req: S3Request<s3s::dto::DeleteBucketMetadataTableConfigurationInput>) -> S3Result<S3Response<s3s::dto::DeleteBucketMetadataTableConfigurationOutput>> {
        self.inner.delete_bucket_metadata_table_configuration(req).await
    }
    async fn delete_bucket_metrics_configuration(&self, req: S3Request<s3s::dto::DeleteBucketMetricsConfigurationInput>) -> S3Result<S3Response<s3s::dto::DeleteBucketMetricsConfigurationOutput>> {
        self.inner.delete_bucket_metrics_configuration(req).await
    }
    async fn delete_bucket_ownership_controls(&self, req: S3Request<s3s::dto::DeleteBucketOwnershipControlsInput>) -> S3Result<S3Response<s3s::dto::DeleteBucketOwnershipControlsOutput>> {
        self.inner.delete_bucket_ownership_controls(req).await
    }
    async fn delete_bucket_policy(&self, req: S3Request<s3s::dto::DeleteBucketPolicyInput>) -> S3Result<S3Response<s3s::dto::DeleteBucketPolicyOutput>> {
        self.inner.delete_bucket_policy(req).await
    }
    async fn delete_bucket_replication(&self, req: S3Request<s3s::dto::DeleteBucketReplicationInput>) -> S3Result<S3Response<s3s::dto::DeleteBucketReplicationOutput>> {
        self.inner.delete_bucket_replication(req).await
    }
    async fn delete_bucket_tagging(&self, req: S3Request<s3s::dto::DeleteBucketTaggingInput>) -> S3Result<S3Response<s3s::dto::DeleteBucketTaggingOutput>> {
        self.inner.delete_bucket_tagging(req).await
    }
    async fn delete_bucket_website(&self, req: S3Request<s3s::dto::DeleteBucketWebsiteInput>) -> S3Result<S3Response<s3s::dto::DeleteBucketWebsiteOutput>> {
        self.inner.delete_bucket_website(req).await
    }
    async fn delete_object_tagging(&self, req: S3Request<s3s::dto::DeleteObjectTaggingInput>) -> S3Result<S3Response<s3s::dto::DeleteObjectTaggingOutput>> {
        self.inner.delete_object_tagging(req).await
    }
    async fn delete_public_access_block(&self, req: S3Request<s3s::dto::DeletePublicAccessBlockInput>) -> S3Result<S3Response<s3s::dto::DeletePublicAccessBlockOutput>> {
        self.inner.delete_public_access_block(req).await
    }
    async fn get_bucket_accelerate_configuration(&self, req: S3Request<s3s::dto::GetBucketAccelerateConfigurationInput>) -> S3Result<S3Response<s3s::dto::GetBucketAccelerateConfigurationOutput>> {
        self.inner.get_bucket_accelerate_configuration(req).await
    }
    async fn get_bucket_acl(&self, req: S3Request<s3s::dto::GetBucketAclInput>) -> S3Result<S3Response<s3s::dto::GetBucketAclOutput>> {
        self.inner.get_bucket_acl(req).await
    }
    async fn get_bucket_analytics_configuration(&self, req: S3Request<s3s::dto::GetBucketAnalyticsConfigurationInput>) -> S3Result<S3Response<s3s::dto::GetBucketAnalyticsConfigurationOutput>> {
        self.inner.get_bucket_analytics_configuration(req).await
    }
    async fn get_bucket_cors(&self, req: S3Request<s3s::dto::GetBucketCorsInput>) -> S3Result<S3Response<s3s::dto::GetBucketCorsOutput>> {
        self.inner.get_bucket_cors(req).await
    }
    async fn get_bucket_encryption(&self, req: S3Request<s3s::dto::GetBucketEncryptionInput>) -> S3Result<S3Response<s3s::dto::GetBucketEncryptionOutput>> {
        self.inner.get_bucket_encryption(req).await
    }
    async fn get_bucket_intelligent_tiering_configuration(&self, req: S3Request<s3s::dto::GetBucketIntelligentTieringConfigurationInput>) -> S3Result<S3Response<s3s::dto::GetBucketIntelligentTieringConfigurationOutput>> {
        self.inner.get_bucket_intelligent_tiering_configuration(req).await
    }
    async fn get_bucket_inventory_configuration(&self, req: S3Request<s3s::dto::GetBucketInventoryConfigurationInput>) -> S3Result<S3Response<s3s::dto::GetBucketInventoryConfigurationOutput>> {
        self.inner.get_bucket_inventory_configuration(req).await
    }
    async fn get_bucket_lifecycle_configuration(&self, req: S3Request<s3s::dto::GetBucketLifecycleConfigurationInput>) -> S3Result<S3Response<s3s::dto::GetBucketLifecycleConfigurationOutput>> {
        self.inner.get_bucket_lifecycle_configuration(req).await
    }
    async fn get_bucket_location(&self, req: S3Request<s3s::dto::GetBucketLocationInput>) -> S3Result<S3Response<s3s::dto::GetBucketLocationOutput>> {
        self.inner.get_bucket_location(req).await
    }
    async fn get_bucket_logging(&self, req: S3Request<s3s::dto::GetBucketLoggingInput>) -> S3Result<S3Response<s3s::dto::GetBucketLoggingOutput>> {
        self.inner.get_bucket_logging(req).await
    }
    async fn get_bucket_metadata_table_configuration(&self, req: S3Request<s3s::dto::GetBucketMetadataTableConfigurationInput>) -> S3Result<S3Response<s3s::dto::GetBucketMetadataTableConfigurationOutput>> {
        self.inner.get_bucket_metadata_table_configuration(req).await
    }
    async fn get_bucket_metrics_configuration(&self, req: S3Request<s3s::dto::GetBucketMetricsConfigurationInput>) -> S3Result<S3Response<s3s::dto::GetBucketMetricsConfigurationOutput>> {
        self.inner.get_bucket_metrics_configuration(req).await
    }
    async fn get_bucket_notification_configuration(&self, req: S3Request<s3s::dto::GetBucketNotificationConfigurationInput>) -> S3Result<S3Response<s3s::dto::GetBucketNotificationConfigurationOutput>> {
        self.inner.get_bucket_notification_configuration(req).await
    }
    async fn get_bucket_ownership_controls(&self, req: S3Request<s3s::dto::GetBucketOwnershipControlsInput>) -> S3Result<S3Response<s3s::dto::GetBucketOwnershipControlsOutput>> {
        self.inner.get_bucket_ownership_controls(req).await
    }
    async fn get_bucket_policy(&self, req: S3Request<s3s::dto::GetBucketPolicyInput>) -> S3Result<S3Response<s3s::dto::GetBucketPolicyOutput>> {
        self.inner.get_bucket_policy(req).await
    }
    async fn get_bucket_policy_status(&self, req: S3Request<s3s::dto::GetBucketPolicyStatusInput>) -> S3Result<S3Response<s3s::dto::GetBucketPolicyStatusOutput>> {
        self.inner.get_bucket_policy_status(req).await
    }
    async fn get_bucket_replication(&self, req: S3Request<s3s::dto::GetBucketReplicationInput>) -> S3Result<S3Response<s3s::dto::GetBucketReplicationOutput>> {
        self.inner.get_bucket_replication(req).await
    }
    async fn get_bucket_request_payment(&self, req: S3Request<s3s::dto::GetBucketRequestPaymentInput>) -> S3Result<S3Response<s3s::dto::GetBucketRequestPaymentOutput>> {
        self.inner.get_bucket_request_payment(req).await
    }
    async fn get_bucket_tagging(&self, req: S3Request<s3s::dto::GetBucketTaggingInput>) -> S3Result<S3Response<s3s::dto::GetBucketTaggingOutput>> {
        self.inner.get_bucket_tagging(req).await
    }
    async fn get_bucket_versioning(&self, req: S3Request<s3s::dto::GetBucketVersioningInput>) -> S3Result<S3Response<s3s::dto::GetBucketVersioningOutput>> {
        self.inner.get_bucket_versioning(req).await
    }
    async fn get_bucket_website(&self, req: S3Request<s3s::dto::GetBucketWebsiteInput>) -> S3Result<S3Response<s3s::dto::GetBucketWebsiteOutput>> {
        self.inner.get_bucket_website(req).await
    }
    async fn get_object_acl(&self, req: S3Request<s3s::dto::GetObjectAclInput>) -> S3Result<S3Response<s3s::dto::GetObjectAclOutput>> {
        self.inner.get_object_acl(req).await
    }
    async fn get_object_attributes(&self, req: S3Request<s3s::dto::GetObjectAttributesInput>) -> S3Result<S3Response<s3s::dto::GetObjectAttributesOutput>> {
        self.inner.get_object_attributes(req).await
    }
    async fn get_object_legal_hold(&self, req: S3Request<s3s::dto::GetObjectLegalHoldInput>) -> S3Result<S3Response<s3s::dto::GetObjectLegalHoldOutput>> {
        self.inner.get_object_legal_hold(req).await
    }
    async fn get_object_lock_configuration(&self, req: S3Request<s3s::dto::GetObjectLockConfigurationInput>) -> S3Result<S3Response<s3s::dto::GetObjectLockConfigurationOutput>> {
        self.inner.get_object_lock_configuration(req).await
    }
    async fn get_object_retention(&self, req: S3Request<s3s::dto::GetObjectRetentionInput>) -> S3Result<S3Response<s3s::dto::GetObjectRetentionOutput>> {
        self.inner.get_object_retention(req).await
    }
    async fn get_object_tagging(&self, req: S3Request<s3s::dto::GetObjectTaggingInput>) -> S3Result<S3Response<s3s::dto::GetObjectTaggingOutput>> {
        self.inner.get_object_tagging(req).await
    }
    async fn get_object_torrent(&self, req: S3Request<s3s::dto::GetObjectTorrentInput>) -> S3Result<S3Response<s3s::dto::GetObjectTorrentOutput>> {
        self.inner.get_object_torrent(req).await
    }
    async fn get_public_access_block(&self, req: S3Request<s3s::dto::GetPublicAccessBlockInput>) -> S3Result<S3Response<s3s::dto::GetPublicAccessBlockOutput>> {
        self.inner.get_public_access_block(req).await
    }
    async fn head_bucket(&self, req: S3Request<s3s::dto::HeadBucketInput>) -> S3Result<S3Response<s3s::dto::HeadBucketOutput>> {
        self.inner.head_bucket(req).await
    }
    async fn list_bucket_analytics_configurations(&self, req: S3Request<s3s::dto::ListBucketAnalyticsConfigurationsInput>) -> S3Result<S3Response<s3s::dto::ListBucketAnalyticsConfigurationsOutput>> {
        self.inner.list_bucket_analytics_configurations(req).await
    }
    async fn list_bucket_intelligent_tiering_configurations(&self, req: S3Request<s3s::dto::ListBucketIntelligentTieringConfigurationsInput>) -> S3Result<S3Response<s3s::dto::ListBucketIntelligentTieringConfigurationsOutput>> {
        self.inner.list_bucket_intelligent_tiering_configurations(req).await
    }
    async fn list_bucket_inventory_configurations(&self, req: S3Request<s3s::dto::ListBucketInventoryConfigurationsInput>) -> S3Result<S3Response<s3s::dto::ListBucketInventoryConfigurationsOutput>> {
        self.inner.list_bucket_inventory_configurations(req).await
    }
    async fn list_bucket_metrics_configurations(&self, req: S3Request<s3s::dto::ListBucketMetricsConfigurationsInput>) -> S3Result<S3Response<s3s::dto::ListBucketMetricsConfigurationsOutput>> {
        self.inner.list_bucket_metrics_configurations(req).await
    }
    async fn list_buckets(&self, req: S3Request<s3s::dto::ListBucketsInput>) -> S3Result<S3Response<s3s::dto::ListBucketsOutput>> {
        self.inner.list_buckets(req).await
    }
    async fn list_directory_buckets(&self, req: S3Request<s3s::dto::ListDirectoryBucketsInput>) -> S3Result<S3Response<s3s::dto::ListDirectoryBucketsOutput>> {
        self.inner.list_directory_buckets(req).await
    }
    async fn list_multipart_uploads(&self, req: S3Request<s3s::dto::ListMultipartUploadsInput>) -> S3Result<S3Response<s3s::dto::ListMultipartUploadsOutput>> {
        self.inner.list_multipart_uploads(req).await
    }
    async fn list_object_versions(&self, req: S3Request<s3s::dto::ListObjectVersionsInput>) -> S3Result<S3Response<s3s::dto::ListObjectVersionsOutput>> {
        self.inner.list_object_versions(req).await
    }
    async fn list_objects(&self, req: S3Request<s3s::dto::ListObjectsInput>) -> S3Result<S3Response<s3s::dto::ListObjectsOutput>> {
        self.inner.list_objects(req).await
    }
    async fn list_parts(&self, req: S3Request<s3s::dto::ListPartsInput>) -> S3Result<S3Response<s3s::dto::ListPartsOutput>> {
        self.inner.list_parts(req).await
    }
    async fn put_bucket_accelerate_configuration(&self, req: S3Request<s3s::dto::PutBucketAccelerateConfigurationInput>) -> S3Result<S3Response<s3s::dto::PutBucketAccelerateConfigurationOutput>> {
        self.inner.put_bucket_accelerate_configuration(req).await
    }
    async fn put_bucket_acl(&self, req: S3Request<s3s::dto::PutBucketAclInput>) -> S3Result<S3Response<s3s::dto::PutBucketAclOutput>> {
        self.inner.put_bucket_acl(req).await
    }
    async fn put_bucket_analytics_configuration(&self, req: S3Request<s3s::dto::PutBucketAnalyticsConfigurationInput>) -> S3Result<S3Response<s3s::dto::PutBucketAnalyticsConfigurationOutput>> {
        self.inner.put_bucket_analytics_configuration(req).await
    }
    async fn put_bucket_cors(&self, req: S3Request<s3s::dto::PutBucketCorsInput>) -> S3Result<S3Response<s3s::dto::PutBucketCorsOutput>> {
        self.inner.put_bucket_cors(req).await
    }
    async fn put_bucket_encryption(&self, req: S3Request<s3s::dto::PutBucketEncryptionInput>) -> S3Result<S3Response<s3s::dto::PutBucketEncryptionOutput>> {
        self.inner.put_bucket_encryption(req).await
    }
    async fn put_bucket_intelligent_tiering_configuration(&self, req: S3Request<s3s::dto::PutBucketIntelligentTieringConfigurationInput>) -> S3Result<S3Response<s3s::dto::PutBucketIntelligentTieringConfigurationOutput>> {
        self.inner.put_bucket_intelligent_tiering_configuration(req).await
    }
    async fn put_bucket_inventory_configuration(&self, req: S3Request<s3s::dto::PutBucketInventoryConfigurationInput>) -> S3Result<S3Response<s3s::dto::PutBucketInventoryConfigurationOutput>> {
        self.inner.put_bucket_inventory_configuration(req).await
    }
    async fn put_bucket_lifecycle_configuration(&self, req: S3Request<s3s::dto::PutBucketLifecycleConfigurationInput>) -> S3Result<S3Response<s3s::dto::PutBucketLifecycleConfigurationOutput>> {
        self.inner.put_bucket_lifecycle_configuration(req).await
    }
    async fn put_bucket_logging(&self, req: S3Request<s3s::dto::PutBucketLoggingInput>) -> S3Result<S3Response<s3s::dto::PutBucketLoggingOutput>> {
        self.inner.put_bucket_logging(req).await
    }
    async fn put_bucket_metrics_configuration(&self, req: S3Request<s3s::dto::PutBucketMetricsConfigurationInput>) -> S3Result<S3Response<s3s::dto::PutBucketMetricsConfigurationOutput>> {
        self.inner.put_bucket_metrics_configuration(req).await
    }
    async fn put_bucket_notification_configuration(&self, req: S3Request<s3s::dto::PutBucketNotificationConfigurationInput>) -> S3Result<S3Response<s3s::dto::PutBucketNotificationConfigurationOutput>> {
        self.inner.put_bucket_notification_configuration(req).await
    }
    async fn put_bucket_ownership_controls(&self, req: S3Request<s3s::dto::PutBucketOwnershipControlsInput>) -> S3Result<S3Response<s3s::dto::PutBucketOwnershipControlsOutput>> {
        self.inner.put_bucket_ownership_controls(req).await
    }
    async fn put_bucket_policy(&self, req: S3Request<s3s::dto::PutBucketPolicyInput>) -> S3Result<S3Response<s3s::dto::PutBucketPolicyOutput>> {
        self.inner.put_bucket_policy(req).await
    }
    async fn put_bucket_replication(&self, req: S3Request<s3s::dto::PutBucketReplicationInput>) -> S3Result<S3Response<s3s::dto::PutBucketReplicationOutput>> {
        self.inner.put_bucket_replication(req).await
    }
    async fn put_bucket_request_payment(&self, req: S3Request<s3s::dto::PutBucketRequestPaymentInput>) -> S3Result<S3Response<s3s::dto::PutBucketRequestPaymentOutput>> {
        self.inner.put_bucket_request_payment(req).await
    }
    async fn put_bucket_tagging(&self, req: S3Request<s3s::dto::PutBucketTaggingInput>) -> S3Result<S3Response<s3s::dto::PutBucketTaggingOutput>> {
        self.inner.put_bucket_tagging(req).await
    }
    async fn put_bucket_versioning(&self, req: S3Request<s3s::dto::PutBucketVersioningInput>) -> S3Result<S3Response<s3s::dto::PutBucketVersioningOutput>> {
        self.inner.put_bucket_versioning(req).await
    }
    async fn put_bucket_website(&self, req: S3Request<s3s::dto::PutBucketWebsiteInput>) -> S3Result<S3Response<s3s::dto::PutBucketWebsiteOutput>> {
        self.inner.put_bucket_website(req).await
    }
    async fn put_object_acl(&self, req: S3Request<s3s::dto::PutObjectAclInput>) -> S3Result<S3Response<s3s::dto::PutObjectAclOutput>> {
        self.inner.put_object_acl(req).await
    }
    async fn put_object_legal_hold(&self, req: S3Request<s3s::dto::PutObjectLegalHoldInput>) -> S3Result<S3Response<s3s::dto::PutObjectLegalHoldOutput>> {
        self.inner.put_object_legal_hold(req).await
    }
    async fn put_object_lock_configuration(&self, req: S3Request<s3s::dto::PutObjectLockConfigurationInput>) -> S3Result<S3Response<s3s::dto::PutObjectLockConfigurationOutput>> {
        self.inner.put_object_lock_configuration(req).await
    }
    async fn put_object_retention(&self, req: S3Request<s3s::dto::PutObjectRetentionInput>) -> S3Result<S3Response<s3s::dto::PutObjectRetentionOutput>> {
        self.inner.put_object_retention(req).await
    }
    async fn put_object_tagging(&self, req: S3Request<s3s::dto::PutObjectTaggingInput>) -> S3Result<S3Response<s3s::dto::PutObjectTaggingOutput>> {
        self.inner.put_object_tagging(req).await
    }
    async fn put_public_access_block(&self, req: S3Request<s3s::dto::PutPublicAccessBlockInput>) -> S3Result<S3Response<s3s::dto::PutPublicAccessBlockOutput>> {
        self.inner.put_public_access_block(req).await
    }
    async fn restore_object(&self, req: S3Request<s3s::dto::RestoreObjectInput>) -> S3Result<S3Response<s3s::dto::RestoreObjectOutput>> {
        self.inner.restore_object(req).await
    }
    async fn select_object_content(&self, req: S3Request<s3s::dto::SelectObjectContentInput>) -> S3Result<S3Response<s3s::dto::SelectObjectContentOutput>> {
        self.inner.select_object_content(req).await
    }
    async fn upload_part(&self, req: S3Request<s3s::dto::UploadPartInput>) -> S3Result<S3Response<s3s::dto::UploadPartOutput>> {
        self.inner.upload_part(req).await
    }
    async fn upload_part_copy(&self, req: S3Request<s3s::dto::UploadPartCopyInput>) -> S3Result<S3Response<s3s::dto::UploadPartCopyOutput>> {
        self.inner.upload_part_copy(req).await
    }
    async fn write_get_object_response(&self, req: S3Request<s3s::dto::WriteGetObjectResponseInput>) -> S3Result<S3Response<s3s::dto::WriteGetObjectResponseOutput>> {
        self.inner.write_get_object_response(req).await
    }
}
