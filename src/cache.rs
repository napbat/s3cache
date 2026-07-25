//! Caching layer over the upstream `s3s_aws::Proxy`.
//!
//! Every client request funnels through this proxy, so it sees every write. That lets
//! it answer **LIST** from an in-memory key index (LISTs are R2's expensive Class-A
//! tier) and serve small GET/HEAD bodies from an LRU, while writes forward through to
//! the upstream — which stays the authority for conditional (OCC) writes.
//!
//! Correctness rests on one property: this proxy is the *only* path to the bucket. The
//! index warms lazily — LISTs pass through until a bucket's background full-LIST sync
//! completes ([`CachingProxy::is_synced`] gates the flip), then observed writes keep it
//! current. The body cache is separately lazy: populate on miss, invalidate on write.
//! Cross-node coherence rides the gossip write feed (see [`crate::sync`]): peers' writes
//! fold into the index and invalidate local copies; strict reads barrier on feed heads.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock};
use std::time::SystemTime;

use async_trait::async_trait;
use bytes::Bytes;
use s3s::dto::{
    CompleteMultipartUploadInput, CompleteMultipartUploadOutput, CopyObjectInput, CopyObjectOutput,
    DeleteObjectInput, DeleteObjectOutput, DeleteObjectsInput, DeleteObjectsOutput, GetObjectInput,
    GetObjectOutput, HeadObjectInput, HeadObjectOutput, ListObjectsV2Input, ListObjectsV2Output,
    PutObjectInput, PutObjectOutput, StreamingBlob,
};
use s3s::{S3, S3Request, S3Response, S3Result};
use tracing::info;

use crate::index::{
    BucketState, apply_del, apply_put, list_objects_v2_from_index, sync_bucket_into,
};
use crate::metrics::Metrics;
use crate::sync::{READ_TOKEN_HEADER, WRITE_TOKEN_HEADER, WriteSync};
use crate::tier::{self, CachedObject, TieredCache, WarmPair};
use http::{HeaderMap, HeaderName, HeaderValue};

/// Sizing for the object cache, passed to [`CachingProxy::new`].
#[derive(Clone, Copy)]
pub(crate) struct CacheConfig {
    /// Total hot (heap) tier capacity in bytes.
    pub(crate) cache_bytes: u64,
    /// Objects larger than this are never cached.
    pub(crate) max_obj_bytes: usize,
}

/// Where a read may be served after the barrier: node-local state, or the
/// origin — when a client-presented session token could not be verified in
/// time, strictness is honoured at origin cost instead of silently dropped
/// (the origin is never stale).
#[derive(Clone, Copy, PartialEq, Eq)]
enum ReadRoute {
    Local,
    Origin,
}

/// How long a strict read waits for the freshness barrier before serving
/// current state anyway (degrading to eventual rather than hanging).
const READ_BARRIER_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);

/// S3 service that caches LIST (from an in-memory index) and small GET/HEAD bodies (the
/// hot/warm/cold [`TieredCache`]) in front of an upstream `s3s_aws::Proxy`, forwarding
/// every write.
pub(crate) struct CachingProxy {
    inner: s3s_aws::Proxy,
    /// Direct client used only for the background full LIST warm-up sync.
    client: aws_sdk_s3::Client,
    /// The LIST index, `Arc` so the background warm-up task shares it with the serving
    /// proxy (the service takes the proxy by value, so the sync can't borrow `self`).
    state: Arc<RwLock<HashMap<String, BucketState>>>,
    /// Object-body cache: hot heap in front of an optional node-local disk tier.
    obj_cache: TieredCache,
    /// Objects larger than this are never cached (segments stream straight through).
    max_obj_bytes: usize,
    /// Gossip write feed — the whole cross-node coherence layer, when
    /// configured (see [`crate::sync`]).
    sync: Option<Arc<WriteSync>>,
    metrics: Arc<Metrics>,
}

impl CachingProxy {
    /// Wire up the proxy. `cfg` sizes the hot tier; `warm` is the optional node-local
    /// disk tier and `sync` the gossip write feed (both built by the caller). `metrics`
    /// is shared so the tiers, the feed, and the stats task all report into it.
    pub(crate) fn new(
        inner: s3s_aws::Proxy,
        client: aws_sdk_s3::Client,
        cfg: CacheConfig,
        warm: Option<WarmPair>,
        sync: Option<Arc<WriteSync>>,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            inner,
            client,
            state: Arc::new(RwLock::new(HashMap::new())),
            obj_cache: TieredCache::new(cfg.cache_bytes, warm, metrics.clone()),
            max_obj_bytes: cfg.max_obj_bytes,
            sync,
            metrics,
        }
    }

    /// The shared LIST index, for the gossip apply loop.
    pub(crate) fn index_state(&self) -> Arc<RwLock<HashMap<String, BucketState>>> {
        self.state.clone()
    }

    /// The gap remediation for the apply loop: reset every bucket to
    /// passthrough (unsynced LISTs are always correct) and re-warm the
    /// configured ones from the origin — the authority the index caches.
    pub(crate) fn gap_resync_handle(&self, buckets: Vec<String>) -> Arc<dyn Fn() + Send + Sync> {
        let client = self.client.clone();
        let state = self.state.clone();
        Arc::new(move || {
            {
                let mut g = state.write().unwrap();
                for bucket_state in g.values_mut() {
                    *bucket_state = BucketState::default();
                }
            }
            let (client, state, buckets) = (client.clone(), state.clone(), buckets.clone());
            tokio::spawn(async move {
                for bucket in buckets {
                    if let Err(e) = sync_bucket_into(&client, &state, &bucket).await {
                        tracing::warn!(
                            "gap resync of `{bucket}` failed (staying passthrough): {e}"
                        );
                    }
                }
            });
        })
    }

    /// A handle to this node's local tiers, for the gossip apply loop.
    pub(crate) fn local_cache(&self) -> crate::tier::LocalCache {
        self.obj_cache.local()
    }

    /// Freshness barrier: before serving a read from node-local state (the LIST index,
    /// or a hot/disk body copy), wait until every peer's currently-advertised write-feed
    /// head has been applied locally, so a peer's just-completed write is not read stale.
    /// Freshness is bounded by one push/gossip hop (see [`crate::sync`]); degrades to
    /// serving current state on timeout. No-op without gossip (single-node is strict).
    async fn read_barrier(&self, headers: &HeaderMap) -> ReadRoute {
        let Some(sync) = &self.sync else {
            return ReadRoute::Local; // single node: the sole writer is strict
        };
        if !sync.await_fresh(READ_BARRIER_TIMEOUT).await {
            tracing::debug!("freshness barrier timed out; serving current state");
        }
        // A client-echoed write token upgrades the read to strict
        // read-after-write for that write, independent of propagation timing.
        // Unverifiable-in-time tokens route the read to the origin: slower,
        // never stale — the client asked for strict and gets it.
        if let Some(token) = headers.get(READ_TOKEN_HEADER).and_then(|v| v.to_str().ok())
            && !sync.reached_token(token, READ_BARRIER_TIMEOUT).await
        {
            tracing::debug!("read token not satisfied in time; serving via origin");
            return ReadRoute::Origin;
        }
        ReadRoute::Local
    }

    /// Attach a write's session token to a response, when coherence is on.
    fn attach_token(headers: &mut HeaderMap, token: Option<String>) {
        if let Some(token) = token
            && let Ok(value) = HeaderValue::from_str(&token)
        {
            headers.insert(HeaderName::from_static(WRITE_TOKEN_HEADER), value);
        }
    }

    pub(crate) fn metrics(&self) -> Arc<Metrics> {
        self.metrics.clone()
    }

    /// Record a durable put: fold it into the local index (LWW, same rule the
    /// apply loop uses for peers) and advertise it over the write feed.
    /// Returns the write's session token when coherence is on.
    async fn record_put(&self, bucket: &str, key: &str, size: i64) -> Option<String> {
        let ts = SystemTime::now();
        if apply_put(&self.state, bucket, key, size, ts) {
            self.metrics.writes_indexed.fetch_add(1, Ordering::Relaxed);
        }
        match &self.sync {
            Some(sync) => Some(sync.publish_put(bucket, key, size, ts, &self.metrics).await),
            None => None,
        }
    }

    /// Record a durable delete: tombstone + remove locally, advertise to
    /// peers. Returns the write's session token when coherence is on.
    async fn record_del(&self, bucket: &str, key: &str) -> Option<String> {
        let ts = SystemTime::now();
        apply_del(&self.state, bucket, key, ts);
        match &self.sync {
            Some(sync) => Some(sync.publish_del(bucket, key, ts, &self.metrics).await),
            None => None,
        }
    }

    /// Warm each bucket's LIST index in the background so startup stays instant and
    /// independent of bucket size. Until a bucket's full sync finishes its LISTs pass
    /// through to the upstream (always correct), then flip to index-served.
    pub(crate) fn spawn_background_sync(&self, buckets: Vec<String>) {
        let client = self.client.clone();
        let state = self.state.clone();
        tokio::spawn(async move {
            for bucket in buckets {
                match sync_bucket_into(&client, &state, &bucket).await {
                    Ok(n) => info!("warmed LIST index for `{bucket}`: {n} keys"),
                    Err(e) => {
                        tracing::warn!(
                            "background sync of `{bucket}` failed (staying passthrough): {e}"
                        );
                    }
                }
            }
        });
    }

    /// Index a size observed on the READ path (a successful origin GET): the
    /// key provably exists at the origin now, so LWW at `now` is correct and
    /// nothing is advertised (peers learn real writes from their writers).
    fn index_insert(&self, bucket: &str, key: &str, size: i64) {
        if apply_put(&self.state, bucket, key, size, SystemTime::now()) {
            self.metrics.writes_indexed.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// The indexed size of a key, if this proxy has seen it (write-through or
    /// LIST warm-up). Drives the range-promotion decision without a HEAD.
    fn index_size(&self, bucket: &str, key: &str) -> Option<i64> {
        self.state
            .read()
            .unwrap()
            .get(bucket)
            .and_then(|b| b.keys.get(key))
            .map(|e| e.size)
    }

    /// The upstream's actual size of a key (one HEAD via the direct client).
    /// Used where the write path doesn't carry the size (multipart complete,
    /// copy) — indexing those at a placeholder poisons range promotion.
    async fn upstream_size(&self, bucket: &str, key: &str) -> Option<i64> {
        self.client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .ok()?
            .content_length()
    }

    /// Serve an int-range GET by promoting the whole object into the tiered cache (one
    /// upstream fetch, singleflighted when hot is active) and slicing it. `Some` when served — a
    /// slice, or `InvalidRange` past EOF; `None` when the promote was refused/failed and
    /// the caller should stream the range through (the loader gets its own range-cleared
    /// request so the caller's survives for that fallback). The index size can lie, so the
    /// loader trusts the upstream's Content-Length, refuses oversize bodies before
    /// buffering, and writes the real size back on reject so the next range bypasses.
    async fn promote_range(
        &self,
        ckey: &(String, String),
        req: &S3Request<GetObjectInput>,
        first: u64,
        last: Option<u64>,
    ) -> Option<S3Result<S3Response<GetObjectOutput>>> {
        let whole = S3Request {
            input: {
                let mut i = req.input.clone();
                i.range = None;
                i
            },
            method: req.method.clone(),
            uri: req.uri.clone(),
            headers: req.headers.clone(),
            extensions: req.extensions.clone(),
            credentials: req.credentials.clone(),
            region: req.region.clone(),
            service: req.service.clone(),
            trailing_headers: None,
        };
        let cap = self.max_obj_bytes;
        let inner = &self.inner;
        let origin = async move {
            let mut resp = inner.get_object(whole).await.map_err(|e| e.to_string())?;
            let declared = resp.output.content_length.unwrap_or(-1);
            if declared < 0 || usize::try_from(declared).unwrap_or(usize::MAX) > cap {
                return Err(format!("oversize {declared}"));
            }
            let body = match resp.output.body.take() {
                Some(b) => tier::buffer_body(b, cap)
                    .await
                    .ok_or_else(|| format!("oversize {declared} (body past declared length)"))?,
                None => Bytes::new(),
            };
            Ok::<_, String>(Arc::new(CachedObject::from_get(&resp.output, body)))
        };
        // Box the promotion future: it carries a full cloned request + buffered body,
        // large enough that keeping it on the stack trips clippy's `large_futures`.
        let fetched = Box::pin(self.obj_cache.get_or_fetch(ckey, origin)).await;
        match fetched {
            Ok(obj) => {
                self.metrics.range_promote.fetch_add(1, Ordering::Relaxed);
                Some(obj.to_get_range(first, last).map_or_else(
                    || {
                        Err(s3s::s3_error!(
                            InvalidRange,
                            "range start past end of object"
                        ))
                    },
                    |out| Ok(S3Response::new(out)),
                ))
            }
            Err(e) => {
                // Self-heal a lying index entry so this object stops
                // re-attempting promotion, then let the caller serve upstream.
                if let Some(sz) = e
                    .strip_prefix("oversize ")
                    .and_then(|r| r.split(' ').next())
                    .and_then(|n| n.parse::<i64>().ok())
                    && sz >= 0
                {
                    self.index_insert(&ckey.0, &ckey.1, sz);
                }
                self.metrics
                    .range_promote_reject
                    .fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    "range promote of {}/{} failed ({e}); falling back to passthrough",
                    ckey.0,
                    ckey.1
                );
                None
            }
        }
    }

    fn is_synced(&self, bucket: &str) -> bool {
        self.state
            .read()
            .unwrap()
            .get(bucket)
            .is_some_and(|b| b.synced)
    }

    /// Build a `ListObjectsV2` response from this bucket's index. See
    /// [`list_objects_v2_from_index`] for the algorithm.
    fn list_from_index(&self, inp: &ListObjectsV2Input) -> ListObjectsV2Output {
        let g = self.state.read().unwrap();
        list_objects_v2_from_index(g.get(inp.bucket.as_str()).map(|b| &b.keys), inp)
    }
}

#[async_trait]
impl s3s::S3 for CachingProxy {
    // LIST served from the index when the bucket is synced; else passthrough.
    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        if self.is_synced(req.input.bucket.as_str())
            && self.read_barrier(&req.headers).await == ReadRoute::Local
        {
            self.metrics.list_from_index.fetch_add(1, Ordering::Relaxed);
            let out = self.list_from_index(&req.input);
            return Ok(S3Response::new(out));
        }
        self.metrics
            .list_passthrough
            .fetch_add(1, Ordering::Relaxed);
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
        let mut resp = self.inner.put_object(req).await?;
        let token = self.record_put(&bucket, &key, size).await;
        self.obj_cache.invalidate(&(bucket, key)).await;
        Self::attach_token(&mut resp.headers, token);
        Ok(resp)
    }

    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        let bucket = req.input.bucket.clone();
        let key = req.input.key.clone();
        let mut resp = self.inner.delete_object(req).await?;
        let token = self.record_del(&bucket, &key).await;
        self.obj_cache.invalidate(&(bucket, key)).await;
        Self::attach_token(&mut resp.headers, token);
        Ok(resp)
    }

    async fn delete_objects(
        &self,
        req: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        let bucket = req.input.bucket.clone();
        let keys: Vec<String> = req
            .input
            .delete
            .objects
            .iter()
            .map(|o| o.key.clone())
            .collect();
        let mut resp = self.inner.delete_objects(req).await?;
        let mut token = None;
        for k in keys {
            // Keep the newest token: it covers the whole batch (one writer,
            // ordered feed).
            token = self.record_del(&bucket, &k).await.or(token);
            self.obj_cache.invalidate(&(bucket.clone(), k)).await;
        }
        Self::attach_token(&mut resp.headers, token);
        Ok(resp)
    }

    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        let bucket = req.input.bucket.clone();
        let key = req.input.key.clone();
        let mut resp = self.inner.complete_multipart_upload(req).await?;
        // Multipart is how the big objects arrive, and indexing them at a
        // placeholder size poisoned the range-promotion decision (a "0-byte"
        // entry promoted a multi-GB fetch). One HEAD learns the real size.
        let size = self.upstream_size(&bucket, &key).await.unwrap_or(0);
        let token = self.record_put(&bucket, &key, size).await;
        self.obj_cache.invalidate(&(bucket, key)).await;
        Self::attach_token(&mut resp.headers, token);
        Ok(resp)
    }

    async fn copy_object(
        &self,
        req: S3Request<CopyObjectInput>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        let bucket = req.input.bucket.clone();
        let key = req.input.key.clone();
        let mut resp = self.inner.copy_object(req).await?;
        let size = self.upstream_size(&bucket, &key).await.unwrap_or(0);
        let token = self.record_put(&bucket, &key, size).await;
        self.obj_cache.invalidate(&(bucket, key)).await;
        Self::attach_token(&mut resp.headers, token);
        Ok(resp)
    }

    // GET: cacheable (no part/conditional) small objects are served from the tiered
    // cache; a miss buffers the body and caches it. Ranged reads of cacheable-size
    // objects are served by slicing the cached whole object, promoted on first touch.
    // Oversized/unknown-size ranges and conditional requests stream straight through.
    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        // Requests whose response the cache can't faithfully reproduce must go to the
        // origin: a specific version, origin-computed checksums, or SSE-C (whose bytes
        // must never be served without the caller's key).
        let origin_only = req.input.version_id.is_some()
            || req.input.checksum_mode.is_some()
            || req.input.sse_customer_key.is_some();
        let unconditional = !origin_only
            && req.input.part_number.is_none()
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
        // Before any read served from a node-local hot copy, barrier so a peer's
        // overwrite is never read stale; a token-carrying read that cannot be
        // verified in time skips every local copy and streams from the origin.
        let local_ok = if cacheable || int_range.is_some() {
            self.read_barrier(&req.headers).await == ReadRoute::Local
        } else {
            true
        };
        if let Some((first, last)) = int_range {
            // Cached whole object → serve the slice locally.
            if local_ok
                && let Some(obj) = self.obj_cache.get(&ckey).await
                && let Some(out) = obj.to_get_range(first, last)
            {
                self.metrics.range_hit.fetch_add(1, Ordering::Relaxed);
                return Ok(S3Response::new(out));
            }
            // Promote when caching is on and the index says the whole object fits: one
            // upstream GET (deduped across concurrent ranges when hot is active), then
            // every range — this one included — is a slice. A refused/failed promote
            // degrades to the passthrough below, never an error.
            let small = self.index_size(&ckey.0, &ckey.1).is_some_and(|sz| {
                sz >= 0 && usize::try_from(sz).unwrap_or(usize::MAX) <= self.max_obj_bytes
            });
            if local_ok
                && small
                && let Some(resp) = self.promote_range(&ckey, &req, first, last).await
            {
                return resp;
            }
            // Big, not-yet-indexed, unverified-token, or a failed promote: stream through.
            self.metrics.get_bypass.fetch_add(1, Ordering::Relaxed);
            return self.inner.get_object(req).await;
        }
        if local_ok
            && cacheable
            && let Some(obj) = self.obj_cache.get(&ckey).await
        {
            self.metrics.get_hit.fetch_add(1, Ordering::Relaxed);
            return Ok(S3Response::new(obj.to_get()));
        }
        let mut resp = self.inner.get_object(req).await?;
        let len = resp.output.content_length.unwrap_or(-1);
        let small = len >= 0 && usize::try_from(len).unwrap_or(usize::MAX) <= self.max_obj_bytes;
        if cacheable
            && small
            && let Some(body) = resp.output.body.take()
        {
            match tier::buffer_body(body, self.max_obj_bytes).await {
                Some(bytes) => {
                    self.obj_cache
                        .insert(
                            ckey,
                            Arc::new(CachedObject::from_get(&resp.output, bytes.clone())),
                        )
                        .await;
                    self.metrics.get_miss.fetch_add(1, Ordering::Relaxed);
                    resp.output.body =
                        Some(StreamingBlob::wrap(futures::stream::once(async move {
                            Ok::<Bytes, std::io::Error>(bytes)
                        })));
                }
                None => {
                    return Err(s3s::s3_error!(
                        InternalError,
                        "s3cache: failed to buffer body"
                    ));
                }
            }
        } else {
            self.metrics.get_bypass.fetch_add(1, Ordering::Relaxed);
        }
        Ok(resp)
    }

    // HEAD served from the object cache when the body is already cached. Requests that
    // need the origin (range, part, specific version, checksums, SSE-C) pass through.
    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        let cache_eligible = req.input.range.is_none()
            && req.input.part_number.is_none()
            && req.input.version_id.is_none()
            && req.input.checksum_mode.is_none()
            && req.input.sse_customer_key.is_none();
        if cache_eligible && self.read_barrier(&req.headers).await == ReadRoute::Local {
            let ckey = (req.input.bucket.clone(), req.input.key.clone());
            if let Some(obj) = self.obj_cache.get(&ckey).await {
                self.metrics.get_hit.fetch_add(1, Ordering::Relaxed);
                return Ok(S3Response::new(obj.to_head()));
            }
        }
        self.inner.head_object(req).await
    }

    // Full S3 passthrough: every other op forwards to the upstream so any S3
    // client works, not just the cached read/write paths. Generated from the s3s S3 trait.
    async fn abort_multipart_upload(
        &self,
        req: S3Request<s3s::dto::AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<s3s::dto::AbortMultipartUploadOutput>> {
        self.inner.abort_multipart_upload(req).await
    }
    async fn create_bucket(
        &self,
        req: S3Request<s3s::dto::CreateBucketInput>,
    ) -> S3Result<S3Response<s3s::dto::CreateBucketOutput>> {
        self.inner.create_bucket(req).await
    }
    async fn create_bucket_metadata_table_configuration(
        &self,
        req: S3Request<s3s::dto::CreateBucketMetadataTableConfigurationInput>,
    ) -> S3Result<S3Response<s3s::dto::CreateBucketMetadataTableConfigurationOutput>> {
        self.inner
            .create_bucket_metadata_table_configuration(req)
            .await
    }
    async fn create_multipart_upload(
        &self,
        req: S3Request<s3s::dto::CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<s3s::dto::CreateMultipartUploadOutput>> {
        self.inner.create_multipart_upload(req).await
    }
    async fn create_session(
        &self,
        req: S3Request<s3s::dto::CreateSessionInput>,
    ) -> S3Result<S3Response<s3s::dto::CreateSessionOutput>> {
        self.inner.create_session(req).await
    }
    async fn delete_bucket(
        &self,
        req: S3Request<s3s::dto::DeleteBucketInput>,
    ) -> S3Result<S3Response<s3s::dto::DeleteBucketOutput>> {
        self.inner.delete_bucket(req).await
    }
    async fn delete_bucket_analytics_configuration(
        &self,
        req: S3Request<s3s::dto::DeleteBucketAnalyticsConfigurationInput>,
    ) -> S3Result<S3Response<s3s::dto::DeleteBucketAnalyticsConfigurationOutput>> {
        self.inner.delete_bucket_analytics_configuration(req).await
    }
    async fn delete_bucket_cors(
        &self,
        req: S3Request<s3s::dto::DeleteBucketCorsInput>,
    ) -> S3Result<S3Response<s3s::dto::DeleteBucketCorsOutput>> {
        self.inner.delete_bucket_cors(req).await
    }
    async fn delete_bucket_encryption(
        &self,
        req: S3Request<s3s::dto::DeleteBucketEncryptionInput>,
    ) -> S3Result<S3Response<s3s::dto::DeleteBucketEncryptionOutput>> {
        self.inner.delete_bucket_encryption(req).await
    }
    async fn delete_bucket_intelligent_tiering_configuration(
        &self,
        req: S3Request<s3s::dto::DeleteBucketIntelligentTieringConfigurationInput>,
    ) -> S3Result<S3Response<s3s::dto::DeleteBucketIntelligentTieringConfigurationOutput>> {
        self.inner
            .delete_bucket_intelligent_tiering_configuration(req)
            .await
    }
    async fn delete_bucket_inventory_configuration(
        &self,
        req: S3Request<s3s::dto::DeleteBucketInventoryConfigurationInput>,
    ) -> S3Result<S3Response<s3s::dto::DeleteBucketInventoryConfigurationOutput>> {
        self.inner.delete_bucket_inventory_configuration(req).await
    }
    async fn delete_bucket_lifecycle(
        &self,
        req: S3Request<s3s::dto::DeleteBucketLifecycleInput>,
    ) -> S3Result<S3Response<s3s::dto::DeleteBucketLifecycleOutput>> {
        self.inner.delete_bucket_lifecycle(req).await
    }
    async fn delete_bucket_metadata_table_configuration(
        &self,
        req: S3Request<s3s::dto::DeleteBucketMetadataTableConfigurationInput>,
    ) -> S3Result<S3Response<s3s::dto::DeleteBucketMetadataTableConfigurationOutput>> {
        self.inner
            .delete_bucket_metadata_table_configuration(req)
            .await
    }
    async fn delete_bucket_metrics_configuration(
        &self,
        req: S3Request<s3s::dto::DeleteBucketMetricsConfigurationInput>,
    ) -> S3Result<S3Response<s3s::dto::DeleteBucketMetricsConfigurationOutput>> {
        self.inner.delete_bucket_metrics_configuration(req).await
    }
    async fn delete_bucket_ownership_controls(
        &self,
        req: S3Request<s3s::dto::DeleteBucketOwnershipControlsInput>,
    ) -> S3Result<S3Response<s3s::dto::DeleteBucketOwnershipControlsOutput>> {
        self.inner.delete_bucket_ownership_controls(req).await
    }
    async fn delete_bucket_policy(
        &self,
        req: S3Request<s3s::dto::DeleteBucketPolicyInput>,
    ) -> S3Result<S3Response<s3s::dto::DeleteBucketPolicyOutput>> {
        self.inner.delete_bucket_policy(req).await
    }
    async fn delete_bucket_replication(
        &self,
        req: S3Request<s3s::dto::DeleteBucketReplicationInput>,
    ) -> S3Result<S3Response<s3s::dto::DeleteBucketReplicationOutput>> {
        self.inner.delete_bucket_replication(req).await
    }
    async fn delete_bucket_tagging(
        &self,
        req: S3Request<s3s::dto::DeleteBucketTaggingInput>,
    ) -> S3Result<S3Response<s3s::dto::DeleteBucketTaggingOutput>> {
        self.inner.delete_bucket_tagging(req).await
    }
    async fn delete_bucket_website(
        &self,
        req: S3Request<s3s::dto::DeleteBucketWebsiteInput>,
    ) -> S3Result<S3Response<s3s::dto::DeleteBucketWebsiteOutput>> {
        self.inner.delete_bucket_website(req).await
    }
    async fn delete_object_tagging(
        &self,
        req: S3Request<s3s::dto::DeleteObjectTaggingInput>,
    ) -> S3Result<S3Response<s3s::dto::DeleteObjectTaggingOutput>> {
        self.inner.delete_object_tagging(req).await
    }
    async fn delete_public_access_block(
        &self,
        req: S3Request<s3s::dto::DeletePublicAccessBlockInput>,
    ) -> S3Result<S3Response<s3s::dto::DeletePublicAccessBlockOutput>> {
        self.inner.delete_public_access_block(req).await
    }
    async fn get_bucket_accelerate_configuration(
        &self,
        req: S3Request<s3s::dto::GetBucketAccelerateConfigurationInput>,
    ) -> S3Result<S3Response<s3s::dto::GetBucketAccelerateConfigurationOutput>> {
        self.inner.get_bucket_accelerate_configuration(req).await
    }
    async fn get_bucket_acl(
        &self,
        req: S3Request<s3s::dto::GetBucketAclInput>,
    ) -> S3Result<S3Response<s3s::dto::GetBucketAclOutput>> {
        self.inner.get_bucket_acl(req).await
    }
    async fn get_bucket_analytics_configuration(
        &self,
        req: S3Request<s3s::dto::GetBucketAnalyticsConfigurationInput>,
    ) -> S3Result<S3Response<s3s::dto::GetBucketAnalyticsConfigurationOutput>> {
        self.inner.get_bucket_analytics_configuration(req).await
    }
    async fn get_bucket_cors(
        &self,
        req: S3Request<s3s::dto::GetBucketCorsInput>,
    ) -> S3Result<S3Response<s3s::dto::GetBucketCorsOutput>> {
        self.inner.get_bucket_cors(req).await
    }
    async fn get_bucket_encryption(
        &self,
        req: S3Request<s3s::dto::GetBucketEncryptionInput>,
    ) -> S3Result<S3Response<s3s::dto::GetBucketEncryptionOutput>> {
        self.inner.get_bucket_encryption(req).await
    }
    async fn get_bucket_intelligent_tiering_configuration(
        &self,
        req: S3Request<s3s::dto::GetBucketIntelligentTieringConfigurationInput>,
    ) -> S3Result<S3Response<s3s::dto::GetBucketIntelligentTieringConfigurationOutput>> {
        self.inner
            .get_bucket_intelligent_tiering_configuration(req)
            .await
    }
    async fn get_bucket_inventory_configuration(
        &self,
        req: S3Request<s3s::dto::GetBucketInventoryConfigurationInput>,
    ) -> S3Result<S3Response<s3s::dto::GetBucketInventoryConfigurationOutput>> {
        self.inner.get_bucket_inventory_configuration(req).await
    }
    async fn get_bucket_lifecycle_configuration(
        &self,
        req: S3Request<s3s::dto::GetBucketLifecycleConfigurationInput>,
    ) -> S3Result<S3Response<s3s::dto::GetBucketLifecycleConfigurationOutput>> {
        self.inner.get_bucket_lifecycle_configuration(req).await
    }
    async fn get_bucket_location(
        &self,
        req: S3Request<s3s::dto::GetBucketLocationInput>,
    ) -> S3Result<S3Response<s3s::dto::GetBucketLocationOutput>> {
        self.inner.get_bucket_location(req).await
    }
    async fn get_bucket_logging(
        &self,
        req: S3Request<s3s::dto::GetBucketLoggingInput>,
    ) -> S3Result<S3Response<s3s::dto::GetBucketLoggingOutput>> {
        self.inner.get_bucket_logging(req).await
    }
    async fn get_bucket_metadata_table_configuration(
        &self,
        req: S3Request<s3s::dto::GetBucketMetadataTableConfigurationInput>,
    ) -> S3Result<S3Response<s3s::dto::GetBucketMetadataTableConfigurationOutput>> {
        self.inner
            .get_bucket_metadata_table_configuration(req)
            .await
    }
    async fn get_bucket_metrics_configuration(
        &self,
        req: S3Request<s3s::dto::GetBucketMetricsConfigurationInput>,
    ) -> S3Result<S3Response<s3s::dto::GetBucketMetricsConfigurationOutput>> {
        self.inner.get_bucket_metrics_configuration(req).await
    }
    async fn get_bucket_notification_configuration(
        &self,
        req: S3Request<s3s::dto::GetBucketNotificationConfigurationInput>,
    ) -> S3Result<S3Response<s3s::dto::GetBucketNotificationConfigurationOutput>> {
        self.inner.get_bucket_notification_configuration(req).await
    }
    async fn get_bucket_ownership_controls(
        &self,
        req: S3Request<s3s::dto::GetBucketOwnershipControlsInput>,
    ) -> S3Result<S3Response<s3s::dto::GetBucketOwnershipControlsOutput>> {
        self.inner.get_bucket_ownership_controls(req).await
    }
    async fn get_bucket_policy(
        &self,
        req: S3Request<s3s::dto::GetBucketPolicyInput>,
    ) -> S3Result<S3Response<s3s::dto::GetBucketPolicyOutput>> {
        self.inner.get_bucket_policy(req).await
    }
    async fn get_bucket_policy_status(
        &self,
        req: S3Request<s3s::dto::GetBucketPolicyStatusInput>,
    ) -> S3Result<S3Response<s3s::dto::GetBucketPolicyStatusOutput>> {
        self.inner.get_bucket_policy_status(req).await
    }
    async fn get_bucket_replication(
        &self,
        req: S3Request<s3s::dto::GetBucketReplicationInput>,
    ) -> S3Result<S3Response<s3s::dto::GetBucketReplicationOutput>> {
        self.inner.get_bucket_replication(req).await
    }
    async fn get_bucket_request_payment(
        &self,
        req: S3Request<s3s::dto::GetBucketRequestPaymentInput>,
    ) -> S3Result<S3Response<s3s::dto::GetBucketRequestPaymentOutput>> {
        self.inner.get_bucket_request_payment(req).await
    }
    async fn get_bucket_tagging(
        &self,
        req: S3Request<s3s::dto::GetBucketTaggingInput>,
    ) -> S3Result<S3Response<s3s::dto::GetBucketTaggingOutput>> {
        self.inner.get_bucket_tagging(req).await
    }
    async fn get_bucket_versioning(
        &self,
        req: S3Request<s3s::dto::GetBucketVersioningInput>,
    ) -> S3Result<S3Response<s3s::dto::GetBucketVersioningOutput>> {
        self.inner.get_bucket_versioning(req).await
    }
    async fn get_bucket_website(
        &self,
        req: S3Request<s3s::dto::GetBucketWebsiteInput>,
    ) -> S3Result<S3Response<s3s::dto::GetBucketWebsiteOutput>> {
        self.inner.get_bucket_website(req).await
    }
    async fn get_object_acl(
        &self,
        req: S3Request<s3s::dto::GetObjectAclInput>,
    ) -> S3Result<S3Response<s3s::dto::GetObjectAclOutput>> {
        self.inner.get_object_acl(req).await
    }
    async fn get_object_attributes(
        &self,
        req: S3Request<s3s::dto::GetObjectAttributesInput>,
    ) -> S3Result<S3Response<s3s::dto::GetObjectAttributesOutput>> {
        self.inner.get_object_attributes(req).await
    }
    async fn get_object_legal_hold(
        &self,
        req: S3Request<s3s::dto::GetObjectLegalHoldInput>,
    ) -> S3Result<S3Response<s3s::dto::GetObjectLegalHoldOutput>> {
        self.inner.get_object_legal_hold(req).await
    }
    async fn get_object_lock_configuration(
        &self,
        req: S3Request<s3s::dto::GetObjectLockConfigurationInput>,
    ) -> S3Result<S3Response<s3s::dto::GetObjectLockConfigurationOutput>> {
        self.inner.get_object_lock_configuration(req).await
    }
    async fn get_object_retention(
        &self,
        req: S3Request<s3s::dto::GetObjectRetentionInput>,
    ) -> S3Result<S3Response<s3s::dto::GetObjectRetentionOutput>> {
        self.inner.get_object_retention(req).await
    }
    async fn get_object_tagging(
        &self,
        req: S3Request<s3s::dto::GetObjectTaggingInput>,
    ) -> S3Result<S3Response<s3s::dto::GetObjectTaggingOutput>> {
        self.inner.get_object_tagging(req).await
    }
    async fn get_object_torrent(
        &self,
        req: S3Request<s3s::dto::GetObjectTorrentInput>,
    ) -> S3Result<S3Response<s3s::dto::GetObjectTorrentOutput>> {
        self.inner.get_object_torrent(req).await
    }
    async fn get_public_access_block(
        &self,
        req: S3Request<s3s::dto::GetPublicAccessBlockInput>,
    ) -> S3Result<S3Response<s3s::dto::GetPublicAccessBlockOutput>> {
        self.inner.get_public_access_block(req).await
    }
    async fn head_bucket(
        &self,
        req: S3Request<s3s::dto::HeadBucketInput>,
    ) -> S3Result<S3Response<s3s::dto::HeadBucketOutput>> {
        self.inner.head_bucket(req).await
    }
    async fn list_bucket_analytics_configurations(
        &self,
        req: S3Request<s3s::dto::ListBucketAnalyticsConfigurationsInput>,
    ) -> S3Result<S3Response<s3s::dto::ListBucketAnalyticsConfigurationsOutput>> {
        self.inner.list_bucket_analytics_configurations(req).await
    }
    async fn list_bucket_intelligent_tiering_configurations(
        &self,
        req: S3Request<s3s::dto::ListBucketIntelligentTieringConfigurationsInput>,
    ) -> S3Result<S3Response<s3s::dto::ListBucketIntelligentTieringConfigurationsOutput>> {
        self.inner
            .list_bucket_intelligent_tiering_configurations(req)
            .await
    }
    async fn list_bucket_inventory_configurations(
        &self,
        req: S3Request<s3s::dto::ListBucketInventoryConfigurationsInput>,
    ) -> S3Result<S3Response<s3s::dto::ListBucketInventoryConfigurationsOutput>> {
        self.inner.list_bucket_inventory_configurations(req).await
    }
    async fn list_bucket_metrics_configurations(
        &self,
        req: S3Request<s3s::dto::ListBucketMetricsConfigurationsInput>,
    ) -> S3Result<S3Response<s3s::dto::ListBucketMetricsConfigurationsOutput>> {
        self.inner.list_bucket_metrics_configurations(req).await
    }
    async fn list_buckets(
        &self,
        req: S3Request<s3s::dto::ListBucketsInput>,
    ) -> S3Result<S3Response<s3s::dto::ListBucketsOutput>> {
        self.inner.list_buckets(req).await
    }
    async fn list_directory_buckets(
        &self,
        req: S3Request<s3s::dto::ListDirectoryBucketsInput>,
    ) -> S3Result<S3Response<s3s::dto::ListDirectoryBucketsOutput>> {
        self.inner.list_directory_buckets(req).await
    }
    async fn list_multipart_uploads(
        &self,
        req: S3Request<s3s::dto::ListMultipartUploadsInput>,
    ) -> S3Result<S3Response<s3s::dto::ListMultipartUploadsOutput>> {
        self.inner.list_multipart_uploads(req).await
    }
    async fn list_object_versions(
        &self,
        req: S3Request<s3s::dto::ListObjectVersionsInput>,
    ) -> S3Result<S3Response<s3s::dto::ListObjectVersionsOutput>> {
        self.inner.list_object_versions(req).await
    }
    async fn list_objects(
        &self,
        req: S3Request<s3s::dto::ListObjectsInput>,
    ) -> S3Result<S3Response<s3s::dto::ListObjectsOutput>> {
        self.inner.list_objects(req).await
    }
    async fn list_parts(
        &self,
        req: S3Request<s3s::dto::ListPartsInput>,
    ) -> S3Result<S3Response<s3s::dto::ListPartsOutput>> {
        self.inner.list_parts(req).await
    }
    async fn put_bucket_accelerate_configuration(
        &self,
        req: S3Request<s3s::dto::PutBucketAccelerateConfigurationInput>,
    ) -> S3Result<S3Response<s3s::dto::PutBucketAccelerateConfigurationOutput>> {
        self.inner.put_bucket_accelerate_configuration(req).await
    }
    async fn put_bucket_acl(
        &self,
        req: S3Request<s3s::dto::PutBucketAclInput>,
    ) -> S3Result<S3Response<s3s::dto::PutBucketAclOutput>> {
        self.inner.put_bucket_acl(req).await
    }
    async fn put_bucket_analytics_configuration(
        &self,
        req: S3Request<s3s::dto::PutBucketAnalyticsConfigurationInput>,
    ) -> S3Result<S3Response<s3s::dto::PutBucketAnalyticsConfigurationOutput>> {
        self.inner.put_bucket_analytics_configuration(req).await
    }
    async fn put_bucket_cors(
        &self,
        req: S3Request<s3s::dto::PutBucketCorsInput>,
    ) -> S3Result<S3Response<s3s::dto::PutBucketCorsOutput>> {
        self.inner.put_bucket_cors(req).await
    }
    async fn put_bucket_encryption(
        &self,
        req: S3Request<s3s::dto::PutBucketEncryptionInput>,
    ) -> S3Result<S3Response<s3s::dto::PutBucketEncryptionOutput>> {
        self.inner.put_bucket_encryption(req).await
    }
    async fn put_bucket_intelligent_tiering_configuration(
        &self,
        req: S3Request<s3s::dto::PutBucketIntelligentTieringConfigurationInput>,
    ) -> S3Result<S3Response<s3s::dto::PutBucketIntelligentTieringConfigurationOutput>> {
        self.inner
            .put_bucket_intelligent_tiering_configuration(req)
            .await
    }
    async fn put_bucket_inventory_configuration(
        &self,
        req: S3Request<s3s::dto::PutBucketInventoryConfigurationInput>,
    ) -> S3Result<S3Response<s3s::dto::PutBucketInventoryConfigurationOutput>> {
        self.inner.put_bucket_inventory_configuration(req).await
    }
    async fn put_bucket_lifecycle_configuration(
        &self,
        req: S3Request<s3s::dto::PutBucketLifecycleConfigurationInput>,
    ) -> S3Result<S3Response<s3s::dto::PutBucketLifecycleConfigurationOutput>> {
        self.inner.put_bucket_lifecycle_configuration(req).await
    }
    async fn put_bucket_logging(
        &self,
        req: S3Request<s3s::dto::PutBucketLoggingInput>,
    ) -> S3Result<S3Response<s3s::dto::PutBucketLoggingOutput>> {
        self.inner.put_bucket_logging(req).await
    }
    async fn put_bucket_metrics_configuration(
        &self,
        req: S3Request<s3s::dto::PutBucketMetricsConfigurationInput>,
    ) -> S3Result<S3Response<s3s::dto::PutBucketMetricsConfigurationOutput>> {
        self.inner.put_bucket_metrics_configuration(req).await
    }
    async fn put_bucket_notification_configuration(
        &self,
        req: S3Request<s3s::dto::PutBucketNotificationConfigurationInput>,
    ) -> S3Result<S3Response<s3s::dto::PutBucketNotificationConfigurationOutput>> {
        self.inner.put_bucket_notification_configuration(req).await
    }
    async fn put_bucket_ownership_controls(
        &self,
        req: S3Request<s3s::dto::PutBucketOwnershipControlsInput>,
    ) -> S3Result<S3Response<s3s::dto::PutBucketOwnershipControlsOutput>> {
        self.inner.put_bucket_ownership_controls(req).await
    }
    async fn put_bucket_policy(
        &self,
        req: S3Request<s3s::dto::PutBucketPolicyInput>,
    ) -> S3Result<S3Response<s3s::dto::PutBucketPolicyOutput>> {
        self.inner.put_bucket_policy(req).await
    }
    async fn put_bucket_replication(
        &self,
        req: S3Request<s3s::dto::PutBucketReplicationInput>,
    ) -> S3Result<S3Response<s3s::dto::PutBucketReplicationOutput>> {
        self.inner.put_bucket_replication(req).await
    }
    async fn put_bucket_request_payment(
        &self,
        req: S3Request<s3s::dto::PutBucketRequestPaymentInput>,
    ) -> S3Result<S3Response<s3s::dto::PutBucketRequestPaymentOutput>> {
        self.inner.put_bucket_request_payment(req).await
    }
    async fn put_bucket_tagging(
        &self,
        req: S3Request<s3s::dto::PutBucketTaggingInput>,
    ) -> S3Result<S3Response<s3s::dto::PutBucketTaggingOutput>> {
        self.inner.put_bucket_tagging(req).await
    }
    async fn put_bucket_versioning(
        &self,
        req: S3Request<s3s::dto::PutBucketVersioningInput>,
    ) -> S3Result<S3Response<s3s::dto::PutBucketVersioningOutput>> {
        self.inner.put_bucket_versioning(req).await
    }
    async fn put_bucket_website(
        &self,
        req: S3Request<s3s::dto::PutBucketWebsiteInput>,
    ) -> S3Result<S3Response<s3s::dto::PutBucketWebsiteOutput>> {
        self.inner.put_bucket_website(req).await
    }
    async fn put_object_acl(
        &self,
        req: S3Request<s3s::dto::PutObjectAclInput>,
    ) -> S3Result<S3Response<s3s::dto::PutObjectAclOutput>> {
        self.inner.put_object_acl(req).await
    }
    async fn put_object_legal_hold(
        &self,
        req: S3Request<s3s::dto::PutObjectLegalHoldInput>,
    ) -> S3Result<S3Response<s3s::dto::PutObjectLegalHoldOutput>> {
        self.inner.put_object_legal_hold(req).await
    }
    async fn put_object_lock_configuration(
        &self,
        req: S3Request<s3s::dto::PutObjectLockConfigurationInput>,
    ) -> S3Result<S3Response<s3s::dto::PutObjectLockConfigurationOutput>> {
        self.inner.put_object_lock_configuration(req).await
    }
    async fn put_object_retention(
        &self,
        req: S3Request<s3s::dto::PutObjectRetentionInput>,
    ) -> S3Result<S3Response<s3s::dto::PutObjectRetentionOutput>> {
        self.inner.put_object_retention(req).await
    }
    async fn put_object_tagging(
        &self,
        req: S3Request<s3s::dto::PutObjectTaggingInput>,
    ) -> S3Result<S3Response<s3s::dto::PutObjectTaggingOutput>> {
        self.inner.put_object_tagging(req).await
    }
    async fn put_public_access_block(
        &self,
        req: S3Request<s3s::dto::PutPublicAccessBlockInput>,
    ) -> S3Result<S3Response<s3s::dto::PutPublicAccessBlockOutput>> {
        self.inner.put_public_access_block(req).await
    }
    async fn restore_object(
        &self,
        req: S3Request<s3s::dto::RestoreObjectInput>,
    ) -> S3Result<S3Response<s3s::dto::RestoreObjectOutput>> {
        self.inner.restore_object(req).await
    }
    async fn select_object_content(
        &self,
        req: S3Request<s3s::dto::SelectObjectContentInput>,
    ) -> S3Result<S3Response<s3s::dto::SelectObjectContentOutput>> {
        self.inner.select_object_content(req).await
    }
    async fn upload_part(
        &self,
        req: S3Request<s3s::dto::UploadPartInput>,
    ) -> S3Result<S3Response<s3s::dto::UploadPartOutput>> {
        self.inner.upload_part(req).await
    }
    async fn upload_part_copy(
        &self,
        req: S3Request<s3s::dto::UploadPartCopyInput>,
    ) -> S3Result<S3Response<s3s::dto::UploadPartCopyOutput>> {
        self.inner.upload_part_copy(req).await
    }
    async fn write_get_object_response(
        &self,
        req: S3Request<s3s::dto::WriteGetObjectResponseInput>,
    ) -> S3Result<S3Response<s3s::dto::WriteGetObjectResponseOutput>> {
        self.inner.write_get_object_response(req).await
    }
}
