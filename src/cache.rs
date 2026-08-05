//! Caching layer over the upstream `s3s_aws::Proxy`.
//!
//! Every client request funnels through this proxy, so it sees every write. That lets
//! it answer **LIST** and **HEAD** from an in-memory key index (LISTs are R2's expensive
//! Class-A tier, and a HEAD-per-key existence probe its most voluminous Class-B one) and
//! serve small GET/HEAD bodies from an LRU, while writes forward through to the upstream
//! — which stays the authority for conditional (OCC) writes.
//!
//! Correctness rests on one property: this proxy is the *only* path to the bucket. The
//! index warms lazily — LISTs pass through until a bucket's background full-LIST sync
//! completes ([`CachingProxy::is_synced`] gates the flip), then observed writes keep it
//! current. The body cache is separately lazy: populate on miss, invalidate on write.
//! Cross-node coherence rides the gossip write feed (see [`crate::sync`]): peers' writes
//! fold into the index and invalidate local copies; strict reads barrier on feed heads.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use bytes::Bytes;
use s3s::dto::{
    CompleteMultipartUploadInput, CompleteMultipartUploadOutput, CopyObjectInput, CopyObjectOutput,
    DeleteObjectInput, DeleteObjectOutput, DeleteObjectsInput, DeleteObjectsOutput, ETag,
    GetObjectInput, GetObjectOutput, HeadObjectInput, HeadObjectOutput, ListObjectsV2Input,
    ListObjectsV2Output, ObjectStorageClass, PutObjectInput, PutObjectOutput, StreamingBlob,
    Timestamp,
};
use s3s::{S3, S3Request, S3Response, S3Result};
use tracing::info;

use crate::index::{
    BucketState, Completion, EntryFill, IndexedHead, ObjEntry, ObjMeta, apply_del, apply_put,
    complete_entry, head_object_from_index, list_objects_v2_from_index, standard_class,
    sync_bucket_into,
};
use crate::metrics::Metrics;
use crate::sync::{READ_TOKEN_HEADER, WRITE_TOKEN_HEADER, WriteReceipt, WriteSync, wire_stamp};
use crate::tier::{self, CachedObject, TieredCache, WarmPair};
use http::{HeaderMap, HeaderName, HeaderValue};

/// Sizing for the object cache, passed to [`CachingProxy::new`].
#[derive(Clone, Copy)]
pub struct CacheConfig {
    /// Total hot (heap) tier capacity in bytes.
    pub cache_bytes: u64,
    /// Objects larger than this are never cached.
    pub max_obj_bytes: usize,
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

/// How long a write response may be held waiting for every alive peer to
/// acknowledge applying its invalidation. Generously above the SWIM suspect
/// timeout, so a dying peer is excluded from the wait before this fires.
const WRITE_ACK_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

/// How long a strict read waits for the freshness barrier before serving
/// current state anyway (degrading to eventual rather than hanging).
const READ_BARRIER_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);

/// First and longest wait between attempts at a bucket's warm-up sync. A bucket whose
/// LIST fails is not lost for the process lifetime — it stays passthrough (correct, just
/// expensive) and keeps trying, because the origin being briefly unreachable at startup
/// is the ordinary case, not a permanent verdict.
const SYNC_RETRY_MIN: Duration = Duration::from_secs(1);
const SYNC_RETRY_MAX: Duration = Duration::from_mins(1);

/// The per-request `response-*` overrides on a read: headers the origin applies to that
/// one response, and a property of the request rather than of the stored object. They
/// are stripped on the way in — so a fill always caches the object's own headers and no
/// client's formatting is served to the next — and applied on the way out, hit or miss,
/// so an overriding read is answered exactly as the origin would answer it.
#[derive(Clone, Default)]
struct ResponseOverrides {
    content_type: Option<String>,
    content_disposition: Option<String>,
    content_encoding: Option<String>,
    content_language: Option<String>,
    cache_control: Option<String>,
    expires: Option<Timestamp>,
}

/// Moves the `response-*` fields out of an input into a [`ResponseOverrides`]; one macro
/// so GET and HEAD (which spell them identically) cannot drift apart.
macro_rules! take_overrides {
    ($input:expr) => {{
        let input = $input;
        ResponseOverrides {
            content_type: input.response_content_type.take(),
            content_disposition: input.response_content_disposition.take(),
            content_encoding: input.response_content_encoding.take(),
            content_language: input.response_content_language.take(),
            cache_control: input.response_cache_control.take(),
            expires: input.response_expires.take(),
        }
    }};
}

/// What an origin response says about an object — the shape [`CachingProxy::observe`]
/// folds into the LIST index. `size: None` is a size that was never learned, and stays
/// that way: an entry is never given a number the origin did not report.
#[derive(Clone)]
struct ObservedObject {
    size: Option<i64>,
    etag: Option<ETag>,
    content_type: Option<String>,
    storage_class: ObjectStorageClass,
    meta: ObjMeta,
}

/// The storage class a write puts a key in: what the request asked for, or S3's default.
fn write_storage_class(requested: Option<&s3s::dto::StorageClass>) -> ObjectStorageClass {
    requested.map_or_else(standard_class, |class| {
        ObjectStorageClass::from(class.as_str().to_owned())
    })
}

/// An index entry for a key the write path cannot describe itself (a copy, a completed
/// multipart): everything an origin HEAD reported, or — when the HEAD would not answer
/// even on retry — a skeletal entry with **no size**, which neither LIST nor HEAD will
/// serve until something completes it. Nothing is invented: the old `size: 0` was served
/// as an authoritative `Content-Length` forever. `last_modified` is stamped by
/// [`CachingProxy::record_put`].
fn observed_entry(observed: Option<&ObservedObject>) -> ObjEntry {
    let Some(observed) = observed else {
        return ObjEntry {
            size: None,
            last_modified: SystemTime::UNIX_EPOCH,
            etag: None,
            storage_class: standard_class(),
            content_type: None,
            meta: None,
        };
    };
    ObjEntry {
        size: observed.size,
        last_modified: SystemTime::UNIX_EPOCH,
        etag: observed.etag.clone(),
        storage_class: observed.storage_class.clone(),
        content_type: observed.content_type.clone(),
        meta: Some(Box::new(observed.meta.clone())),
    }
}

/// Reads an origin response into an [`ObservedObject`]. `GetObjectOutput` and
/// `HeadObjectOutput` spell these fields identically, so one macro drives both and the
/// two can never learn to disagree.
macro_rules! observed {
    ($out:expr) => {{
        let out = $out;
        ObservedObject {
            size: out.content_length,
            etag: out.e_tag.clone(),
            content_type: out.content_type.clone(),
            storage_class: out
                .storage_class
                .as_ref()
                .map_or_else(standard_class, |class| {
                    ObjectStorageClass::from(class.as_str().to_owned())
                }),
            meta: ObjMeta {
                cache_control: out.cache_control.clone(),
                content_disposition: out.content_disposition.clone(),
                content_encoding: out.content_encoding.clone(),
                content_language: out.content_language.clone(),
                metadata: out.metadata.clone(),
            },
        }
    }};
}

/// Applies the overrides onto a response, leaving untouched anything the request did not
/// ask to override. `GetObjectOutput` and `HeadObjectOutput` spell these identically, so
/// one macro drives both.
macro_rules! apply_overrides {
    ($overrides:expr, $out:expr) => {{
        let (overrides, out) = ($overrides, $out);
        for (field, value) in [
            (&mut out.content_type, &overrides.content_type),
            (&mut out.content_disposition, &overrides.content_disposition),
            (&mut out.content_encoding, &overrides.content_encoding),
            (&mut out.content_language, &overrides.content_language),
            (&mut out.cache_control, &overrides.cache_control),
        ] {
            if value.is_some() {
                field.clone_from(value);
            }
        }
        if overrides.expires.is_some() {
            out.expires.clone_from(&overrides.expires);
        }
    }};
}

/// Which write folded a key into the LIST index. Each of these is a separately
/// billed upstream (R2 class A) operation, so each gets its own counter — one
/// lumped `writes_indexed` can't attribute the spend.
#[derive(Clone, Copy)]
enum IndexedWrite {
    Put,
    Copy,
    MultipartComplete,
}

impl IndexedWrite {
    fn record(self, metrics: &Metrics) {
        match self {
            Self::Put => metrics.write_indexed_put(),
            Self::Copy => metrics.write_indexed_copy(),
            Self::MultipartComplete => metrics.write_indexed_multipart(),
        }
    }
}

/// S3 service that caches LIST (from an in-memory index) and small GET/HEAD bodies (the
/// hot/warm/cold [`TieredCache`]) in front of an upstream `s3s_aws::Proxy`, forwarding
/// every write.
pub struct CachingProxy {
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
    #[must_use]
    pub fn new(
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

    /// Start the gossip apply loop: peers' events fold into this node's LIST index and
    /// invalidate its body copies; a gap flushes every local tier and resyncs `buckets`
    /// from the origin. A no-op without gossip — single-node is already strict.
    pub fn start_coherence(&self, buckets: &[String]) {
        let Some(sync) = &self.sync else { return };
        sync.start_apply(
            self.obj_cache.local(),
            self.state.clone(),
            self.gap_resync_handle(buckets.to_vec()),
            self.metrics.clone(),
        );
    }

    /// The gap remediation for the apply loop: reset every bucket to
    /// passthrough (unsynced LISTs are always correct) and re-warm the
    /// configured ones from the origin — the authority the index caches.
    fn gap_resync_handle(&self, buckets: Vec<String>) -> Arc<dyn Fn() + Send + Sync> {
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

    /// Freshness barrier: before serving a read from node-local state (the LIST index,
    /// or a hot/disk body copy), wait until every peer's currently-advertised write-feed
    /// head has been applied locally, so a peer's just-completed write is not read stale.
    /// Freshness is bounded by one push/gossip hop (see [`crate::sync`]); degrades to
    /// serving current state on timeout. No-op without gossip (single-node is strict).
    async fn read_barrier(&self, headers: &HeaderMap) -> ReadRoute {
        let Some(sync) = &self.sync else {
            return ReadRoute::Local; // single node: the sole writer is strict
        };
        // The read-side half of transparent coherence: if this node's
        // membership view is not fully alive, it may be the partitioned one —
        // writers can't reach it with invalidations, so its cache is not
        // trustworthy. Serve via the origin until the view heals.
        if !sync.cluster_healthy() {
            self.metrics.unhealthy_bypass();
            return ReadRoute::Origin;
        }
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

    /// The shared counter set, for the periodic stats task.
    #[must_use]
    pub fn metrics(&self) -> Arc<Metrics> {
        self.metrics.clone()
    }

    /// Record a durable put: fold `entry` into the local index (LWW, same rule the apply
    /// loop uses for peers), advertise it over the write feed, and hold the response
    /// until every alive peer has applied the invalidation (a couple of in-cluster hops
    /// behind the origin round-trip already paid), so a subsequent read via ANY node is
    /// fresh with no client cooperation. Returns the write's session token when
    /// coherence is on. The caller has already dropped this node's own body copy — the
    /// writer must not serve its own stale bytes while the cluster round runs.
    async fn record_put(
        &self,
        op: IndexedWrite,
        bucket: &str,
        key: &str,
        mut entry: ObjEntry,
    ) -> Option<String> {
        entry.last_modified = wire_stamp(SystemTime::now());
        // Apply locally first: the writer must never be the one node still answering
        // from the older entry, not even for the length of a publish. The copy is only
        // taken when there is a feed to advertise it on.
        let advertised = self.sync.is_some().then(|| entry.clone());
        if apply_put(&self.state, bucket, key, entry) {
            op.record(&self.metrics);
        }
        let (sync, entry) = (self.sync.as_ref()?, advertised?);
        let receipt = sync.publish_put(bucket, key, &entry, &self.metrics).await;
        sync.ack_write(receipt.token, WRITE_ACK_TIMEOUT, bucket, key, &self.metrics)
            .await;
        Some(receipt.header)
    }

    /// Record a durable delete: tombstone + remove locally and advertise it to peers,
    /// returning the receipt rather than waiting. The caller chooses when to wait — one
    /// key waits immediately, a batch publishes every key first and then waits once (see
    /// [`await_cluster`](Self::await_cluster)).
    async fn record_del(&self, bucket: &str, key: &str) -> Option<WriteReceipt> {
        let ts = wire_stamp(SystemTime::now());
        apply_del(&self.state, bucket, key, ts);
        let sync = self.sync.as_ref()?;
        Some(sync.publish_del(bucket, key, ts, &self.metrics).await)
    }

    /// Hold a write's response until every alive peer has applied it, then hand back the
    /// session token for the response header (see [`record_put`](Self::record_put)).
    async fn await_cluster(
        &self,
        receipt: Option<WriteReceipt>,
        bucket: &str,
        key: &str,
    ) -> Option<String> {
        let (sync, receipt) = (self.sync.as_ref()?, receipt?);
        sync.ack_write(receipt.token, WRITE_ACK_TIMEOUT, bucket, key, &self.metrics)
            .await;
        Some(receipt.header)
    }

    /// Warm each bucket's LIST index in the background so startup stays instant and
    /// independent of bucket size. Until a bucket's full sync finishes its LISTs pass
    /// through to the upstream (always correct), then flip to index-served. A failed
    /// sync retries with capped exponential backoff rather than leaving the bucket
    /// passthrough for the process lifetime — a transient origin outage at startup
    /// should not cost every LIST for the next fortnight.
    pub fn spawn_background_sync(&self, buckets: Vec<String>) {
        for bucket in buckets {
            let client = self.client.clone();
            let state = self.state.clone();
            tokio::spawn(async move {
                let mut backoff = SYNC_RETRY_MIN;
                loop {
                    match sync_bucket_into(&client, &state, &bucket).await {
                        Ok(n) => {
                            info!("warmed LIST index for `{bucket}`: {n} keys");
                            return;
                        }
                        Err(e) => {
                            tracing::warn!(
                                "background sync of `{bucket}` failed (staying passthrough, \
                                 retrying in {backoff:?}): {e}"
                            );
                            tokio::time::sleep(backoff).await;
                            backoff = (backoff * 2).min(SYNC_RETRY_MAX);
                        }
                    }
                }
            });
        }
    }

    /// Fold what an origin response proved into the index. An already-indexed key is
    /// *completed* in place — the response fills the fields a skeletal entry lacks and
    /// nothing else, so this observes rather than writes and cannot reorder against a
    /// concurrent write. A key that is not indexed at all is added as a read
    /// observation, stamped with the origin's own mtime (the truest clock available for
    /// something the origin just described) and advertised to nobody: peers learn real
    /// writes from their writers.
    fn observe(&self, bucket: &str, key: &str, observed: &ObservedObject) {
        let fill = EntryFill {
            size: observed.size,
            etag: observed.etag.clone(),
            content_type: observed.content_type.clone(),
            meta: observed.meta.clone(),
        };
        match complete_entry(&self.state, bucket, key, fill) {
            Completion::Completed => {
                self.metrics.index_backfill();
                return;
            }
            Completion::AlreadyComplete => return,
            Completion::NotIndexed => {}
        }
        let entry = ObjEntry {
            size: observed.size,
            last_modified: wire_stamp(SystemTime::now()),
            etag: observed.etag.clone(),
            storage_class: observed.storage_class.clone(),
            content_type: observed.content_type.clone(),
            meta: Some(Box::new(observed.meta.clone())),
        };
        if apply_put(&self.state, bucket, key, entry) {
            self.metrics.write_indexed_observed();
        }
    }

    /// Index a size learned on the read path where nothing else about the object is in
    /// hand (a refused range promotion reporting the real Content-Length). Skeletal by
    /// construction: it stops the promotion decision re-firing, nothing more.
    fn index_size_only(&self, bucket: &str, key: &str, size: i64) {
        let entry = ObjEntry {
            size: Some(size),
            last_modified: wire_stamp(SystemTime::now()),
            etag: None,
            storage_class: standard_class(),
            content_type: None,
            meta: None,
        };
        if apply_put(&self.state, bucket, key, entry) {
            self.metrics.write_indexed_observed();
        }
    }

    /// The indexed size of a key, if this proxy has seen it *and* learned its size
    /// (write-through or LIST warm-up). Drives the range-promotion decision without a
    /// HEAD.
    fn index_size(&self, bucket: &str, key: &str) -> Option<i64> {
        self.state
            .read()
            .unwrap()
            .get(bucket)
            .and_then(|b| b.keys.get(key))
            .and_then(|e| e.size)
    }

    /// The upstream's own description of a key (one HEAD via the direct client), used
    /// where the write path does not carry it: multipart complete and copy know neither
    /// the assembled size nor the metadata the origin ended up with. Retried once,
    /// because the alternative to knowing is a key the index holds but cannot serve.
    async fn upstream_meta(&self, bucket: &str, key: &str) -> Option<ObservedObject> {
        for attempt in 0..2 {
            match self
                .client
                .head_object()
                .bucket(bucket)
                .key(key)
                .send()
                .await
            {
                Ok(head) => {
                    return Some(ObservedObject {
                        size: head.content_length(),
                        etag: head.e_tag().and_then(|raw| raw.parse().ok()),
                        content_type: head.content_type().map(str::to_owned),
                        storage_class: head.storage_class().map_or_else(standard_class, |class| {
                            ObjectStorageClass::from(class.as_str().to_owned())
                        }),
                        meta: ObjMeta {
                            cache_control: head.cache_control().map(str::to_owned),
                            content_disposition: head.content_disposition().map(str::to_owned),
                            content_encoding: head.content_encoding().map(str::to_owned),
                            content_language: head.content_language().map(str::to_owned),
                            metadata: head.metadata().cloned(),
                        },
                    });
                }
                Err(e) if attempt == 0 => {
                    tracing::debug!("metadata HEAD of {bucket}/{key} failed, retrying: {e}");
                }
                Err(e) => {
                    tracing::warn!(
                        "metadata HEAD of {bucket}/{key} failed twice ({e}); indexing the key \
                         without a size — LIST for the bucket and HEAD for the key fall \
                         through to the origin until a response completes the entry"
                    );
                }
            }
        }
        None
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
            // A range starting past the end is refused by the origin with a shape only
            // the origin knows (AWS sends `Content-Range: bytes */<size>`, MinIO sends
            // none), so it is not synthesized here — it falls through and the origin's
            // own 416 is what the client sees, whichever origin is behind us.
            Ok(obj) => {
                self.metrics.range_promote();
                let out = obj.to_get_range(first, last)?;
                Some(Ok(S3Response::new(out)))
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
                    self.index_size_only(&ckey.0, &ckey.1, sz);
                }
                self.metrics.range_promote_reject();
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

    /// Build a `ListObjectsV2` response from this bucket's index; `None` when the index
    /// cannot answer without inventing a value. See [`list_objects_v2_from_index`].
    fn list_from_index(&self, inp: &ListObjectsV2Input) -> Option<ListObjectsV2Output> {
        let g = self.state.read().unwrap();
        list_objects_v2_from_index(g.get(inp.bucket.as_str()).map(|b| &b.keys), inp)
    }

    /// What this bucket's index can say about a key's HEAD (see
    /// [`head_object_from_index`]).
    fn head_from_index(&self, bucket: &str, key: &str) -> IndexedHead {
        let g = self.state.read().unwrap();
        head_object_from_index(g.get(bucket).map(|b| &b.keys), key)
    }

    /// Whether an index miss may be answered as an authoritative 404. Single-node — no
    /// write feed — always: the sole writer's index is the truth. With gossip, only once
    /// the cluster view has held still long enough that no peer can be holding a write
    /// this node has not seen (see [`WriteSync::settled`]); inside that window the
    /// origin answers instead, which is slower and never wrong.
    fn index_404_trustworthy(&self) -> bool {
        self.sync.as_ref().is_none_or(|sync| sync.settled())
    }

    /// The GET decision tree, with the per-request `response-*` overrides already lifted
    /// off `req`: every copy this serves or fills is of the object as the origin stores
    /// it, so one client's formatting is never handed to the next and the caller puts
    /// the overrides back on the way out.
    async fn serve_get(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        // Requests whose response the cache can't faithfully reproduce must go to the
        // origin: a specific version, origin-computed checksums, SSE-C (whose bytes must
        // never be served without the caller's key), or a bucket-owner guard, which is
        // the origin's to evaluate and would be skipped by a locally-served answer.
        let origin_only = req.input.version_id.is_some()
            || req.input.checksum_mode.is_some()
            || req.input.sse_customer_key.is_some()
            || req.input.expected_bucket_owner.is_some();
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
                self.metrics.range_hit();
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
            // Big, not-yet-indexed, unverified-token, a failed promote, or a range that
            // starts past the end (whose 416 shape is the origin's): stream through.
            self.metrics.get_bypass();
            return self.inner.get_object(req).await;
        }
        if local_ok
            && cacheable
            && let Some(obj) = self.obj_cache.get(&ckey).await
        {
            self.metrics.get_hit();
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
                            ckey.clone(),
                            Arc::new(CachedObject::from_get(&resp.output, bytes.clone())),
                        )
                        .await;
                    // The whole of what a HEAD reports is in hand, so the index entry is
                    // completed here too — a body fill is the cheapest place to turn a
                    // skeletal entry faithful, and it costs the origin nothing extra.
                    self.observe(&ckey.0, &ckey.1, &observed!(&resp.output));
                    self.metrics.get_miss();
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
            self.metrics.get_bypass();
        }
        Ok(resp)
    }

    /// The HEAD decision tree, with the `response-*` overrides already lifted off `req`
    /// (see [`serve_get`](Self::serve_get)).
    async fn serve_head(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        let cache_eligible = req.input.range.is_none()
            && req.input.part_number.is_none()
            && req.input.version_id.is_none()
            && req.input.checksum_mode.is_none()
            && req.input.sse_customer_key.is_none()
            && req.input.expected_bucket_owner.is_none()
            && req.input.if_match.is_none()
            && req.input.if_none_match.is_none()
            && req.input.if_modified_since.is_none()
            && req.input.if_unmodified_since.is_none();
        let ckey = (req.input.bucket.clone(), req.input.key.clone());
        if cache_eligible && self.read_barrier(&req.headers).await == ReadRoute::Local {
            if let Some(obj) = self.obj_cache.get(&ckey).await {
                self.metrics.head_hit();
                return Ok(S3Response::new(obj.to_head()));
            }
            if self.is_synced(&ckey.0) {
                match self.head_from_index(&ckey.0, &ckey.1) {
                    IndexedHead::Faithful(out) => {
                        self.metrics.head_index();
                        return Ok(S3Response::new(*out));
                    }
                    // A key the index does not hold does not exist — but only once no
                    // peer can still be holding a write this node has not seen.
                    IndexedHead::Absent if self.index_404_trustworthy() => {
                        self.metrics.head_404();
                        return Err(s3s::s3_error!(NoSuchKey, "the key does not exist"));
                    }
                    IndexedHead::Absent | IndexedHead::Incomplete => {}
                }
            }
        }
        self.metrics.head_miss();
        let resp = self.inner.head_object(req).await?;
        // This answer is exactly what the entry was missing, so fold it in: the key's
        // next HEAD is local *and* identical to this one, which is the whole point of
        // forwarding a skeletal entry's first. Only for a plain HEAD — a conditional or
        // version-scoped answer describes something other than the current object.
        if cache_eligible {
            self.observe(&ckey.0, &ckey.1, &observed!(&resp.output));
        }
        Ok(resp)
    }
}

#[async_trait]
impl s3s::S3 for CachingProxy {
    // LIST served from the index when the bucket is synced; else passthrough.
    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        // Three things the index cannot answer without guessing at the origin's own
        // wire format or authorisation, so they are forwarded verbatim:
        //   * `encoding-type` — percent-encoding is per-origin (MinIO escapes a space
        //     as `+` and leaves `/*-_.` alone; AWS and R2 differ in the margins), and a
        //     client that decodes what it asked for corrupts every key we guessed at.
        //   * `fetch-owner` — the Owner element is the origin's, and no write path
        //     carries it.
        //   * `x-amz-expected-bucket-owner` — a guard only the origin can evaluate; an
        //     answer served locally would skip a check the origin would have failed.
        let origin_only = req.input.encoding_type.is_some()
            || req.input.fetch_owner.unwrap_or(false)
            || req.input.expected_bucket_owner.is_some();
        if !origin_only
            && self.is_synced(req.input.bucket.as_str())
            && self.read_barrier(&req.headers).await == ReadRoute::Local
            && let Some(out) = self.list_from_index(&req.input)
        {
            self.metrics.list_from_index();
            return Ok(S3Response::new(out));
        }
        self.metrics.list_passthrough();
        self.inner.list_objects_v2(req).await
    }

    // Writes: forward (write-through), then update the index from the result.
    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        let bucket = req.input.bucket.clone();
        let key = req.input.key.clone();
        // Everything a HEAD of this object will report, straight off the request that
        // created it — no HEAD needed to learn what we were just told. Except the
        // Content-Type: with none set the origin invents one, and an entry claiming to
        // know it would answer HEADs the origin answers differently, so such an entry
        // stays skeletal until a forwarded HEAD completes it.
        let faithful = req.input.content_type.is_some();
        let meta = ObjMeta {
            cache_control: req.input.cache_control.clone(),
            content_disposition: req.input.content_disposition.clone(),
            content_encoding: req.input.content_encoding.clone(),
            content_language: req.input.content_language.clone(),
            // `x-amz-meta-*` names are HTTP header names, so the origin reports them
            // lowercased whatever case they were sent in; capturing them verbatim would
            // make a HEAD off this entry differ from the origin's in the key casing.
            metadata: req.input.metadata.as_ref().map(|m| {
                m.iter()
                    .map(|(k, v)| (k.to_ascii_lowercase(), v.clone()))
                    .collect()
            }),
        };
        let mut entry = ObjEntry {
            // A PUT with no Content-Length leaves the size unknown rather than zero: a
            // fabricated `0` is served as an authoritative Content-Length forever.
            size: req.input.content_length,
            last_modified: SystemTime::UNIX_EPOCH, // stamped by `record_put`
            etag: None,
            storage_class: write_storage_class(req.input.storage_class.as_ref()),
            content_type: req.input.content_type.clone(),
            meta: faithful.then(|| Box::new(meta)),
        };
        let mut resp = self.inner.put_object(req).await?;
        // Drop this node's own copy the instant the origin has the new bytes — before
        // the cluster-ack round the index update pays for. Otherwise the writing node
        // is the one node in the fleet still serving the old body.
        self.obj_cache
            .invalidate(&(bucket.clone(), key.clone()))
            .await;
        // The origin's ETag rides back on the response, so the index learns it here
        // rather than paying a HEAD for what a later HEAD will want to report.
        entry.etag = resp.output.e_tag.clone();
        let token = self
            .record_put(IndexedWrite::Put, &bucket, &key, entry)
            .await;
        Self::attach_token(&mut resp.headers, token);
        Ok(resp)
    }

    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        let bucket = req.input.bucket.clone();
        let key = req.input.key.clone();
        let versioned = req.input.version_id.is_some();
        let mut resp = self.inner.delete_object(req).await?;
        self.obj_cache
            .invalidate(&(bucket.clone(), key.clone()))
            .await;
        // A version-scoped delete removes one version, not the key: the current object
        // may be untouched, or may now be a different version entirely. Only the local
        // body copy is provably stale — what the key resolves to stays the origin's to
        // report, so no tombstone is recorded and the entry is left for a HEAD or the
        // next sync to correct.
        let token = if versioned {
            None
        } else {
            let receipt = self.record_del(&bucket, &key).await;
            self.await_cluster(receipt, &bucket, &key).await
        };
        Self::attach_token(&mut resp.headers, token);
        Ok(resp)
    }

    async fn delete_objects(
        &self,
        req: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        let bucket = req.input.bucket.clone();
        let quiet = req.input.delete.quiet.unwrap_or(false);
        let requested: Vec<(String, bool)> = req
            .input
            .delete
            .objects
            .iter()
            .map(|o| (o.key.clone(), o.version_id.is_some()))
            .collect();
        let mut resp = self.inner.delete_objects(req).await?;
        // `DeleteObjects` is partial-failure by contract: the call succeeds while
        // individual keys are refused (a legal hold, a retention lock, a permission).
        // Unindexing every *requested* key makes a key the origin still holds vanish
        // cluster-wide — LIST loses it and HEAD 404s — until the next resync, so the
        // applied set is read off the response. In quiet mode the origin omits the
        // Deleted half, and what was asked for minus what was refused is the same set.
        let refused: BTreeSet<&str> = resp
            .output
            .errors
            .iter()
            .flatten()
            .filter_map(|e| e.key.as_deref())
            .collect();
        let deleted: BTreeSet<&str> = resp
            .output
            .deleted
            .iter()
            .flatten()
            .filter_map(|d| d.key.as_deref())
            .collect();
        let mut receipt = None;
        for (key, versioned) in &requested {
            let applied = if quiet {
                !refused.contains(key.as_str())
            } else {
                deleted.contains(key.as_str())
            };
            if !applied {
                continue;
            }
            self.obj_cache
                .invalidate(&(bucket.clone(), key.clone()))
                .await;
            if *versioned {
                continue; // one version, not the key — see `delete_object`
            }
            // Keep the newest receipt: its token covers the whole batch (one writer,
            // ordered feed), so the cluster round is paid once rather than per key —
            // a 1000-key batch of 2s waits is half an hour of held response.
            receipt = self.record_del(&bucket, key).await.or(receipt);
        }
        let token = self.await_cluster(receipt, &bucket, "<batch delete>").await;
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
        self.obj_cache
            .invalidate(&(bucket.clone(), key.clone()))
            .await;
        // Multipart is how the big objects arrive, and indexing them at a placeholder
        // size poisoned the range-promotion decision (a "0-byte" entry promoted a
        // multi-GB fetch). One HEAD learns the real size — and, since it is being paid
        // for anyway, everything else a HEAD of the assembled object reports.
        let observed = self.upstream_meta(&bucket, &key).await;
        let mut entry = observed_entry(observed.as_ref());
        entry.etag = resp.output.e_tag.clone().or(entry.etag);
        let token = self
            .record_put(IndexedWrite::MultipartComplete, &bucket, &key, entry)
            .await;
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
        self.obj_cache
            .invalidate(&(bucket.clone(), key.clone()))
            .await;
        // A copy's metadata is the source's (or the request's, per the directive) and
        // its size is whatever the origin assembled, neither of which the response
        // carries: one HEAD is what tells us what we just created.
        let observed = self.upstream_meta(&bucket, &key).await;
        let mut entry = observed_entry(observed.as_ref());
        entry.etag = resp
            .output
            .copy_object_result
            .as_ref()
            .and_then(|result| result.e_tag.clone())
            .or(entry.etag);
        let token = self
            .record_put(IndexedWrite::Copy, &bucket, &key, entry)
            .await;
        Self::attach_token(&mut resp.headers, token);
        Ok(resp)
    }

    // GET: cacheable (no part/conditional) small objects are served from the tiered
    // cache; a miss buffers the body and caches it. Ranged reads of cacheable-size
    // objects are served by slicing the cached whole object, promoted on first touch.
    // Oversized/unknown-size ranges and conditional requests stream straight through.
    // The per-request `response-*` overrides are lifted off the request first and put
    // back on the answer last, so they neither reach the cached copy nor go missing on
    // a hit (see [`ResponseOverrides`]).
    async fn get_object(
        &self,
        mut req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let overrides = take_overrides!(&mut req.input);
        let mut resp = self.serve_get(req).await?;
        apply_overrides!(&overrides, &mut resp.output);
        Ok(resp)
    }

    // HEAD served from the object cache when the body is already cached, and from the
    // LIST index when it is not: on a synced bucket the index is authoritative for
    // existence (the property LIST-from-index already rests on) and a *faithful* entry
    // carries everything a HEAD reports — so a HEAD of an uncached object costs nothing,
    // and a HEAD of an absent key is a local 404. A skeletal entry (a bootstrap LIST
    // row, a peer's feed event) is forwarded once and completed from the answer, so the
    // next HEAD of that key is local and identical. Requests that need the origin pass
    // through: a range or part, a specific version, checksums, SSE-C, a bucket-owner
    // guard, and — as on the GET path — anything conditional, since the origin is the
    // authority on whether a precondition holds. So does any bucket whose index has not
    // finished warming.
    async fn head_object(
        &self,
        mut req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        let overrides = take_overrides!(&mut req.input);
        let mut resp = self.serve_head(req).await?;
        apply_overrides!(&overrides, &mut resp.output);
        Ok(resp)
    }

    async fn delete_bucket(
        &self,
        req: S3Request<s3s::dto::DeleteBucketInput>,
    ) -> S3Result<S3Response<s3s::dto::DeleteBucketOutput>> {
        let bucket = req.input.bucket.clone();
        let resp = self.inner.delete_bucket(req).await?;
        // The bucket is gone; its index is not, and would go on answering LIST from a
        // key set the origin no longer has — and HEADs of those keys as authoritative
        // 404s of the wrong kind. Dropping the state returns the name to passthrough.
        self.state.write().unwrap().remove(&bucket);
        Ok(resp)
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
