use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use s3s::dto::{
    ETag, ETagCondition, GetObjectInput, GetObjectOutput, HeadObjectInput, HeadObjectOutput,
    ListObjectsV2Input, ListObjectsV2Output, ObjectStorageClass, PutObjectInput, Timestamp,
};
use s3s::{S3, S3Error, S3Request, S3Response, S3Result};
use tracing::info;

use crate::cache::copy;
use crate::index::{
    BucketState, Completion, EntryFill, IndexedHead, ObjEntry, ObjMeta, apply_del, apply_put,
    begin_bucket_resync, bucket_resync_is_current, complete_entry, entry_matches_body,
    head_object_from_index, list_objects_v2_from_index, restart_bucket_resync_if_current,
    standard_class, sync_bucket_generation, sync_bucket_into,
};
use crate::metrics::Metrics;
use crate::sync::coherence::{READ_TOKEN_HEADER, WRITE_TOKEN_HEADER, WriteReceipt, WriteSync};
use crate::sync::wire::wire_stamp;
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
pub(super) enum ReadRoute {
    Local,
    Origin,
}

/// How long a write response may be held waiting for every alive peer to
/// acknowledge applying its invalidation. Generously above the SWIM suspect
/// timeout, so a dying peer is excluded from the wait before this fires.
///
/// In `strong` this bounds only the *transitional* wait on unleased peers (empty in a
/// uniform leased fleet): the lease wait runs to its own `D + 1s` deadline, and the two
/// run concurrently, so the response is held for the longer of the two — see
/// [`WriteSync::wait_cluster_applied`](crate::sync::coherence::WriteSync).
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
pub(super) struct ResponseOverrides {
    pub(super) content_type: Option<String>,
    pub(super) content_disposition: Option<String>,
    pub(super) content_encoding: Option<String>,
    pub(super) content_language: Option<String>,
    pub(super) cache_control: Option<String>,
    pub(super) expires: Option<Timestamp>,
}

/// What an origin response says about an object — the shape [`CachingProxy::observe`]
/// folds into the LIST index. `size: None` is a size that was never learned, and stays
/// that way: an entry is never given a number the origin did not report.
#[derive(Clone)]
pub(super) struct ObservedObject {
    size: Option<i64>,
    etag: Option<ETag>,
    content_type: Option<String>,
    storage_class: ObjectStorageClass,
    meta: ObjMeta,
}

/// A whole-object cache fill can fail normally, or discover from the origin's
/// response that the object is too large to retain. The latter must hand the
/// original streaming response back to the leader instead of manufacturing an
/// error; followers re-probe after the per-key gate is released and stream their
/// own copies, since an uncacheable body cannot be replayed.
enum WholeGetFillError {
    Origin(S3Error),
    Uncacheable(Box<S3Response<GetObjectOutput>>),
}

/// The storage class a write puts a key in: what the request asked for, or S3's default.
pub(super) fn write_storage_class(
    requested: Option<&s3s::dto::StorageClass>,
) -> ObjectStorageClass {
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
pub(super) fn observed_entry(observed: Option<&ObservedObject>) -> ObjEntry {
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

/// Whether a `PutObject` stores something other than *the body it carries, described by
/// the headers on the request* — the write path's twin of the `origin_only` set on
/// [`CachingProxy::serve_get`]. Each of these makes a later plain GET report something a
/// write-filled copy could not reproduce, so such a write forwards its body and keeps
/// nothing:
///
/// * **SSE-C** — the origin stores ciphertext and refuses a GET that does not present the
///   key. A cached plaintext copy would both differ from the origin's answer and hand the
///   bytes to a caller who never presented the key.
/// * **`write_offset_bytes`** — an append: what is stored is the previous object plus
///   this body, not this body.
/// * **a named storage class** — the origin reports `x-amz-storage-class` for anything
///   but the default, and an archive class is not directly readable at all.
/// * **object lock, tagging, a website redirect, `Expires`** — each surfaces on a GET of
///   the object as a header [`CachedObject`] does not carry.
///
/// Deliberately *not* here: the conditional headers (a refused write returns before the
/// fill), checksums and `Content-MD5` (integrity of this request — a GET reports a
/// checksum only when it asks for one, and such a GET is origin-only), ACLs and grants
/// (invisible on a GET), `expected_bucket_owner` (the guard was evaluated for this write,
/// and a *read* carrying one is origin-only), and SSE-S3/SSE-KMS (the origin decrypts
/// transparently, so the body a GET returns is exactly this one; the
/// `x-amz-server-side-encryption` echo is unmodelled on the read-path fill too, and a
/// bucket's *default* encryption never appears on the request at all — excluding the
/// explicit form would buy no fidelity and cost the common case).
fn put_origin_only(input: &PutObjectInput) -> bool {
    input.sse_customer_algorithm.is_some()
        || input.sse_customer_key.is_some()
        || input.sse_customer_key_md5.is_some()
        || input.write_offset_bytes.is_some()
        || input.storage_class.is_some()
        || input.object_lock_mode.is_some()
        || input.object_lock_retain_until_date.is_some()
        || input.object_lock_legal_hold_status.is_some()
        || input.tagging.is_some()
        || input.website_redirect_location.is_some()
        || input.expires.is_some()
}

/// The GET response a read of the object a `PutObject` just stored would produce: the
/// headers the request described the body with, the length actually written, and the
/// `ETag` the origin answered with. It is handed to the very [`CachedObject::from_get`]
/// the read-path fill uses, so a copy kept by a write and a copy filled by a read can
/// never describe the same object differently.
///
/// `last_modified` is the **local write clock**: a `PutObject` response carries no mtime,
/// and paying a HEAD for one would give back the origin round trip the fill exists to
/// save. The index entry is stamped from the same clock for the same reason (see
/// [`CachingProxy::record_put`]), so the two agree with each other; against the origin's
/// own mtime the guarantee is a bound rather than an equality, which is what the
/// differential suite's `assert_same_moment` asserts.
pub(super) fn written_object(
    content_type: Option<String>,
    e_tag: ETag,
    meta: &ObjMeta,
    len: usize,
) -> GetObjectOutput {
    GetObjectOutput {
        content_length: Some(i64::try_from(len).unwrap_or(i64::MAX)),
        content_type,
        e_tag: Some(e_tag),
        last_modified: Some(Timestamp::from(SystemTime::now())),
        // Every S3 object is byte-range addressable and the origin says so on every read;
        // a locally-served one must not be the exception a client notices (the same
        // header an index-served HEAD carries).
        accept_ranges: Some("bytes".to_owned()),
        cache_control: meta.cache_control.clone(),
        content_disposition: meta.content_disposition.clone(),
        content_encoding: meta.content_encoding.clone(),
        content_language: meta.content_language.clone(),
        metadata: meta.metadata.clone(),
        ..Default::default()
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

/// Which write folded a key into the LIST index. Each of these is a separately
/// billed upstream (R2 class A) operation, so each gets its own counter — one
/// lumped `writes_indexed` can't attribute the spend.
#[derive(Clone, Copy)]
pub(super) enum IndexedWrite {
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

/// Wait for every warm-up in `warmups`, then affirm to the coherence lease that this
/// node has caught up and may serve locally again — on behalf of `generation`, so a gap
/// that arrives while the warm-ups run supersedes the affirmation instead of being
/// papered over by it.
///
/// With **no** buckets the join is vacuous and the affirmation is immediate, which is
/// safe rather than a hole: a node told to warm nothing has empty tiers and a
/// passthrough index, so there is no local state for a lease to license serving — every
/// LIST forwards, every GET misses, and every index miss is a miss rather than a 404. The
/// lease shell's own warm-up guard still holds the window shut for this node's first
/// `detection_window_ms + 2 × anti_entropy_interval` regardless, which is the window that
/// matters: it is what keeps a booting node from serving under a roster it has not
/// finished learning.
pub(super) async fn affirm_after(
    warmups: Vec<tokio::task::JoinHandle<()>>,
    sync: Option<Arc<WriteSync>>,
    generation: Option<u64>,
) {
    for warmup in warmups {
        // A warm-up task only ends by succeeding (it retries forever otherwise), so a
        // join error is this process shutting down — nothing to report and nothing left
        // to affirm to.
        let _ = warmup.await;
    }
    if let (Some(sync), Some(generation)) = (sync, generation) {
        sync.affirm_resynced(generation).await;
    }
}

/// S3 service that caches LIST (from an in-memory index) and small GET/HEAD bodies (the
/// hot/warm/cold [`TieredCache`]) in front of an upstream `s3s_aws::Proxy`, forwarding
/// every write.
#[derive(Clone)]
pub struct CachingProxy {
    pub(super) inner: Arc<s3s_aws::Proxy>,
    /// Copy-only upstream with the pre-signing destination-condition adapter.
    pub(super) copy_inner: Arc<s3s_aws::Proxy>,
    /// Direct client used only for the background full LIST warm-up sync.
    pub(super) client: aws_sdk_s3::Client,
    /// The LIST index, `Arc` so the background warm-up task shares it with the serving
    /// proxy (the service takes the proxy by value, so the sync can't borrow `self`).
    pub(super) state: Arc<RwLock<HashMap<String, BucketState>>>,
    /// Object-body cache: hot heap in front of an optional node-local disk tier.
    pub(super) obj_cache: TieredCache,
    /// Objects larger than this are never cached (segments stream straight through).
    pub(super) max_obj_bytes: usize,
    /// Gossip write feed — the whole cross-node coherence layer, when
    /// configured (see [`crate::sync`]).
    pub(super) sync: Option<Arc<WriteSync>>,
    pub(super) metrics: Arc<Metrics>,
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
        let copy_client = copy::conditioned_client(&client);
        Self {
            inner: Arc::new(inner),
            copy_inner: Arc::new(s3s_aws::Proxy::from(copy_client)),
            client,
            state: Arc::new(RwLock::new(HashMap::new())),
            obj_cache: TieredCache::new(cfg.cache_bytes, warm, metrics.clone()),
            max_obj_bytes: cfg.max_obj_bytes,
            sync,
            metrics,
        }
    }

    /// Start the gossip apply loop: peers' events fold into this node's LIST index and
    /// invalidate its body copies; a gap — or, in `strong`, a serve-lease lapse with no
    /// gap behind it whose staged recovery could not prove the cache — *distrusts* every
    /// local body copy (nothing is dropped; each has to prove itself again) and resyncs
    /// `buckets` from the origin. A no-op without gossip — single-node is already strict.
    pub fn start_coherence(&self, buckets: &[String]) {
        let Some(sync) = &self.sync else { return };
        sync.start_apply(
            self.obj_cache.local(),
            self.state.clone(),
            self.gap_resync_handle(buckets.to_vec()),
            self.metrics.clone(),
        );
    }

    /// The index half of the remediation: reset every bucket to passthrough
    /// (unsynced LISTs are always correct) and re-warm the configured ones from
    /// the origin — the authority the index caches.
    ///
    /// Both triggers in `sync` call this — a write-feed gap and a serve-lease lapse with
    /// no gap behind it — and each has already stood the lease down and *distrusted*
    /// every cached body before it does: the copies are still there, and the index this
    /// re-LISTs is what they now have to prove themselves against.
    ///
    /// It also **owns the affirmation** that puts this node back in service: only work
    /// that actually re-synchronized may lift a stand-down, so the generation is read
    /// here, after that stand-down and before the first LIST, and handed to the
    /// affirmation at the end. A second gap arriving mid-resync moves the generation on,
    /// and this affirmation then declines rather than re-opening a window its own resync
    /// no longer covers.
    fn gap_resync_handle(&self, buckets: Vec<String>) -> Arc<dyn Fn() + Send + Sync> {
        let client = self.client.clone();
        let state = self.state.clone();
        let sync = self.sync.clone();
        Arc::new(move || {
            let mut reset: std::collections::BTreeSet<String> =
                state.read().unwrap().keys().cloned().collect();
            reset.extend(buckets.iter().cloned());
            for bucket in reset {
                begin_bucket_resync(&state, &bucket, None);
            }
            let (client, state, buckets) = (client.clone(), state.clone(), buckets.clone());
            let sync = sync.clone();
            let generation = sync.as_ref().map(|sync| sync.resync_gen());
            tokio::spawn(async move {
                for bucket in buckets {
                    if let Err(e) = sync_bucket_into(&client, &state, &bucket).await {
                        tracing::warn!(
                            "gap resync of `{bucket}` failed (staying passthrough): {e}"
                        );
                    }
                }
                if let (Some(sync), Some(generation)) = (sync, generation) {
                    sync.affirm_resynced(generation).await;
                }
            });
        })
    }

    /// Freshness barrier: before serving a read from node-local state (the LIST index,
    /// or a hot/disk body copy), wait until every peer's currently-advertised write-feed
    /// head has been applied locally, so a peer's just-completed write is not read stale.
    /// Freshness is bounded by one push/gossip hop (see [`crate::sync`]); degrades to
    /// serving current state on timeout. No-op without gossip (single-node is strict).
    pub(super) async fn read_barrier(&self, headers: &HeaderMap) -> ReadRoute {
        let Some(sync) = &self.sync else {
            return ReadRoute::Local; // single node: the sole writer is strict
        };
        // The read-side half of transparent coherence: this node may answer from its
        // own state only while it holds the licence its mode issues — a valid coherence
        // lease in `strong`, a fully-alive membership view in `strong-acks`. Without one
        // it may be the partitioned node that writers cannot reach with invalidations,
        // so it serves via the origin until the licence comes back.
        if !sync.may_serve_local() {
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

    /// Whether this node's currently serveable index says every conditional on `input`
    /// would pass. A contradictory origin 412 is evidence that this locally-vouched view
    /// is stale; a condition the index could not prove is merely the client's stale CAS.
    pub(super) fn locally_vouches_for_put(&self, input: &PutObjectInput) -> bool {
        if input.if_match.is_none() && input.if_none_match.is_none() {
            return false;
        }
        if self
            .sync
            .as_ref()
            .is_some_and(|sync| !sync.may_serve_local())
        {
            return false;
        }
        let g = self.state.read().unwrap();
        let Some(bucket) = g.get(input.bucket.as_str()).filter(|bucket| bucket.synced) else {
            return false;
        };
        if bucket.uncertain_keys.contains(input.key.as_str()) {
            return false;
        }
        let entry = bucket.keys.get(input.key.as_str());
        let if_match_holds = input
            .if_match
            .as_ref()
            .is_none_or(|condition| match condition {
                ETagCondition::Any => entry.is_some(),
                ETagCondition::ETag(expected) => {
                    entry.and_then(|entry| entry.etag.as_ref()) == Some(expected)
                }
            });
        let if_none_match_holds =
            input
                .if_none_match
                .as_ref()
                .is_none_or(|condition| match condition {
                    ETagCondition::Any => entry.is_none(),
                    ETagCondition::ETag(expected) => match entry {
                        None => true,
                        Some(entry) => entry.etag.as_ref().is_some_and(|etag| etag != expected),
                    },
                });
        if_match_holds && if_none_match_holds
    }

    /// Fence an ambiguously-mutated key and rebuild its bucket from the origin. The
    /// generation is checked while each page is published and again at completion, so a
    /// newer uncertainty cannot be cleared by this older task.
    pub(super) fn reconcile_uncertain_put(&self, bucket: &str, key: &str, reason: &str) {
        let generation = begin_bucket_resync(&self.state, bucket, Some(key));
        let client = self.client.clone();
        let state = self.state.clone();
        let bucket = bucket.to_owned();
        let key = key.to_owned();
        let reason = reason.to_owned();
        tracing::warn!(
            "origin outcome for PUT `{bucket}/{key}` is uncertain ({reason}); fencing the key and rebuilding the bucket index"
        );
        tokio::spawn(async move {
            let mut backoff = SYNC_RETRY_MIN;
            let mut generation = generation;
            loop {
                if !bucket_resync_is_current(&state, &bucket, generation) {
                    return;
                }
                match sync_bucket_generation(&client, &state, &bucket, generation).await {
                    Ok(found) => {
                        info!(
                            "reconciled uncertain PUT `{bucket}/{key}` from origin: {found} keys"
                        );
                        return;
                    }
                    Err(error) if !bucket_resync_is_current(&state, &bucket, generation) => {
                        tracing::debug!(
                            "origin reconciliation of `{bucket}/{key}` was superseded: {error}"
                        );
                        return;
                    }
                    Err(error) => {
                        tracing::warn!(
                            "origin reconciliation of `{bucket}/{key}` failed (retrying in {backoff:?}): {error}"
                        );
                        let Some(retry_generation) =
                            restart_bucket_resync_if_current(&state, &bucket, generation)
                        else {
                            return;
                        };
                        generation = retry_generation;
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(SYNC_RETRY_MAX);
                    }
                }
            }
        });
    }

    /// A key fenced by an ambiguous mutation may only be read from the origin until the
    /// generation that fenced it has completed its rebuild.
    fn key_uncertain(&self, bucket: &str, key: &str) -> bool {
        self.state
            .read()
            .unwrap()
            .get(bucket)
            .is_some_and(|bucket| bucket.uncertain_keys.contains(key))
    }

    /// Attach a write's session token to a response, when coherence is on.
    pub(super) fn attach_token(headers: &mut HeaderMap, token: Option<String>) {
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
    pub(super) async fn record_put(
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
    pub(super) async fn record_del(&self, bucket: &str, key: &str) -> Option<WriteReceipt> {
        let ts = wire_stamp(SystemTime::now());
        apply_del(&self.state, bucket, key, ts);
        let sync = self.sync.as_ref()?;
        Some(sync.publish_del(bucket, key, ts, &self.metrics).await)
    }

    /// Hold a write's response until every alive peer has applied it, then hand back the
    /// session token for the response header (see [`record_put`](Self::record_put)).
    pub(super) async fn await_cluster(
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
    ///
    /// The **boot affirmation** rides on the same join (see `affirm_after`): a booting
    /// node starts with no right to serve, and gets one only once every bucket it was
    /// told to warm has landed. The generation is read here, before the first warm-up
    /// runs, so a gap arriving during warm-up supersedes this affirmation rather than
    /// being papered over by it.
    pub fn spawn_background_sync(&self, buckets: Vec<String>) {
        let mut warmups = Vec::with_capacity(buckets.len());
        for bucket in buckets {
            let client = self.client.clone();
            let state = self.state.clone();
            warmups.push(tokio::spawn(async move {
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
            }));
        }
        let sync = self.sync.clone();
        let generation = sync.as_ref().map(|sync| sync.resync_gen());
        tokio::spawn(affirm_after(warmups, sync, generation));
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
    pub(super) async fn upstream_meta(&self, bucket: &str, key: &str) -> Option<ObservedObject> {
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

    /// The PUT body held in memory for a fill, or `None` when this write's body is not
    /// one the cache may keep. Either way the request carries the same bytes onward: a
    /// buffered body is put back as itself, and a body that outgrew the cap is spliced
    /// back together and streams through exactly as it does without a fill — a body can
    /// only be read once, so an attempted fill must never cost the forward its bytes.
    pub(super) async fn buffered_put_body(&self, input: &mut PutObjectInput) -> Option<Bytes> {
        // Nothing faithful can be built without a `Content-Type`: with none set the
        // origin invents one (`application/octet-stream` here, `binary/octet-stream` on
        // AWS), and a cached copy reporting none would answer GETs and HEADs the origin
        // answers differently. This is the same bar an index entry has to clear before it
        // may answer a HEAD (`ObjEntry::is_faithful`), for the same reason — such a write
        // still lands and still indexes, its body just stays the origin's to serve once.
        if input.content_type.is_none() || put_origin_only(input) {
            return None;
        }
        // A declared over-cap length is refused before a byte is read, so an object that
        // could never be cached streams through untouched.
        if input.content_length.is_some_and(|len| {
            len < 0 || usize::try_from(len).unwrap_or(usize::MAX) > self.max_obj_bytes
        }) {
            return None;
        }
        let Some(body) = input.body.take() else {
            return Some(Bytes::new()); // a bodiless PUT stores an empty object
        };
        match tier::buffer_or_forward(body, self.max_obj_bytes).await {
            tier::BufferedBody::Whole(bytes) => {
                input.body = Some(tier::blob_of(bytes.clone()));
                Some(bytes)
            }
            // An undeclared length that outgrew the cap mid-read (or a body that faulted):
            // forward what was read plus the untouched remainder, and cache nothing.
            tier::BufferedBody::Streamed(rest) => {
                input.body = Some(rest);
                None
            }
        }
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
        // Read before the fetch, not after it: a remediation that distrusts the cache
        // while this round-trip is in flight must leave the copy it lands suspect.
        let generation = self.obj_cache.suspect_gen();
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
            let obj = CachedObject::from_get(&resp.output, body);
            obj.mark_trusted(generation);
            Ok::<_, String>(Arc::new(obj))
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

    pub(super) fn is_synced(&self, bucket: &str) -> bool {
        self.state
            .read()
            .unwrap()
            .get(bucket)
            .is_some_and(|b| b.synced)
    }

    /// Build a `ListObjectsV2` response from this bucket's index; `None` when the index
    /// cannot answer without inventing a value. See [`list_objects_v2_from_index`].
    pub(super) fn list_from_index(&self, inp: &ListObjectsV2Input) -> Option<ListObjectsV2Output> {
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
    /// write feed — always: the sole writer's index is the truth. With gossip it is the
    /// mode's own licence for a local answer (see [`WriteSync::may_answer_404`]): a
    /// valid coherence lease in `strong`, a settled view in `strong-acks`/`bounded`.
    /// Without one the origin answers instead, which is slower and never wrong.
    fn index_404_trustworthy(&self) -> bool {
        self.sync.as_ref().is_none_or(|sync| sync.may_answer_404())
    }

    /// A cached body this node is entitled to serve, or `None` — every read that answers
    /// from a local copy goes through here.
    ///
    /// A hit is not by itself a licence. The warm tier outlives the process, so a body on
    /// that disk may predate writes made while this node was down; a body in memory may
    /// predate a remediation that distrusted the whole cache. So a copy is served only
    /// once it is *proved*, and the proof is deliberately cheap in the case that is
    /// almost every case:
    ///
    /// * **Single node** — no write feed, so this proxy is the sole writer and its own
    ///   tiers cannot have missed anything, restart included. Served as-is, which is the
    ///   warm tier's whole value proposition and what `tests/tier_cache.rs` asserts.
    /// * **Already proved** — one relaxed load ([`CachedObject::trusted`]). The steady
    ///   state, and it costs nothing.
    /// * **Suspect, synced bucket** — the LIST index is this node's own re-read of the
    ///   origin, so it can arbitrate: a matching `ETag` and an mtime it has not moved
    ///   past ([`entry_matches_body`]) proves the copy, and the stamp puts the next read
    ///   of it back on the fast path. Anything else — a different version, a comparison
    ///   neither side carries, or a key the index no longer holds, which is precisely the
    ///   DELETE this node missed — drops the copy and sends the read to the origin.
    /// * **Suspect, unsynced bucket** — nothing to arbitrate with, so nothing is served
    ///   and the copy is dropped. That is the same outcome a flush would have produced
    ///   for this key, reached one key at a time instead of all at once.
    ///
    /// Both `None` arms invalidate **before** returning, which is what keeps the fill
    /// that follows honest: [`TieredCache::get_or_fetch`] re-probes the tiers under its
    /// singleflight gate, and without the eviction that probe would hand back the very
    /// copy this call just refused. What the probe *can* still find is a body a
    /// concurrent fill landed in between — fetched from the origin after this eviction,
    /// so newer than what was dropped, and stamped current by that fill.
    pub(super) async fn validated_get(&self, ckey: &(String, String)) -> Option<Arc<CachedObject>> {
        let obj = self.obj_cache.get(ckey).await?;
        if self.sync.is_none() {
            return Some(obj);
        }
        let generation = self.obj_cache.suspect_gen();
        if obj.trusted(generation) {
            return Some(obj);
        }
        // Scoped so the index guard is dropped before the awaits below. `None` is "this
        // bucket cannot arbitrate", which is not the same answer as "it says no".
        let proved = {
            let g = self.state.read().unwrap();
            g.get(ckey.0.as_str()).filter(|b| b.synced).map(|b| {
                b.keys
                    .get(ckey.1.as_str())
                    .is_some_and(|entry| entry_matches_body(entry, &obj))
            })
        };
        match proved {
            Some(true) => {
                obj.mark_trusted(generation);
                self.metrics.body_revalidation();
                Some(obj)
            }
            Some(false) => {
                self.obj_cache.invalidate(ckey).await;
                self.metrics.body_revalidation_eviction();
                None
            }
            None => {
                self.obj_cache.invalidate(ckey).await;
                None
            }
        }
    }

    /// The GET decision tree, with the per-request `response-*` overrides already lifted
    /// off `req`: every copy this serves or fills is of the object as the origin stores
    /// it, so one client's formatting is never handed to the next and the caller puts
    /// the overrides back on the way out.
    pub(super) async fn serve_get(
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
            !self.key_uncertain(&ckey.0, &ckey.1)
                && self.read_barrier(&req.headers).await == ReadRoute::Local
        } else {
            true
        };
        if let Some((first, last)) = int_range {
            // Cached whole object → serve the slice locally.
            if local_ok
                && let Some(obj) = self.validated_get(&ckey).await
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
            && let Some(obj) = self.validated_get(&ckey).await
        {
            self.metrics.get_hit();
            return Ok(S3Response::new(obj.to_get()));
        }
        if !cacheable {
            self.metrics.get_bypass();
            return self.inner.get_object(req).await;
        }
        // LIST/write-through already tells us when a body cannot fit. Preserve
        // streaming throughput for those objects instead of putting them through
        // the fill gate only to rediscover the cap from GET's Content-Length.
        let known_oversized = self.index_size(&ckey.0, &ckey.1).is_some_and(|sz| {
            sz >= 0 && usize::try_from(sz).unwrap_or(usize::MAX) > self.max_obj_bytes
        });
        if known_oversized {
            self.metrics.get_bypass();
            return self.inner.get_object(req).await;
        }

        // Read before the fetch, not after it: a remediation that distrusts the cache
        // while this round-trip is in flight must leave the copy it lands suspect.
        // Whole-object misses use the SAME probe-then-gate singleflight as range
        // promotion. The old path fetched directly here, so a burst of identical cold
        // Docres reads issued one R2 GET per waiter even though the tier already had the
        // machinery to make them share one fill. Different keys still fetch fully in
        // parallel; only duplicate work for this exact key waits on the gate.
        let generation = self.obj_cache.suspect_gen();
        let cap = self.max_obj_bytes;
        let inner = &self.inner;
        let origin = async {
            let mut resp = inner
                .get_object(req)
                .await
                .map_err(WholeGetFillError::Origin)?;
            let len = resp.output.content_length.unwrap_or(-1);
            if len < 0 || usize::try_from(len).unwrap_or(usize::MAX) > cap {
                return Err(WholeGetFillError::Uncacheable(Box::new(resp)));
            }
            let Some(body) = resp.output.body.take() else {
                return Err(WholeGetFillError::Uncacheable(Box::new(resp)));
            };
            let bytes = tier::buffer_body(body, cap).await.ok_or_else(|| {
                WholeGetFillError::Origin(s3s::s3_error!(
                    InternalError,
                    "s3cache: failed to buffer body"
                ))
            })?;
            let observed = observed!(&resp.output);
            let filled = CachedObject::from_get(&resp.output, bytes);
            filled.mark_trusted(generation);
            // The whole of what a HEAD reports is in hand, so the index entry is
            // completed here too — a body fill is the cheapest place to turn a
            // skeletal entry faithful, and it costs the origin nothing extra.
            self.observe(&ckey.0, &ckey.1, &observed);
            self.metrics.get_miss();
            Ok(Arc::new(filled))
        };
        match self.obj_cache.get_or_fetch_with(&ckey, origin).await {
            Ok(filled) => Ok(S3Response::new(filled.to_get())),
            Err(WholeGetFillError::Origin(error)) => Err(error),
            Err(WholeGetFillError::Uncacheable(resp)) => {
                self.metrics.get_bypass();
                Ok(*resp)
            }
        }
    }

    /// The HEAD decision tree, with the `response-*` overrides already lifted off `req`
    /// (see [`serve_get`](Self::serve_get)).
    pub(super) async fn serve_head(
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
        if cache_eligible
            && !self.key_uncertain(&ckey.0, &ckey.1)
            && self.read_barrier(&req.headers).await == ReadRoute::Local
        {
            if let Some(obj) = self.validated_get(&ckey).await {
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
