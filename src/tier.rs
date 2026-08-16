//! Layered object-body cache: **hot** (node-local heap) in front of **warm** (a node-local
//! on-disk cache) in front of **cold** (the S3 origin). Always layered — there is no mode
//! to pick. Built on `tierstore`: the hot tier is a byte-weighted moka cache (sharded,
//! `TinyLFU` admission, via `tierstore-moka`), the warm
//! tier is a byte-budgeted, restart-surviving mmap-disk store. Its record codec keeps
//! metadata in a small bincode header and the body as a raw mmap-backed tail, so warm
//! reads parse metadata without copying object bytes back onto the heap. Blocking file
//! I/O runs on a dedicated offload pool. Warm is inclusive (fills write hot *and* disk) and
//! best-effort by policy: a disk error or oversize rejection never blocks the hot fill or
//! the data plane. Origin fetches are singleflighted probe-then-gate, so only misses
//! contend and concurrent callers share one round-trip. Cross-node coherence is separate
//! (see `sync`): a peer's write invalidates the local hot *and* disk copies, and
//! strict reads barrier on feed heads.

use std::future::Future;
use std::mem::size_of;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use s3s::dto::{ETag, GetObjectOutput, HeadObjectOutput, Metadata, StreamingBlob, Timestamp};
use serde::{Deserialize, Serialize};
use tierstore::{CodecTier, Eviction, KeyStatus, OffloadTier, SingleFlight};
use tierstore_mmap::MmapDiskTier;
use tierstore_moka::MokaTier;

use crate::metrics::Metrics;

/// `(bucket, key)` — the cache's addressing unit.
pub type CacheKey = (String, String);

/// A cached object body plus the response metadata needed to reconstruct a GET/HEAD.
/// `Serialize`/`Deserialize` are retained for backward reads of the original
/// all-bincode warm record; new records encode only metadata and keep `body`
/// as an mmap-backed tail.
///
/// Deliberately **not** `Clone`: the tiers hand out `Arc<CachedObject>`, and a copy's
/// trust stamp (below) is bookkeeping about *that* copy — a clone would have to decide
/// whether to carry it, and either answer would be wrong somewhere.
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
    /// The trust generation this copy was last proved current under (see
    /// [`TieredCache::suspect_gen`]). Interior-mutable because the tiers only ever hand
    /// out `Arc<CachedObject>`, and this is proof-of-freshness *about* a copy rather
    /// than part of the object it describes.
    ///
    /// **Skipped by serde**, which is load-bearing twice over: the warm tier's on-disk
    /// encoding does not move (entries written by a binary without this field decode
    /// unchanged), and — because a decoded entry defaults to `0` while a live cache's
    /// generation starts at `1` — every copy that comes back off disk is born
    /// *suspect*. That is the point: a node whose disk outlives its process cannot
    /// vouch for what happened to those objects while it was down.
    #[serde(skip)]
    trusted_gen: AtomicU64,
}

/// Small serialized prefix for the current warm record format. The body is
/// deliberately absent: it follows this header verbatim and becomes a
/// zero-copy [`Bytes::slice`] of the mmap on decode.
#[derive(Serialize, Deserialize)]
struct WarmHeader {
    key: CacheKey,
    body_len: u64,
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

impl WarmHeader {
    fn capture(key: &CacheKey, object: &CachedObject) -> Self {
        Self {
            key: key.clone(),
            body_len: u64::try_from(object.body.len()).unwrap_or(u64::MAX),
            content_length: object.content_length,
            content_type: object.content_type.clone(),
            e_tag: object.e_tag.clone(),
            last_modified: object.last_modified.clone(),
            cache_control: object.cache_control.clone(),
            content_encoding: object.content_encoding.clone(),
            content_language: object.content_language.clone(),
            content_disposition: object.content_disposition.clone(),
            accept_ranges: object.accept_ranges.clone(),
            metadata: object.metadata.clone(),
        }
    }

    fn finish(self, body: Bytes) -> (CacheKey, Arc<CachedObject>) {
        let object = CachedObject {
            body,
            content_length: self.content_length,
            content_type: self.content_type,
            e_tag: self.e_tag,
            last_modified: self.last_modified,
            cache_control: self.cache_control,
            content_encoding: self.content_encoding,
            content_language: self.content_language,
            content_disposition: self.content_disposition,
            accept_ranges: self.accept_ranges,
            metadata: self.metadata,
            trusted_gen: AtomicU64::new(0),
        };
        (self.key, Arc::new(object))
    }
}

/// Replays the cached response fields into `$out`, defaulting the rest. `GetObjectOutput`
/// and `HeadObjectOutput` spell these fields identically, so one list drives both: a
/// cached HEAD reports exactly what a cached GET does, and neither can drift from the
/// other as fields are added.
macro_rules! replay_meta {
    ($src:expr, $out:ident) => {{
        let src = $src;
        $out {
            content_length: src.content_length,
            content_type: src.content_type.clone(),
            e_tag: src.e_tag.clone(),
            last_modified: src.last_modified.clone(),
            cache_control: src.cache_control.clone(),
            content_encoding: src.content_encoding.clone(),
            content_language: src.content_language.clone(),
            content_disposition: src.content_disposition.clone(),
            accept_ranges: src.accept_ranges.clone(),
            metadata: src.metadata.clone(),
            ..Default::default()
        }
    }};
}

impl CachedObject {
    /// Capture a GET response's metadata alongside its (already-buffered) body.
    #[must_use]
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
            // Born suspect. Only the fill site knows which generation the bytes were
            // fetched or written under, and it stamps this on the way into the tiers.
            trusted_gen: AtomicU64::new(0),
        }
    }

    /// Whether this copy was proved current under `generation` — one relaxed load, which
    /// is the whole of the steady-state read path's coherence check.
    ///
    /// `Relaxed` orders nothing because there is nothing to order: the bytes this
    /// describes are immutable and were published by the `Arc` itself. Reading a stale
    /// `false` costs one revalidation. A read that races a generation bump is the same
    /// race a read racing an invalidation always is, and is bounded the same way — by
    /// the barrier the caller already passed.
    #[must_use]
    pub fn trusted(&self, generation: u64) -> bool {
        self.trusted_gen.load(Ordering::Relaxed) == generation
    }

    /// Stamp this copy as proved current under `generation`: what a fill does on the way
    /// in, and what a revalidation does once the LIST index has confirmed the copy.
    pub fn mark_trusted(&self, generation: u64) {
        self.trusted_gen.store(generation, Ordering::Relaxed);
    }

    /// The `ETag` the origin reported for these bytes, when it reported one — the handle
    /// a revalidation compares against the LIST index.
    pub(crate) fn e_tag(&self) -> Option<&ETag> {
        self.e_tag.as_ref()
    }

    /// When the object these bytes came from was last modified, as the fill learned it:
    /// the origin's `Last-Modified` on a read fill, the local write clock on a write one.
    pub(crate) fn last_modified(&self) -> Option<&Timestamp> {
        self.last_modified.as_ref()
    }

    fn body_blob(&self) -> StreamingBlob {
        blob_of(self.body.clone())
    }

    /// The cached response metadata as a body-less GET — the base every GET-shaped
    /// response fills in.
    fn meta_get(&self) -> GetObjectOutput {
        replay_meta!(self, GetObjectOutput)
    }

    /// Reconstruct a full-body GET response from the cached copy.
    #[must_use]
    pub fn to_get(&self) -> GetObjectOutput {
        GetObjectOutput {
            body: Some(self.body_blob()),
            ..self.meta_get()
        }
    }

    /// A 206-shaped GET for an inclusive byte range sliced out of the cached body
    /// (clamped at EOF). `None` when the range start is past the object.
    #[must_use]
    pub fn to_get_range(&self, first: u64, last: Option<u64>) -> Option<GetObjectOutput> {
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
            ..self.meta_get()
        })
    }

    /// Reconstruct a HEAD response from the cached metadata.
    #[must_use]
    pub fn to_head(&self) -> HeadObjectOutput {
        replay_meta!(self, HeadObjectOutput)
    }
}

/// A one-shot body stream over bytes already in memory.
#[must_use]
pub fn blob_of(bytes: Bytes) -> StreamingBlob {
    StreamingBlob::wrap(futures::stream::once(async move {
        Ok::<Bytes, std::io::Error>(bytes)
    }))
}

/// What draining a body against a cap produced (see [`buffer_or_forward`]).
pub enum BufferedBody {
    /// The whole body, within the cap.
    Whole(Bytes),
    /// The body did not fit the cap, or the stream faulted: what was drained, spliced
    /// back in front of the untouched remainder.
    Streamed(StreamingBlob),
}

/// Drain a streamed body into memory, up to `cap` bytes.
///
/// Past the cap the prefix is **not** discarded — it is spliced back in front of the rest
/// of the stream, so a caller that has to forward the body anyway forwards exactly the
/// bytes the client sent. A body can only be read once, and a proxy that has started
/// reading one has no way to un-read it; this is what lets the write path attempt a cache
/// fill without committing to it.
pub async fn buffer_or_forward(mut blob: StreamingBlob, cap: usize) -> BufferedBody {
    let mut buf: Vec<u8> = Vec::new();
    while let Some(chunk) = blob.next().await {
        match chunk {
            Ok(b) if buf.len() + b.len() <= cap => buf.extend_from_slice(&b),
            over => {
                let head = Bytes::from(buf);
                let tail = futures::stream::once(async move { over.map_err(io_error) })
                    .chain(blob.map_err(io_error));
                return BufferedBody::Streamed(StreamingBlob::wrap(
                    futures::stream::once(async move { Ok(head) }).chain(tail),
                ));
            }
        }
    }
    BufferedBody::Whole(Bytes::from(buf))
}

/// A body stream's boxed error as an `io::Error`, the error type a re-wrapped
/// [`StreamingBlob`] carries.
fn io_error(e: s3s::StdError) -> std::io::Error {
    std::io::Error::other(e)
}

/// Drain a streamed body into memory, bailing (`None`) past `cap` bytes or on error. The
/// unread remainder is dropped with it — for the paths that have no use for a body they
/// cannot cache (see [`buffer_or_forward`] for the one that does).
pub async fn buffer_body(blob: StreamingBlob, cap: usize) -> Option<Bytes> {
    match buffer_or_forward(blob, cap).await {
        BufferedBody::Whole(bytes) => Some(bytes),
        BufferedBody::Streamed(_) => None,
    }
}

/// Blake3 of `bucket\0key` — a fixed-length, path-safe, collision-free warm-tier key.
fn warm_key(bucket: &str, key: &str) -> String {
    let mut h = blake3::Hasher::new();
    h.update(bucket.as_bytes());
    h.update(&[0]);
    h.update(key.as_bytes());
    h.finalize().to_hex().to_string()
}

/// Current warm-record discriminator. A legacy bincode tuple starts with a
/// string length, so no valid legacy record can collide with these bytes.
const WARM_MAGIC: &[u8; 8] = b"S3CWRM01";
const WARM_PREFIX_LEN: usize = WARM_MAGIC.len() + size_of::<u32>();

/// Encodes the metadata header and raw body tail. `None` means the complete
/// record exceeds the configured per-object cap; codec failures stay errors.
fn encode_warm_record(
    key: &CacheKey,
    object: &CachedObject,
    max_bytes: usize,
) -> Result<Option<Bytes>, tierstore::BoxError> {
    let header = bincode::serialize(&WarmHeader::capture(key, object))?;
    let header_len = u32::try_from(header.len())
        .map_err(|_| -> tierstore::BoxError { "warm record header exceeds u32".into() })?;
    let total = WARM_PREFIX_LEN
        .checked_add(header.len())
        .and_then(|len| len.checked_add(object.body.len()))
        .ok_or_else(|| -> tierstore::BoxError { "warm record length overflow".into() })?;
    if total > max_bytes {
        return Ok(None);
    }

    let mut encoded = Vec::with_capacity(total);
    encoded.extend_from_slice(WARM_MAGIC);
    encoded.extend_from_slice(&header_len.to_le_bytes());
    encoded.extend_from_slice(&header);
    encoded.extend_from_slice(&object.body);
    Ok(Some(Bytes::from(encoded)))
}

/// Decodes current framed records without copying the body. Original
/// all-bincode entries remain readable and age out naturally under LRU.
fn decode_warm_record(bytes: &Bytes) -> Result<(CacheKey, Arc<CachedObject>), tierstore::BoxError> {
    if !bytes.starts_with(WARM_MAGIC) {
        let (key, object): (CacheKey, CachedObject) = bincode::deserialize(bytes)?;
        return Ok((key, Arc::new(object)));
    }

    let header_len_bytes = bytes
        .get(WARM_MAGIC.len()..WARM_PREFIX_LEN)
        .ok_or_else(|| -> tierstore::BoxError { "truncated warm record prefix".into() })?;
    let header_len =
        usize::try_from(u32::from_le_bytes(header_len_bytes.try_into().map_err(
            |_| -> tierstore::BoxError { "invalid warm header length".into() },
        )?))
        .map_err(|_| -> tierstore::BoxError { "warm header length exceeds usize".into() })?;
    let body_start = WARM_PREFIX_LEN
        .checked_add(header_len)
        .filter(|end| *end <= bytes.len())
        .ok_or_else(|| -> tierstore::BoxError { "truncated warm record header".into() })?;
    let header: WarmHeader = bincode::deserialize(&bytes[WARM_PREFIX_LEN..body_start])?;
    let body = bytes.slice(body_start..);
    if u64::try_from(body.len()).unwrap_or(u64::MAX) != header.body_len {
        return Err("warm record body length mismatch".into());
    }
    Ok(header.finish(body))
}

/// The warm tier: typed objects over a byte-budgeted, restart-surviving mmap-disk store,
/// with the blocking file I/O on a dedicated worker pool. The codec embeds the cache key
/// in the encoding, so entries the disk tier evicts decode back fully typed.
pub type WarmTier = OffloadTier<CodecTier<Arc<MmapDiskTier>, CacheKey, Arc<CachedObject>>>;

/// A warm tier plus a maintenance handle to its underlying disk store (for
/// [`LocalCache::flush`] — the tier stack has no whole-cache clear).
pub type WarmPair = (WarmTier, Arc<MmapDiskTier>);

/// Worker threads for the warm tier's blocking file I/O.
const WARM_IO_THREADS: usize = 4;

/// The codec's rejection message for an object whose encoding does not fit the
/// per-object cap.
const WARM_TOO_LARGE: &str = "encoded object exceeds S3CACHE_MAX_OBJECT_BYTES";

/// Open (creating if needed) the warm disk tier under `dir`, byte-bounded to `disk_bytes`;
/// files already present are re-indexed so the cache survives restarts. Objects whose
/// encoding exceeds `max_obj_bytes` are rejected by the codec — under the cache's
/// best-effort write policy that skips the disk fill without failing the hot one, which
/// is also why the rejection is counted inside the codec: it never reaches a caller.
///
/// # Errors
///
/// The I/O error from creating or re-indexing `dir`.
pub fn open_warm(
    dir: PathBuf,
    disk_bytes: u64,
    max_obj_bytes: usize,
    metrics: Arc<Metrics>,
) -> anyhow::Result<WarmPair> {
    let budget = NonZeroU64::new(disk_bytes.max(1)).unwrap_or(NonZeroU64::MIN);
    let disk = Arc::new(MmapDiskTier::open_bounded(dir, budget)?.with_eviction(Eviction::Lru));
    metrics.observe_warm(disk.stats());
    let codec = CodecTier::new(
        Arc::clone(&disk),
        |key: &CacheKey| warm_key(&key.0, &key.1),
        move |key: &CacheKey, obj: &Arc<CachedObject>| {
            if let Some(bytes) = encode_warm_record(key, obj, max_obj_bytes)? {
                Ok(bytes)
            } else {
                // The configured cap doing its job, not a failure — kept out of
                // `warm_error` so that counter stays something an operator can page on.
                metrics.warm_reject();
                Err(WARM_TOO_LARGE.into())
            }
        },
        |bytes: Bytes| decode_warm_record(&bytes),
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
    /// The generation a copy must carry to be served without proving itself first.
    ///
    /// It starts at **1**, never 0, and that one digit is the whole PVC-restart story:
    /// a warm-tier entry decodes with `trusted_gen: 0` (the field is serde-skipped), so
    /// on the far side of a restart every object on disk is suspect until something
    /// revalidates it. Bumping this ([`LocalCache::distrust_all`]) makes every copy
    /// currently held suspect without dropping one.
    suspect_gen: AtomicU64,
}

impl Core {
    fn observe_warm(&self) {
        if let Some(disk) = &self.warm_disk {
            self.metrics.observe_warm(disk.stats());
        }
    }

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
                self.observe_warm();
            }
            Some(value)
        } else {
            if self.has_warm {
                self.metrics.warm_miss();
                self.observe_warm();
            }
            None
        }
    }

    /// Best-effort fill of hot and (inclusively) warm: a disk rejection or I/O error is
    /// skipped by policy, never surfaced to the data plane. The per-object-cap rejection
    /// is counted by the codec itself (see [`open_warm`]), precisely because the policy
    /// means it never arrives here.
    async fn insert(&self, key: CacheKey, obj: Arc<CachedObject>) {
        if self.cache.put(key, obj).await.is_err() {
            self.metrics.warm_error();
        }
        self.observe_warm();
    }

    /// Drop an object from every local tier. A tier that fails to delete is counted; its
    /// copy lingers only until eviction and is never authoritative.
    async fn invalidate(&self, key: &CacheKey) {
        if self.cache.invalidate(key).await.is_err() {
            self.metrics.warm_error();
        }
        self.observe_warm();
    }

    /// Drop EVERY local copy: the hot tier empties immediately, the disk store unlinks
    /// its files on a blocking worker.
    ///
    /// The blunt instrument. It throws away every *correct* body along with the suspect
    /// ones and buys them all back from the origin, which is why the retention path
    /// ([`Core::suspect_gen`]) exists — see [`LocalCache::flush`] for what still uses it.
    async fn flush(&self) {
        self.hot.inner().invalidate_all();
        if let Some(disk) = &self.warm_disk {
            let disk = Arc::clone(disk);
            let cleared = tokio::task::spawn_blocking(move || disk.clear()).await;
            if !matches!(cleared, Ok(Ok(()))) {
                self.metrics.warm_error();
            }
            self.observe_warm();
        }
    }
}

/// The layered object-body cache handle held by the proxy.
#[derive(Clone)]
pub struct TieredCache {
    core: Arc<Core>,
}

impl TieredCache {
    /// Build the cache: a hot LRU weighted by body bytes up to `cache_bytes`, plus the
    /// optional warm disk tier. Fill singleflight is handled here (probe-then-gate), so
    /// the tierstore-level gate is disabled.
    #[must_use]
    pub fn new(cache_bytes: u64, warm: Option<WarmPair>, metrics: Arc<Metrics>) -> Self {
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
                suspect_gen: AtomicU64::new(1),
            }),
        }
    }

    /// The generation a copy must carry to be served without revalidation (see
    /// [`CachedObject::trusted`]). A fill stamps what this reads; a hit compares against
    /// it; [`LocalCache::distrust_all`] moves it on.
    #[must_use]
    pub fn suspect_gen(&self) -> u64 {
        self.core.suspect_gen.load(Ordering::Relaxed)
    }

    /// A handle the commit-log consumer uses to invalidate this node's local copies.
    #[must_use]
    pub fn local(&self) -> LocalCache {
        LocalCache {
            core: Arc::clone(&self.core),
        }
    }

    /// Look up a whole cached object: hot, then warm disk (a disk hit promotes to hot).
    pub async fn get(&self, key: &CacheKey) -> Option<Arc<CachedObject>> {
        self.core.lookup(key).await
    }

    /// Store into hot and (inclusively) the warm disk tier, best-effort.
    pub async fn insert(&self, key: CacheKey, obj: Arc<CachedObject>) {
        self.core.insert(key, obj).await;
    }

    /// Drop an object from every local tier.
    pub async fn invalidate(&self, key: &CacheKey) {
        self.core.invalidate(key).await;
    }

    /// Get `key`, or run `origin` to fetch it and populate the tiers. Kept local (rather
    /// than `tierstore::TieredCache::get_or_load`) because the probes here feed the
    /// per-request warm metrics via tier provenance. Probe-then-gate
    /// singleflight: hot hits never touch the gate; concurrent misses for one key share a
    /// single origin round-trip (followers re-probe under the gate and reuse the leader's
    /// fill). Errors are not cached.
    ///
    /// # Errors
    ///
    /// Exactly `origin`'s error, when the object was not cached and the fetch failed.
    pub async fn get_or_fetch<Fut>(
        &self,
        key: &CacheKey,
        origin: Fut,
    ) -> Result<Arc<CachedObject>, String>
    where
        Fut: Future<Output = Result<Arc<CachedObject>, String>> + Send,
    {
        self.get_or_fetch_with(key, origin).await
    }

    /// The typed-error form used by the S3 proxy, whose leader must preserve an
    /// origin [`s3s::S3Error`] or an uncacheable streaming response verbatim.
    pub(crate) async fn get_or_fetch_with<Fut, E>(
        &self,
        key: &CacheKey,
        origin: Fut,
    ) -> Result<Arc<CachedObject>, E>
    where
        Fut: Future<Output = Result<Arc<CachedObject>, E>> + Send,
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
pub struct LocalCache {
    core: Arc<Core>,
}

impl LocalCache {
    /// Drop a key from every node-local tier so a peer's overwrite is never read stale.
    pub async fn invalidate(&self, key: &CacheKey) {
        self.core.invalidate(key).await;
    }

    /// Distrust every node-local copy without dropping one: the generation moves on, so
    /// each cached body must prove itself current — against the LIST index, which the
    /// same remediation re-LISTs from the origin — before it is served again, and is
    /// dropped only if it cannot. The retention half of the pair; nothing correct is
    /// thrown away, and a copy that is still current costs one index lookup rather than
    /// an origin GET.
    pub fn distrust_all(&self) {
        self.core.suspect_gen.fetch_add(1, Ordering::Relaxed);
    }

    /// Drop every node-local copy (see `Core::flush`) — the escape hatch, for a stale
    /// set that cannot be revalidated at all. It is strictly more expensive than
    /// [`distrust_all`](Self::distrust_all) and never more correct, so a remediation
    /// reaches for it only when there is nothing left to revalidate *against*.
    pub async fn flush(&self) {
        self.core.flush().await;
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BufferedBody, CachedObject, TieredCache, buffer_body, buffer_or_forward, open_warm,
    };
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

    /// A counter set for a test that does not read it back.
    fn metrics() -> Arc<Metrics> {
        Arc::new(Metrics::default())
    }

    #[test]
    fn legacy_cached_object_bincode_roundtrip() {
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

    #[test]
    fn framed_warm_decode_slices_the_record_body_without_copying() {
        let key = ck("b", "k");
        let object = sample();
        let encoded = super::encode_warm_record(&key, &object, 1024)
            .expect("encode")
            .expect("within cap");
        let mapping = encoded.as_ptr().addr()..encoded.as_ptr().addr() + encoded.len();

        let (decoded_key, decoded) = super::decode_warm_record(&encoded).expect("decode");

        assert_eq!(decoded_key, key);
        assert_eq!(decoded.body, object.body);
        assert!(
            mapping.contains(&decoded.body.as_ptr().addr()),
            "the decoded body must remain a slice of the encoded owner"
        );
    }

    #[tokio::test]
    async fn current_warm_records_keep_the_body_mmap_backed() {
        let dir = temp_dir();
        let _ = std::fs::remove_dir_all(&dir);
        let (warm, disk) =
            open_warm(dir.clone(), 10 * 1024 * 1024, 8 * 1024 * 1024, metrics()).unwrap();
        let key = ck("b", "mapped");
        warm.put(key.clone(), Arc::new(sample()))
            .await
            .expect("put");
        let mapped = disk
            .get(&super::warm_key(&key.0, &key.1))
            .await
            .expect("disk get")
            .expect("mapped record");
        let mapping = mapped.as_ptr().addr()..mapped.as_ptr().addr() + mapped.len();

        let decoded = warm.get(&key).await.expect("warm get").expect("hit");

        assert!(
            mapping.contains(&decoded.body.as_ptr().addr()),
            "the served body must point into the mmap rather than anonymous heap"
        );
        assert_eq!(disk.stats().mapped_entries, 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn legacy_warm_records_remain_readable() {
        let dir = temp_dir();
        let _ = std::fs::remove_dir_all(&dir);
        let (warm, disk) =
            open_warm(dir.clone(), 10 * 1024 * 1024, 8 * 1024 * 1024, metrics()).unwrap();
        let key = ck("b", "legacy");
        let object = sample();
        let legacy = Bytes::from(bincode::serialize(&(&key, &object)).expect("legacy encode"));
        disk.put(super::warm_key(&key.0, &key.1), legacy)
            .await
            .expect("seed legacy record");

        let decoded = warm.get(&key).await.expect("warm get").expect("hit");

        assert_eq!(decoded.body, object.body);
        assert_eq!(decoded.e_tag, object.e_tag);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The trust stamp is bookkeeping about a *copy*, not part of the object, and
    /// `#[serde(skip)]` is what keeps it off the warm tier's disk format.
    ///
    /// Asserted without a fixture of the old bytes, which would only ever prove what
    /// this build's serializer does anyway: the stamp is varied across the widest value
    /// it can hold and the encoding must not move by a byte. A field that reached the
    /// wire — at any width, in any position, even as a length — could not survive that.
    /// The decode side then pins the consequence: whatever was stamped, what comes back
    /// off disk is suspect.
    #[test]
    fn the_trust_stamp_never_reaches_the_warm_tier() {
        let obj = sample();
        let unstamped = bincode::serialize(&obj).unwrap();
        for generation in [1, 0x0102_0304_0506_0708, u64::MAX] {
            obj.mark_trusted(generation);
            assert_eq!(
                bincode::serialize(&obj).unwrap(),
                unstamped,
                "generation {generation} left a trace in the encoding"
            );
        }

        let back: CachedObject = bincode::deserialize(&unstamped).unwrap();
        assert_eq!(back.body, obj.body, "the object itself round-trips");
        assert!(
            back.trusted(0),
            "a decoded entry carries the default generation"
        );
        assert!(
            !back.trusted(1),
            "which a live cache never issues — so warm entries are born suspect"
        );
    }

    /// A restart is the case the generation floor exists for: the object comes back off
    /// disk intact and is *not* trusted, because nothing on this node saw what happened
    /// to it in between.
    #[tokio::test]
    async fn a_warm_tier_entry_comes_back_suspect() {
        let dir = temp_dir();
        let _ = std::fs::remove_dir_all(&dir);
        let cap = 10 * 1024 * 1024;

        let (warm, _disk) = open_warm(dir.clone(), cap, 8 * 1024 * 1024, metrics()).unwrap();
        let obj = Arc::new(sample());
        obj.mark_trusted(1); // trusted in the process that filled it
        warm.put(ck("b", "k"), obj).await.unwrap();

        // A fresh cache over the same directory: a new process, a new hot tier.
        let warm2 = open_warm(dir.clone(), cap, 8 * 1024 * 1024, metrics()).unwrap();
        let cache = TieredCache::new(1024 * 1024, Some(warm2), metrics());
        let back = cache
            .get(&ck("b", "k"))
            .await
            .expect("re-indexed from disk");
        assert!(
            !back.trusted(cache.suspect_gen()),
            "a warm-decoded body is suspect until something revalidates it"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The stamp/compare mechanics, and the generation floor that makes a decoded `0`
    /// mean "unproved" rather than "proved under generation zero".
    #[test]
    fn trust_is_stamped_and_read_per_generation() {
        let cache = TieredCache::new(1024, None, metrics());
        assert_eq!(cache.suspect_gen(), 1, "a live cache never issues 0");

        let obj = sample();
        assert!(!obj.trusted(cache.suspect_gen()), "born suspect");
        obj.mark_trusted(cache.suspect_gen());
        assert!(obj.trusted(cache.suspect_gen()));
        assert!(
            !obj.trusted(2),
            "a stamp only speaks for its own generation"
        );
    }

    /// Distrusting keeps every copy and invalidates every proof: the object is still
    /// served by the tiers, it just has to prove itself again.
    #[tokio::test]
    async fn distrust_all_moves_the_generation_on_without_dropping_a_copy() {
        let cache = TieredCache::new(1024 * 1024, None, metrics());
        let obj = Arc::new(sample());
        obj.mark_trusted(cache.suspect_gen());
        cache.insert(ck("b", "k"), Arc::clone(&obj)).await;

        let before = cache.suspect_gen();
        cache.local().distrust_all();
        assert_eq!(cache.suspect_gen(), before + 1);

        let held = cache
            .get(&ck("b", "k"))
            .await
            .expect("the copy is still here");
        assert!(
            !held.trusted(cache.suspect_gen()),
            "but it no longer counts as proved"
        );
        held.mark_trusted(cache.suspect_gen());
        assert!(obj.trusted(cache.suspect_gen()), "one copy, one stamp");
    }

    #[tokio::test]
    async fn buffer_body_respects_cap() {
        let blob = s3s::dto::StreamingBlob::wrap(futures::stream::once(async {
            Ok::<Bytes, std::io::Error>(Bytes::from_static(b"0123456789"))
        }));
        assert!(buffer_body(blob, 4).await.is_none()); // over cap -> None
    }

    /// A body read past the cap is not a body lost: what was drained is spliced back in
    /// front of the untouched rest, because the caller still has to forward the bytes the
    /// client sent and a stream cannot be read twice.
    #[tokio::test]
    async fn a_body_over_the_cap_is_handed_back_whole() {
        let chunks = ["0123", "4567", "89ab"].map(|s| Bytes::from_static(s.as_bytes()));
        let blob = s3s::dto::StreamingBlob::wrap(futures::stream::iter(
            chunks.map(Ok::<Bytes, std::io::Error>),
        ));
        let BufferedBody::Streamed(rest) = buffer_or_forward(blob, 6).await else {
            panic!("12 bytes do not fit a 6-byte cap");
        };
        assert_eq!(
            buffer_body(rest, usize::MAX).await.expect("readable"),
            Bytes::from_static(b"0123456789ab"),
            "every byte forwards, in order, exactly once"
        );
    }

    /// A body inside the cap comes back as bytes, chunking and all.
    #[tokio::test]
    async fn a_body_inside_the_cap_is_buffered() {
        let chunks = ["ab", "cd"].map(|s| Bytes::from_static(s.as_bytes()));
        let blob = s3s::dto::StreamingBlob::wrap(futures::stream::iter(
            chunks.map(Ok::<Bytes, std::io::Error>),
        ));
        let BufferedBody::Whole(bytes) = buffer_or_forward(blob, 4).await else {
            panic!("4 bytes fit a 4-byte cap");
        };
        assert_eq!(bytes, Bytes::from_static(b"abcd"));
    }

    #[tokio::test]
    async fn warm_tier_roundtrip_and_restart_recovery() {
        let dir = temp_dir();
        let _ = std::fs::remove_dir_all(&dir);
        let cap = 10 * 1024 * 1024;

        let (warm, _disk) = open_warm(dir.clone(), cap, 8 * 1024 * 1024, metrics()).unwrap();
        let obj = Arc::new(sample());
        warm.put(ck("b", "k"), obj.clone()).await.unwrap();
        assert_eq!(
            warm.get(&ck("b", "k")).await.unwrap().expect("hit").body,
            obj.body
        );
        assert!(warm.get(&ck("b", "missing")).await.unwrap().is_none());

        // A fresh tier over the same dir re-indexes the file — survives a restart.
        let (warm2, _disk2) = open_warm(dir.clone(), cap, 8 * 1024 * 1024, metrics()).unwrap();
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

    /// An object too big for the warm tier is the per-object cap doing its job, not a
    /// disk failure. The two are counted apart so `warm_error` stays a counter an
    /// operator can page on — a workload that simply holds large objects must not keep
    /// it permanently lit.
    #[tokio::test]
    async fn an_oversize_object_counts_a_rejection_not_an_error() {
        let dir = temp_dir();
        let _ = std::fs::remove_dir_all(&dir);
        let metrics = Arc::new(Metrics::default());
        // A cap below anything's encoded size, so the codec refuses every disk fill.
        let warm = open_warm(dir.clone(), 10 * 1024 * 1024, 1, Arc::clone(&metrics)).unwrap();
        let cache = TieredCache::new(1024 * 1024, Some(warm), Arc::clone(&metrics));

        cache.insert(ck("b", "k"), Arc::new(sample())).await;
        assert!(
            cache.get(&ck("b", "k")).await.is_some(),
            "the disk rejection never blocks the hot fill"
        );
        let text = metrics.prometheus_text();
        assert!(text.contains("\ns3cache_warm_rejects 1\n"), "{text}");
        assert!(text.contains("\ns3cache_warm_error 0\n"), "{text}");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn layered_cache_fills_and_invalidates() {
        let dir = temp_dir();
        let _ = std::fs::remove_dir_all(&dir);
        let warm = open_warm(dir.clone(), 10 * 1024 * 1024, 8 * 1024 * 1024, metrics()).unwrap();
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
