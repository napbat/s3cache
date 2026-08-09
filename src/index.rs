//! The in-memory LIST index: per-bucket sorted key sets, the `ListObjectsV2` algorithm
//! over them, and the background full-LIST bootstrap that warms a bucket.
//!
//! Every entry is one of two things. A **faithful** entry carries everything a HEAD
//! reports — captured from an origin response, or from a write this proxy performed — so
//! a HEAD answered from it is the origin's answer field for field. A **skeletal** entry
//! (a bootstrap LIST row, a peer's feed event) proves the key exists and carries what
//! LIST reports, but not the `Content-Type` or the user metadata a HEAD does; it never
//! answers a HEAD locally. The first HEAD that forwards completes it in place
//! (`complete_entry`), so the second HEAD of that key is local *and* identical to the
//! origin's.
//!
//! Sizes are never fabricated. A path that could not learn one (a metadata HEAD that
//! failed, a PUT with no `Content-Length`) indexes `size: None`, and an entry with an
//! unknown size is served by neither HEAD nor LIST — the request falls through to the
//! origin, which is the authority the index caches.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::Bound;
use std::sync::RwLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use s3s::dto::{
    CommonPrefix, ETag, HeadObjectOutput, ListObjectsV2Input, ListObjectsV2Output, Metadata,
    Object, ObjectStorageClass, Timestamp,
};
use tracing::info;

use crate::tier::CachedObject;

/// S3's default storage class, and what an object reports when it carries no
/// `x-amz-storage-class`. Used where a path proves a key exists without saying which
/// class it is in; the next origin sync corrects it.
#[must_use]
pub(crate) fn standard_class() -> ObjectStorageClass {
    ObjectStorageClass::from_static(ObjectStorageClass::STANDARD)
}

/// The rest of what a HEAD reports, beyond size, mtime, `ETag`, storage class and
/// `Content-Type`. Only an origin response — or a write this proxy performed, which
/// carries the same fields on the way in — has all of it, and only an entry holding it
/// can answer a HEAD identically to the origin. Boxed because most entries never carry
/// one and the index holds millions of them.
#[derive(Clone, Default)]
pub(crate) struct ObjMeta {
    pub(crate) cache_control: Option<String>,
    pub(crate) content_disposition: Option<String>,
    pub(crate) content_encoding: Option<String>,
    pub(crate) content_language: Option<String>,
    pub(crate) metadata: Option<Metadata>,
}

/// One indexed key's metadata: what LIST reports about it, plus — on a faithful entry —
/// what a HEAD does. In memory only: the index is rebuilt from the origin at startup, so
/// this struct is never serialized and carries no format compatibility.
#[derive(Clone)]
pub(crate) struct ObjEntry {
    /// The object's size, or `None` when the path that indexed the key could not learn
    /// it. Never fabricated: an unknown size is not reported as a `Content-Length` and
    /// not reported as a LIST row's `Size`.
    pub(crate) size: Option<i64>,
    /// The write's timestamp: both the last-writer-wins clock and the `Last-Modified`
    /// reported. It is the origin's own mtime wherever the indexing path carried one
    /// (the bootstrap LIST, a read observation) and the local write clock otherwise.
    pub(crate) last_modified: SystemTime,
    /// The origin's entity tag, when the path that indexed the key carried one: LIST,
    /// write responses and feed events do. An entry without one still answers
    /// existence, size and mtime — but never a HEAD.
    pub(crate) etag: Option<ETag>,
    pub(crate) storage_class: ObjectStorageClass,
    pub(crate) content_type: Option<String>,
    /// The rest of what a HEAD reports. `Some` is what makes an entry faithful (see the
    /// module docs); a bootstrap LIST row and a peer's feed event leave it `None`.
    pub(crate) meta: Option<Box<ObjMeta>>,
}

impl ObjEntry {
    /// Whether a HEAD answered from this entry would be exactly the origin's answer.
    /// Everything the origin reports has to be here — a missing `Content-Type` or a
    /// missing `x-amz-meta-*` set is a difference a client branches on.
    fn is_faithful(&self) -> bool {
        self.size.is_some()
            && self.etag.is_some()
            && self.content_type.is_some()
            && self.meta.is_some()
    }
}

/// How far the LIST index's mtime for a key may run *ahead* of a cached body's and still
/// describe the same version of the object. It absorbs two known, bounded disagreements
/// and nothing else:
///
/// * **Stamp order on a write fill.** A `PutObject` stamps the body from
///   `SystemTime::now()` and then stamps the index entry from a second `now()`, so the
///   entry is always microseconds *later* than the body it describes.
/// * **Wire precision.** A LIST row carries milliseconds; the `Last-Modified` a GET or
///   HEAD carries is an HTTP-date, whole seconds, rounded **down**. The same origin
///   write therefore reads as `T.813` in the index and `T` on the body.
///
/// A second is comfortably above both and far below anything that matters: the corner it
/// is guarding is a rewrite with byte-identical content (same `ETag`, new mtime), and a
/// rewrite inside a one-second window of the fill it is racing was already indistinguishable
/// from the fill itself.
const BODY_MTIME_SLACK: Duration = Duration::from_secs(1);

/// Whether this bucket's index entry for a key describes the **same version of the
/// object** as a cached body of it — the question a suspect body has to answer before it
/// may be served (see `CachingProxy::validated_get`).
///
/// The `ETag` is the version identity and carries the decision. It is not enough on its
/// own: a rewrite that stores byte-identical content produces the *same* `ETag`, so an
/// `ETag` match alone would happily serve a body from before a write this node missed.
/// What separates them is the mtime — the origin moves it on for every write, identical
/// bytes or not — so the entry's mtime must not run ahead of the body's by more than
/// [`BODY_MTIME_SLACK`].
///
/// Anything missing is a mismatch, never a pass: an entry with no `ETag` (a bootstrap
/// row or skeletal write) or a body with no `ETag`/`Last-Modified` cannot be compared,
/// and an uncomparable copy is one the origin has to re-serve.
pub(crate) fn entry_matches_body(entry: &ObjEntry, obj: &CachedObject) -> bool {
    let (Some(indexed), Some(cached)) = (entry.etag.as_ref(), obj.e_tag()) else {
        return false;
    };
    let Some(filled_at) = obj.last_modified() else {
        return false;
    };
    // `entry.last_modified <= filled_at + SLACK`, with the slack taken off the entry
    // rather than added to the body: a `Timestamp` has no way back to a `SystemTime`
    // without pulling in the `time` crate, and shifting the other operand is the same
    // comparison. A subtraction that underflows loses the slack, which is the strict
    // direction and therefore the safe one.
    let floor = entry
        .last_modified
        .checked_sub(BODY_MTIME_SLACK)
        .unwrap_or(entry.last_modified);
    indexed == cached && Timestamp::from(floor) <= *filled_at
}

/// What an origin response adds to an already-indexed entry (see [`complete_entry`]).
/// Only absent fields are filled, so a completion can never overwrite a fresher write.
pub(crate) struct EntryFill {
    pub(crate) size: Option<i64>,
    pub(crate) etag: Option<ETag>,
    pub(crate) content_type: Option<String>,
    pub(crate) meta: ObjMeta,
}

/// What [`complete_entry`] did.
#[derive(PartialEq, Eq, Debug)]
pub(crate) enum Completion {
    /// The key is not indexed at all; the caller indexes it as a read observation.
    NotIndexed,
    /// The entry was already holding everything the response carried.
    AlreadyComplete,
    /// Fields the entry lacked were filled in from the response.
    Completed,
}

/// What the index can say about a key on a synced bucket.
pub(crate) enum IndexedHead {
    /// The entry is faithful: this is exactly what the origin would answer.
    Faithful(Box<HeadObjectOutput>),
    /// Indexed, but the entry does not carry everything a HEAD reports (or its size was
    /// never learned). The origin has to answer, and its answer completes the entry.
    Incomplete,
    /// Not indexed — on a synced bucket, an authoritative 404.
    Absent,
}

/// Per-bucket LIST index: the sorted key set, whether its warm-up sync has
/// finished, and delete tombstones so a late-arriving cross-writer put
/// cannot resurrect a deleted key.
#[derive(Default)]
pub(crate) struct BucketState {
    pub(crate) synced: bool,
    pub(crate) keys: BTreeMap<String, ObjEntry>,
    /// Deleted keys and when: consulted by [`apply_put`], pruned amortized.
    pub(crate) gone: BTreeMap<String, SystemTime>,
    /// Generation of the origin rebuild currently allowed to publish into this bucket.
    /// A mutation whose outcome is ambiguous increments it before starting a rebuild, so
    /// an older in-flight LIST cannot make the bucket authoritative again.
    pub(crate) sync_generation: u64,
    /// Keys whose last mutation outcome is not yet reconciled with the origin. Reads of
    /// these keys stay origin-only until the matching bucket rebuild completes.
    pub(crate) uncertain_keys: BTreeSet<String>,
}

/// How long a delete tombstone shields its key from older, late-arriving
/// cross-writer puts. Conflicts past this window resolve at the next origin
/// sync — the origin stays the authority, this index is a cache of it.
const TOMBSTONE_TTL: Duration = Duration::from_hours(1);

/// Tombstones per bucket before an amortized TTL prune runs.
const TOMBSTONE_PRUNE_LEN: usize = 65_536;

/// Applies an observed put (a local write, a peer's feed event, or a read
/// observation) by per-key last-writer-wins: a strictly newer entry or an
/// equal-or-newer tombstone rejects it; ties between puts fall to
/// last-applied (cross-writer same-microsecond puts are healed by the next
/// origin sync). `entry.last_modified` is the write's timestamp — the LWW
/// clock. Returns whether the index changed.
pub(crate) fn apply_put(
    state: &RwLock<HashMap<String, BucketState>>,
    bucket: &str,
    key: &str,
    entry: ObjEntry,
) -> bool {
    let ts = entry.last_modified;
    let mut g = state.write().unwrap();
    let b = g.entry(bucket.to_owned()).or_default();
    if b.gone.get(key).is_some_and(|dead| *dead >= ts) {
        return false; // deletes win ties: never resurrect
    }
    if b.keys.get(key).is_some_and(|e| e.last_modified > ts) {
        return false; // a newer put is already indexed
    }
    b.keys.insert(key.to_owned(), entry);
    true
}

/// Reset a bucket to origin-serving state and advance the generation that may rebuild it.
/// `uncertain_key` is retained across overlapping resets because the newest full rebuild
/// covers every earlier uncertainty too. Returns the new generation.
pub(crate) fn begin_bucket_resync(
    state: &RwLock<HashMap<String, BucketState>>,
    bucket: &str,
    uncertain_key: Option<&str>,
) -> u64 {
    let mut g = state.write().unwrap();
    let previous = g.remove(bucket).unwrap_or_default();
    let generation = previous.sync_generation.wrapping_add(1);
    let mut uncertain_keys = previous.uncertain_keys;
    if let Some(key) = uncertain_key {
        uncertain_keys.insert(key.to_owned());
    }
    g.insert(
        bucket.to_owned(),
        BucketState {
            sync_generation: generation,
            uncertain_keys,
            ..BucketState::default()
        },
    );
    generation
}

/// Discard a failed rebuild's partial rows and advance to a clean retry, but only while
/// `generation` still owns the bucket. `None` means a newer fence already superseded it.
pub(crate) fn restart_bucket_resync_if_current(
    state: &RwLock<HashMap<String, BucketState>>,
    bucket: &str,
    generation: u64,
) -> Option<u64> {
    let mut g = state.write().unwrap();
    let current = g.get(bucket)?;
    if current.sync_generation != generation {
        return None;
    }
    let previous = g.remove(bucket).unwrap_or_default();
    let next = generation.wrapping_add(1);
    g.insert(
        bucket.to_owned(),
        BucketState {
            sync_generation: next,
            uncertain_keys: previous.uncertain_keys,
            ..BucketState::default()
        },
    );
    Some(next)
}

/// Whether `generation` is still the rebuild authorised for `bucket`.
pub(crate) fn bucket_resync_is_current(
    state: &RwLock<HashMap<String, BucketState>>,
    bucket: &str,
    generation: u64,
) -> bool {
    state
        .read()
        .unwrap()
        .get(bucket)
        .is_some_and(|state| state.sync_generation == generation)
}

/// Applies an observed delete: removes any not-newer entry and records a
/// tombstone (see [`apply_put`]). Returns whether a live entry was removed.
pub(crate) fn apply_del(
    state: &RwLock<HashMap<String, BucketState>>,
    bucket: &str,
    key: &str,
    ts: SystemTime,
) -> bool {
    let mut g = state.write().unwrap();
    let b = g.entry(bucket.to_owned()).or_default();
    if b.gone.len() > TOMBSTONE_PRUNE_LEN
        && let Some(cutoff) = ts.checked_sub(TOMBSTONE_TTL)
    {
        b.gone.retain(|_, dead| *dead >= cutoff);
    }
    let dead = b.gone.entry(key.to_owned()).or_insert(ts);
    if *dead < ts {
        *dead = ts;
    }
    if b.keys.get(key).is_some_and(|e| e.last_modified > ts) {
        return false; // the key was rewritten after this delete
    }
    b.keys.remove(key).is_some()
}

/// Completes an indexed entry from an origin response: fills the fields it does not
/// carry and leaves everything else — including the last-writer-wins stamp — alone. This
/// *observes* what is already indexed, it does not write, so it can neither reorder
/// against a concurrent write nor resurrect a deleted key.
pub(crate) fn complete_entry(
    state: &RwLock<HashMap<String, BucketState>>,
    bucket: &str,
    key: &str,
    fill: EntryFill,
) -> Completion {
    let mut g = state.write().unwrap();
    let Some(entry) = g.get_mut(bucket).and_then(|b| b.keys.get_mut(key)) else {
        return Completion::NotIndexed;
    };
    let mut filled = false;
    if entry.size.is_none() {
        entry.size = fill.size;
        filled |= entry.size.is_some();
    }
    if entry.etag.is_none() {
        entry.etag = fill.etag;
        filled |= entry.etag.is_some();
    }
    if entry.content_type.is_none() {
        entry.content_type = fill.content_type;
        filled |= entry.content_type.is_some();
    }
    if entry.meta.is_none() {
        entry.meta = Some(Box::new(fill.meta));
        filled = true;
    }
    if filled {
        Completion::Completed
    } else {
        Completion::AlreadyComplete
    }
}

/// The `ListObjectsV2` algorithm over an already-borrowed key index — free-standing so it
/// is unit-testable without a live proxy. Matches S3: sorted keys, prefix filter,
/// delimiter roll-up into common prefixes, max-keys paging with a key continuation token
/// (resumed *inclusively*, since the token is the next key to return), and `start_after`
/// (exclusive, first page only).
///
/// `None` when the index cannot answer this request without inventing something — a row
/// it would have to emit has no known size — and the caller must forward to the origin.
pub(crate) fn list_objects_v2_from_index(
    keys: Option<&BTreeMap<String, ObjEntry>>,
    inp: &ListObjectsV2Input,
) -> Option<ListObjectsV2Output> {
    let bucket = inp.bucket.as_str();
    let prefix = inp.prefix.clone().unwrap_or_default();
    let delim = inp.delimiter.clone();
    // `max-keys=0` asks for no keys at all, and S3 answers exactly that: an empty,
    // untruncated page echoing 0. Clamping it up to 1 would answer a request the client
    // did not make, so the floor is 0 and the loop below simply does not run.
    let max = usize::try_from(inp.max_keys.unwrap_or(1000).clamp(0, 1000)).unwrap_or(1000);

    let mut contents: Vec<Object> = Vec::new();
    let mut common: BTreeSet<String> = BTreeSet::new();
    let mut truncated = false;
    let mut next_token = None;

    if let Some(keys) = keys
        && max > 0
    {
        let lower = if let Some(token) = &inp.continuation_token {
            Bound::Included(token.clone())
        } else if let Some(sa) = &inp.start_after {
            Bound::Excluded(sa.clone())
        } else {
            Bound::Unbounded
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
            // A row whose size was never learned cannot be emitted: reporting `0` is a
            // number a client would act on, and omitting `Size` is not a LIST S3 ever
            // sends. The whole page goes to the origin instead.
            contents.push(Object {
                key: Some(key.clone()),
                size: Some(entry.size?),
                last_modified: Some(Timestamp::from(entry.last_modified)),
                e_tag: entry.etag.clone(),
                storage_class: Some(entry.storage_class.clone()),
                ..Default::default()
            });
        }
    }

    let key_count = i32::try_from(contents.len() + common.len()).unwrap_or(i32::MAX);
    Some(ListObjectsV2Output {
        name: Some(bucket.to_owned()),
        prefix: Some(prefix),
        max_keys: Some(i32::try_from(max).unwrap_or(1000)),
        key_count: Some(key_count),
        is_truncated: Some(truncated),
        continuation_token: inp.continuation_token.clone(),
        next_continuation_token: next_token,
        contents: (!contents.is_empty()).then_some(contents),
        common_prefixes: (!common.is_empty()).then(|| {
            common
                .into_iter()
                .map(|p| CommonPrefix { prefix: Some(p) })
                .collect()
        }),
        delimiter: delim,
        start_after: inp.start_after.clone(),
        ..Default::default()
    })
}

/// A HEAD answered from an already-borrowed key index. The index is authoritative for
/// *existence* on a synced bucket (this proxy is the only writer — the property
/// `ListObjectsV2` correctness already rests on), so an absent key is a local 404; but it
/// is only authoritative for the *answer* when the entry is faithful, which is what
/// [`IndexedHead`] distinguishes. Free-standing alongside [`list_objects_v2_from_index`],
/// and unit-testable the same way.
pub(crate) fn head_object_from_index(
    keys: Option<&BTreeMap<String, ObjEntry>>,
    key: &str,
) -> IndexedHead {
    let Some(entry) = keys.and_then(|keys| keys.get(key)) else {
        return IndexedHead::Absent;
    };
    if !entry.is_faithful() {
        return IndexedHead::Incomplete;
    }
    let meta = entry.meta.as_deref().cloned().unwrap_or_default();
    IndexedHead::Faithful(Box::new(HeadObjectOutput {
        content_length: entry.size,
        last_modified: Some(Timestamp::from(entry.last_modified)),
        e_tag: entry.etag.clone(),
        content_type: entry.content_type.clone(),
        // Every S3 object is byte-range addressable and the origin says so on every
        // HEAD; a locally-served one must not be the exception a client notices.
        accept_ranges: Some("bytes".to_owned()),
        cache_control: meta.cache_control,
        content_disposition: meta.content_disposition,
        content_encoding: meta.content_encoding,
        content_language: meta.content_language,
        metadata: meta.metadata,
        ..Default::default()
    }))
}

/// Folds listed rows into `state` through [`apply_put`], so a bootstrap or a gap resync
/// obeys exactly the tombstones and per-key last-writer-wins a write does: the origin's
/// view of a key is old news the moment a newer local write or a delete has been applied,
/// and folding it in raw would resurrect deleted keys and clobber fresh ones. Returns how
/// many rows the origin listed (not how many changed).
#[cfg(test)]
pub(crate) fn sync_listing_into(
    state: &RwLock<HashMap<String, BucketState>>,
    bucket: &str,
    rows: impl IntoIterator<Item = (String, ObjEntry)>,
) -> usize {
    let mut found = 0usize;
    for (key, entry) in rows {
        apply_put(state, bucket, &key, entry);
        found += 1;
    }
    found
}

/// Fold one origin LIST page only when it still belongs to the bucket's current rebuild.
/// The generation check and every row application happen under the same lock, which is
/// what prevents a superseded rebuild from slipping a stale page in after a newer reset.
fn sync_listing_into_generation(
    state: &RwLock<HashMap<String, BucketState>>,
    bucket: &str,
    generation: u64,
    rows: impl IntoIterator<Item = (String, ObjEntry)>,
) -> Option<usize> {
    let mut g = state.write().unwrap();
    let b = g.entry(bucket.to_owned()).or_default();
    if b.sync_generation != generation {
        return None;
    }
    let mut found = 0usize;
    for (key, entry) in rows {
        let ts = entry.last_modified;
        if b.gone.get(&key).is_none_or(|dead| *dead < ts)
            && b.keys
                .get(&key)
                .is_none_or(|current| current.last_modified <= ts)
        {
            b.keys.insert(key, entry);
        }
        found += 1;
    }
    Some(found)
}

/// Publish completion for `generation`; false means a newer rebuild owns the bucket.
fn finish_bucket_sync_generation(
    state: &RwLock<HashMap<String, BucketState>>,
    bucket: &str,
    generation: u64,
) -> bool {
    let mut g = state.write().unwrap();
    let b = g.entry(bucket.to_owned()).or_default();
    if b.sync_generation != generation {
        return false;
    }
    b.synced = true;
    b.uncertain_keys.clear();
    true
}

/// Full paginated LIST of a bucket into `state`, then mark it synced. Merges (never
/// clears) so a write that raced the sync isn't lost. Free-standing (takes the client +
/// shared index) so the background warm-up task can run it without borrowing the proxy,
/// which the S3 service owns by value.
///
/// # Errors
///
/// The upstream LIST error, leaving the bucket unsynced (and therefore passthrough).
pub(crate) async fn sync_bucket_into(
    client: &aws_sdk_s3::Client,
    state: &RwLock<HashMap<String, BucketState>>,
    bucket: &str,
) -> anyhow::Result<usize> {
    let generation = {
        let mut g = state.write().unwrap();
        g.entry(bucket.to_owned()).or_default().sync_generation
    };
    let result = sync_bucket_generation(client, state, bucket, generation).await;
    if result.is_err() {
        // A failed page must not remain to be mistaken for part of a later successful
        // snapshot. If another fence already moved the generation, it owns the reset.
        restart_bucket_resync_if_current(state, bucket, generation);
    }
    result
}

/// Full paginated LIST for one already-established rebuild generation. A newer reset
/// aborts this run before any later page or completion flag can become visible.
///
/// # Errors
///
/// The upstream LIST error, or an error indicating that a newer rebuild superseded this
/// one. In both cases the bucket remains origin-serving.
pub(crate) async fn sync_bucket_generation(
    client: &aws_sdk_s3::Client,
    state: &RwLock<HashMap<String, BucketState>>,
    bucket: &str,
    generation: u64,
) -> anyhow::Result<usize> {
    let mut token: Option<String> = None;
    let mut found = 0usize;
    loop {
        let mut req = client.list_objects_v2().bucket(bucket).max_keys(1000);
        if let Some(t) = &token {
            req = req.continuation_token(t);
        }
        let resp = req.send().await?;
        let rows = resp.contents().iter().filter_map(|obj| {
            let key = obj.key()?;
            // Whole seconds are not what the origin reports: the LIST XML carries
            // milliseconds, and a client comparing mtimes sees every indexed one land
            // up to a second early. Keep the sub-second part the origin sent.
            let last_modified = obj.last_modified().map_or_else(SystemTime::now, |d| {
                u64::try_from(d.secs()).map_or_else(
                    |_| SystemTime::now(),
                    |secs| UNIX_EPOCH + Duration::new(secs, d.subsec_nanos()),
                )
            });
            Some((
                key.to_owned(),
                ObjEntry {
                    size: obj.size(),
                    last_modified,
                    // LIST reports the ETag and storage class per key, so the bootstrap
                    // is where the index learns them; an unparseable ETag is simply not
                    // carried.
                    etag: obj.e_tag().and_then(|raw| raw.parse().ok()),
                    storage_class: obj.storage_class().map_or_else(standard_class, |class| {
                        ObjectStorageClass::from(class.as_str().to_owned())
                    }),
                    // A LIST row says nothing about Content-Type or user metadata, so
                    // the entry is skeletal: it answers LIST, never a HEAD.
                    content_type: None,
                    meta: None,
                },
            ))
        });
        let Some(page_len) = sync_listing_into_generation(state, bucket, generation, rows) else {
            anyhow::bail!("bucket sync superseded by a newer origin rebuild");
        };
        found += page_len;
        if resp.is_truncated().unwrap_or(false) {
            token = resp.next_continuation_token().map(str::to_owned);
            if token.is_none() {
                break;
            }
        } else {
            break;
        }
    }
    if !finish_bucket_sync_generation(state, bucket, generation) {
        anyhow::bail!("bucket sync superseded by a newer origin rebuild");
    }
    info!("synced bucket `{bucket}` into index: {found} keys");
    Ok(found)
}

#[cfg(test)]
mod tests {
    use super::{
        BODY_MTIME_SLACK, Completion, EntryFill, IndexedHead, ObjEntry, ObjMeta, apply_del,
        apply_put, begin_bucket_resync, complete_entry, entry_matches_body,
        finish_bucket_sync_generation, head_object_from_index, list_objects_v2_from_index,
        restart_bucket_resync_if_current, standard_class, sync_listing_into,
        sync_listing_into_generation,
    };
    use crate::tier::CachedObject;
    use std::collections::HashMap;
    use std::sync::RwLock;
    use std::time::Duration;

    type Index = RwLock<HashMap<String, super::BucketState>>;

    fn ts(secs: u64) -> std::time::SystemTime {
        UNIX_EPOCH + Duration::from_secs(secs)
    }

    /// A skeletal entry of `size` at `secs` — the shape a bootstrap LIST row or a peer's
    /// feed event produces.
    fn entry(size: i64, secs: u64) -> ObjEntry {
        ObjEntry {
            size: Some(size),
            last_modified: ts(secs),
            etag: None,
            storage_class: standard_class(),
            content_type: None,
            meta: None,
        }
    }

    /// A put of `size` at `secs`, the way every write path applies one.
    fn put(state: &Index, key: &str, size: i64, secs: u64) -> bool {
        apply_put(state, "b", key, entry(size, secs))
    }

    fn size_of(state: &Index, key: &str) -> Option<i64> {
        state
            .read()
            .unwrap()
            .get("b")
            .and_then(|b| b.keys.get(key))
            .and_then(|e| e.size)
    }

    /// Cross-writer events arrive in any order; per-key LWW must converge:
    /// older puts lose, deletes win ties, and a delete's tombstone blocks a
    /// late older put from resurrecting the key.
    #[test]
    fn lww_applies_out_of_order_events_convergently() {
        let state: Index = RwLock::new(HashMap::new());
        assert!(put(&state, "k", 1, 10));
        assert!(!put(&state, "k", 9, 5), "older put loses");
        assert_eq!(size_of(&state, "k"), Some(1));
        assert!(put(&state, "k", 2, 20), "newer put wins");
        assert_eq!(size_of(&state, "k"), Some(2));

        // Delete at t=30; an older put (t=25) must NOT resurrect the key.
        assert!(apply_del(&state, "b", "k", ts(30)));
        assert!(!put(&state, "k", 3, 25), "tombstoned");
        assert_eq!(size_of(&state, "k"), None);
        // Deletes win timestamp ties, in either arrival order.
        assert!(put(&state, "tie", 1, 40));
        apply_del(&state, "b", "tie", ts(40));
        assert_eq!(size_of(&state, "tie"), None, "delete wins the tie");
        assert!(!put(&state, "tie", 2, 40), "still tombstoned");

        // A genuinely newer put after a delete brings the key back.
        assert!(put(&state, "k", 4, 35));
        assert_eq!(size_of(&state, "k"), Some(4));
    }

    /// A delete observed before its key's put (cross-writer reorder) still
    /// suppresses the older put.
    #[test]
    fn delete_first_reorder_suppresses_the_put() {
        let state: Index = RwLock::new(HashMap::new());
        assert!(!apply_del(&state, "b", "k", ts(50)), "nothing live yet");
        assert!(!put(&state, "k", 1, 45), "arrives late, loses");
        assert_eq!(size_of(&state, "k"), None);
    }

    /// The origin's listing is old news the moment a newer local event has been applied,
    /// so a bootstrap (or gap resync) folding it in must go through the same
    /// tombstone + last-writer-wins gate a write does — not a raw insert, which
    /// resurrects deleted keys and overwrites fresh ones with the origin's stale view.
    #[test]
    fn a_listing_cannot_resurrect_a_delete_or_clobber_a_newer_write() {
        let state: Index = RwLock::new(HashMap::new());

        // A delete races a bootstrap that still lists the key: the key stays gone.
        assert!(put(&state, "deleted", 7, 10));
        assert!(apply_del(&state, "b", "deleted", ts(20)));
        assert_eq!(
            sync_listing_into(&state, "b", [("deleted".to_owned(), entry(7, 15))]),
            1,
            "the row was listed, whatever the index did with it"
        );
        assert_eq!(
            size_of(&state, "deleted"),
            None,
            "a tombstoned key is not resurrected by a listing that predates the delete"
        );

        // A local write races a bootstrap holding the previous version: the write wins.
        assert!(put(&state, "rewritten", 99, 30));
        sync_listing_into(&state, "b", [("rewritten".to_owned(), entry(7, 25))]);
        assert_eq!(
            size_of(&state, "rewritten"),
            Some(99),
            "an older listed row does not clobber a newer local write"
        );

        // And a listing of a key nobody has touched still lands.
        sync_listing_into(&state, "b", [("fresh".to_owned(), entry(3, 40))]);
        assert_eq!(size_of(&state, "fresh"), Some(3));
    }

    /// Once a newer uncertainty fences a bucket, an older rebuild may neither publish a
    /// late page nor clear the newer key's origin-only marker at completion.
    #[test]
    fn a_superseded_rebuild_cannot_clear_a_newer_uncertainty() {
        let state: Index = RwLock::new(HashMap::new());
        let older = begin_bucket_resync(&state, "b", Some("first"));
        let current = begin_bucket_resync(&state, "b", Some("second"));

        assert_eq!(
            sync_listing_into_generation(&state, "b", older, [("stale".to_owned(), entry(1, 10))]),
            None,
            "a stale page is rejected atomically with its generation check"
        );
        assert!(!finish_bucket_sync_generation(&state, "b", older));
        {
            let g = state.read().unwrap();
            let bucket = &g["b"];
            assert!(!bucket.synced);
            assert_eq!(
                bucket
                    .uncertain_keys
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>(),
                ["first", "second"]
            );
            assert!(!bucket.keys.contains_key("stale"));
        }

        assert_eq!(
            sync_listing_into_generation(
                &state,
                "b",
                current,
                [("fresh".to_owned(), entry(2, 20))]
            ),
            Some(1)
        );
        assert!(finish_bucket_sync_generation(&state, "b", current));
        let g = state.read().unwrap();
        assert!(g["b"].synced);
        assert!(g["b"].uncertain_keys.is_empty());
        assert!(g["b"].keys.contains_key("fresh"));
    }

    /// A failed paginated attempt may have published rows before the failing page. The
    /// clean retry must not retain one that the origin no longer lists, or completing the
    /// retry would make that absent key authoritative again.
    #[test]
    fn a_retry_discards_rows_from_its_failed_partial_attempt() {
        let state: Index = RwLock::new(HashMap::new());
        let failed = begin_bucket_resync(&state, "b", Some("writer"));
        assert_eq!(
            sync_listing_into_generation(
                &state,
                "b",
                failed,
                [("deleted-before-retry".to_owned(), entry(1, 10))]
            ),
            Some(1)
        );

        let retry = restart_bucket_resync_if_current(&state, "b", failed)
            .expect("the failed attempt still owns the generation");
        assert!(
            !state.read().unwrap()["b"]
                .keys
                .contains_key("deleted-before-retry"),
            "the retry starts from a clean origin snapshot"
        );
        assert_eq!(
            sync_listing_into_generation(
                &state,
                "b",
                retry,
                [("still-present".to_owned(), entry(2, 20))]
            ),
            Some(1)
        );
        assert!(finish_bucket_sync_generation(&state, "b", retry));

        let g = state.read().unwrap();
        assert!(g["b"].synced);
        assert!(!g["b"].keys.contains_key("deleted-before-retry"));
        assert!(g["b"].keys.contains_key("still-present"));
    }

    use s3s::dto::{ETag, ListObjectsV2Input, ListObjectsV2Output};
    use std::collections::BTreeMap;
    use std::time::UNIX_EPOCH;

    fn index(keys: &[&str]) -> BTreeMap<String, ObjEntry> {
        keys.iter()
            .map(|k| {
                (
                    (*k).to_owned(),
                    ObjEntry {
                        size: Some(1),
                        last_modified: UNIX_EPOCH,
                        etag: Some(ETag::Strong(format!("etag-{k}"))),
                        storage_class: standard_class(),
                        content_type: None,
                        meta: None,
                    },
                )
            })
            .collect()
    }

    fn list_input(
        max: i32,
        token: Option<&str>,
        prefix: &str,
        delim: Option<&str>,
        start_after: Option<&str>,
    ) -> ListObjectsV2Input {
        ListObjectsV2Input {
            bucket: "b".to_owned(),
            max_keys: Some(max),
            continuation_token: token.map(str::to_owned),
            prefix: (!prefix.is_empty()).then(|| prefix.to_owned()),
            delimiter: delim.map(str::to_owned),
            start_after: start_after.map(str::to_owned),
            ..Default::default()
        }
    }

    /// The index's answer, which every row here expects it to be able to give.
    fn listed(
        idx: Option<&BTreeMap<String, ObjEntry>>,
        inp: &ListObjectsV2Input,
    ) -> ListObjectsV2Output {
        list_objects_v2_from_index(idx, inp).expect("the index can answer this list")
    }

    fn page_keys(out: &ListObjectsV2Output) -> Vec<String> {
        out.contents
            .iter()
            .flatten()
            .filter_map(|o| o.key.clone())
            .collect()
    }
    fn page_prefixes(out: &ListObjectsV2Output) -> Vec<String> {
        out.common_prefixes
            .iter()
            .flatten()
            .filter_map(|c| c.prefix.clone())
            .collect()
    }

    /// Follow the continuation tokens like a real client and collect every key + prefix.
    fn walk_pages(
        idx: &BTreeMap<String, ObjEntry>,
        max: i32,
        prefix: &str,
        delim: Option<&str>,
    ) -> (Vec<String>, Vec<String>) {
        let (mut keys, mut prefixes) = (Vec::new(), Vec::new());
        let mut token: Option<String> = None;
        for _ in 0..10_000 {
            let inp = list_input(max, token.as_deref(), prefix, delim, None);
            let out = listed(Some(idx), &inp);
            keys.extend(page_keys(&out));
            prefixes.extend(page_prefixes(&out));
            match (out.is_truncated, out.next_continuation_token) {
                (Some(true), Some(t)) => token = Some(t),
                _ => break,
            }
        }
        (keys, prefixes)
    }

    #[test]
    fn list_pagination_loses_nothing() {
        let idx = index(&["a", "b", "c", "d", "e"]);
        // Every page size must reproduce the full ordered key set — no gaps, no dups.
        for max in 1..=6 {
            let (keys, _) = walk_pages(&idx, max, "", None);
            assert_eq!(keys, ["a", "b", "c", "d", "e"], "max_keys={max}");
        }
    }

    #[test]
    fn list_prefix_and_delimiter() {
        let idx = index(&["p/a/1", "p/a/2", "p/b/1", "p/top"]);
        let out = listed(Some(&idx), &list_input(1000, None, "p/a/", None, None));
        assert_eq!(page_keys(&out), ["p/a/1", "p/a/2"]);
        let out = listed(Some(&idx), &list_input(1000, None, "p/", Some("/"), None));
        assert_eq!(page_prefixes(&out), ["p/a/", "p/b/"]);
        assert_eq!(page_keys(&out), ["p/top"]);
    }

    #[test]
    fn list_delimiter_pagination_no_dup_prefix() {
        let idx = index(&["a/1", "a/2", "b/1", "c/1"]);
        // Each common prefix must appear exactly once across paged results.
        let (keys, prefixes) = walk_pages(&idx, 1, "", Some("/"));
        assert!(keys.is_empty());
        assert_eq!(prefixes, ["a/", "b/", "c/"]);
    }

    #[test]
    fn list_start_after_is_exclusive() {
        let idx = index(&["a", "b", "c", "d"]);
        let out = listed(Some(&idx), &list_input(1000, None, "", None, Some("b")));
        assert_eq!(page_keys(&out), ["c", "d"]);
    }

    #[test]
    fn list_empty_bucket() {
        let out = listed(None, &list_input(1000, None, "", None, None));
        assert_eq!(out.is_truncated, Some(false));
        assert!(out.contents.is_none());
        assert_eq!(out.key_count, Some(0));
    }

    /// `max-keys=0` is a request for nothing, and the answer is nothing — echoed as `0`,
    /// untruncated, with no continuation token to follow.
    #[test]
    fn list_max_keys_zero_returns_an_empty_page() {
        let idx = index(&["a", "b", "c"]);
        let out = listed(Some(&idx), &list_input(0, None, "", None, None));
        assert!(out.contents.is_none());
        assert_eq!(out.key_count, Some(0));
        assert_eq!(out.max_keys, Some(0));
        assert_eq!(out.is_truncated, Some(false));
        assert!(out.next_continuation_token.is_none());
    }

    /// An entry whose size was never learned cannot be listed at any size, so the index
    /// declines the whole request and the caller forwards it to the origin.
    #[test]
    fn list_declines_a_row_whose_size_is_unknown() {
        let mut idx = index(&["a", "b"]);
        idx.get_mut("b").expect("seeded key").size = None;
        assert!(
            list_objects_v2_from_index(Some(&idx), &list_input(1000, None, "", None, None))
                .is_none(),
            "no fabricated size is ever listed"
        );
        // A page that never reaches the unknown-size row is still answerable.
        let out = listed(Some(&idx), &list_input(1, None, "", None, None));
        assert_eq!(page_keys(&out), ["a"]);
    }

    /// The metadata an origin response adds, for completing a skeletal entry.
    fn fill() -> EntryFill {
        EntryFill {
            size: Some(12),
            etag: Some(ETag::Strong("abc".to_owned())),
            content_type: Some("text/x-fixture".to_owned()),
            meta: ObjMeta {
                metadata: Some(HashMap::from([("k".to_owned(), "v".to_owned())])),
                ..ObjMeta::default()
            },
        }
    }

    /// A skeletal entry (bootstrap row, peer event) answers LIST but never a HEAD: it
    /// does not carry `Content-Type` or user metadata, and a HEAD that omitted them
    /// would differ from the origin's. One origin response completes it in place, and
    /// every HEAD after that is local *and* identical.
    #[test]
    fn a_skeletal_entry_answers_a_head_only_once_completed() {
        let state: Index = RwLock::new(HashMap::new());
        assert!(put(&state, "k", 12, 10));
        {
            let g = state.read().unwrap();
            assert!(matches!(
                head_object_from_index(g.get("b").map(|b| &b.keys), "k"),
                IndexedHead::Incomplete
            ));
            assert!(matches!(
                head_object_from_index(g.get("b").map(|b| &b.keys), "ghost"),
                IndexedHead::Absent
            ));
        }

        assert_eq!(
            complete_entry(&state, "b", "k", fill()),
            Completion::Completed
        );
        assert_eq!(
            complete_entry(&state, "b", "k", fill()),
            Completion::AlreadyComplete,
            "a second response adds nothing"
        );
        let g = state.read().unwrap();
        let IndexedHead::Faithful(out) = head_object_from_index(g.get("b").map(|b| &b.keys), "k")
        else {
            panic!("a completed entry answers the HEAD");
        };
        assert_eq!(out.content_length, Some(12));
        assert_eq!(out.content_type.as_deref(), Some("text/x-fixture"));
        assert_eq!(out.accept_ranges.as_deref(), Some("bytes"));
        assert_eq!(out.e_tag, Some(ETag::Strong("abc".to_owned())));
        assert_eq!(
            out.metadata,
            Some(HashMap::from([("k".to_owned(), "v".to_owned())]))
        );
        assert_eq!(
            out.last_modified,
            Some(super::Timestamp::from(ts(10))),
            "completing an entry never moves its timestamp"
        );
    }

    /// A cached body carrying `etag`/`modified`, the way a fill builds one.
    fn body(etag: Option<&str>, modified: Option<std::time::SystemTime>) -> CachedObject {
        let out = s3s::dto::GetObjectOutput {
            e_tag: etag.map(|tag| ETag::Strong((*tag).to_owned())),
            last_modified: modified.map(super::Timestamp::from),
            ..Default::default()
        };
        CachedObject::from_get(&out, bytes::Bytes::from_static(b"body"))
    }

    /// An index entry stamped `modified`, carrying `etag`.
    fn indexed(etag: Option<&str>, modified: std::time::SystemTime) -> ObjEntry {
        ObjEntry {
            size: Some(4),
            last_modified: modified,
            etag: etag.map(|tag| ETag::Strong((*tag).to_owned())),
            storage_class: standard_class(),
            content_type: None,
            meta: None,
        }
    }

    /// The `ETag` decides, and both sides have to have one to decide with. An entry that
    /// cannot be compared to the body is not a match — the origin re-serves it.
    #[test]
    fn a_body_matches_its_entry_only_on_an_etag_both_sides_carry() {
        let now = ts(1_700_000_000);
        assert!(
            entry_matches_body(&indexed(Some("v1"), now), &body(Some("v1"), Some(now))),
            "same version, same moment"
        );
        assert!(
            !entry_matches_body(&indexed(Some("v2"), now), &body(Some("v1"), Some(now))),
            "the index holds a version this body is not"
        );
        assert!(
            !entry_matches_body(&indexed(None, now), &body(Some("v1"), Some(now))),
            "an entry with no ETag proves nothing about the body"
        );
        assert!(
            !entry_matches_body(&indexed(Some("v1"), now), &body(None, Some(now))),
            "and neither does a body with no ETag"
        );
        assert!(
            !entry_matches_body(&indexed(Some("v1"), now), &body(Some("v1"), None)),
            "a body with no mtime cannot answer the rewrite question at all"
        );
    }

    /// The clause that closes the byte-identical rewrite: same content re-PUT keeps the
    /// `ETag` and moves the origin's mtime on, so an entry whose mtime runs ahead of the
    /// body's by more than the slack describes a write this copy predates.
    ///
    /// The slack itself is not fudge: it is the width of two disagreements the system
    /// really has (write-fill stamp order, and LIST's milliseconds against an HTTP-date's
    /// whole seconds), so the boundary is asserted on both sides of exactly one second.
    #[test]
    fn an_entry_newer_than_the_body_by_more_than_the_slack_is_a_rewrite() {
        let filled = ts(1_700_000_000);
        let matches = |entry_at: std::time::SystemTime| {
            entry_matches_body(
                &indexed(Some("same"), entry_at),
                &body(Some("same"), Some(filled)),
            )
        };

        assert!(
            matches(filled - Duration::from_mins(1)),
            "an entry older than the body is fine: the body is the newer observation"
        );
        assert!(
            matches(filled + BODY_MTIME_SLACK),
            "exactly the slack still describes the same write"
        );
        assert!(
            !matches(filled + BODY_MTIME_SLACK + Duration::from_millis(1)),
            "a millisecond past it is a rewrite this copy missed"
        );
        assert!(
            !matches(filled + Duration::from_hours(1)),
            "and an hour past it certainly is"
        );
    }

    /// The two real fills, end to end. A write fill stamps the body and then the entry
    /// from consecutive clock reads; a read fill takes the body's mtime off an HTTP-date
    /// (whole seconds, rounded down) while the index holds LIST's milliseconds. Both must
    /// validate, or every restart refetches the whole cache.
    #[test]
    fn both_fill_paths_validate_against_their_own_entries() {
        let write_body = ts(1_700_000_000);
        assert!(
            entry_matches_body(
                &indexed(Some("w"), write_body + Duration::from_micros(120)),
                &body(Some("w"), Some(write_body)),
            ),
            "a write fill's entry is stamped a moment after its body"
        );

        let http_date = ts(1_700_000_000);
        assert!(
            entry_matches_body(
                &indexed(Some("r"), http_date + Duration::from_millis(813)),
                &body(Some("r"), Some(http_date)),
            ),
            "a read fill's body carries the HTTP-date's whole seconds, the index LIST's \
             milliseconds"
        );
    }

    /// Completion fills gaps and nothing else: a key nobody indexed is not created by
    /// one, and a field the entry already holds is not overwritten by a response that
    /// may describe an older version.
    #[test]
    fn completion_only_fills_what_is_missing() {
        let state: Index = RwLock::new(HashMap::new());
        assert_eq!(
            complete_entry(&state, "b", "ghost", fill()),
            Completion::NotIndexed,
            "completion never creates a key"
        );
        let mut seeded = entry(3, 10);
        seeded.etag = Some(ETag::Strong("original".to_owned()));
        apply_put(&state, "b", "k", seeded);
        assert_eq!(
            complete_entry(&state, "b", "k", fill()),
            Completion::Completed
        );
        let g = state.read().unwrap();
        let entry = &g["b"].keys["k"];
        assert_eq!(entry.etag, Some(ETag::Strong("original".to_owned())));
        assert_eq!(entry.size, Some(3), "the indexed size stands");
        assert_eq!(entry.content_type.as_deref(), Some("text/x-fixture"));
    }
}
