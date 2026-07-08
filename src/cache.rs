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

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::Bound;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use fred::prelude::*;
use s3s::dto::{ListObjectsV2Input, ListObjectsV2Output, Object, CommonPrefix, PutObjectInput, PutObjectOutput, DeleteObjectInput, DeleteObjectOutput, DeleteObjectsInput, DeleteObjectsOutput, CompleteMultipartUploadInput, CompleteMultipartUploadOutput, CopyObjectInput, CopyObjectOutput, GetObjectInput, GetObjectOutput, HeadObjectInput, HeadObjectOutput, StreamingBlob, Timestamp};
use s3s::{S3, S3Request, S3Response, S3Result};
use tracing::info;

use crate::tier::{self, CacheMode, CachedObject, HotCache, TieredCache, WarmCache};

/// A commit-log Valkey op may never stall a write: abandoned after this, treated as a
/// dropped append (the peers re-converge on their next full sync).
const LOG_OP_TIMEOUT: Duration = Duration::from_secs(2);

/// One indexed key's LIST metadata: its size and last-modified time.
struct ObjEntry {
    size: i64,
    last_modified: SystemTime,
}

/// Per-bucket LIST index: the sorted key set plus whether its warm-up sync has finished.
#[derive(Default)]
struct BucketState {
    synced: bool,
    keys: BTreeMap<String, ObjEntry>,
}

/// Cache-effectiveness counters, logged periodically by [`spawn_stats`]. The `warm_*`
/// counters cover the shared Valkey tier; the others cover the index and the hot path.
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
    range_promote_reject: AtomicU64,
    warm_hit: AtomicU64,
    warm_miss: AtomicU64,
    warm_error: AtomicU64,
    log_appended: AtomicU64,
    log_applied: AtomicU64,
    log_error: AtomicU64,
}

impl Metrics {
    /// Record a warm-tier (Valkey) hit — the object was served from the shared cache.
    pub(crate) fn warm_hit(&self) {
        self.warm_hit.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a warm-tier miss — the key was absent in Valkey.
    pub(crate) fn warm_miss(&self) {
        self.warm_miss.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a warm-tier error/timeout/decode failure (all handled as a miss/drop).
    pub(crate) fn warm_error(&self) {
        self.warm_error.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an index-log event appended for peers (a local write).
    pub(crate) fn log_appended(&self) {
        self.log_appended.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an index-log event consumed from the stream (own or a peer's).
    pub(crate) fn log_applied(&self) {
        self.log_applied.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an index-log append/read error or timeout.
    pub(crate) fn log_error(&self) {
        self.log_error.fetch_add(1, Ordering::Relaxed);
    }
}

/// Sizing and mode for the object cache, passed to [`CachingProxy::new`].
#[derive(Clone, Copy)]
pub struct CacheConfig {
    /// Which tiers sit in front of the S3 origin.
    pub mode: CacheMode,
    /// Total hot-tier capacity in bytes.
    pub cache_bytes: u64,
    /// Objects larger than this are never cached.
    pub max_obj_bytes: usize,
}

/// The shared, ordered commit log of index mutations (a Valkey Stream). Each write
/// appends one event; every node tails the stream and applies peers' events to its local
/// index and hot cache, so a write on one node reaches all of them. The log is
/// replayable — a reconnecting node resumes from its last-applied ID and can't miss a
/// write, the failure mode raw pub/sub has — which is what makes it OCC-safe.
pub struct IndexLog {
    /// Pool for appends (`XADD`) and one-shot reads — non-blocking, shared with warm.
    pool: Pool,
    /// Dedicated connection for the consumer's blocking `XREAD` (see `connect_valkey_client`).
    read_client: Client,
    stream: String,
    maxlen: i64,
    /// This process's id (e.g. the pod name) so it can skip replaying its own events.
    node: String,
    metrics: Arc<Metrics>,
}

impl IndexLog {
    /// Wrap a connected Valkey pool (appends) plus a dedicated read connection (the
    /// blocking consumer) as the index commit log.
    #[must_use]
    pub fn new(pool: Pool, read_client: Client, stream: String, maxlen: u64, node: String, metrics: Arc<Metrics>) -> Self {
        Self {
            pool,
            read_client,
            stream,
            maxlen: i64::try_from(maxlen).unwrap_or(i64::MAX),
            node,
            metrics,
        }
    }

    /// Append a write event, capped with approximate `MAXLEN` trimming. Best-effort with
    /// a timeout: an unreachable Valkey drops the event (logged + counted) rather than
    /// blocking the write; peers re-converge on their next full sync.
    async fn append(&self, op: &str, bucket: &str, key: &str, size: i64) {
        let ts = SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_millis());
        let fields: Vec<(&str, String)> = vec![
            ("op", op.to_owned()),
            ("bucket", bucket.to_owned()),
            ("key", key.to_owned()),
            ("size", size.to_string()),
            ("node", self.node.clone()),
            ("ts", ts.to_string()),
        ];
        let add = self
            .pool
            .xadd::<String, _, _, _, _>(&self.stream, false, ("MAXLEN", "~", self.maxlen), "*", fields);
        match tokio::time::timeout(LOG_OP_TIMEOUT, add).await {
            Ok(Ok(_)) => self.metrics.log_appended(),
            Ok(Err(e)) => {
                tracing::warn!("index log append failed for {op} {bucket}/{key}: {e}");
                self.metrics.log_error();
            }
            Err(_) => {
                tracing::warn!("index log append timed out for {op} {bucket}/{key}");
                self.metrics.log_error();
            }
        }
    }

    async fn append_put(&self, bucket: &str, key: &str, size: i64) {
        self.append("put", bucket, key, size).await;
    }

    async fn append_del(&self, bucket: &str, key: &str) {
        self.append("del", bucket, key, -1).await;
    }

    /// The stream's current tail ID (or `"0"` if empty / unreachable). Captured before the
    /// startup LIST bootstrap so the consumer replays everything appended from that point
    /// — nothing is missed, and re-applying what the bootstrap already saw is idempotent.
    async fn tail_id(&self) -> String {
        let res: FredResult<Vec<(String, HashMap<String, String>)>> =
            self.pool.xrevrange(&self.stream, "+", "-", Some(1)).await;
        match res {
            Ok(entries) => entries.into_iter().next().map_or_else(|| "0".to_owned(), |(id, _)| id),
            Err(_) => "0".to_owned(),
        }
    }

    /// Spawn the background consumer that tails the stream from just after `start_id` and
    /// applies peers' events to `state` and `hot`.
    fn spawn_consumer(
        &self,
        start_id: String,
        state: Arc<RwLock<HashMap<String, BucketState>>>,
        hot: Option<HotCache>,
    ) {
        let (read, stream, node, metrics) =
            (self.read_client.clone(), self.stream.clone(), self.node.clone(), self.metrics.clone());
        tokio::spawn(async move {
            consume_index_log(&read, &stream, &node, start_id, &state, hot.as_ref(), &metrics).await;
        });
    }
}

/// Tail the index commit log forever, applying each event to the local index and hot
/// cache. A read error just backs off and retries — the position (`last_id`) is kept, so
/// no event is skipped across a transient Valkey blip.
async fn consume_index_log(
    read: &Client,
    stream: &str,
    node: &str,
    mut last_id: String,
    state: &RwLock<HashMap<String, BucketState>>,
    hot: Option<&HotCache>,
    metrics: &Arc<Metrics>,
) {
    loop {
        // Raw `xread` + manual conversion: a BLOCK that times out with no new entries
        // returns nil, which `xread_map` would reject as a decode error — treat nil as
        // "nothing yet". `into_xread_response` then normalizes the RESP2/RESP3 encoding.
        let reply: FredResult<Value> = read.xread(Some(500), Some(5000), stream, &last_id).await;
        let map = match reply {
            Ok(v) if v.is_null() => continue, // BLOCK timed out with no new entries
            Ok(v) => match v.into_xread_response::<String, String, String, String>() {
                Ok(map) => map,
                Err(e) => {
                    metrics.log_error();
                    tracing::warn!("index log decode failed: {e}; retrying");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            },
            Err(e) => {
                metrics.log_error();
                tracing::warn!("index log read failed: {e}; retrying");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        let Some(entries) = map.get(stream) else { continue };
        for (id, fields) in entries {
            apply_log_event(node, state, hot, fields).await;
            last_id.clone_from(id);
            metrics.log_applied();
        }
    }
}

/// Apply one commit-log event to the local index and drop the key from the local hot
/// cache. Skips this node's own events (already reflected locally) and any malformed one.
async fn apply_log_event(
    node: &str,
    state: &RwLock<HashMap<String, BucketState>>,
    hot: Option<&HotCache>,
    fields: &HashMap<String, String>,
) {
    if fields.get("node").map(String::as_str) == Some(node) {
        return;
    }
    let (Some(op), Some(bucket), Some(key)) = (fields.get("op"), fields.get("bucket"), fields.get("key")) else {
        return;
    };
    match op.as_str() {
        "put" => {
            let size = fields.get("size").and_then(|s| s.parse::<i64>().ok()).unwrap_or(-1);
            let last_modified = fields
                .get("ts")
                .and_then(|s| s.parse::<u64>().ok())
                .map_or_else(SystemTime::now, |ms| UNIX_EPOCH + Duration::from_millis(ms));
            state
                .write()
                .unwrap()
                .entry(bucket.clone())
                .or_default()
                .keys
                .insert(key.clone(), ObjEntry { size, last_modified });
        }
        "del" => {
            if let Some(b) = state.write().unwrap().get_mut(bucket) {
                b.keys.remove(key);
            }
        }
        _ => return,
    }
    if let Some(h) = hot {
        h.invalidate(&(bucket.clone(), key.clone())).await;
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
    /// Object-body cache: hot heap and/or shared Valkey, per [`CacheMode`].
    obj_cache: TieredCache,
    /// Objects larger than this are never cached (segments stream straight through).
    max_obj_bytes: usize,
    /// Shared commit log for cross-node index coherence, when configured.
    index_log: Option<IndexLog>,
    metrics: Arc<Metrics>,
}

impl CachingProxy {
    /// Wire up the proxy. `cfg` selects the object-cache tiers and sizing; `warm` is the
    /// connected Valkey object cache (ignored unless the mode enables it) and `index_log`
    /// the shared commit log (both built by the caller). `metrics` is shared so the tiers,
    /// the log, and the stats task all report into it.
    pub fn new(
        inner: s3s_aws::Proxy,
        client: aws_sdk_s3::Client,
        cfg: CacheConfig,
        warm: Option<WarmCache>,
        index_log: Option<IndexLog>,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            inner,
            client,
            state: Arc::new(RwLock::new(HashMap::new())),
            obj_cache: TieredCache::new(cfg.mode, cfg.cache_bytes, warm),
            max_obj_bytes: cfg.max_obj_bytes,
            index_log,
            metrics,
        }
    }

    pub fn metrics(&self) -> Arc<Metrics> {
        self.metrics.clone()
    }

    /// Start tailing the shared commit log (if configured) so this node applies peers'
    /// writes. Captures the stream tail *before* returning, so it must be called before
    /// [`spawn_background_sync`](Self::spawn_background_sync) to avoid a gap between the
    /// bootstrap LIST and the replay window.
    pub async fn start_index_log(&self) {
        if let Some(log) = &self.index_log {
            let start_id = log.tail_id().await;
            info!("index log: tailing `{}` from {start_id}", log.stream);
            log.spawn_consumer(start_id, self.state.clone(), self.obj_cache.hot_handle());
        }
    }

    /// Append a write to the shared commit log, if configured. No-op otherwise.
    async fn log_put(&self, bucket: &str, key: &str, size: i64) {
        if let Some(log) = &self.index_log {
            log.append_put(bucket, key, size).await;
        }
    }

    async fn log_del(&self, bucket: &str, key: &str) {
        if let Some(log) = &self.index_log {
            log.append_del(bucket, key).await;
        }
    }

    /// Warm each bucket's LIST index in the background so startup stays instant and
    /// independent of bucket size. Until a bucket's full sync finishes its LISTs pass
    /// through to the upstream (always correct), then flip to index-served.
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

    /// The upstream's actual size of a key (one HEAD via the direct client).
    /// Used where the write path doesn't carry the size (multipart complete,
    /// copy) — indexing those at a placeholder poisons range promotion.
    async fn upstream_size(&self, bucket: &str, key: &str) -> Option<i64> {
        self.client.head_object().bucket(bucket).key(key).send().await.ok()?.content_length()
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
            input: { let mut i = req.input.clone(); i.range = None; i },
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
                    || Err(s3s::s3_error!(InvalidRange, "range start past end of object")),
                    |out| Ok(S3Response::new(out)),
                ))
            }
            Err(e) => {
                // Self-heal a lying index entry so this object stops
                // re-attempting promotion, then let the caller serve upstream.
                if let Some(sz) = e.strip_prefix("oversize ").and_then(|r| r.split(' ').next()).and_then(|n| n.parse::<i64>().ok())
                    && sz >= 0
                {
                    self.index_insert(&ckey.0, &ckey.1, sz);
                }
                self.metrics.range_promote_reject.fetch_add(1, Ordering::Relaxed);
                tracing::warn!("range promote of {}/{} failed ({e}); falling back to passthrough", ckey.0, ckey.1);
                None
            }
        }
    }

    fn is_synced(&self, bucket: &str) -> bool {
        self.state.read().unwrap().get(bucket).is_some_and(|b| b.synced)
    }

    /// Build a `ListObjectsV2` response from this bucket's index. See
    /// [`list_objects_v2_from_index`] for the algorithm.
    fn list_from_index(&self, inp: &ListObjectsV2Input) -> ListObjectsV2Output {
        let g = self.state.read().unwrap();
        list_objects_v2_from_index(g.get(inp.bucket.as_str()).map(|b| &b.keys), inp)
    }
}

/// The `ListObjectsV2` algorithm over an already-borrowed key index — free-standing so it
/// is unit-testable without a live proxy. Matches S3: sorted keys, prefix filter,
/// delimiter roll-up into common prefixes, max-keys paging with a key continuation token
/// (resumed *inclusively*, since the token is the next key to return), and `start_after`
/// (exclusive, first page only).
fn list_objects_v2_from_index(
    keys: Option<&BTreeMap<String, ObjEntry>>,
    inp: &ListObjectsV2Input,
) -> ListObjectsV2Output {
    let bucket = inp.bucket.as_str();
    let prefix = inp.prefix.clone().unwrap_or_default();
    let delim = inp.delimiter.clone();
    let max = usize::try_from(inp.max_keys.unwrap_or(1000).clamp(1, 1000)).unwrap_or(1000);

    let mut contents: Vec<Object> = Vec::new();
    let mut common: BTreeSet<String> = BTreeSet::new();
    let mut truncated = false;
    let mut next_token = None;

    if let Some(keys) = keys {
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
                 get_hit={} get_miss={} get_bypass={} range_hit={} range_promote={} range_promote_reject={} \
                 warm_hit={} warm_miss={} warm_error={} log_appended={} log_applied={} log_error={}",
                metrics.list_from_index.load(Ordering::Relaxed),
                metrics.list_passthrough.load(Ordering::Relaxed),
                metrics.writes_indexed.load(Ordering::Relaxed),
                metrics.get_hit.load(Ordering::Relaxed),
                metrics.get_miss.load(Ordering::Relaxed),
                metrics.get_bypass.load(Ordering::Relaxed),
                metrics.range_hit.load(Ordering::Relaxed),
                metrics.range_promote.load(Ordering::Relaxed),
                metrics.range_promote_reject.load(Ordering::Relaxed),
                metrics.warm_hit.load(Ordering::Relaxed),
                metrics.warm_miss.load(Ordering::Relaxed),
                metrics.warm_error.load(Ordering::Relaxed),
                metrics.log_appended.load(Ordering::Relaxed),
                metrics.log_applied.load(Ordering::Relaxed),
                metrics.log_error.load(Ordering::Relaxed),
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
        self.log_put(&bucket, &key, size).await;
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
        self.log_del(&bucket, &key).await;
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
            self.log_del(&bucket, &k).await;
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
        // Multipart is how the big objects arrive, and indexing them at a
        // placeholder size poisoned the range-promotion decision (a "0-byte"
        // entry promoted a multi-GB fetch). One HEAD learns the real size.
        let size = self.upstream_size(&bucket, &key).await.unwrap_or(0);
        self.index_insert(&bucket, &key, size);
        self.log_put(&bucket, &key, size).await;
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
        let size = self.upstream_size(&bucket, &key).await.unwrap_or(0);
        self.index_insert(&bucket, &key, size);
        self.log_put(&bucket, &key, size).await;
        self.obj_cache.invalidate(&(bucket, key)).await;
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
            if let Some(obj) = self.obj_cache.get(&ckey).await
                && let Some(out) = obj.to_get_range(first, last)
            {
                self.metrics.range_hit.fetch_add(1, Ordering::Relaxed);
                return Ok(S3Response::new(out));
            }
            // Promote when caching is on and the index says the whole object fits: one
            // upstream GET (deduped across concurrent ranges when hot is active), then
            // every range — this one included — is a slice. A refused/failed promote
            // degrades to the passthrough below, never an error.
            let small = self.obj_cache.is_enabled()
                && self
                    .index_size(&ckey.0, &ckey.1)
                    .is_some_and(|sz| sz >= 0 && usize::try_from(sz).unwrap_or(usize::MAX) <= self.max_obj_bytes);
            if small
                && let Some(resp) = self.promote_range(&ckey, &req, first, last).await
            {
                return resp;
            }
            // Big, not-yet-indexed, or a failed promote: stream the range through.
            self.metrics.get_bypass.fetch_add(1, Ordering::Relaxed);
            return self.inner.get_object(req).await;
        }
        if cacheable
            && let Some(obj) = self.obj_cache.get(&ckey).await
        {
            self.metrics.get_hit.fetch_add(1, Ordering::Relaxed);
            return Ok(S3Response::new(obj.to_get()));
        }
        let mut resp = self.inner.get_object(req).await?;
        let len = resp.output.content_length.unwrap_or(-1);
        let small = len >= 0 && usize::try_from(len).unwrap_or(usize::MAX) <= self.max_obj_bytes;
        if cacheable && small && self.obj_cache.is_enabled()
            && let Some(body) = resp.output.body.take()
        {
            match tier::buffer_body(body, self.max_obj_bytes).await {
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

#[cfg(test)]
mod tests {
    use super::{apply_log_event, list_objects_v2_from_index, BucketState, IndexLog, Metrics, ObjEntry};
    use crate::tier::{connect_valkey, CachedObject, HotCache};
    use fred::prelude::*;
    use s3s::dto::{GetObjectOutput, ListObjectsV2Input};
    use std::collections::{BTreeMap, HashMap};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, RwLock};
    use std::time::{Duration, UNIX_EPOCH};

    type Index = Arc<RwLock<HashMap<String, BucketState>>>;

    // ---- helpers -----------------------------------------------------------------

    static STREAM_SEQ: AtomicU64 = AtomicU64::new(0);

    /// A unique stream key per test so the (parallel) tests never interfere.
    fn unique_stream() -> String {
        format!("s3cache:test:{}:{}", std::process::id(), STREAM_SEQ.fetch_add(1, Ordering::Relaxed))
    }

    /// Connect a Valkey pool, or `None` (test skips) when `S3CACHE_TEST_VALKEY_URL` is unset.
    async fn valkey_pool() -> Option<Pool> {
        let url = std::env::var("S3CACHE_TEST_VALKEY_URL").ok()?;
        let pool = connect_valkey(&url, 3).unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await; // let it connect
        Some(pool)
    }

    fn read_client() -> Client {
        let url = std::env::var("S3CACHE_TEST_VALKEY_URL").unwrap();
        crate::tier::connect_valkey_client(&url).unwrap()
    }

    fn log(pool: &Pool, stream: &str, node: &str) -> IndexLog {
        IndexLog::new(pool.clone(), read_client(), stream.to_owned(), 10_000, node.to_owned(), Arc::new(Metrics::default()))
    }

    fn event(op: &str, bucket: &str, key: &str, size: &str, node: &str) -> HashMap<String, String> {
        HashMap::from([
            ("op".to_owned(), op.to_owned()),
            ("bucket".to_owned(), bucket.to_owned()),
            ("key".to_owned(), key.to_owned()),
            ("size".to_owned(), size.to_owned()),
            ("node".to_owned(), node.to_owned()),
            ("ts".to_owned(), "0".to_owned()),
        ])
    }

    fn indexed_size(state: &Index, bucket: &str, key: &str) -> Option<i64> {
        state.read().unwrap().get(bucket).and_then(|b| b.keys.get(key)).map(|e| e.size)
    }

    /// Poll `cond` for up to ~3s (the consumer applies within a few ms in practice).
    async fn wait_until(mut cond: impl FnMut() -> bool) -> bool {
        for _ in 0..60 {
            if cond() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        cond()
    }

    fn hot_cache() -> HotCache {
        moka::future::Cache::builder().max_capacity(1024).build()
    }

    fn cached(body: &'static [u8]) -> Arc<CachedObject> {
        Arc::new(CachedObject::from_get(&GetObjectOutput::default(), bytes::Bytes::from_static(body)))
    }

    // ---- pure apply logic (no Valkey) --------------------------------------------

    #[tokio::test]
    async fn apply_peer_own_and_malformed() {
        let state: Index = Arc::new(RwLock::new(HashMap::new()));

        // A peer's put applies.
        apply_log_event("me", &state, None, &event("put", "b", "k", "7", "peer")).await;
        assert_eq!(indexed_size(&state, "b", "k"), Some(7));

        // Our own event is ignored — the local index already reflects it.
        apply_log_event("me", &state, None, &event("del", "b", "k", "-1", "me")).await;
        assert_eq!(indexed_size(&state, "b", "k"), Some(7));

        // Malformed (missing key) and unknown ops are skipped, not panics.
        let mut no_key = event("put", "b", "k2", "1", "peer");
        no_key.remove("key");
        apply_log_event("me", &state, None, &no_key).await;
        apply_log_event("me", &state, None, &event("frobnicate", "b", "k", "1", "peer")).await;
        assert_eq!(indexed_size(&state, "b", "k2"), None);
        assert_eq!(indexed_size(&state, "b", "k"), Some(7));

        // A peer's delete applies.
        apply_log_event("me", &state, None, &event("del", "b", "k", "-1", "peer")).await;
        assert_eq!(indexed_size(&state, "b", "k"), None);
    }

    // ---- ListObjectsV2 parity with S3 (pure, no Valkey) --------------------------

    fn index(keys: &[&str]) -> BTreeMap<String, ObjEntry> {
        keys.iter().map(|k| ((*k).to_owned(), ObjEntry { size: 1, last_modified: UNIX_EPOCH })).collect()
    }

    fn list_input(max: i32, token: Option<&str>, prefix: &str, delim: Option<&str>, start_after: Option<&str>) -> ListObjectsV2Input {
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

    fn page_keys(out: &s3s::dto::ListObjectsV2Output) -> Vec<String> {
        out.contents.iter().flatten().filter_map(|o| o.key.clone()).collect()
    }
    fn page_prefixes(out: &s3s::dto::ListObjectsV2Output) -> Vec<String> {
        out.common_prefixes.iter().flatten().filter_map(|c| c.prefix.clone()).collect()
    }

    /// Follow the continuation tokens like a real client and collect every key + prefix.
    fn walk_pages(idx: &BTreeMap<String, ObjEntry>, max: i32, prefix: &str, delim: Option<&str>) -> (Vec<String>, Vec<String>) {
        let (mut keys, mut prefixes) = (Vec::new(), Vec::new());
        let mut token: Option<String> = None;
        for _ in 0..10_000 {
            let inp = list_input(max, token.as_deref(), prefix, delim, None);
            let out = list_objects_v2_from_index(Some(idx), &inp);
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
        // Prefix filter only.
        let out = list_objects_v2_from_index(Some(&idx), &list_input(1000, None, "p/a/", None, None));
        assert_eq!(page_keys(&out), ["p/a/1", "p/a/2"]);
        // Delimiter roll-up: sub-prefixes become common prefixes, bare keys stay.
        let out = list_objects_v2_from_index(Some(&idx), &list_input(1000, None, "p/", Some("/"), None));
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
        let out = list_objects_v2_from_index(Some(&idx), &list_input(1000, None, "", None, Some("b")));
        assert_eq!(page_keys(&out), ["c", "d"]);
    }

    #[test]
    fn list_empty_bucket() {
        let out = list_objects_v2_from_index(None, &list_input(1000, None, "", None, None));
        assert_eq!(out.is_truncated, Some(false));
        assert!(out.contents.is_none());
        assert_eq!(out.key_count, Some(0));
    }

    // ---- live cross-node coherence (Valkey-gated) --------------------------------

    macro_rules! valkey_or_skip {
        () => {
            match valkey_pool().await {
                Some(p) => p,
                None => {
                    eprintln!("skip: set S3CACHE_TEST_VALKEY_URL to run");
                    return;
                }
            }
        };
    }

    /// A put and a delete on node A both reach node B's index via the real consumer.
    #[tokio::test]
    async fn cross_node_put_and_delete() {
        let pool = valkey_or_skip!();
        let stream = unique_stream();
        let (a, b) = (log(&pool, &stream, "A"), log(&pool, &stream, "B"));
        let state: Index = Arc::new(RwLock::new(HashMap::new()));
        b.spawn_consumer(b.tail_id().await, state.clone(), None);

        a.append_put("bkt", "obj1", 42).await;
        assert!(wait_until(|| indexed_size(&state, "bkt", "obj1") == Some(42)).await, "B should see A's put");

        a.append_del("bkt", "obj1").await;
        assert!(wait_until(|| indexed_size(&state, "bkt", "obj1").is_none()).await, "B should see A's delete");

        let _: FredResult<i64> = pool.del(stream.as_str()).await;
    }

    /// Many keys across multiple buckets converge on the peer.
    #[tokio::test]
    async fn cross_node_multi_bucket() {
        let pool = valkey_or_skip!();
        let stream = unique_stream();
        let (a, b) = (log(&pool, &stream, "A"), log(&pool, &stream, "B"));
        let state: Index = Arc::new(RwLock::new(HashMap::new()));
        b.spawn_consumer(b.tail_id().await, state.clone(), None);

        for i in 0..25 {
            a.append_put("b1", &format!("k{i}"), i).await;
            a.append_put("b2", &format!("k{i}"), i + 1000).await;
        }
        let all = |st: &Index| (0..25).all(|i| {
            indexed_size(st, "b1", &format!("k{i}")) == Some(i)
                && indexed_size(st, "b2", &format!("k{i}")) == Some(i + 1000)
        });
        assert!(wait_until(|| all(&state)).await, "B should converge on all keys in both buckets");

        let _: FredResult<i64> = pool.del(stream.as_str()).await;
    }

    /// A peer's write invalidates this node's local hot object copy (no stale reads).
    #[tokio::test]
    async fn peer_write_invalidates_local_hot() {
        let pool = valkey_or_skip!();
        let stream = unique_stream();
        let (a, b) = (log(&pool, &stream, "A"), log(&pool, &stream, "B"));

        let hot = hot_cache();
        let ck = ("bkt".to_owned(), "obj".to_owned());
        hot.insert(ck.clone(), cached(b"stale")).await;
        assert!(hot.contains_key(&ck));

        let state: Index = Arc::new(RwLock::new(HashMap::new()));
        b.spawn_consumer(b.tail_id().await, state, Some(hot.clone()));

        a.append_put("bkt", "obj", 5).await;
        assert!(wait_until(|| !hot.contains_key(&ck)).await, "peer write must drop the local hot copy");

        let _: FredResult<i64> = pool.del(stream.as_str()).await;
    }

    /// Starting from a captured position delivers only newer events — the resume
    /// guarantee: no re-processing of old entries, no missing of new ones.
    #[tokio::test]
    async fn consumer_starts_from_position() {
        let pool = valkey_or_skip!();
        let stream = unique_stream();
        let (a, b) = (log(&pool, &stream, "A"), log(&pool, &stream, "B"));

        a.append_put("b", "before", 1).await;
        let start = b.tail_id().await; // position after `before`
        let state: Index = Arc::new(RwLock::new(HashMap::new()));
        b.spawn_consumer(start, state.clone(), None);

        a.append_put("b", "after", 2).await;
        assert!(wait_until(|| indexed_size(&state, "b", "after") == Some(2)).await, "sees events after the start");
        assert_eq!(indexed_size(&state, "b", "before"), None, "does not replay events before the start");

        let _: FredResult<i64> = pool.del(stream.as_str()).await;
    }

    /// Capturing the tail *before* the bootstrap window, then replaying from it, loses no
    /// write that lands during bootstrap — the ordering `main` relies on at startup.
    #[tokio::test]
    async fn bootstrap_replay_has_no_gap() {
        let pool = valkey_or_skip!();
        let stream = unique_stream();
        let (a, b) = (log(&pool, &stream, "A"), log(&pool, &stream, "B"));

        let start = b.tail_id().await; // captured before the "bootstrap"
        a.append_put("b", "during1", 1).await; // writes that race the bootstrap LIST
        a.append_put("b", "during2", 2).await;

        let state: Index = Arc::new(RwLock::new(HashMap::new()));
        b.spawn_consumer(start, state.clone(), None);

        assert!(
            wait_until(|| indexed_size(&state, "b", "during1") == Some(1) && indexed_size(&state, "b", "during2") == Some(2)).await,
            "replay from the pre-bootstrap tail must not miss writes"
        );

        let _: FredResult<i64> = pool.del(stream.as_str()).await;
    }

    /// Stream order is preserved through the consumer: put-then-del clears, del-then-put sets.
    #[tokio::test]
    async fn ordering_converges() {
        let pool = valkey_or_skip!();
        let stream = unique_stream();
        let (a, b) = (log(&pool, &stream, "A"), log(&pool, &stream, "B"));
        let state: Index = Arc::new(RwLock::new(HashMap::new()));
        b.spawn_consumer(b.tail_id().await, state.clone(), None);

        a.append_put("b", "x", 5).await;
        a.append_del("b", "x").await;
        assert!(wait_until(|| indexed_size(&state, "b", "x").is_none()).await, "put then del => absent");

        a.append_del("b", "y").await;
        a.append_put("b", "y", 9).await;
        assert!(wait_until(|| indexed_size(&state, "b", "y") == Some(9)).await, "del then put => present");

        let _: FredResult<i64> = pool.del(stream.as_str()).await;
    }

    /// Approximate `MAXLEN` trimming bounds the stream instead of growing without limit.
    #[tokio::test]
    async fn maxlen_bounds_the_stream() {
        let pool = valkey_or_skip!();
        let stream = unique_stream();
        let small = IndexLog::new(pool.clone(), read_client(), stream.clone(), 20, "A".to_owned(), Arc::new(Metrics::default()));
        for i in 0..200 {
            small.append_put("b", &format!("k{i}"), i).await;
        }
        let len: i64 = pool.xlen(stream.as_str()).await.unwrap();
        assert!((20..150).contains(&len), "MAXLEN ~20 should bound the stream (got {len})");

        let _: FredResult<i64> = pool.del(stream.as_str()).await;
    }
}
