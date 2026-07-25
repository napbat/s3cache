//! Cross-node index coherence: a shared, ordered commit log on a Valkey Stream. Each
//! write appends one event; every node tails the stream and applies peers' events to its
//! local index and hot cache, so a write on one node reaches all of them. The consumer
//! also publishes its applied position, which the proxy's read-barrier uses to make
//! cross-node reads strongly consistent.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fred::prelude::*;

use crate::index::{BucketState, ObjEntry};
use crate::metrics::Metrics;
use crate::tier::LocalCache;

/// Build a Valkey connection pool from `url` and start connecting in the background (for
/// the index-log appends + one-shot reads). Errors only on a bad URL or pool size — never
/// for the server being down, which self-heals via the reconnect policy.
pub(crate) fn connect_valkey(url: &str, pool_size: usize) -> anyhow::Result<Pool> {
    let config = Config::from_url(url)?;
    let mut builder = Builder::from_config(config);
    builder.set_policy(ReconnectPolicy::new_exponential(0, 100, 30_000, 2));
    let pool = builder.build_pool(pool_size.max(1))?;
    pool.connect();
    Ok(pool)
}

/// A single dedicated Valkey connection for the consumer's blocking `XREAD`: a blocking
/// read monopolizes its connection, so it must not share the pool with the write-path
/// appends (they would stall behind it).
pub(crate) fn connect_valkey_client(url: &str) -> anyhow::Result<Client> {
    let config = Config::from_url(url)?;
    let mut builder = Builder::from_config(config);
    builder.set_policy(ReconnectPolicy::new_exponential(0, 100, 30_000, 2));
    let client = builder.build()?;
    client.connect();
    Ok(client)
}

/// A commit-log Valkey op may never stall a write: abandoned after this, treated as a
/// dropped append (the peers re-converge on their next full sync).
const LOG_OP_TIMEOUT: Duration = Duration::from_secs(2);

/// How long a strict LIST waits for the local consumer to catch up to the stream tail
/// before serving anyway (degrading to eventual rather than hanging the request).
const LIST_BARRIER_TIMEOUT: Duration = Duration::from_secs(1);

/// A Valkey Stream entry ID `<ms>-<seq>`, parsed into an ordered pair (`"0"` = the
/// origin). Tuple ordering matches stream order, so positions compare with `>=`.
pub(crate) type StreamId = (u64, u64);

pub(crate) fn parse_stream_id(id: &str) -> StreamId {
    let (ms, seq) = id.split_once('-').unwrap_or((id, "0"));
    (ms.parse().unwrap_or(0), seq.parse().unwrap_or(0))
}

/// The shared, ordered commit log of index mutations (a Valkey Stream). Replayable — a
/// reconnecting node resumes from its last-applied ID and can't miss a write, the failure
/// mode raw pub/sub has — which is what makes it OCC-safe.
pub(crate) struct IndexLog {
    /// Pool for appends (`XADD`) and one-shot reads — non-blocking, shared with warm.
    pool: Pool,
    /// Dedicated connection for the consumer's blocking `XREAD` (see `connect_valkey_client`).
    read_client: Client,
    pub(crate) stream: String,
    maxlen: i64,
    /// This process's id (e.g. the pod name) so it can skip replaying its own events.
    node: String,
    /// The highest stream ID this node's consumer has processed. Read by the strict-LIST
    /// barrier to tell whether the local index has caught up to a given stream position.
    applied: Arc<RwLock<StreamId>>,
    metrics: Arc<Metrics>,
}

impl IndexLog {
    /// Wrap a connected Valkey pool (appends) plus a dedicated read connection (the
    /// blocking consumer) as the index commit log.
    #[must_use]
    pub(crate) fn new(pool: Pool, read_client: Client, stream: String, maxlen: u64, node: String, metrics: Arc<Metrics>) -> Self {
        Self {
            pool,
            read_client,
            stream,
            maxlen: i64::try_from(maxlen).unwrap_or(i64::MAX),
            node,
            applied: Arc::new(RwLock::new((0, 0))),
            metrics,
        }
    }

    /// Wait until this node's consumer has applied every event committed to the stream as
    /// of now, so a following index-served LIST reflects all writes that completed before
    /// this call (strong cross-node read-after-write for LIST). Best-effort: returns
    /// immediately if Valkey is unreachable or the stream is empty, and gives up after
    /// `LIST_BARRIER_TIMEOUT` rather than hanging the request. Returns whether it caught up.
    pub(crate) async fn await_fresh(&self) -> bool {
        let Some(target) = self.latest_id().await else {
            return true; // empty stream / unreachable -> nothing to wait for
        };
        let mut waited = Duration::ZERO;
        let step = Duration::from_millis(5);
        loop {
            if *self.applied.read().unwrap() >= target {
                return true;
            }
            if waited >= LIST_BARRIER_TIMEOUT {
                return false;
            }
            tokio::time::sleep(step).await;
            waited += step;
        }
    }

    /// The stream's current tail as a parsed [`StreamId`], or `None` if empty/unreachable.
    async fn latest_id(&self) -> Option<StreamId> {
        if !self.pool.is_connected() {
            return None;
        }
        let fetch = self.pool.xrevrange(&self.stream, "+", "-", Some(1));
        let res: FredResult<Vec<(String, HashMap<String, String>)>> =
            tokio::time::timeout(LOG_OP_TIMEOUT, fetch).await.ok()?;
        res.ok()?.into_iter().next().map(|(id, _)| parse_stream_id(&id))
    }

    /// Append a write event, capped with approximate `MAXLEN` trimming. Best-effort with
    /// a timeout: an unreachable Valkey drops the event (logged + counted) rather than
    /// blocking the write; peers re-converge on their next full sync.
    async fn append(&self, op: &str, bucket: &str, key: &str, size: i64) {
        // Skip instantly when Valkey is down rather than queuing into the timeout — a
        // dropped append just delays a peer until its next full sync, but it must never
        // add seconds of latency to the write.
        if !self.pool.is_connected() {
            self.metrics.log_error();
            return;
        }
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

    pub(crate) async fn append_put(&self, bucket: &str, key: &str, size: i64) {
        self.append("put", bucket, key, size).await;
    }

    pub(crate) async fn append_del(&self, bucket: &str, key: &str) {
        self.append("del", bucket, key, -1).await;
    }

    /// The stream's current tail ID (or `"0"` if empty / unreachable). Captured before the
    /// startup LIST bootstrap so the consumer replays everything appended from that point
    /// — nothing is missed, and re-applying what the bootstrap already saw is idempotent.
    pub(crate) async fn tail_id(&self) -> String {
        // Bounded by a timeout so a down Valkey at startup can't hang the serve loop
        // (start_index_log is awaited before binding). Falls back to "0" (replay from the
        // beginning, which is idempotent) if Valkey is unreachable.
        let fetch = self.pool.xrevrange(&self.stream, "+", "-", Some(1));
        let res: FredResult<Vec<(String, HashMap<String, String>)>> =
            match tokio::time::timeout(LOG_OP_TIMEOUT, fetch).await {
                Ok(r) => r,
                Err(_) => return "0".to_owned(),
            };
        match res {
            Ok(entries) => entries.into_iter().next().map_or_else(|| "0".to_owned(), |(id, _)| id),
            Err(_) => "0".to_owned(),
        }
    }

    /// Spawn the background consumer that tails the stream from just after `start_id` and
    /// applies peers' events to `state` and `hot`.
    pub(crate) fn spawn_consumer(
        &self,
        start_id: String,
        state: Arc<RwLock<HashMap<String, BucketState>>>,
        local: Option<LocalCache>,
    ) {
        let (read, stream, node, metrics, applied) = (
            self.read_client.clone(),
            self.stream.clone(),
            self.node.clone(),
            self.metrics.clone(),
            self.applied.clone(),
        );
        // The consumer starts at start_id, so the local index reflects everything up to it.
        *applied.write().unwrap() = parse_stream_id(&start_id);
        tokio::spawn(async move {
            let cx = ConsumerCtx { read: &read, stream: &stream, node: &node, state: &state, local: local.as_ref(), applied: &applied, metrics: &metrics };
            consume_index_log(&cx, start_id).await;
        });
    }
}

/// The shared handles a running index-log consumer borrows for its lifetime.
struct ConsumerCtx<'a> {
    read: &'a Client,
    stream: &'a str,
    node: &'a str,
    state: &'a RwLock<HashMap<String, BucketState>>,
    local: Option<&'a LocalCache>,
    applied: &'a RwLock<StreamId>,
    metrics: &'a Arc<Metrics>,
}

/// Tail the index commit log forever, applying each event to the local index and hot
/// cache. A read error just backs off and retries — the position (`last_id`) is kept, so
/// no event is skipped across a transient Valkey blip.
async fn consume_index_log(cx: &ConsumerCtx<'_>, mut last_id: String) {
    loop {
        // While Valkey is unreachable, idle instead of hammering it with blocking reads
        // that would queue and time out; fred reconnects in the background.
        if !cx.read.is_connected() {
            tokio::time::sleep(Duration::from_millis(200)).await;
            continue;
        }
        // Raw `xread` + manual conversion: a BLOCK that times out with no new entries
        // returns nil, which `xread_map` would reject as a decode error — treat nil as
        // "nothing yet". `into_xread_response` then normalizes the RESP2/RESP3 encoding.
        let reply: FredResult<Value> = cx.read.xread(Some(500), Some(5000), cx.stream, &last_id).await;
        let map = match reply {
            Ok(v) if v.is_null() => continue, // BLOCK timed out with no new entries
            Ok(v) => match v.into_xread_response::<String, String, String, String>() {
                Ok(map) => map,
                Err(e) => {
                    cx.metrics.log_error();
                    tracing::warn!("index log decode failed: {e}; retrying");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            },
            Err(e) => {
                cx.metrics.log_error();
                tracing::warn!("index log read failed: {e}; retrying");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        let Some(entries) = map.get(cx.stream) else { continue };
        for (id, fields) in entries {
            apply_log_event(cx.node, cx.state, cx.local, fields).await;
            last_id.clone_from(id);
            // Advance the applied position for every entry (including our own, which
            // apply skips) so the strict-LIST barrier tracks true stream progress.
            *cx.applied.write().unwrap() = parse_stream_id(id);
            cx.metrics.log_applied();
        }
    }
}

/// Apply one commit-log event to the local index and drop the key from the local hot
/// cache. Skips this node's own events (already reflected locally) and any malformed one.
pub(crate) async fn apply_log_event(
    node: &str,
    state: &RwLock<HashMap<String, BucketState>>,
    local: Option<&LocalCache>,
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
    if let Some(local) = local {
        local.invalidate(&(bucket.clone(), key.clone())).await;
    }
}

#[cfg(test)]
mod tests {
    use super::{apply_log_event, connect_valkey, connect_valkey_client, parse_stream_id, IndexLog};
    use crate::index::BucketState;
    use crate::metrics::Metrics;
    use crate::tier::{CachedObject, TieredCache};
    use fred::prelude::*;
    use s3s::dto::GetObjectOutput;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, RwLock};
    use std::time::Duration;

    type Index = Arc<RwLock<HashMap<String, BucketState>>>;

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
        connect_valkey_client(&url).unwrap()
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

    fn hot_cache() -> TieredCache {
        TieredCache::new(1024 * 1024, None, Arc::new(Metrics::default()))
    }

    fn cached(body: &'static [u8]) -> Arc<CachedObject> {
        Arc::new(CachedObject::from_get(&GetObjectOutput::default(), bytes::Bytes::from_static(body)))
    }

    #[tokio::test]
    async fn apply_peer_own_and_malformed() {
        let state: Index = Arc::new(RwLock::new(HashMap::new()));
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

    #[test]
    fn stream_id_orders_numerically() {
        assert_eq!(parse_stream_id("0"), (0, 0));
        assert_eq!(parse_stream_id("1700000000000-5"), (1_700_000_000_000, 5));
        assert!(parse_stream_id("10-0") > parse_stream_id("9-0")); // 10 > 9, not "10" < "9"
        assert!(parse_stream_id("100-2") > parse_stream_id("100-1"));
        assert!(parse_stream_id("101-0") > parse_stream_id("100-9"));
    }

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

    #[tokio::test]
    async fn peer_write_invalidates_local_hot() {
        let pool = valkey_or_skip!();
        let stream = unique_stream();
        let (a, b) = (log(&pool, &stream, "A"), log(&pool, &stream, "B"));

        let hot = hot_cache();
        let ck = ("bkt".to_owned(), "obj".to_owned());
        hot.insert(ck.clone(), cached(b"stale")).await;
        assert!(hot.get(&ck).await.is_some());

        let state: Index = Arc::new(RwLock::new(HashMap::new()));
        b.spawn_consumer(b.tail_id().await, state, Some(hot.local()));

        a.append_put("bkt", "obj", 5).await;
        let mut gone = false;
        for _ in 0..60 {
            if hot.get(&ck).await.is_none() {
                gone = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(gone, "peer write must drop the local hot copy");

        let _: FredResult<i64> = pool.del(stream.as_str()).await;
    }

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

    #[tokio::test]
    async fn strict_barrier_blocks_until_caught_up() {
        let pool = valkey_or_skip!();
        let stream = unique_stream();
        let _: FredResult<i64> = pool.del(stream.as_str()).await;
        let lg = IndexLog::new(pool.clone(), read_client(), stream.clone(), 10_000, "n".to_owned(), Arc::new(Metrics::default()));

        assert!(lg.await_fresh().await, "empty stream: nothing to wait for");

        lg.append_put("b", "k", 1).await; // tail advances; applied is still (0,0)
        assert!(!lg.await_fresh().await, "must NOT release while behind the tail (times out)");

        *lg.applied.write().unwrap() = lg.latest_id().await.unwrap(); // simulate catch-up
        assert!(lg.await_fresh().await, "releases once caught up to the tail");

        let _: FredResult<i64> = pool.del(stream.as_str()).await;
    }
}
