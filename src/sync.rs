//! Cross-node coherence over groupnet's consistency layer — the whole of it:
//! no raft, no shared broker. Each node publishes its durable writes as
//! typed [`IndexEvent`]s into a per-node [`WriteFeed`]; every peer's apply
//! loop folds them into the LIST index (per-key last-writer-wins with
//! delete-wins-ties tombstones — see [`crate::index`]) and drops the stale
//! body-cache copies. Events reach live peers at network latency (the engine
//! pushes deltas eagerly).
//!
//! Honest semantics, in one paragraph: each node's events arrive in its own
//! write order; there is no cross-writer total order, so concurrent writes to
//! one key through different nodes resolve by timestamp (deletes win ties)
//! and the origin — which serves conditional (OCC) writes untouched — stays
//! the authority the index is a cache of. Provably-missed events (ring
//! overflow, a peer restart) surface as a gap: the local tiers flush and the
//! LIST index resyncs from the origin. The strict-LIST barrier
//! ([`WriteSync::await_fresh`]) waits until every peer's currently-advertised
//! feed head has been applied locally — freshness bounded by one push/gossip
//! hop, degrading to serving current state on timeout.
//!
//! Read-your-writes tokens are returned by the feed but not yet surfaced in
//! the API (needs a header design).

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, OnceLock, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use groupnet::consistency::{
    AckLedger, Frontier, PeerWrite, PeerWrites, WriteFeed, WriteToken, advertised_head,
    applied_cluster_wide,
};
use groupnet::core::{NodeId, Status};
use groupnet::runtime::{Group, Node};
use groupnet::transport::udp::UdpTransport;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::index::{BucketState, apply_del, apply_put};
use crate::metrics::Metrics;
use crate::tier::LocalCache;

/// Ring capacity: a peer that falls further behind than this many writes
/// gets a gap (flush + origin resync) instead of per-event application.
const FEED_CAPACITY: usize = 4096;

/// How much coherence the cluster pays for.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Consistency {
    /// Indistinguishable from a single S3 node with zero client
    /// cooperation: writes wait for every alive peer to apply their
    /// invalidation, and a node whose membership view is not fully alive
    /// serves via the origin. Costs one ack round (~2 in-cluster hops,
    /// behind the origin round-trip already paid) per write, plus one
    /// ledger-entry republish per applied event — right for fleet-sized
    /// clusters, wrong past the scaling envelope (run `bounded` + cells).
    Strong,
    /// The gossip bound only: reads are fresh within ~one push hop, session
    /// tokens still upgrade individual reads to strict, but writes return as
    /// soon as the origin acks and no ledger traffic flows. For large
    /// clusters where per-write cluster acks are unaffordable. Must be set
    /// uniformly: a bounded node never acks, so strong writers would wait
    /// out their ack timeout on every write (loud, not stale).
    Bounded,
}

/// A published write: the raw token (for the cluster-wide ack wait) and its
/// header form (for the response).
pub(crate) struct WriteReceipt {
    pub(crate) token: WriteToken,
    pub(crate) header: String,
}

/// Response header carrying a write's session token (`writer:epoch:seq`).
/// A client that echoes it on later reads gets strict read-after-write for
/// that write on any node, regardless of propagation timing.
pub(crate) const WRITE_TOKEN_HEADER: &str = "x-s3cache-write-token";

/// Request header echoing a [`WRITE_TOKEN_HEADER`] value: the read barriers
/// on that specific write having been applied locally.
pub(crate) const READ_TOKEN_HEADER: &str = "x-s3cache-read-token";

/// Splits `writer:epoch:seq` from the right, so writer names may contain
/// colons. `None` for anything else.
fn parse_token(value: &str) -> Option<(&str, u64, u64)> {
    let (rest, seq) = value.rsplit_once(':')?;
    let (writer, epoch) = rest.rsplit_once(':')?;
    Some((writer, epoch.parse().ok()?, seq.parse().ok()?))
}

/// Attempts (one per second) to resolve a gossip seed's DNS name within one
/// refresh cycle — a `StatefulSet` peer's record can lag its own startup.
const SEED_RESOLVE_ATTEMPTS: u32 = 30;

/// How often each seed is re-resolved, forever. Pod IPs churn on restarts
/// and the seed's DNS record follows; re-resolution is the recovery channel
/// that works even when gossip cannot deliver the new address (a rebooted
/// peer is deaf to us until OUR datagrams come from an address it knows).
const SEED_REFRESH: Duration = Duration::from_secs(15);

/// Resolves `host:port` (a DNS name or a literal address) to a socket
/// address, retrying briefly; `None` when it never resolves.
async fn resolve_seed(addr: &str) -> Option<std::net::SocketAddr> {
    for attempt in 0..SEED_RESOLVE_ATTEMPTS {
        if attempt > 0 {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        match tokio::net::lookup_host(addr).await {
            Ok(mut addrs) => {
                if let Some(sock) = addrs.next() {
                    return Some(sock);
                }
            }
            Err(error) => tracing::debug!("resolving gossip seed `{addr}`: {error}"),
        }
    }
    None
}

/// What one durable write did, as advertised to peers.
#[derive(Serialize, Deserialize)]
pub(crate) enum IndexOp {
    /// The key now holds an object of `size` bytes.
    Put {
        /// Object size, for the LIST index.
        size: i64,
    },
    /// The key was deleted.
    Del,
}

/// One durable write: the operation, its `(bucket, key)`, and the writer's
/// wall-clock timestamp (millis) — the cross-writer LWW tiebreak.
#[derive(Serialize, Deserialize)]
pub(crate) struct IndexEvent {
    pub(crate) op: IndexOp,
    pub(crate) bucket: String,
    pub(crate) key: String,
    pub(crate) ts_ms: u64,
}

fn to_millis(ts: SystemTime) -> u64 {
    ts.duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

fn from_millis(ms: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(ms)
}

fn encode_event(event: &IndexEvent) -> Vec<u8> {
    bincode::serialize(event).unwrap_or_default()
}

fn decode_event(bytes: &[u8]) -> Option<IndexEvent> {
    bincode::deserialize(bytes).ok()
}

/// The publishing half of the write feed, plus the barrier view.
pub struct WriteSync {
    feed: WriteFeed<IndexEvent>,
    group: Group,
    me: NodeId,
    consistency: Consistency,
    /// Set by [`start_apply`](Self::start_apply); the freshness barrier reads it.
    view: OnceLock<groupnet::consistency::FrontierView>,
    /// Keeps the gossip node (receive loop, group actors) alive for the
    /// process lifetime. `None` when a test drives a raw group directly.
    _node: Option<Node<UdpTransport>>,
}

impl WriteSync {
    /// Attach a feed to `group` as `me`. Start the apply loop separately with
    /// [`start_apply`](Self::start_apply) once the local cache exists.
    pub(crate) fn attach(
        group: Group,
        me: NodeId,
        consistency: Consistency,
        node: Option<Node<UdpTransport>>,
    ) -> Self {
        let capacity = NonZeroUsize::new(FEED_CAPACITY).unwrap_or(NonZeroUsize::MIN);
        let feed = WriteFeed::new(group.clone(), capacity, encode_event);
        Self {
            feed,
            group,
            me,
            consistency,
            view: OnceLock::new(),
            _node: node,
        }
    }

    /// Advertise a durable put to peers, stamped with the local index's `ts`.
    pub(crate) async fn publish_put(
        &self,
        bucket: &str,
        key: &str,
        size: i64,
        ts: SystemTime,
        metrics: &Metrics,
    ) -> WriteReceipt {
        self.publish(IndexOp::Put { size }, bucket, key, ts, metrics)
            .await
    }

    /// Advertise a durable delete to peers.
    pub(crate) async fn publish_del(
        &self,
        bucket: &str,
        key: &str,
        ts: SystemTime,
        metrics: &Metrics,
    ) -> WriteReceipt {
        self.publish(IndexOp::Del, bucket, key, ts, metrics).await
    }

    async fn publish(
        &self,
        op: IndexOp,
        bucket: &str,
        key: &str,
        ts: SystemTime,
        metrics: &Metrics,
    ) -> WriteReceipt {
        let event = IndexEvent {
            op,
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            ts_ms: to_millis(ts),
        };
        let token = self.feed.publish(&event).await;
        metrics.feed_published();
        WriteReceipt {
            token,
            header: format!("{}:{}:{}", self.me.as_str(), token.epoch, token.seq),
        }
    }

    /// Waits (bounded) until every currently-alive peer has applied this
    /// node's write — the write-side half of transparent coherence: once
    /// this resolves, no peer's cache can serve the overwritten state.
    pub(crate) async fn wait_cluster_applied(&self, token: WriteToken, timeout: Duration) -> bool {
        if self.consistency == Consistency::Bounded {
            return true; // bounded mode: the origin ack is the write's end
        }
        applied_cluster_wide(&self.group, &self.me, token, timeout).await
    }

    /// Whether this node's membership view is fully alive. A node that sees
    /// a suspect or dead peer may itself be the partitioned one, so its
    /// cache-served reads route to the origin until the view heals — the
    /// read-side half of transparent coherence.
    pub(crate) fn cluster_healthy(&self) -> bool {
        if self.consistency == Consistency::Bounded {
            return true; // bounded mode: serve local, freshness is the bound
        }
        self.group
            .statuses()
            .into_iter()
            .all(|(_, status)| status == Status::Alive)
    }

    /// Waits (bounded) until one specific write — a [`WRITE_TOKEN_HEADER`]
    /// value echoed by a client — has been applied locally. Tokens this node
    /// issued are trivially satisfied (its own writes are already local);
    /// garbled tokens are ignored (the freshness barrier still ran); a
    /// foreign token that cannot be verified in time returns `false`, and the
    /// caller serves from the origin instead of local state.
    pub(crate) async fn reached_token(&self, header: &str, timeout: Duration) -> bool {
        let Some((writer, epoch, seq)) = parse_token(header) else {
            return true;
        };
        if writer == self.me.as_str() {
            return true;
        }
        let Some(view) = self.view.get() else {
            return false; // no apply loop: a foreign token is unverifiable
        };
        let write = WriteToken { epoch, seq };
        tokio::time::timeout(timeout, view.reached(&NodeId::new(writer), write))
            .await
            .unwrap_or(false)
    }

    /// Spawn the apply loop: peers' events fold into the LIST index and drop
    /// the local body copies; a gap flushes every local tier and triggers
    /// `resync` (an origin re-LIST) since the stale subset is unknowable.
    pub(crate) fn start_apply(
        &self,
        local: LocalCache,
        state: Arc<RwLock<HashMap<String, BucketState>>>,
        resync: Arc<dyn Fn() + Send + Sync>,
        metrics: Arc<Metrics>,
    ) {
        let (frontier, view) = Frontier::new();
        let _ = self.view.set(view);
        // Bounded mode publishes no acks (that is its point at scale).
        let ledger =
            (self.consistency == Consistency::Strong).then(|| AckLedger::new(self.group.clone()));
        let mut peers = PeerWrites::new(self.group.clone(), self.me.clone(), decode_event);
        tokio::spawn(async move {
            while let Some(event) = peers.next().await {
                match event {
                    PeerWrite::Wrote {
                        peer,
                        token,
                        key: event,
                    } => {
                        let ts = from_millis(event.ts_ms);
                        match event.op {
                            IndexOp::Put { size } => {
                                apply_put(&state, &event.bucket, &event.key, size, ts);
                            }
                            IndexOp::Del => {
                                apply_del(&state, &event.bucket, &event.key, ts);
                            }
                        }
                        local.invalidate(&(event.bucket, event.key)).await;
                        frontier.advance(&peer, token);
                        if let Some(ledger) = &ledger {
                            ledger.record(&peer, token).await;
                        }
                        metrics.feed_applied();
                    }
                    PeerWrite::Gap {
                        peer,
                        missed_through,
                    } => {
                        warn!("write-feed gap from `{peer}`: flushing tiers, resyncing index");
                        // Flush first: with every local copy gone (and the
                        // index bucket back to passthrough), acking the gap
                        // is truthful even while the origin resync runs.
                        local.flush().await;
                        resync();
                        frontier.advance(&peer, missed_through);
                        if let Some(ledger) = &ledger {
                            ledger.record(&peer, missed_through).await;
                        }
                        metrics.feed_gap();
                    }
                }
            }
        });
    }

    /// Wait (bounded by `timeout`) until every peer's currently-advertised
    /// feed head has been applied locally. Returns whether it fully caught
    /// up; freshness is bounded by one push/gossip hop (see module docs).
    pub(crate) async fn await_fresh(&self, timeout: Duration) -> bool {
        let Some(view) = self.view.get() else {
            return true; // apply loop not started: nothing is being tracked
        };
        let deadline = Instant::now() + timeout;
        for member in self.group.members() {
            if member == self.me {
                continue;
            }
            let Some(head) = advertised_head(&self.group, &member) else {
                continue; // no feed advertised: nothing to wait for
            };
            let remaining = deadline.saturating_duration_since(Instant::now());
            match tokio::time::timeout(remaining, view.reached(&member, head)).await {
                Ok(true) => {}
                Ok(false) | Err(_) => return false, // apply loop gone, or out of time
            }
        }
        true
    }
}

/// Build the gossip node and write feed from `S3CACHE_GOSSIP_*`, or `None`
/// when `S3CACHE_GOSSIP_BIND` is unset (single-node: the sole writer is
/// already strict). Seeds are comma-separated `id=host:port` pairs; every
/// other peer resolves itself through gossiped advertisements, so only seeds
/// need static addressing.
pub async fn from_env(node_name: &str) -> Option<WriteSync> {
    let bind = std::env::var("S3CACHE_GOSSIP_BIND").ok()?;
    let consistency = match std::env::var("S3CACHE_CONSISTENCY")
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "" | "strong" => Consistency::Strong,
        "bounded" => Consistency::Bounded,
        other => {
            warn!("unknown S3CACHE_CONSISTENCY `{other}`; using strong");
            Consistency::Strong
        }
    };
    let me = NodeId::new(node_name);
    let transport = match UdpTransport::bind(me.clone(), bind.as_str()).await {
        Ok(transport) => transport,
        Err(error) => {
            warn!("gossip disabled: cannot bind `{bind}`: {error}");
            return None;
        }
    };
    let advertise = std::env::var("S3CACHE_GOSSIP_ADVERTISE")
        .ok()
        .or_else(|| transport.local_addr().ok().map(|addr| addr.to_string()));
    let mut builder = Node::builder(me.clone(), transport.clone());
    if let Some(advertise) = advertise {
        builder = builder.advertise_addr(advertise);
    }
    let node = builder.spawn();
    let group = node.join_group("s3cache");
    // Seeds resolve off the startup path (DNS for a just-starting peer may
    // lag, and a slow resolver must not delay serving): each one registers
    // with the transport and joins via `add_peer` once its address is known.
    let seeds = std::env::var("S3CACHE_GOSSIP_SEEDS").unwrap_or_default();
    for seed in seeds.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let Some((id, addr)) = seed.split_once('=') else {
            warn!("ignoring malformed gossip seed `{seed}` (want id=host:port)");
            continue;
        };
        if id == node_name {
            continue; // a pod seeding itself (uniform config) is a no-op
        }
        let (id, addr) = (id.to_owned(), addr.to_owned());
        let (transport, group) = (transport.clone(), group.clone());
        tokio::spawn(async move {
            let mut registered: Option<std::net::SocketAddr> = None;
            loop {
                match resolve_seed(&addr).await {
                    Some(sock) if registered != Some(sock) => {
                        if registered.is_some() {
                            info!("gossip seed `{id}` moved to {sock}; re-registering");
                        }
                        transport.register_peer(NodeId::new(id.as_str()), sock);
                        group.add_peer(NodeId::new(id.as_str()));
                        registered = Some(sock);
                    }
                    None if registered.is_none() => {
                        warn!("gossip seed `{id}={addr}` not resolving yet; will keep trying");
                    }
                    Some(_) | None => {}
                }
                tokio::time::sleep(SEED_REFRESH).await;
            }
        });
    }
    let mode = if consistency == Consistency::Strong {
        "strong"
    } else {
        "bounded"
    };
    info!("gossip coherence bound on `{bind}` as `{node_name}` (consistency: {mode})");
    Some(WriteSync::attach(group, me, consistency, Some(node)))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, RwLock};
    use std::time::{Duration, SystemTime};

    use groupnet::core::NodeId;
    use groupnet::runtime::{Group, Node};
    use groupnet::transport::mem::{MemTransport, Network};
    use s3s::dto::GetObjectOutput;

    use super::{Consistency, IndexEvent, IndexOp, WriteSync, decode_event, encode_event};
    use crate::index::BucketState;
    use crate::metrics::Metrics;
    use crate::tier::{CachedObject, TieredCache};

    type Index = Arc<RwLock<HashMap<String, BucketState>>>;

    fn spawn_node(net: &Network, id: &str, peer: &str) -> (NodeId, Node<MemTransport>, Group) {
        let me = NodeId::new(id);
        let node = Node::builder(me.clone(), net.endpoint(me.clone()))
            .seed(NodeId::new(peer))
            .gossip_interval_ms(10)
            .anti_entropy_interval_ms(25)
            .spawn();
        let group = node.join_group("s3cache");
        (me, node, group)
    }

    fn cached(body: &'static [u8]) -> Arc<CachedObject> {
        Arc::new(CachedObject::from_get(
            &GetObjectOutput::default(),
            bytes::Bytes::from_static(body),
        ))
    }

    fn indexed_size(state: &Index, bucket: &str, key: &str) -> Option<i64> {
        state
            .read()
            .unwrap()
            .get(bucket)
            .and_then(|b| b.keys.get(key))
            .map(|e| e.size)
    }

    /// A fully-wired pair: node A publishes, node B applies into `state` and
    /// its local cache. Returns A's publisher, B's sync, and B's state and cache.
    fn wired_pair(net: &Network) -> (WriteSync, WriteSync, Index, TieredCache) {
        let (a_id, _a_node, a_group) = spawn_node(net, "sync-a", "sync-b");
        let (b_id, _b_node, b_group) = spawn_node(net, "sync-b", "sync-a");
        let metrics = Arc::new(Metrics::default());
        let cache = TieredCache::new(1024 * 1024, None, metrics.clone());
        let state: Index = Arc::new(RwLock::new(HashMap::new()));
        let sync_b = WriteSync::attach(b_group, b_id, Consistency::Strong, None);
        sync_b.start_apply(cache.local(), state.clone(), Arc::new(|| {}), metrics);
        let sync_a = WriteSync::attach(a_group, a_id, Consistency::Strong, None);
        (sync_a, sync_b, state, cache)
    }

    async fn eventually(mut cond: impl FnMut() -> bool, what: &str) {
        for _ in 0..300 {
            if cond() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("timed out waiting for: {what}");
    }

    #[test]
    fn event_codec_round_trips_and_rejects_garbage() {
        let event = IndexEvent {
            op: IndexOp::Put { size: 42 },
            bucket: "bucket-1".to_owned(),
            key: "a/b weird\0key".to_owned(),
            ts_ms: 1_700_000_000_000,
        };
        let back = decode_event(&encode_event(&event)).expect("round trip");
        assert!(matches!(back.op, IndexOp::Put { size: 42 }));
        assert_eq!(back.bucket, event.bucket);
        assert_eq!(back.key, event.key);
        assert_eq!(back.ts_ms, event.ts_ms);
        assert!(decode_event(b"\xff\xff").is_none());
    }

    /// The full coherence story on one writer: put indexes + invalidates on
    /// the peer, delete removes — in the writer's order.
    #[tokio::test]
    async fn peer_events_fold_into_index_and_invalidate() {
        let net = Network::new();
        let (sync_a, _sync_b, state, cache) = wired_pair(&net);
        let metrics = Metrics::default();

        // B holds a soon-stale body copy.
        let ckey = ("bkt".to_owned(), "obj".to_owned());
        cache.insert(ckey.clone(), cached(b"stale")).await;

        let now = SystemTime::now();
        sync_a.publish_put("bkt", "obj", 42, now, &metrics).await;
        eventually(
            || indexed_size(&state, "bkt", "obj") == Some(42),
            "put reaches the peer index",
        )
        .await;
        let mut invalidated = false;
        for _ in 0..300 {
            if cache.get(&ckey).await.is_none() {
                invalidated = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(invalidated, "put invalidates the peer's body copy");

        sync_a
            .publish_del("bkt", "obj", SystemTime::now(), &metrics)
            .await;
        eventually(
            || indexed_size(&state, "bkt", "obj").is_none(),
            "delete reaches the peer index",
        )
        .await;
    }

    /// The strict-LIST barrier: after a publish, `await_fresh` on the peer
    /// returns true only once the event is actually applied.
    #[tokio::test]
    async fn await_fresh_reflects_the_publishers_head() {
        let net = Network::new();
        let (sync_a, sync_b, state, _cache) = wired_pair(&net);
        let metrics = Metrics::default();

        sync_a
            .publish_put("bkt", "fresh", 7, SystemTime::now(), &metrics)
            .await;
        // Wait until the peer has applied the write, then barrier: a caught-up
        // node must pass promptly, and a passed barrier implies the applied
        // index reflects every head the barrier saw.
        eventually(
            || indexed_size(&state, "bkt", "fresh") == Some(7),
            "apply loop catches the write",
        )
        .await;
        let caught_up = sync_b.await_fresh(Duration::from_secs(5)).await;
        assert!(
            caught_up,
            "barrier must pass once the apply loop is caught up"
        );
    }

    /// Session tokens: the issuer satisfies its own instantly, a peer only
    /// once the write is applied, and garbage never blocks a read.
    #[tokio::test]
    async fn write_tokens_upgrade_reads_to_strict() {
        let net = Network::new();
        let (sync_a, sync_b, state, _cache) = wired_pair(&net);
        let metrics = Metrics::default();

        let receipt = sync_a
            .publish_put("bkt", "tok", 1, SystemTime::now(), &metrics)
            .await;
        assert!(
            sync_a
                .reached_token(&receipt.header, Duration::from_millis(50))
                .await,
            "the issuer's own token is trivially satisfied"
        );
        assert!(
            sync_b
                .reached_token(&receipt.header, Duration::from_secs(5))
                .await,
            "a peer satisfies the token once the write is applied"
        );
        assert!(
            sync_a
                .wait_cluster_applied(receipt.token, Duration::from_secs(5))
                .await,
            "the write-ack wait resolves once every alive peer applied"
        );
        assert_eq!(indexed_size(&state, "bkt", "tok"), Some(1));
        assert!(
            sync_b
                .reached_token("not-a-token", Duration::from_millis(10))
                .await,
            "garbage tokens never block"
        );
        assert!(
            !sync_b
                .reached_token("ghost:9:9", Duration::from_millis(200))
                .await,
            "an unsatisfiable foreign token must fail closed"
        );
        assert!(
            !sync_a
                .reached_token("ghost:1:1", Duration::from_millis(10))
                .await,
            "without an apply loop a foreign token is unverifiable"
        );
    }

    #[tokio::test]
    async fn flush_drops_every_local_copy() {
        let cache = TieredCache::new(1024 * 1024, None, Arc::new(Metrics::default()));
        let key = ("bkt".to_owned(), "obj".to_owned());
        cache.insert(key.clone(), cached(b"body")).await;
        assert!(cache.get(&key).await.is_some());
        cache.local().flush().await;
        assert!(
            cache.get(&key).await.is_none(),
            "flush empties the hot tier"
        );
    }
}
