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
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use groupnet::consistency::{
    AckLedger, Frontier, PeerWrite, PeerWrites, WriteFeed, WriteToken, advertised_head,
    applied_cluster_wide,
};
use groupnet::core::{NodeId, Status};
use groupnet::runtime::{Group, Node};
use groupnet::transport::udp::UdpTransport;
use s3s::dto::{ETag, ObjectStorageClass};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::index::{BucketState, ObjEntry, apply_del, apply_put, standard_class};
use crate::metrics::Metrics;
use crate::tier::LocalCache;

/// Ring capacity: a peer that falls further behind than this many writes
/// gets a gap (flush + origin resync) instead of per-event application.
const FEED_CAPACITY: usize = 4096;

/// How much coherence the cluster pays for.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Consistency {
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

impl Consistency {
    /// Read the `S3CACHE_CONSISTENCY` spelling, defaulting (loudly, for anything
    /// unrecognised) to the safe mode.
    #[must_use]
    pub fn parse(raw: &str) -> Self {
        match raw.trim().to_ascii_lowercase().as_str() {
            "" | "strong" => Self::Strong,
            "bounded" => Self::Bounded,
            other => {
                warn!("unknown S3CACHE_CONSISTENCY `{other}`; using strong");
                Self::Strong
            }
        }
    }

    /// The mode's name, for logs.
    fn label(self) -> &'static str {
        match self {
            Self::Strong => "strong",
            Self::Bounded => "bounded",
        }
    }
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

/// What one durable write did, as the original (v1) envelope carried it. Kept exactly as
/// it was: it is still the wire format of every node in a cluster that has not finished
/// restarting, and this node has to keep reading it.
#[derive(Serialize, Deserialize)]
enum IndexOpV1 {
    /// The key now holds an object of `size` bytes.
    Put {
        /// Object size, for the LIST index.
        size: i64,
    },
    /// The key was deleted.
    Del,
}

/// One durable write in the v1 envelope: the operation, its `(bucket, key)`, and the
/// writer's wall-clock timestamp in **milliseconds**.
#[derive(Serialize, Deserialize)]
struct IndexEventV1 {
    op: IndexOpV1,
    bucket: String,
    key: String,
    ts_ms: u64,
}

/// First byte of a v2 envelope. A v1 encoding is bincode of [`IndexEventV1`], whose
/// leading field is an enum — a little-endian `u32` variant index — so every v1 encoding
/// begins `0x00` or `0x01`. `0xFF` can begin none of them, which is what lets one decoder
/// accept both formats through a rolling upgrade with no flag day.
const V2_MAGIC: u8 = 0xFF;

/// What one durable write did, as advertised to peers.
#[derive(Serialize, Deserialize)]
pub(crate) enum IndexOp {
    /// The key now holds an object.
    Put {
        /// Object size, for the LIST index. `None` when the writer could not learn it —
        /// never fabricated (see [`crate::index::ObjEntry::size`]).
        size: Option<i64>,
        /// The origin's entity tag, in its header spelling (`"v"` / `W/"v"`), so a peer
        /// can report an `ETag` for the key without an origin round-trip.
        etag: Option<String>,
        /// The object's `Content-Type`. A peer still cannot answer a HEAD from this (no
        /// user metadata rides the feed — see [`crate::index`]), but the entry carries
        /// the one response header clients branch on, and keeps it when a later HEAD
        /// completes the entry.
        content_type: Option<String>,
        /// The object's storage class, so a peer's LIST reports the writer's class
        /// rather than assuming the default.
        storage_class: Option<String>,
    },
    /// The key was deleted.
    Del,
}

/// One durable write: the operation, its `(bucket, key)`, and the writer's wall-clock
/// timestamp — the cross-writer LWW tiebreak, in **microseconds** so it is not coarser
/// than the clock a local write is stamped with (see [`wire_stamp`]).
#[derive(Serialize, Deserialize)]
pub(crate) struct IndexEvent {
    pub(crate) op: IndexOp,
    pub(crate) bucket: String,
    pub(crate) key: String,
    pub(crate) ts_us: u64,
}

fn to_micros(ts: SystemTime) -> u64 {
    ts.duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_micros()).unwrap_or(u64::MAX))
}

fn from_micros(us: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_micros(us)
}

/// A timestamp truncated to the precision the write feed carries. Local writes are
/// stamped through this before they enter the index, so a local entry and a peer's event
/// describing the same instant compare equal instead of the finer local clock silently
/// winning every last-writer-wins tie.
#[must_use]
pub(crate) fn wire_stamp(ts: SystemTime) -> SystemTime {
    from_micros(to_micros(ts))
}

/// The `ETag` in its header spelling, which [`ETag`]'s own parser round-trips exactly —
/// a string rather than the DTO's serde shape, so the wire format does not move when the
/// DTO does.
fn etag_to_wire(tag: &ETag) -> String {
    match tag {
        ETag::Strong(value) => format!("\"{value}\""),
        ETag::Weak(value) => format!("W/\"{value}\""),
    }
}

fn encode_event(event: &IndexEvent) -> Vec<u8> {
    let mut out = vec![V2_MAGIC];
    match bincode::serialize(event) {
        Ok(body) => out.extend_from_slice(&body),
        Err(_) => return Vec::new(),
    }
    out
}

/// Decodes either envelope: the magic byte selects v2, anything else is read as v1 and
/// lifted into the v2 shape, so a node mid-upgrade applies its peers' writes whichever
/// version wrote them.
fn decode_event(bytes: &[u8]) -> Option<IndexEvent> {
    match bytes.split_first() {
        Some((&V2_MAGIC, body)) => bincode::deserialize(body).ok(),
        _ => decode_v1(bytes),
    }
}

fn decode_v1(bytes: &[u8]) -> Option<IndexEvent> {
    let event: IndexEventV1 = bincode::deserialize(bytes).ok()?;
    Some(IndexEvent {
        op: match event.op {
            IndexOpV1::Put { size } => IndexOp::Put {
                size: Some(size),
                etag: None,
                content_type: None,
                storage_class: None,
            },
            IndexOpV1::Del => IndexOp::Del,
        },
        bucket: event.bucket,
        key: event.key,
        ts_us: event.ts_ms.saturating_mul(1000),
    })
}

/// How long this node's view of the cluster must have held still before it trusts its
/// own index enough to answer an authoritative 404. One full failure-detection cycle: a
/// peer that stops answering is `Suspect` only after a probe interval plus its timeout,
/// and `Dead` a suspect timeout after that — so inside this window a peer may be writing
/// keys this node will never hear about while [`WriteSync::cluster_healthy`] still says
/// everything is fine. Read off groupnet's own defaults, which is what the node is built
/// with ([`Node::builder`] is never handed a `config`).
fn detection_window() -> Duration {
    let cfg = groupnet::core::Config::default();
    Duration::from_millis(cfg.probe_interval_ms + cfg.probe_timeout_ms + cfg.suspect_timeout_ms)
}

/// How long this node's picture of the cluster has been unchanged. A membership or
/// status change, a write held past its ack window, or a write-feed gap each mean the
/// picture may be incomplete, and each restarts the clock.
struct Stability {
    window: Duration,
    /// The last observed `(member, status)` view and when it was first seen.
    seen: Mutex<(Vec<(NodeId, Status)>, Instant)>,
}

impl Stability {
    fn new() -> Self {
        Self {
            window: detection_window(),
            seen: Mutex::new((Vec::new(), Instant::now())),
        }
    }

    /// Note that something happened this node's index may not have caught up with.
    fn disturb(&self) {
        self.seen.lock().unwrap().1 = Instant::now();
    }

    /// Whether the view has held still for the whole detection window.
    fn settled(&self, group: &Group) -> bool {
        let now = Instant::now();
        let statuses = group.statuses();
        let mut seen = self.seen.lock().unwrap();
        if seen.0 != statuses {
            seen.0 = statuses;
            seen.1 = now;
        }
        now.duration_since(seen.1) >= self.window
    }
}

/// The publishing half of the write feed, plus the barrier view.
pub struct WriteSync {
    feed: WriteFeed<IndexEvent>,
    group: Group,
    me: NodeId,
    consistency: Consistency,
    /// Set by [`start_apply`](Self::start_apply); the freshness barrier reads it.
    view: OnceLock<groupnet::consistency::FrontierView>,
    /// Shared with the apply loop, which restarts the clock on a feed gap.
    stability: Arc<Stability>,
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
            stability: Arc::new(Stability::new()),
            _node: node,
        }
    }

    /// Whether this node's index may be treated as authoritative for a key's *absence*.
    /// An index miss is only a 404 if no peer could be holding a write this node has not
    /// seen — which needs both a fully-alive view and that view having held still for
    /// the failure detector's whole window (see [`detection_window`]). Otherwise the
    /// origin answers: slower, never a 404 for a key that exists.
    pub(crate) fn settled(&self) -> bool {
        self.cluster_healthy() && self.stability.settled(&self.group)
    }

    /// Advertise a durable put to peers: everything the entry carries that a peer can
    /// use, stamped with the local index's timestamp.
    pub(crate) async fn publish_put(
        &self,
        bucket: &str,
        key: &str,
        entry: &ObjEntry,
        metrics: &Metrics,
    ) -> WriteReceipt {
        let op = IndexOp::Put {
            size: entry.size,
            etag: entry.etag.as_ref().map(etag_to_wire),
            content_type: entry.content_type.clone(),
            storage_class: Some(entry.storage_class.as_str().to_owned()),
        };
        self.publish(op, bucket, key, entry.last_modified, metrics)
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
            ts_us: to_micros(ts),
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

    /// [`wait_cluster_applied`](Self::wait_cluster_applied) with the bookkeeping every
    /// write path wants: a timeout is counted, logged, and restarts the settle clock —
    /// a peer that did not ack in time is a peer whose writes this node may also be
    /// missing, so its authoritative 404s stand down until the view holds still again.
    /// Never an error: the unresponsive peer is either dying (SWIM will exclude it) or
    /// partitioned (its own health gate stops it serving cached state).
    pub(crate) async fn ack_write(
        &self,
        token: WriteToken,
        timeout: Duration,
        bucket: &str,
        key: &str,
        metrics: &Metrics,
    ) {
        if self.wait_cluster_applied(token, timeout).await {
            return;
        }
        warn!("write ack timed out for {bucket}/{key}; a peer may lag");
        metrics.ack_timeout();
        self.stability.disturb();
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
        let stability = Arc::clone(&self.stability);
        tokio::spawn(async move {
            while let Some(event) = peers.next().await {
                match event {
                    PeerWrite::Wrote {
                        peer,
                        token,
                        key: event,
                    } => {
                        let ts = from_micros(event.ts_us);
                        match event.op {
                            IndexOp::Put {
                                size,
                                etag,
                                content_type,
                                storage_class,
                            } => {
                                // The feed carries what a peer can act on without an
                                // origin round-trip: existence, size, ETag, Content-Type
                                // and storage class. It deliberately does not carry user
                                // metadata, so the entry stays *skeletal* — it answers
                                // LIST, and the first HEAD completes it from the origin
                                // (see `crate::index`).
                                let entry = ObjEntry {
                                    size,
                                    last_modified: ts,
                                    etag: etag.and_then(|raw| raw.parse().ok()),
                                    storage_class: storage_class
                                        .map_or_else(standard_class, ObjectStorageClass::from),
                                    content_type,
                                    meta: None,
                                };
                                apply_put(&state, &event.bucket, &event.key, entry);
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
                        // Writes were provably missed, so this node's index is not
                        // authoritative for absence until the view settles again.
                        stability.disturb();
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

/// Everything the gossip layer needs, independent of where it came from.
/// [`from_env`] fills it from `S3CACHE_GOSSIP_*`; anything driving two nodes in
/// one process (tests) constructs it directly, since mutating the environment
/// to configure a node is neither safe nor parallel-friendly in edition 2024.
pub struct SyncConfig {
    /// UDP address to bind the gossip transport to (`S3CACHE_GOSSIP_BIND`).
    pub bind: String,
    /// The address peers should reach this node on; the bound address when
    /// `None` (`S3CACHE_GOSSIP_ADVERTISE`).
    pub advertise: Option<String>,
    /// Statically-addressed peers as `(node id, host:port)`. Every other peer
    /// resolves itself through gossiped advertisements, so only seeds need
    /// static addressing (`S3CACHE_GOSSIP_SEEDS`).
    pub seeds: Vec<(String, String)>,
    /// This node's identity in the cluster (the pod name, in the chart).
    pub node_id: String,
    /// How much coherence the cluster pays for (`S3CACHE_CONSISTENCY`).
    pub consistency: Consistency,
}

impl WriteSync {
    /// Bind the gossip transport, join the cluster group and attach the write
    /// feed. `None` when the bind address is unusable — gossip is optional, and
    /// a node that cannot join is a strict single node, not a dead one.
    /// Start the apply loop with [`start_apply`](Self::start_apply) once the
    /// local cache exists (the proxy does this in `start_coherence`).
    pub async fn new(cfg: SyncConfig) -> Option<Self> {
        let me = NodeId::new(cfg.node_id.as_str());
        let transport = match UdpTransport::bind(me.clone(), cfg.bind.as_str()).await {
            Ok(transport) => transport,
            Err(error) => {
                let bind = &cfg.bind;
                warn!("gossip disabled: cannot bind `{bind}`: {error}");
                return None;
            }
        };
        let advertise = cfg
            .advertise
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
        for (id, addr) in cfg.seeds {
            if id == cfg.node_id {
                continue; // a pod seeding itself (uniform config) is a no-op
            }
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
        let (bind, node_id, mode) = (&cfg.bind, &cfg.node_id, cfg.consistency.label());
        info!("gossip coherence bound on `{bind}` as `{node_id}` (consistency: {mode})");
        Some(WriteSync::attach(group, me, cfg.consistency, Some(node)))
    }
}

/// Build the gossip node and write feed from `S3CACHE_GOSSIP_*`, or `None`
/// when `S3CACHE_GOSSIP_BIND` is unset (single-node: the sole writer is
/// already strict). A thin read of the environment over [`WriteSync::new`].
pub async fn from_env(node_name: &str) -> Option<WriteSync> {
    let cfg = SyncConfig {
        bind: env_var("S3CACHE_GOSSIP_BIND")?,
        advertise: env_var("S3CACHE_GOSSIP_ADVERTISE"),
        seeds: parse_seeds(&env_var("S3CACHE_GOSSIP_SEEDS").unwrap_or_default()),
        node_id: node_name.to_owned(),
        consistency: Consistency::parse(&env_var("S3CACHE_CONSISTENCY").unwrap_or_default()),
    };
    WriteSync::new(cfg).await
}

/// An environment variable, treating "set but empty" as unset — a Helm value
/// that renders to `""` (an unset optional knob) must read as absent.
fn env_var(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|value| !value.is_empty())
}

/// Comma-separated `id=host:port` seeds; malformed entries are dropped loudly
/// rather than taking gossip down.
fn parse_seeds(raw: &str) -> Vec<(String, String)> {
    raw.split(',')
        .map(str::trim)
        .filter(|seed| !seed.is_empty())
        .filter_map(|seed| {
            let Some((id, addr)) = seed.split_once('=') else {
                warn!("ignoring malformed gossip seed `{seed}` (want id=host:port)");
                return None;
            };
            Some((id.to_owned(), addr.to_owned()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, RwLock};
    use std::time::{Duration, SystemTime};

    use groupnet::core::{NodeId, Status};
    use groupnet::runtime::{Group, Node};
    use groupnet::transport::mem::{MemTransport, Network};
    use s3s::dto::GetObjectOutput;

    use super::{
        Consistency, IndexEvent, IndexEventV1, IndexOp, IndexOpV1, V2_MAGIC, WriteSync,
        decode_event, encode_event, parse_seeds, wire_stamp,
    };
    use crate::index::{BucketState, ObjEntry, standard_class};
    use crate::metrics::Metrics;
    use crate::tier::{CachedObject, TieredCache};

    type Index = Arc<RwLock<HashMap<String, BucketState>>>;

    /// The entry a write path hands [`WriteSync::publish_put`]: a size, an `ETag` the
    /// origin gave it, and the local write clock at the wire's precision.
    fn written(size: i64) -> ObjEntry {
        ObjEntry {
            size: Some(size),
            last_modified: wire_stamp(SystemTime::now()),
            etag: Some(s3s::dto::ETag::Strong("deadbeef".to_owned())),
            storage_class: standard_class(),
            content_type: Some("text/x-fixture".to_owned()),
            meta: None,
        }
    }

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
            .and_then(|e| e.size)
    }

    /// One indexed entry, cloned out for the assertions that look past its size.
    fn indexed(state: &Index, bucket: &str, key: &str) -> Option<ObjEntry> {
        state
            .read()
            .unwrap()
            .get(bucket)
            .and_then(|b| b.keys.get(key))
            .cloned()
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

    /// The env spellings [`WriteSync::new`] is configured through: seeds split on the
    /// first `=` (host:port may not contain one), blanks and malformed entries drop,
    /// and an unknown consistency mode falls back to the safe one.
    #[test]
    fn env_spellings_parse_into_a_config() {
        assert_eq!(
            parse_seeds("a=host-a:1, b=host-b:2 ,,"),
            [
                ("a".to_owned(), "host-a:1".to_owned()),
                ("b".to_owned(), "host-b:2".to_owned())
            ]
        );
        assert!(parse_seeds("").is_empty());
        assert!(parse_seeds("no-equals-sign").is_empty(), "malformed drops");
        assert!(Consistency::parse("") == Consistency::Strong);
        assert!(Consistency::parse(" Bounded ") == Consistency::Bounded);
        assert!(
            Consistency::parse("eventual") == Consistency::Strong,
            "an unknown mode falls back to strong"
        );
    }

    #[test]
    fn event_codec_round_trips_and_rejects_garbage() {
        let event = IndexEvent {
            op: IndexOp::Put {
                size: Some(42),
                etag: Some("\"deadbeef\"".to_owned()),
                content_type: Some("text/x-fixture".to_owned()),
                storage_class: Some("STANDARD".to_owned()),
            },
            bucket: "bucket-1".to_owned(),
            key: "a/b weird\0key".to_owned(),
            ts_us: 1_700_000_000_000_000,
        };
        let encoded = encode_event(&event);
        assert_eq!(encoded.first(), Some(&V2_MAGIC), "the sender emits v2");
        let back = decode_event(&encoded).expect("round trip");
        let IndexOp::Put {
            size,
            etag,
            content_type,
            storage_class,
        } = back.op
        else {
            panic!("a put decodes as a put");
        };
        assert_eq!(size, Some(42));
        assert_eq!(etag.as_deref(), Some("\"deadbeef\""));
        assert_eq!(content_type.as_deref(), Some("text/x-fixture"));
        assert_eq!(storage_class.as_deref(), Some("STANDARD"));
        assert_eq!(back.bucket, event.bucket);
        assert_eq!(back.key, event.key);
        assert_eq!(back.ts_us, event.ts_us);
        assert!(decode_event(b"\xff\xff").is_none());
    }

    /// Mixed-version safety: a node that has already been upgraded still applies the
    /// writes of one that has not. The v1 envelope is what a pre-upgrade peer puts on
    /// the wire, and its millisecond stamp has to land as the same instant.
    #[test]
    fn a_v1_event_still_decodes() {
        let legacy = bincode::serialize(&IndexEventV1 {
            op: IndexOpV1::Put { size: 7 },
            bucket: "bucket-1".to_owned(),
            key: "legacy".to_owned(),
            ts_ms: 1_700_000_000_123,
        })
        .expect("v1 encodes");
        assert_ne!(
            legacy.first(),
            Some(&V2_MAGIC),
            "the magic byte cannot begin a v1 encoding, which is what makes them separable"
        );
        let back = decode_event(&legacy).expect("v1 decodes");
        assert!(matches!(
            back.op,
            IndexOp::Put {
                size: Some(7),
                etag: None,
                content_type: None,
                storage_class: None
            }
        ));
        assert_eq!(back.key, "legacy");
        assert_eq!(back.ts_us, 1_700_000_000_123_000);

        let deleted = bincode::serialize(&IndexEventV1 {
            op: IndexOpV1::Del,
            bucket: "bucket-1".to_owned(),
            key: "legacy".to_owned(),
            ts_ms: 1,
        })
        .expect("v1 encodes");
        assert!(matches!(
            decode_event(&deleted).expect("v1 delete decodes").op,
            IndexOp::Del
        ));
    }

    /// The LWW clock is only comparable if both sides carry the same precision: a local
    /// stamp is truncated to what the wire holds, so a peer's event for the same instant
    /// ties (and deletes win ties) instead of always losing to the finer local clock.
    #[test]
    fn local_stamps_are_truncated_to_the_wire_precision() {
        let now = SystemTime::now();
        let stamped = wire_stamp(now);
        assert!(stamped <= now);
        assert_eq!(
            super::from_micros(super::to_micros(stamped)),
            stamped,
            "a stamped time survives the wire unchanged"
        );
        assert!(now.duration_since(stamped).expect("not in the future") < Duration::from_micros(1));
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

        sync_a
            .publish_put("bkt", "obj", &written(42), &metrics)
            .await;
        eventually(
            || indexed_size(&state, "bkt", "obj") == Some(42),
            "put reaches the peer index",
        )
        .await;
        let entry = indexed(&state, "bkt", "obj").expect("the peer indexed the write");
        assert_eq!(
            entry.etag.as_ref().map(s3s::dto::ETag::value),
            Some("deadbeef"),
            "the v2 envelope carries the origin's ETag to peers"
        );
        assert_eq!(entry.content_type.as_deref(), Some("text/x-fixture"));
        assert!(
            entry.meta.is_none(),
            "no user metadata rides the feed, so the entry stays skeletal"
        );
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
            .publish_put("bkt", "fresh", &written(7), &metrics)
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
            .publish_put("bkt", "tok", &written(1), &metrics)
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

    /// The write-ack wait has to actually fire when a peer does not acknowledge — the
    /// counter operators watch is only worth watching if it moves. And an unacked write
    /// stands this node's authoritative 404s down: a peer that did not apply our write
    /// in time is a peer whose writes we may equally be missing, so an index miss is no
    /// longer proof the key does not exist.
    #[tokio::test]
    async fn an_unacked_write_is_counted_and_stands_the_404_down() {
        let net = Network::new();
        let (_sync_a, sync_b, _state, _cache) = wired_pair(&net);
        let metrics = Metrics::default();
        // A was attached without an apply loop, so it publishes no ack ledger — but it
        // is alive, so B has to wait for it and then give up.
        eventually(
            || {
                sync_b
                    .group
                    .statuses()
                    .iter()
                    .any(|(id, status)| id.as_str() == "sync-a" && *status == Status::Alive)
            },
            "A to appear alive in B's membership view",
        )
        .await;

        let receipt = sync_b
            .publish_put("bkt", "unacked", &written(1), &metrics)
            .await;
        sync_b
            .ack_write(
                receipt.token,
                Duration::from_millis(200),
                "bkt",
                "unacked",
                &metrics,
            )
            .await;
        assert!(
            metrics
                .prometheus_text()
                .contains("\ns3cache_ack_timeouts 1\n"),
            "the ack timeout is counted"
        );
        assert!(
            !sync_b.settled(),
            "and an index miss is no longer an authoritative 404"
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
