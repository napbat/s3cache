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
//!
//! In `strong` mode the read side is licensed by a **coherence lease** rather
//! than by a heuristic: this node may answer from local state only while it
//! holds an unexpired serve-lease its peers granted, and a write ends when
//! every lease-holder has either applied it or had its lease lapse. See
//! [`Consistency::Strong`] and groupnet's `consistency::lease` honesty box.
//! Losing that licence is a latch, so every way of losing it needs a way back:
//! a gap has the apply loop, and a lapse with no gap behind it — a peer scaled
//! in, lost, or restarted quietly — has [`watch_lapses`], which runs the same
//! remediation on the same generation.

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use groupnet::consistency::{
    AckLedger, CAP_ACKS, CAP_LEASE, CoherenceOutcome, Frontier, LeaseConfig, LeaseView, Leases,
    PeerWrite, PeerWrites, WriteFeed, WriteToken, advertised_head, applied_by_selected,
};
use groupnet::core::{Config, NodeId, Status};
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
    /// Indistinguishable from a single S3 node with zero client cooperation,
    /// bought with **coherence leases** (groupnet's T3): a node may answer a
    /// read — or an authoritative 404 — from local state only while it holds
    /// an unexpired serve-lease its peers granted it, so a write ends when
    /// every lease-holder has either applied the invalidation (the fast path,
    /// exactly one ack round) or had its lease lapse (the slow path, bounded
    /// by the lease duration `D` and ended by the *stale node's own clock*
    /// rather than by anyone's patience).
    ///
    /// That is the whole point of the tier and the difference from
    /// [`StrongAcks`](Self::StrongAcks): an ack timeout there ends in a
    /// degradation that depends on the stale peer *learning* it should stand
    /// down — exactly what an asymmetrically-partitioned peer cannot do —
    /// whereas a lapse here is a guarantee, because a lapsed reader serves
    /// nothing cached until it re-synchronizes and affirms it.
    ///
    /// Costs one ack round (~2 in-cluster hops, behind the origin round-trip
    /// already paid) per write, plus one ledger-entry republish per applied
    /// event and one small renewal entry per node per `D/3` — right for
    /// fleet-sized clusters, wrong past the scaling envelope (run `bounded`
    /// and cells). The named failure modes are in the README's consistency
    /// section, and in groupnet's `consistency::lease` honesty box.
    Strong,
    /// [`Strong`](Self::Strong) bought with acknowledgements alone — the ack
    /// round and nothing above it, with the view-stability heuristic
    /// ([`Stability`]) as the read-side licence. Deprecated on arrival: it
    /// exists so a deployment can pin the pre-lease mechanism for exactly one
    /// release while the lease tier rolls through the fleet. Nothing new
    /// should choose it, and the next release removes it.
    StrongAcks,
    /// The gossip bound only: reads are fresh within ~one push hop, session
    /// tokens still upgrade individual reads to strict, but writes return as
    /// soon as the origin acks and no ledger traffic flows. For large
    /// clusters where per-write cluster acks are unaffordable. A bounded node
    /// never acks, so it declares itself as such ([`CAP_BOUNDED`]) and strong
    /// writers skip it rather than waiting out an ack timeout per write.
    Bounded,
}

/// The capability a node advertises to declare it does *not* participate in
/// the ack tier — a positive statement of non-participation, since the absence
/// of an advertisement means something else entirely (see [`waits_on`]).
/// Namespaced, per groupnet's convention for consumer-defined capabilities.
const CAP_BOUNDED: &str = "s3cache:bounded";

impl Consistency {
    /// Read the `S3CACHE_CONSISTENCY` spelling, defaulting (loudly, for anything
    /// unrecognised) to the safe mode.
    #[must_use]
    pub fn parse(raw: &str) -> Self {
        match raw.trim().to_ascii_lowercase().as_str() {
            "" | "strong" => Self::Strong,
            "strong-acks" => Self::StrongAcks,
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
            Self::StrongAcks => "strong-acks",
            Self::Bounded => "bounded",
        }
    }

    /// Whether this mode participates in the ack tier — publishes an
    /// [`AckLedger`] and waits on other participants' watermarks. Exhaustive
    /// on purpose: a mode added later has to answer this question rather than
    /// inherit an answer from a `!= Bounded`.
    fn acks(self) -> bool {
        match self {
            Self::Strong | Self::StrongAcks => true,
            Self::Bounded => false,
        }
    }

    /// Whether this mode runs a lease set — grants its peers' serve-leases,
    /// renews its own, and ends its writes on an ack *or* a lapse.
    fn leases(self) -> bool {
        match self {
            Self::Strong => true,
            Self::StrongAcks | Self::Bounded => false,
        }
    }

    /// What this node advertises to the group about its participation.
    /// Non-empty in every mode — see [`advertise`].
    ///
    /// [`CAP_LEASE`] is advertised **only** by a mode that constructs a
    /// [`Leases`], and the two are wired together in [`WriteSync::attach`] for
    /// that reason. Readers wait for a confirmation from every not-reaped
    /// advertiser, so advertising without a running granter freezes every
    /// other reader's confirmation cluster-wide until membership reaps this
    /// node — the lease shell's own named failure, and the more expensive half
    /// of the same footgun the ack tier documents.
    fn capabilities(self) -> &'static [&'static str] {
        match self {
            Self::Strong => &[CAP_ACKS, CAP_LEASE],
            Self::StrongAcks => &[CAP_ACKS],
            Self::Bounded => &[CAP_BOUNDED],
        }
    }
}

/// The default coherence-lease duration `D` (`S3CACHE_LEASE_MS`): two seconds,
/// groupnet's own default and the same order as its Hosted-mode host lease.
///
/// It is the knob trading write-stall-under-failure against renewal traffic —
/// a writer whose peer goes silent stalls at most one lease remainder, and each
/// reader republishes one 16-byte entry every `D/3` to keep its lease.
pub const DEFAULT_LEASE_MS: u64 = 2_000;

/// The floor under the tuned [`Config::dead_timeout_ms`] — see
/// [`WriteSync::new`], which prices what that tuning buys and what it costs.
const DEAD_TIMEOUT_FLOOR_MS: u64 = 2_000;

/// How far past one lease duration a leased write waits before abandoning the
/// guarantee. Anything at or below `D` would turn every ordinary lapse into a
/// [`CoherenceOutcome::TimedOut`], which is the one outcome carrying no
/// guarantee at all; the second is slack for scheduling, not for hope.
const WRITE_WAIT_SLACK: Duration = Duration::from_secs(1);

/// How often a pending affirmation retries while the lease declines it.
const AFFIRM_POLL: Duration = Duration::from_millis(50);

/// How long an affirmation keeps trying before giving up **loudly**. Generous
/// on purpose: the two things that decline it — this node's own warm-up window
/// and a frozen confirmation behind an unreaped granter — both clear on their
/// own, the second only at the reap horizon. Giving up leaves the node serving
/// via the origin, which is correct and slow, never stale.
const AFFIRM_DEADLINE: Duration = Duration::from_mins(1);

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
/// own index enough to answer an authoritative 404: one full failure-detection cycle, as
/// the group itself is configured to run it. Inside this window a peer may be writing
/// keys this node will never hear about while [`WriteSync::cluster_healthy`] still says
/// everything is fine.
///
/// This is the **pre-lease** licence, and it survives only for [`Consistency::StrongAcks`]
/// and [`Consistency::Bounded`]. A heuristic is what it is — "the picture has held still,
/// so probably nobody is writing behind my back" — and replacing it with a mechanism a
/// reader holds and a writer can wait out is the whole of the lease migration. In
/// [`Consistency::Strong`] nothing reads it.
///
/// Read off the group's *effective* config — what the builder actually spawned this node
/// with, not the library defaults — and off the membership it currently has to sweep,
/// which is why it is computed per call rather than frozen at construction. Two caveats
/// come with the number:
///
/// * groupnet's bound holds for **at most two concurrent silences**; past that a suspect
///   peer can stall the probe ring more than once and the sweep overruns. Two is the
///   right envelope for a cluster this size — a fleet of pods, not a datacentre — and a
///   deployment that outgrows it should be running `bounded` and cells anyway.
/// * `.max(2)` is a floor, not a fudge: a node that has just booted and not yet heard
///   from its peer sees a membership of one, and must not size its trust window as if it
///   were alone in the world — that is precisely the moment a peer it cannot see is
///   writing keys it has never heard of.
fn settle_window(group: &Group) -> Duration {
    Duration::from_millis(
        group
            .config()
            .detection_window_ms(group.members().len().max(2)),
    )
}

/// How long this node's picture of the cluster has been unchanged. A membership or
/// status change, a write held past its ack window, or a write-feed gap each mean the
/// picture may be incomplete, and each restarts the clock.
struct Stability {
    /// The last observed `(member, status)` view and when it was first seen.
    seen: Mutex<(Vec<(NodeId, Status)>, Instant)>,
}

impl Stability {
    fn new() -> Self {
        Self {
            seen: Mutex::new((Vec::new(), Instant::now())),
        }
    }

    /// Note that something happened this node's index may not have caught up with.
    fn disturb(&self) {
        self.seen.lock().unwrap().1 = Instant::now();
    }

    /// Whether the view has held still for the whole detection window (see
    /// [`settle_window`], which this re-reads on every call — the window a two-member
    /// cluster needs is not the window it needs after it scales out).
    fn settled(&self, group: &Group) -> bool {
        let window = settle_window(group);
        let now = Instant::now();
        let statuses = group.statuses();
        let mut seen = self.seen.lock().unwrap();
        if seen.0 != statuses {
            seen.0 = statuses;
            seen.1 = now;
        }
        now.duration_since(seen.1) >= window
    }
}

/// Whether an affirmation was accepted, declined for now, or belongs to a
/// resync that a later one has already replaced.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Affirmation {
    /// The lease took it: this node may serve again.
    Took,
    /// Declined — this node's warm-up window, or a confirmation frozen behind
    /// a granter that has stopped publishing. Poll, don't conclude.
    NotYet,
    /// A newer gap has closed the window since this resync started; only that
    /// resync's own affirmation may re-open it.
    Superseded,
}

/// The generation counter and the lapse watermark, under one lock.
struct GateState {
    /// The resync generation an affirmation must speak for.
    generation: u64,
    /// The value of [`LeaseView::lapses`] when the newest remediation stood the
    /// lease down. Every lapse counted at or below it is *covered*: the flush
    /// and the origin re-LIST that follow the stand-down happen after it, so
    /// nothing that lapse could have made stale survives them.
    covered_lapses: u64,
}

/// The lease's resync generation, and the affirmation that answers it.
///
/// A gap is proof that invalidations were missed, so it forces the reader into
/// `NeedsResync` — and a *later* affirmation may only re-open the window if it
/// speaks for the resync that generation started. Both halves take the one
/// lock, which is the whole reason this is a struct rather than an
/// [`std::sync::atomic::AtomicU64`]: an affirmation that read the generation,
/// then lost the CPU while a gap closed the window, would otherwise re-open it
/// for a resync that never ran — the one direction this must not fail in.
///
/// The same lock carries the lapse watermark [`watch_lapses`] reads, so a lapse
/// and the gap that stood the lease down for it cannot each buy a remediation.
struct ResyncGate {
    /// `None` in every mode but [`Consistency::Strong`]; the generation still
    /// turns, so a consumer's bookkeeping needs no mode branch of its own.
    view: Option<LeaseView>,
    state: Mutex<GateState>,
}

impl ResyncGate {
    fn new(view: Option<LeaseView>) -> Self {
        Self {
            view,
            state: Mutex::new(GateState {
                generation: 0,
                covered_lapses: 0,
            }),
        }
    }

    /// Stand this node's serve-lease down and start a new generation: it
    /// provably missed invalidations, and must not answer one more read from
    /// local state under the window it still holds.
    ///
    /// Recording the lapse count here — under the lock, before the remediation
    /// runs — is what makes this the *one* remediation for every lapse observed
    /// so far, whichever trigger got here first.
    fn require_resync(&self) {
        let mut state = self.state.lock().unwrap();
        if let Some(view) = &self.view {
            view.require_resync();
            state.covered_lapses = view.lapses();
        }
        state.generation += 1;
    }

    /// Whether this node's serve-lease has lapsed since the newest remediation
    /// stood it down — [`watch_lapses`]' whole question.
    ///
    /// A lapse latches `NeedsResync` until an affirmation lifts it, and the
    /// state machine reports no further lapse *edges* while it is latched, so
    /// this can only turn true once per stretch of service. `false` in every
    /// mode but [`Consistency::Strong`]: without a lease there is nothing to
    /// lapse.
    fn lapse_uncovered(&self) -> bool {
        let Some(view) = &self.view else {
            return false;
        };
        // Monotone and edge-proof: the lease shell's own view task usually
        // consumes the `Lapsed` edge before anything here could ask for it, so
        // the counter — not the state — is what a watcher may read (see
        // `LeaseView::state`).
        let lapses = view.lapses();
        lapses > self.state.lock().unwrap().covered_lapses
    }

    /// The generation a resync starting now would answer for.
    fn generation(&self) -> u64 {
        self.state.lock().unwrap().generation
    }

    /// Affirm catch-up on behalf of `generation` — see [`Affirmation`].
    fn affirm(&self, generation: u64) -> Affirmation {
        let current = self.state.lock().unwrap();
        if current.generation != generation {
            return Affirmation::Superseded;
        }
        let Some(view) = &self.view else {
            // No lease set: there is no window to open, and nothing to poll
            // for. Answering `Took` is what stops a caller spinning until the
            // deadline in `bounded` and `strong-acks`.
            return Affirmation::Took;
        };
        if view.mark_caught_up() {
            Affirmation::Took
        } else {
            Affirmation::NotYet
        }
    }
}

/// The remediation for proof that this node's local state may have missed
/// invalidations, in the one order that is safe.
///
/// The licence goes **first**, before any remediation: this node must not answer
/// one more read from local state under a window it no longer deserves. Then the
/// flush — with every local copy gone (and the index bucket back to passthrough)
/// there is nothing stale left to serve even while the origin re-LIST runs — and
/// then `resync`, which owns the affirmation that puts this node back in service
/// under the generation this stand-down just started (see
/// `CachingProxy::gap_resync_handle`).
///
/// Both triggers run exactly this: a write-feed [`PeerWrite::Gap`] and a lease
/// lapse with no gap behind it ([`watch_lapses`]). They are the same proof —
/// "writes happened that this node cannot account for" — arriving by different
/// routes, and a difference in remediation between them would be a difference in
/// what a reader may serve afterwards.
async fn remediate(gate: &ResyncGate, local: &LocalCache, resync: &Arc<dyn Fn() + Send + Sync>) {
    gate.require_resync();
    local.flush().await;
    resync();
}

/// The floor under [`lapse_poll`]: a lease tuned absurdly short must not turn the
/// watch into a spin.
const LAPSE_POLL_FLOOR: Duration = Duration::from_millis(25);

/// How often [`watch_lapses`] looks: a quarter of the lease duration `D`, so a
/// lapse is picked up well inside the lease period it happened in. The watch is
/// two counter reads and no allocation — cadence buys latency here, not load.
fn lapse_poll(duration: Duration) -> Duration {
    (duration / 4).max(LAPSE_POLL_FLOOR)
}

/// Watch this node's own serve-lease and remediate a lapse that arrives with **no**
/// write-feed gap behind it. `strong` only — nothing else holds a lease to lapse.
///
/// A gap is not the only way this node loses its licence, and the other ways carry
/// no event: a peer scaled in or lost for good, a partition that heals without the
/// ring ever overflowing, a rolling restart of a read-mostly peer. In each of them
/// this node's confirmation freezes, its window closes within `D`, and the lapse
/// latches [`groupnet::consistency::LeaseState::NeedsResync`] — which nothing would
/// ever lift, because the affirmation that lifts it belongs to a resync that no gap
/// ever triggered. That is permanent origin-serving for a node whose peers are
/// perfectly healthy: correct, and wrong about the price.
///
/// So the lapse gets the gap's remediation, and gets it exactly: the generation
/// bump, the flush, the origin re-LIST, and the affirmation the re-LIST owns. The
/// affirmation is what recovers the node, and it recovers at the moment the lease
/// can be confirmed again — the reap horizon, for a peer that is never coming back.
///
/// **The interlock**: a gap racing the same lapse must not buy a second
/// remediation. [`ResyncGate::require_resync`] records the lapse count it covers
/// while it stands the lease down, so whichever trigger arrives first owns every
/// lapse observed before it and this watch yields (see
/// [`ResyncGate::lapse_uncovered`]). The reverse — this watch remediating first and
/// a gap then arriving — is not deduplicated and must not be: a gap is independent
/// proof of missed writes, and it is entitled to its own resync.
///
/// One lapse is one remediation, and that is also the residual: a stand-down
/// latches, so no *further* lapse edge can fire until an affirmation lifts it. If
/// that affirmation gives up at [`AFFIRM_DEADLINE`] — a granter that neither
/// publishes nor gets reaped, the fail-slow shape the lease tier names — this node
/// stays origin-serving until the next gap, exactly as it would after a gap whose
/// affirmation gave up. The remedy for that one is operational, and the warning
/// [`WriteSync::affirm_resynced`] logs is where it is stated.
fn watch_lapses(
    gate: Arc<ResyncGate>,
    local: LocalCache,
    resync: Arc<dyn Fn() + Send + Sync>,
    metrics: Arc<Metrics>,
    poll: Duration,
) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(poll).await;
            if !gate.lapse_uncovered() {
                continue;
            }
            warn!(
                "coherence lease lapsed with no write-feed gap (a peer stopped granting): \
                 flushing tiers, resyncing index"
            );
            metrics.lease_lapse_resync();
            remediate(&gate, &local, &resync).await;
        }
    });
}

/// Attempts to get the capability advertisement enqueued after a rejection. A rejection
/// is a full actor inbox at startup, which drains in milliseconds; the advertisement is
/// state and the last call wins, so re-trying the same set costs nothing.
const ADVERTISE_RETRIES: u32 = 30;

/// How long between those attempts.
const ADVERTISE_RETRY_DELAY: Duration = Duration::from_millis(100);

/// Declare this node's coherence participation to the group. Every attach path goes
/// through [`WriteSync::attach`], which is where this is called, so no mode can reach a
/// group without saying what it is.
///
/// Two things make the call unconditional — including for `bounded`, whose whole point
/// is *not* participating:
///
/// * The declaration must be **non-empty**. Never-advertised and advertised-empty are
///   indistinguishable to a reader (`node_capabilities` answers both with an empty set),
///   and the transition rule in [`waits_on`] keys on exactly that distinction: an empty
///   set means "unknown, assume the old contract and wait". A bounded node that
///   advertised nothing would therefore be waited on by every strong writer — the
///   timeout-per-write this whole mechanism exists to remove.
/// * The **call itself** must happen regardless of mode. groupnet's restart recovery
///   re-adopts un-authored entries from peers' echoes, so a node that comes back without
///   authoring `~caps` this life inherits its previous life's set. A pod redeployed from
///   strong to bounded would keep advertising `acks` and haunt every writer in the
///   cluster until it was reaped; authoring the entry is what buries the ghost.
fn advertise(group: &Group, consistency: Consistency) {
    let caps = consistency.capabilities();
    if group.advertise_capabilities(caps).is_ok() {
        return;
    }
    // Rejection is backpressure, not refusal. Retry off the startup path: serving must
    // not wait on the advertisement, and the advertisement must not be dropped because
    // an inbox was briefly full.
    let group = group.clone();
    tokio::spawn(async move {
        for _ in 0..ADVERTISE_RETRIES {
            tokio::time::sleep(ADVERTISE_RETRY_DELAY).await;
            if group.advertise_capabilities(caps).is_ok() {
                return;
            }
        }
        warn!("could not advertise coherence capabilities; peers will read this node as unknown");
    });
}

/// Whether a write's cluster-wide ack wait has to include `node`: iff it advertises
/// [`CAP_ACKS`], **or** it advertises nothing at all.
///
/// Absence is not participation — but it is not non-participation either, and that is
/// the whole rule. An empty advertisement is read as "unknown, assume the old contract
/// and wait", which lands every case in the safe direction:
///
/// * an old node, which never advertises anything, is waited for exactly as it is today;
/// * a new bounded node has declared itself ([`CAP_BOUNDED`]) and is skipped, instead of
///   costing the writer an ack timeout it was never going to satisfy;
/// * an advertisement that has not converged here yet reads as unknown, so the writer
///   waits — a slow write while gossip catches up, never a stale read.
fn waits_on(group: &Group, node: &NodeId) -> bool {
    group.node_has_capability(node, CAP_ACKS) || group.node_capabilities(node).is_empty()
}

/// [`waits_on`], narrowed to the peers the **lease** wait cannot cover: those
/// that publish no `~lease` entry for a writer to wait on or to expire.
///
/// This is the transition set, and it is empty in a uniform leased fleet. A
/// node from before this change advertises nothing — so [`waits_on`] admits it
/// and it holds no lease — and a `strong-acks` node advertises [`CAP_ACKS`] and
/// deliberately runs no lease set; both are waited for exactly as they are
/// today, under the old ack timeout. A [`CAP_LEASE`] advertiser is left out
/// because waiting on it here would be a second, redundant copy of the ack
/// round the lease wait's own fast path already is.
fn waits_on_unleased(group: &Group, node: &NodeId) -> bool {
    waits_on(group, node) && !group.node_has_capability(node, CAP_LEASE)
}

/// Node ids for a log line. `(unnamed)` rather than an empty string because the
/// transitional ack wait answers yes or no and names nobody — an operator reading the
/// line should see that the wait could not say, not a blank where a name should be.
fn node_names(nodes: &[NodeId]) -> String {
    if nodes.is_empty() {
        return "unnamed".to_owned();
    }
    nodes
        .iter()
        .map(NodeId::as_str)
        .collect::<Vec<_>>()
        .join(", ")
}

/// How a write's cluster-wide wait ended.
///
/// The first two are the coherence guarantee — nothing that could be serving
/// the state this write invalidated still can — and the third is the absence
/// of it.
pub(crate) enum WriteWait {
    /// Every peer this write had to reach applied it: the fast path, and the
    /// only outcome in a healthy cluster.
    Applied,
    /// Some lease-holders never acknowledged, and this node's engine has
    /// expired their serve-leases: they are out of service until they
    /// re-synchronize, so they cannot serve what this write invalidated. The
    /// guarantee held — this is not alarm-class.
    Lapsed(Vec<NodeId>),
    /// The deadline passed with peers still live and still behind. **No
    /// guarantee holds.**
    Stalled {
        /// The peers the wait was still on, when it could say: named by the
        /// lease tier, empty when the transitional ack wait — which answers
        /// only yes or no — is what timed out.
        waiting_on: Vec<NodeId>,
        /// Lease-holders whose serve-leases lapsed under the *same* write while
        /// something else stalled it. Observability only: the verdict stays
        /// `Stalled`, because a lapse this write also has to survive an
        /// unleased peer's silence carries no guarantee on its own. Empty
        /// whenever the coherence wait itself is what ran out of time.
        lapsed: Vec<NodeId>,
    },
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
    /// The read-side licence in `strong-acks` and `bounded`; in `strong` the
    /// lease is, and this stays untouched.
    stability: Arc<Stability>,
    /// This node's participation in the coherence-lease tier — `Some` only in
    /// [`Consistency::Strong`]. Held here because **dropping it stops the
    /// protocol**: no renewals (this node's own window closes within `D`), no
    /// grants (every peer's confirmation freezes until membership reaps this
    /// node), no ingest.
    leases: Option<Leases>,
    /// The read handle of the lease above: the whole read-side licence in
    /// `strong` mode, and a lock-free borrow plus one compare per request.
    lease_view: Option<LeaseView>,
    /// Shared with the apply loop, which stands the lease down on a feed gap,
    /// and with the resync that affirms catch-up afterwards.
    resync: Arc<ResyncGate>,
    /// The deadline the coherence wait gets: one lease duration (the longest a
    /// silent holder's lapse can take) plus [`WRITE_WAIT_SLACK`].
    write_wait: Duration,
    /// Keeps the gossip node (receive loop, group actors) alive for the
    /// process lifetime. `None` when a test drives a raw group directly.
    _node: Option<Node<UdpTransport>>,
}

impl WriteSync {
    /// Attach a feed to `group` as `me`. Start the apply loop separately with
    /// [`start_apply`](Self::start_apply) once the local cache exists.
    ///
    /// Both ways into a group land here — [`WriteSync::new`] (the gossip node it just
    /// spawned) and a caller handing over a group it drives itself (the tests) — so this
    /// is where the node declares what it participates in (see [`advertise`]) and, in
    /// `strong`, where it joins the lease set.
    ///
    /// The lease set is constructed **before** the advertisement, and the order is the
    /// contract rather than a style: [`CAP_LEASE`] says "I grant your leases and I block
    /// my writes on yours", and a node that advertises it without a running granter
    /// freezes every other reader's confirmation until membership reaps it.
    ///
    /// # Panics
    /// If called outside a Tokio runtime in a mode that runs a lease set — the tier
    /// spawns its renew/grant/ingest tasks here.
    pub(crate) fn attach(
        group: Group,
        me: NodeId,
        consistency: Consistency,
        lease: LeaseConfig,
        node: Option<Node<UdpTransport>>,
    ) -> Self {
        debug_assert!(
            lease.validate().is_ok(),
            "lease config outside its own envelope: {:?}",
            lease.validate()
        );
        let capacity = NonZeroUsize::new(FEED_CAPACITY).unwrap_or(NonZeroUsize::MIN);
        let feed = WriteFeed::new(group.clone(), capacity, encode_event);
        let leases = consistency
            .leases()
            .then(|| Leases::new(group.clone(), me.clone(), lease));
        let lease_view = leases.as_ref().map(Leases::view);
        advertise(&group, consistency);
        Self {
            feed,
            group,
            me,
            consistency,
            view: OnceLock::new(),
            stability: Arc::new(Stability::new()),
            leases,
            lease_view: lease_view.clone(),
            resync: Arc::new(ResyncGate::new(lease_view)),
            write_wait: lease.duration + WRITE_WAIT_SLACK,
            _node: node,
        }
    }

    /// Whether this node may answer a cache-eligible read from node-local state — the
    /// read-side half of transparent coherence, and the question each mode answers with
    /// its own licence.
    ///
    /// * `strong`: does this node hold a valid serve-lease? A mechanism, not a heuristic:
    ///   `false` covers booting, the lease shell's warm-up window, a lapse, an
    ///   unaffirmed resync, a granter gone silent, and a partition — every one of which
    ///   sends the read to the origin.
    /// * `strong-acks`: the pre-lease heuristic — a node whose membership view is not
    ///   fully alive may itself be the partitioned one, so it benches itself.
    /// * `bounded`: always. Freshness is the bound, and the barrier is what enforces it.
    pub(crate) fn may_serve_local(&self) -> bool {
        match self.consistency {
            Consistency::Strong => self.lease_view.as_ref().is_some_and(LeaseView::valid),
            Consistency::StrongAcks => self.cluster_healthy(),
            Consistency::Bounded => true,
        }
    }

    /// Whether this node's index may be treated as authoritative for a key's *absence*.
    ///
    /// In `strong` this is the same lease that licenses any other local answer, which is
    /// the point of the tier: "may I answer a 404?" stops being a hand-rolled
    /// view-stability heuristic and becomes "do I hold a lease". In the other two modes
    /// it stays what it was — an index miss is only a 404 if no peer could be holding a
    /// write this node has not seen, which needs both a fully-alive view and that view
    /// having held still for the failure detector's whole window (see [`settle_window`]).
    /// Otherwise the origin answers: slower, never a 404 for a key that exists.
    pub(crate) fn may_answer_404(&self) -> bool {
        match self.consistency {
            Consistency::Strong => self.lease_view.as_ref().is_some_and(LeaseView::valid),
            Consistency::StrongAcks | Consistency::Bounded => {
                self.cluster_healthy() && self.stability.settled(&self.group)
            }
        }
    }

    /// The resync generation a catch-up starting **now** would answer for. Read it
    /// before the re-synchronization runs and hand it back to
    /// [`affirm_resynced`](Self::affirm_resynced): a gap that lands in between
    /// supersedes this catch-up, and the affirmation must not re-open a window for work
    /// that no longer covers it.
    pub(crate) fn resync_gen(&self) -> u64 {
        self.resync.generation()
    }

    /// Affirm that this node has re-synchronized and may serve locally again, on behalf
    /// of the resync that started at `generation`.
    ///
    /// Polls rather than concluding, because the two things that decline an affirmation
    /// both clear on their own: this node's own lease warm-up window, and a confirmation
    /// frozen behind a granter that has stopped publishing. One `false` is "not yet",
    /// never a verdict. Returns as soon as it takes, as soon as a later gap supersedes
    /// it, or — loudly — at [`AFFIRM_DEADLINE`], which leaves this node serving via the
    /// origin until the next resync: correct and slow, never stale.
    ///
    /// A no-op in every mode but `strong`: there is no window to open.
    pub(crate) async fn affirm_resynced(&self, generation: u64) {
        let deadline = Instant::now() + AFFIRM_DEADLINE;
        loop {
            match self.resync.affirm(generation) {
                Affirmation::Took | Affirmation::Superseded => return,
                Affirmation::NotYet => {}
            }
            if Instant::now() >= deadline {
                warn!(
                    "coherence lease never accepted this node's catch-up; serving via the \
                     origin until the next resync (a granter may have stopped publishing)"
                );
                return;
            }
            tokio::time::sleep(AFFIRM_POLL).await;
        }
    }

    /// Stand this node's serve-lease down: the gap arm of the apply loop, reachable from
    /// a test that must not fabricate a ring overflow to get at it.
    #[cfg(test)]
    pub(crate) fn require_resync(&self) {
        self.resync.require_resync();
    }

    /// How many times this node's own serve-lease has lapsed — groupnet's monotone
    /// counter, which misses no edge (unlike the state, whose `Lapsed` edge the lease
    /// shell's view task usually consumes first). `0` in a mode that holds no lease.
    /// A test's way of waiting for the lapse [`watch_lapses`] exists to remediate.
    #[cfg(test)]
    pub(crate) fn lease_lapses(&self) -> u64 {
        self.lease_view.as_ref().map_or(0, LeaseView::lapses)
    }

    /// Every peer holding a live `~lease` entry in this node's view — the wait set a
    /// coherent write would resolve against. A test's way of asking "has this writer
    /// actually adopted that reader's lease yet", which is the precondition every
    /// assertion about a *lapse* rests on.
    #[cfg(test)]
    pub(crate) fn lease_holders(&self) -> Vec<NodeId> {
        self.leases
            .as_ref()
            .map(Leases::holders)
            .unwrap_or_default()
    }

    /// Retract this node's serve-lease: the graceful counterpart to the process dying,
    /// for a stop that was planned (`SIGTERM` from a rolling deploy, a scale-in, an
    /// operator's `ctrl_c`).
    ///
    /// A dropped lease set deliberately leaves the `~lease` entry behind, because a
    /// crash must cost a writer the lapse this tier is built on. A planned stop is the
    /// one case where that bound is pure waste: this node is going away on purpose and
    /// will serve nothing, so retracting the entry spares every peer's *first* write
    /// after the stop the up-to-`D` wait it would otherwise spend proving what this
    /// node already knows.
    ///
    /// It does **not** shorten the reader-side freeze, and must not be sold as if it
    /// did: this node's `~caps` advertisement lives in every peer's roster until
    /// membership reaps it, so every other reader's confirmation stays frozen for the
    /// reap horizon exactly as it would after a crash. That half is [`watch_lapses`]'
    /// business — it ends the freeze with a remediation, not with a shorter wait — and
    /// the two are complements: `leave` is the write side, the watcher is the read side.
    ///
    /// A no-op in every mode but `strong`, and never an error: a rejected retraction
    /// (a full actor inbox on the way out) just means the entry expires by TTL instead,
    /// which is where it started.
    pub fn leave(&self) {
        let Some(leases) = &self.leases else {
            return;
        };
        match leases.leave() {
            Ok(()) => info!("retracted this node's serve-lease for a planned stop"),
            Err(error) => warn!(
                "could not retract this node's serve-lease ({error}); a peer's first \
                 write after this stop waits it out instead"
            ),
        }
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

    /// Waits (bounded) until nothing that could be serving this key still can — the
    /// write-side half of transparent coherence. Each mode buys that differently:
    ///
    /// * `strong` runs the leased wait below;
    /// * `strong-acks` waits on every peer [`waits_on`] admits, under the caller's own
    ///   timeout — the pre-lease machinery, verbatim;
    /// * `bounded` returns at once: the origin ack is the write's end.
    pub(crate) async fn wait_cluster_applied(
        &self,
        token: WriteToken,
        timeout: Duration,
    ) -> WriteWait {
        match (self.consistency, self.leases.as_ref()) {
            (Consistency::Bounded, _) => WriteWait::Applied,
            (Consistency::Strong, Some(leases)) => self.wait_leased(leases, token, timeout).await,
            // `strong-acks`, and the unreachable `strong`-without-a-lease-set: the ack
            // tier, unchanged. A mode that grew a lease set later would have to answer
            // this arm rather than inherit it.
            (Consistency::Strong | Consistency::StrongAcks, _) => {
                let acked = applied_by_selected(
                    &self.group,
                    &self.me,
                    token,
                    |node| waits_on(&self.group, node),
                    timeout,
                )
                .await;
                if acked {
                    WriteWait::Applied
                } else {
                    WriteWait::Stalled {
                        waiting_on: Vec::new(),
                        lapsed: Vec::new(),
                    }
                }
            }
        }
    }

    /// The leased wait: two races run together, only one of which is the guarantee.
    ///
    /// **(a)** [`Leases::invalidated_coherently`] is the tier. It ends when every
    /// lease-holder has either applied the write (an ack round — the fast path costs
    /// exactly what `strong-acks` costs when healthy) or had its serve-lease expire in
    /// *this* node's engine, which puts it out of service until it re-synchronizes.
    ///
    /// **(b)** is transition insurance, and it exists only for a mixed fleet. A node
    /// that does not run the lease tier publishes no `~lease` entry, so (a) can neither
    /// wait on it nor excuse it — it is simply invisible there. [`waits_on_unleased`] is
    /// exactly that set, so in a uniform leased fleet (b) resolves instantly against an
    /// empty selection and costs a write nothing.
    ///
    /// They run concurrently rather than in sequence because neither implies the other
    /// and a write must not pay for both in series.
    async fn wait_leased(
        &self,
        leases: &Leases,
        token: WriteToken,
        timeout: Duration,
    ) -> WriteWait {
        let (coherent, insured) = tokio::join!(
            leases.invalidated_coherently(&self.me, token, self.write_wait),
            applied_by_selected(
                &self.group,
                &self.me,
                token,
                |node| waits_on_unleased(&self.group, node),
                timeout,
            ),
        );
        match (coherent, insured) {
            // A deadline the lease tier could not meet is the one outcome with no
            // guarantee behind it, and it names who — so it is reported ahead of the
            // insurance wait, which can only answer yes or no.
            (CoherenceOutcome::TimedOut { waiting_on }, _) => WriteWait::Stalled {
                waiting_on,
                lapsed: Vec::new(),
            },
            // The insurance wait timed out while the lease tier resolved. The verdict
            // is its — no guarantee — but a lapse it resolved *on* is a real event on a
            // real peer, and dropping it here is how a straggler goes uncounted for the
            // whole time an unrelated unleased peer is silent. Carried, not promoted.
            (coherent, false) => WriteWait::Stalled {
                waiting_on: Vec::new(),
                lapsed: match coherent {
                    CoherenceOutcome::LeaseLapsed { stragglers } => stragglers,
                    _ => Vec::new(),
                },
            },
            (CoherenceOutcome::LeaseLapsed { stragglers }, true) => WriteWait::Lapsed(stragglers),
            (CoherenceOutcome::AllApplied, true) => WriteWait::Applied,
        }
    }

    /// [`wait_cluster_applied`](Self::wait_cluster_applied) with the bookkeeping every
    /// write path wants. Never an error: an unresponsive peer is either dying (SWIM will
    /// exclude it), lapsed (out of service by its own clock), or partitioned.
    pub(crate) async fn ack_write(
        &self,
        token: WriteToken,
        timeout: Duration,
        bucket: &str,
        key: &str,
        metrics: &Metrics,
    ) {
        match self.wait_cluster_applied(token, timeout).await {
            WriteWait::Applied => {}
            WriteWait::Lapsed(stragglers) => {
                // Deliberately `info!` and a counter of its own: the guarantee HELD.
                // The stragglers cannot serve what this write invalidated, because a
                // lapsed reader serves nothing cached until it re-synchronizes. Worth
                // seeing — it is a peer that stopped acknowledging — but it is not the
                // alarm `ack_timeouts` is.
                let who = node_names(&stragglers);
                info!(
                    "write of {bucket}/{key} completed on a lease lapse ({who}); \
                     they serve nothing cached until they re-synchronize"
                );
                metrics.write_lease_lapse();
            }
            WriteWait::Stalled { waiting_on, lapsed } => {
                let who = node_names(&waiting_on);
                warn!("write ack timed out for {bucket}/{key} ({who}); a peer may lag");
                metrics.ack_timeout();
                if !lapsed.is_empty() {
                    // The lease tier *did* resolve this write, on a lapse, and the
                    // transitional ack wait is what ran out of time. The verdict above
                    // stands — that wait carries no guarantee — but the lapse happened,
                    // and an operator looking for "which peer stopped acknowledging"
                    // must find it under the same counter and the same names as any
                    // other lapse, rather than have it vanish with the outcome that lost.
                    let who = node_names(&lapsed);
                    info!(
                        "the same write also lapsed the serve-leases of {who}; \
                         they serve nothing cached until they re-synchronize"
                    );
                    metrics.write_lease_lapse();
                }
                // The settle clock is the read-side licence in `strong-acks` only, so
                // only `strong-acks` restarts it: a peer that did not ack in time is a
                // peer whose writes this node may also be missing. In `strong` the
                // lease carries that honesty per reader, on the reader's own clock, and
                // there is no cluster-wide clock here to disturb.
                if self.consistency == Consistency::StrongAcks {
                    self.stability.disturb();
                }
            }
        }
    }

    /// Whether this node's membership view is fully alive — the `strong-acks` and
    /// `bounded` read-side gate (see [`may_serve_local`](Self::may_serve_local)).
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
    ///
    /// In `strong` this also spawns [`watch_lapses`], because a gap is not the only
    /// way this node loses its right to serve and the others arrive as no event at
    /// all. Both live here for the same reason: this is where the two things a
    /// remediation needs — the local tiers and the origin re-LIST — first exist.
    pub(crate) fn start_apply(
        &self,
        local: LocalCache,
        state: Arc<RwLock<HashMap<String, BucketState>>>,
        resync: Arc<dyn Fn() + Send + Sync>,
        metrics: Arc<Metrics>,
    ) {
        let (frontier, view) = Frontier::new();
        let _ = self.view.set(view);
        if let Some(leases) = &self.leases {
            watch_lapses(
                Arc::clone(&self.resync),
                local.clone(),
                Arc::clone(&resync),
                Arc::clone(&metrics),
                lapse_poll(leases.config().duration),
            );
        }
        // Bounded mode publishes no acks (that is its point at scale); both strong
        // spellings do, and say so through their capability advertisement.
        let ledger = self
            .consistency
            .acks()
            .then(|| AckLedger::new(self.group.clone()));
        let mut peers = PeerWrites::new(self.group.clone(), self.me.clone(), decode_event);
        let stability = Arc::clone(&self.stability);
        let gate = Arc::clone(&self.resync);
        // The settle clock is the read-side licence wherever the lease is not — see
        // `may_answer_404`. In `strong` the gap arm stands the lease down instead, and
        // disturbing a clock nothing reads would only be a lie about what protects the
        // node.
        let disturbs = self.lease_view.is_none();
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
                        // Stand the licence down, flush, re-LIST — see `remediate`,
                        // which is also what a lapse with no gap behind it runs. The
                        // node stays out of service until the resync affirms catch-up
                        // for *this* generation: a reader that missed invalidations
                        // missed exactly the ones whose writers proceeded because it
                        // had. With every local copy gone (and the index bucket back
                        // to passthrough), acking the gap is truthful even while the
                        // origin re-LIST is still running.
                        remediate(&gate, &local, &resync).await;
                        if disturbs {
                            stability.disturb();
                        }
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
    /// The coherence-lease duration `D` in milliseconds (`S3CACHE_LEASE_MS`),
    /// used only by [`Consistency::Strong`]. [`DEFAULT_LEASE_MS`] is the value
    /// the environment defaults to, and the one a caller with no opinion wants.
    pub lease_ms: u64,
}

impl WriteSync {
    /// Bind the gossip transport, join the cluster group and attach the write
    /// feed. `None` when the bind address is unusable — gossip is optional, and
    /// a node that cannot join is a strict single node, not a dead one.
    /// Start the apply loop with [`start_apply`](Self::start_apply) once the
    /// local cache exists (the proxy does this in `start_coherence`).
    ///
    /// # The one tuned protocol knob, and what it buys
    ///
    /// `dead_timeout_ms` is pulled down from groupnet's 10s default to the lease
    /// duration `D` (floored at [`DEAD_TIMEOUT_FLOOR_MS`]), uniformly in every mode so a
    /// mixed fleet has one membership timing rather than two. The tuning **is** part of
    /// the lease migration, not decoration around it:
    ///
    /// * A reader's confirmation is a min over its whole roster, and only a **reap**
    ///   removes a member from it. So one `CAP_LEASE` member that stops publishing
    ///   grants — crashed, hung, partitioned — freezes *every* reader's confirmation
    ///   cluster-wide. Each reader's window closes within one `D` of the freeze and
    ///   cannot reopen until membership reaps the silent member, at the reap horizon:
    ///   `2 × dead_timeout_ms` past the `Dead` verdict, itself up to
    ///   `detection_window_ms` past the silence.
    /// * Untuned that is `0.9 + 20 − 2` ≈ **19s of cluster-wide origin-serving** —
    ///   correct reads throughout, none of them cached. At `dead_timeout_ms = D = 2s` it
    ///   is ≈ **3s**.
    ///
    /// What it costs is the other end of the same horizon: `2 × dead_timeout_ms` is also
    /// how long a returning node's entries stay recoverable by a digest, so a partition
    /// outliving ~4s lands on the write-feed **gap** path instead of reconciling — flush
    /// the tiers, re-LIST from the origin. That is not a regression to work around; the
    /// origin is the authority this index caches, and the gap path is s3cache's standing
    /// remedy for "this node provably missed writes". Trading a rare, loud, correct
    /// resync for 16s off every unreaped-granter freeze is the right side of that deal
    /// for a cache.
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
        let lease = LeaseConfig::for_duration(Duration::from_millis(cfg.lease_ms));
        debug_assert!(
            lease.validate().is_ok(),
            "S3CACHE_LEASE_MS outside the lease tier's envelope: {:?}",
            lease.validate()
        );
        let advertise = cfg
            .advertise
            .or_else(|| transport.local_addr().ok().map(|addr| addr.to_string()));
        let mut builder = Node::builder(me.clone(), transport.clone()).config(Config {
            dead_timeout_ms: cfg.lease_ms.max(DEAD_TIMEOUT_FLOOR_MS),
            ..Config::default()
        });
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
        let lease_ms = cfg.lease_ms;
        info!(
            "gossip coherence bound on `{bind}` as `{node_id}` (consistency: {mode}, lease: {lease_ms}ms)"
        );
        Some(WriteSync::attach(
            group,
            me,
            cfg.consistency,
            lease,
            Some(node),
        ))
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
        lease_ms: parse_lease_ms(env_var("S3CACHE_LEASE_MS").as_deref()),
    };
    WriteSync::new(cfg).await
}

/// Read the `S3CACHE_LEASE_MS` spelling: the coherence-lease duration `D`, in
/// milliseconds. Anything unusable — unset, not a number, or zero, which is the
/// engine's "never expires" and so precisely the stale claim the tier exists to
/// prevent — falls back to [`DEFAULT_LEASE_MS`], loudly when it was set at all.
fn parse_lease_ms(raw: Option<&str>) -> u64 {
    let Some(raw) = raw else {
        return DEFAULT_LEASE_MS;
    };
    match raw.trim().parse::<u64>() {
        Ok(ms) if ms > 0 => ms,
        _ => {
            warn!("unusable S3CACHE_LEASE_MS `{raw}`; using {DEFAULT_LEASE_MS}ms");
            DEFAULT_LEASE_MS
        }
    }
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
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::{Arc, RwLock};
    use std::time::{Duration, Instant, SystemTime};

    use groupnet::consistency::{CAP_LEASE, LeaseConfig};
    use groupnet::core::{Config, NodeId, Status};
    use groupnet::runtime::{Group, Node};
    use groupnet::transport::mem::{MemTransport, Network};
    use groupnet::transport::{Inbound, Transport};
    use s3s::dto::GetObjectOutput;

    use super::{
        CAP_ACKS, CAP_BOUNDED, Consistency, DEFAULT_LEASE_MS, IndexEvent, IndexEventV1, IndexOp,
        IndexOpV1, V2_MAGIC, WriteSync, WriteWait, decode_event, encode_event, lapse_poll,
        parse_lease_ms, parse_seeds, settle_window, waits_on, waits_on_unleased, wire_stamp,
    };
    use crate::index::{BucketState, ObjEntry, standard_class};
    use crate::metrics::Metrics;
    use crate::tier::{CachedObject, LocalCache, TieredCache};

    type Index = Arc<RwLock<HashMap<String, BucketState>>>;

    /// A lease short enough to watch lapse inside a test, and still comfortably inside
    /// its own envelope: `D = 300ms`, renewed every 100ms, 5ms of rate margin.
    const TEST_LEASE: Duration = Duration::from_millis(300);

    fn test_lease() -> LeaseConfig {
        LeaseConfig::for_duration(TEST_LEASE)
    }

    /// Test timings: the shipped tuning in miniature. `dead_timeout_ms` tracks the lease
    /// duration exactly as [`WriteSync::new`] makes it, so what a test observes about
    /// reap-bounded behaviour is the same shape production gets, only faster.
    fn brisk() -> Config {
        Config {
            gossip_interval_ms: 10,
            probe_interval_ms: 20,
            probe_timeout_ms: 10,
            suspect_timeout_ms: 50,
            dead_timeout_ms: 300,
            anti_entropy_interval_ms: 25,
            ..Config::default()
        }
    }

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
            .config(brisk())
            .spawn();
        let group = node.join_group("s3cache");
        (me, node, group)
    }

    /// A [`MemTransport`] endpoint with a kill switch — the only way to make a peer
    /// actually *die* in-process. Dropping a [`Node`] does not stop it (its receive loop
    /// holds its own handle), and a peer that keeps gossiping is never reaped, so nothing
    /// else can produce the input the lapse watcher exists for: a lease-granting peer that
    /// goes silent for good. Unplugged, this neither sends nor delivers, which is what a
    /// dead process looks like from the outside.
    struct Unpluggable {
        inner: MemTransport,
        plugged: Arc<AtomicBool>,
    }

    impl Transport for Unpluggable {
        type Error = <MemTransport as Transport>::Error;

        async fn send(&self, to: &NodeId, msg: &[u8]) -> Result<(), Self::Error> {
            if self.plugged.load(Ordering::Relaxed) {
                self.inner.send(to, msg).await
            } else {
                Ok(()) // best-effort transports drop, they do not error
            }
        }

        async fn recv(&self) -> Result<Inbound, Self::Error> {
            loop {
                let inbound = self.inner.recv().await?;
                if self.plugged.load(Ordering::Relaxed) {
                    return Ok(inbound);
                }
            }
        }
    }

    /// [`spawn_node`] with the kill switch; storing `false` in the returned flag is the
    /// peer dying.
    fn spawn_killable(
        net: &Network,
        id: &str,
        peer: &str,
    ) -> (NodeId, Node<Unpluggable>, Group, Arc<AtomicBool>) {
        let me = NodeId::new(id);
        let plugged = Arc::new(AtomicBool::new(true));
        let transport = Unpluggable {
            inner: net.endpoint(me.clone()),
            plugged: Arc::clone(&plugged),
        };
        let node = Node::builder(me.clone(), transport)
            .seed(NodeId::new(peer))
            .config(brisk())
            .spawn();
        let group = node.join_group("s3cache");
        (me, node, group, plugged)
    }

    /// The resync handle production hands the apply loop, in miniature: it counts the
    /// origin re-LIST it stands in for, and then — the half that matters — **owns the
    /// affirmation**, read at call time and answered for that generation, exactly as
    /// `CachingProxy::gap_resync_handle` does. A remediation that did not affirm would
    /// leave the node correct and permanently out of service, which is the very bug the
    /// watcher exists to prevent.
    fn counting_resync(
        sync: &Arc<WriteSync>,
        count: &Arc<AtomicU64>,
    ) -> Arc<dyn Fn() + Send + Sync> {
        // Weak, so the handle the apply loop holds forever does not keep the `WriteSync`
        // (which owns that loop) alive in a cycle.
        let (sync, count) = (Arc::downgrade(sync), Arc::clone(count));
        Arc::new(move || {
            count.fetch_add(1, Ordering::Relaxed);
            let Some(sync) = sync.upgrade() else {
                return;
            };
            let generation = sync.resync_gen();
            tokio::spawn(async move { sync.affirm_resynced(generation).await });
        })
    }

    /// A leased node with its apply loop (and so its lapse watcher) running, its own
    /// tiers, and a counter for the remediations its resync handle ran.
    fn leased_reader(
        group: Group,
        me: NodeId,
        metrics: &Arc<Metrics>,
    ) -> (Arc<WriteSync>, TieredCache, Arc<AtomicU64>) {
        let cache = TieredCache::new(1024 * 1024, None, Arc::clone(metrics));
        let sync = Arc::new(attach(group, me, Consistency::Strong));
        let resyncs = Arc::new(AtomicU64::new(0));
        (sync, cache, resyncs)
    }

    /// Starts the apply loop of a [`leased_reader`] — separately, because one test has to
    /// decide a race by starting the watcher after it.
    fn start_watching(
        sync: &Arc<WriteSync>,
        cache: &TieredCache,
        resyncs: &Arc<AtomicU64>,
        metrics: &Arc<Metrics>,
    ) -> Index {
        let state: Index = Arc::new(RwLock::new(HashMap::new()));
        let local: LocalCache = cache.local();
        sync.start_apply(
            local,
            Arc::clone(&state),
            counting_resync(sync, resyncs),
            Arc::clone(metrics),
        );
        state
    }

    /// A [`WriteSync`] on `group`, leased at [`TEST_LEASE`] unless the mode says
    /// otherwise.
    fn attach(group: Group, me: NodeId, consistency: Consistency) -> WriteSync {
        WriteSync::attach(group, me, consistency, test_lease(), None)
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
        wired_pair_named(net, ("sync-a", "sync-b"), Consistency::Strong)
    }

    /// [`wired_pair`] with the gossip identities and the mode spelled out — unique ids
    /// per test, so parallel tests never share one.
    fn wired_pair_named(
        net: &Network,
        ids: (&str, &str),
        consistency: Consistency,
    ) -> (WriteSync, WriteSync, Index, TieredCache) {
        let (a_id, _a_node, a_group) = spawn_node(net, ids.0, ids.1);
        let (b_id, _b_node, b_group) = spawn_node(net, ids.1, ids.0);
        let metrics = Arc::new(Metrics::default());
        let cache = TieredCache::new(1024 * 1024, None, metrics.clone());
        let state: Index = Arc::new(RwLock::new(HashMap::new()));
        let sync_b = attach(b_group, b_id, consistency);
        sync_b.start_apply(cache.local(), state.clone(), Arc::new(|| {}), metrics);
        let sync_a = attach(a_group, a_id, consistency);
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
            Consistency::parse(" Strong-Acks ") == Consistency::StrongAcks,
            "the ack-only spelling is its own mode, not a synonym for strong"
        );
        assert!(
            Consistency::parse("eventual") == Consistency::Strong,
            "an unknown mode falls back to strong"
        );

        assert_eq!(
            parse_lease_ms(None),
            DEFAULT_LEASE_MS,
            "unset is the default"
        );
        assert_eq!(parse_lease_ms(Some(" 750 ")), 750);
        assert_eq!(
            parse_lease_ms(Some("0")),
            DEFAULT_LEASE_MS,
            "zero is the engine's `never expires` — the stale claim the tier prevents"
        );
        assert_eq!(parse_lease_ms(Some("soon")), DEFAULT_LEASE_MS);
    }

    /// Only the leased mode advertises [`CAP_LEASE`], because only it constructs a
    /// lease set — a node advertising it without a running granter freezes every other
    /// reader's confirmation until membership reaps it.
    #[test]
    fn only_a_mode_that_grants_leases_advertises_that_it_does() {
        assert_eq!(Consistency::Strong.capabilities(), [CAP_ACKS, CAP_LEASE]);
        assert_eq!(Consistency::StrongAcks.capabilities(), [CAP_ACKS]);
        assert_eq!(Consistency::Bounded.capabilities(), [CAP_BOUNDED]);
        for mode in [
            Consistency::Strong,
            Consistency::StrongAcks,
            Consistency::Bounded,
        ] {
            assert_eq!(
                mode.leases(),
                mode.capabilities().contains(&CAP_LEASE),
                "the advertisement and the granter are one decision, in both directions"
            );
        }
    }

    /// Which peers a strong writer's ack wait includes. The rule is a
    /// three-way one, and the third case is the one that matters: a peer that
    /// has advertised nothing is *unknown*, not absent, so it is waited for.
    #[tokio::test]
    async fn the_ack_wait_covers_advertisers_and_unknowns_but_not_declared_bounded_peers() {
        let net = Network::new();
        let (_a_id, _a_node, a_group) = spawn_node(&net, "cap-a", "cap-b");
        let (b_id, _b_node, b_group) = spawn_node(&net, "cap-b", "cap-a");
        let (c_id, _c_node, c_group) = spawn_node(&net, "cap-c", "cap-a");
        // D runs, joins, and never advertises — a node from before this change.
        let (d_id, _d_node, _d_group) = spawn_node(&net, "cap-d", "cap-a");
        b_group
            .advertise_capabilities([CAP_ACKS])
            .expect("B advertises the ack tier");
        c_group
            .advertise_capabilities([CAP_BOUNDED])
            .expect("C declares itself bounded");

        eventually(
            || !waits_on(&a_group, &c_id),
            "C's bounded declaration to converge at the writer",
        )
        .await;
        assert!(
            waits_on(&a_group, &b_id),
            "a CAP_ACKS advertiser is waited for"
        );
        assert!(
            waits_on(&a_group, &d_id),
            "so is a peer that has never advertised: absence is not non-participation"
        );
    }

    /// The **transition** set a leased writer's second wait covers: exactly the peers
    /// the lease wait cannot, because they publish no `~lease` entry for it to wait on
    /// or to expire. A uniform leased fleet makes it empty, which is what keeps the
    /// insurance from costing a healthy cluster a redundant ack round per write.
    #[tokio::test]
    async fn the_transition_wait_covers_exactly_what_the_lease_cannot() {
        let net = Network::new();
        let (_a_id, _a_node, a_group) = spawn_node(&net, "mix-a", "mix-b");
        let (b_id, _b_node, b_group) = spawn_node(&net, "mix-b", "mix-a");
        let (c_id, _c_node, c_group) = spawn_node(&net, "mix-c", "mix-a");
        // D runs, joins, and never advertises — a node from before any of this.
        let (d_id, _d_node, _d_group) = spawn_node(&net, "mix-d", "mix-a");
        let (e_id, _e_node, e_group) = spawn_node(&net, "mix-e", "mix-a");
        b_group
            .advertise_capabilities(Consistency::StrongAcks.capabilities().iter().copied())
            .expect("B pins the ack tier");
        c_group
            .advertise_capabilities(Consistency::Bounded.capabilities().iter().copied())
            .expect("C declares itself bounded");
        e_group
            .advertise_capabilities(Consistency::Strong.capabilities().iter().copied())
            .expect("E runs the lease tier");

        eventually(
            || !waits_on_unleased(&a_group, &c_id) && !waits_on_unleased(&a_group, &e_id),
            "C's and E's declarations to converge at the writer",
        )
        .await;
        assert!(
            waits_on_unleased(&a_group, &b_id),
            "a strong-acks peer holds no lease, so only this wait can cover it"
        );
        assert!(
            waits_on_unleased(&a_group, &d_id),
            "and so does a peer that has never advertised — the whole point of the set"
        );
        assert!(
            waits_on(&a_group, &e_id) && !waits_on_unleased(&a_group, &e_id),
            "a lease advertiser is the coherence wait's business, not this one's"
        );
    }

    /// The settle window is computed per call, from the group's own config
    /// and the membership it currently has to sweep — and never from a view
    /// of one, which is what a node that has not yet met its peer sees.
    #[tokio::test]
    async fn the_settle_window_is_sized_per_call_and_floors_at_two_members() {
        let alone_net = Network::new();
        let alone_id = NodeId::new("win-alone");
        // The same timings the pair below runs, so the comparison at the end is about
        // membership size and nothing else.
        let alone_node = Node::builder(alone_id.clone(), alone_net.endpoint(alone_id))
            .config(brisk())
            .spawn();
        let alone = alone_node.join_group("s3cache");
        assert_eq!(alone.members().len(), 1, "nobody to gossip with");
        let cfg = alone.config();
        assert_eq!(
            settle_window(&alone),
            Duration::from_millis(cfg.detection_window_ms(2)),
            "a node that has not seen its peer still budgets for one"
        );
        assert!(
            settle_window(&alone) > Duration::from_millis(cfg.detection_window_ms(1)),
            "the floor is strictly longer than the group-of-one bound it replaces"
        );

        let net = Network::new();
        let (_a_id, _a_node, a_group) = spawn_node(&net, "win-a", "win-b");
        let (_b_id, _b_node, _b_group) = spawn_node(&net, "win-b", "win-a");
        let (_c_id, _c_node, _c_group) = spawn_node(&net, "win-c", "win-a");
        eventually(
            || a_group.members().len() == 3,
            "a three-member view to converge",
        )
        .await;
        assert!(
            settle_window(&a_group) > settle_window(&alone),
            "the window grows with the membership the detector has to sweep"
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
            matches!(
                sync_a
                    .wait_cluster_applied(receipt.token, Duration::from_secs(5))
                    .await,
                WriteWait::Applied
            ),
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

    /// The pre-lease mode's contract, kept exactly as it was for the one release it
    /// survives: the write-ack wait has to actually fire when a peer does not
    /// acknowledge — the counter operators watch is only worth watching if it moves —
    /// and an unacked write stands this node's authoritative 404s down, because a peer
    /// that did not apply our write in time is a peer whose writes we may equally be
    /// missing. (In `strong` there is no cluster-wide clock to stand down: each reader's
    /// own lease carries that honesty, on that reader's own clock.)
    #[tokio::test]
    async fn in_strong_acks_an_unacked_write_is_counted_and_stands_the_404_down() {
        let net = Network::new();
        let (_sync_a, sync_b, _state, _cache) =
            wired_pair_named(&net, ("acks-a", "acks-b"), Consistency::StrongAcks);
        let metrics = Metrics::default();
        // A was attached without an apply loop, so it publishes no ack ledger — but it
        // is alive, so B has to wait for it and then give up.
        eventually(
            || {
                sync_b
                    .group
                    .statuses()
                    .iter()
                    .any(|(id, status)| id.as_str() == "acks-a" && *status == Status::Alive)
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
            !sync_b.may_answer_404(),
            "and an index miss is no longer an authoritative 404"
        );
    }

    /// The fast path: a leased write is an ack round and nothing more. When it returns,
    /// the peer already holds the write — asserted with no polling in between, which is
    /// the whole claim `strong` makes to a client.
    #[tokio::test]
    async fn a_leased_write_resolves_on_acks_and_the_peer_already_has_it() {
        let net = Network::new();
        let (sync_a, _sync_b, state, _cache) =
            wired_pair_named(&net, ("fast-a", "fast-b"), Consistency::Strong);
        let metrics = Metrics::default();
        // Nothing about a *fast* path is provable until there is somebody to be fast
        // against: an empty wait set resolves immediately and would pass this vacuously.
        eventually(
            || sync_a.lease_holders().len() == 1,
            "A to adopt B's serve-lease",
        )
        .await;

        let started = Instant::now();
        let receipt = sync_a
            .publish_put("bkt", "fast", &written(9), &metrics)
            .await;
        let outcome = sync_a
            .wait_cluster_applied(receipt.token, Duration::from_secs(5))
            .await;

        assert!(
            matches!(outcome, WriteWait::Applied),
            "a healthy leased write ends on acknowledgement, not on a lapse"
        );
        assert_eq!(
            indexed_size(&state, "bkt", "fast"),
            Some(9),
            "and B had applied it before the write returned"
        );
        assert!(
            started.elapsed() < TEST_LEASE,
            "an ack round costs well under one lease duration ({:?})",
            started.elapsed()
        );
    }

    /// **The lapse bound**, which is the whole reason this tier exists: a peer that stops
    /// renewing does not tax writes forever waiting for it to *learn* it should stand
    /// down. Its serve-lease expires on the writer's own engine, the write completes with
    /// the guarantee intact — the straggler serves nothing cached until it
    /// re-synchronizes — and the writes after it are free again.
    #[tokio::test]
    async fn a_dropped_peers_lease_lapses_and_the_write_completes_inside_one_duration() {
        let net = Network::new();
        let (a_id, _a_node, a_group) = spawn_node(&net, "lapse-a", "lapse-b");
        let (b_id, _b_node, b_group) = spawn_node(&net, "lapse-b", "lapse-a");
        let sync_a = attach(a_group, a_id, Consistency::Strong);
        // B renews and grants but never applies: no apply loop, so no ack ledger. It is
        // the fail-slow reader, which is exactly the peer a lapse has to rescue.
        let sync_b = attach(b_group, b_id.clone(), Consistency::Strong);
        let metrics = Metrics::default();
        // Both halves of B's participation have to have converged: the lease A will
        // wait on, and the advertisement that keeps B out of the transitional ack wait.
        // Otherwise this measures gossip convergence rather than the lapse.
        eventually(
            || sync_a.lease_holders() == [b_id.clone()] && !waits_on_unleased(&sync_a.group, &b_id),
            "A to adopt B's serve-lease and its lease advertisement",
        )
        .await;

        // A drop is the shape of the process dying: renewals stop, and the entry is
        // deliberately *not* retracted — it has to lapse on the writer's clock, which is
        // the bound under test.
        drop(sync_b);

        let started = Instant::now();
        let receipt = sync_a
            .publish_put("bkt", "lapsed", &written(1), &metrics)
            .await;
        sync_a
            .ack_write(
                receipt.token,
                Duration::from_millis(200),
                "bkt",
                "lapsed",
                &metrics,
            )
            .await;
        let stalled = started.elapsed();
        assert!(
            stalled < TEST_LEASE * 3,
            "the write ends at the lapse (~one lease duration), not at its own deadline ({stalled:?})"
        );
        let text = metrics.prometheus_text();
        assert!(
            text.contains("\ns3cache_write_lease_lapses 1\n"),
            "the lapse is counted as the guarantee it is"
        );
        assert!(
            text.contains("\ns3cache_ack_timeouts 0\n"),
            "and never as the alarm it is not"
        );

        // And the cost is not recurring: the lapsed holder has left the wait set, so the
        // next write is back on the fast path.
        let started = Instant::now();
        let receipt = sync_a
            .publish_put("bkt", "after", &written(2), &metrics)
            .await;
        assert!(matches!(
            sync_a
                .wait_cluster_applied(receipt.token, Duration::from_secs(5))
                .await,
            WriteWait::Applied
        ));
        assert!(
            started.elapsed() < TEST_LEASE,
            "a lapsed peer is excused once, not once per write"
        );
    }

    /// The read-side licence, end to end: a booting reader holds none, an affirmed lease
    /// is one, a gap takes it back — and only the resync that actually ran may give it
    /// back. The last part is the generation guard, and it is what stops a resync that a
    /// second gap superseded from re-opening a window nothing re-synchronized for.
    #[tokio::test]
    async fn the_readers_licence_is_its_lease_and_only_a_current_resync_restores_it() {
        let net = Network::new();
        let (_sync_a, sync_b, _state, _cache) =
            wired_pair_named(&net, ("gate-a", "gate-b"), Consistency::Strong);

        assert!(
            !sync_b.may_serve_local(),
            "a booting reader has missed every invalidation issued while it was down"
        );
        assert!(
            !sync_b.may_answer_404(),
            "and an index miss is certainly not an authoritative absence"
        );

        // The shell's reader boot guard latches after one detection window plus two
        // anti-entropy rounds, and declines the affirmation until it does — so the
        // affirmation polls rather than treating one refusal as a verdict.
        sync_b.affirm_resynced(sync_b.resync_gen()).await;
        assert!(sync_b.may_serve_local(), "an affirmed lease is the licence");
        assert!(sync_b.may_answer_404());

        sync_b.require_resync();
        assert!(
            !sync_b.may_serve_local(),
            "a gap takes it back immediately — not at the end of the remediation"
        );

        // Two gaps, one after the other: the first resync's affirmation must not speak
        // for the second's, which has not run yet.
        let superseded = sync_b.resync_gen();
        sync_b.require_resync();
        sync_b.affirm_resynced(superseded).await;
        assert!(
            !sync_b.may_serve_local(),
            "a superseded resync cannot affirm catch-up it never caught up on"
        );

        sync_b.affirm_resynced(sync_b.resync_gen()).await;
        assert!(
            sync_b.may_serve_local(),
            "and the resync that did run gives the licence back"
        );
    }

    /// **The lapse watcher**, and the failure it exists for: a lapse that arrives with no
    /// write-feed gap behind it.
    ///
    /// B does not gap A — it simply dies, with the feed quiet so no ring can overflow,
    /// which is what a scale-in, a permanent loss and a rolling restart of a read-mostly
    /// peer all look like from A. A's confirmation freezes on B's silence, its lease
    /// lapses, and the lapse **latches**: the apply loop will never hear about it, so
    /// without a watcher A serves every read from the origin for the rest of its life.
    /// With one, the lapse gets the gap's remediation — flush, origin re-LIST, affirm —
    /// and A is back in service the moment membership reaps B and its lease can be
    /// confirmed again.
    #[tokio::test]
    async fn a_lapse_with_no_gap_is_remediated_and_the_reader_recovers() {
        let net = Network::new();
        let (a_id, _a_node, a_group) = spawn_node(&net, "lapse-watch-a", "lapse-watch-b");
        let (b_id, _b_node, b_group, b_alive) =
            spawn_killable(&net, "lapse-watch-b", "lapse-watch-a");
        let metrics = Arc::new(Metrics::default());
        let (sync_a, cache, resyncs) = leased_reader(a_group, a_id, &metrics);
        let _sync_b = attach(b_group, b_id.clone(), Consistency::Strong);
        let _state = start_watching(&sync_a, &cache, &resyncs, &metrics);

        // A has to *hold* a lease before losing one can mean anything: B's grant, its own
        // warm-up window, and the boot affirmation.
        sync_a.affirm_resynced(sync_a.resync_gen()).await;
        assert!(sync_a.may_serve_local(), "A takes its serve-lease");
        // A body copy, so the flush half of the remediation is observed rather than
        // inferred from the licence coming back.
        let key = ("bkt".to_owned(), "obj".to_owned());
        cache.insert(key.clone(), cached(b"stale")).await;
        assert!(cache.get(&key).await.is_some());

        b_alive.store(false, Ordering::Relaxed); // B dies. No gap, no event, no warning.

        eventually(
            || sync_a.lease_lapses() == 1,
            "A's lease to lapse on B's silence",
        )
        .await;
        assert!(
            !sync_a.may_serve_local(),
            "a lapsed reader serves nothing cached — that is the latch"
        );

        eventually(
            || resyncs.load(Ordering::Relaxed) == 1,
            "the watcher to run the gap remediation for the lapse",
        )
        .await;
        assert!(
            cache.get(&key).await.is_none(),
            "the remediation flushed every local copy, exactly as a gap would"
        );
        eventually(
            || sync_a.may_serve_local(),
            "A to affirm its way back into service once the reap frees its confirmation",
        )
        .await;

        let text = metrics.prometheus_text();
        assert!(
            text.contains("\ns3cache_lease_lapse_resyncs 1\n"),
            "the read-side lapse is counted where an operator looks for it"
        );
        assert!(
            text.contains("\ns3cache_feed_gaps 0\n"),
            "and no write-feed gap was involved in any of it"
        );
        assert_eq!(
            resyncs.load(Ordering::Relaxed),
            1,
            "one lapse, one remediation — the watcher does not re-run while it waits"
        );
    }

    /// The interlock: a lapse a gap has **already** stood the lease down for buys one
    /// remediation, not two.
    ///
    /// The order under test is the racing one — the lease lapses, then a gap arrives
    /// before anything has remediated the lapse — and the race is *decided* here rather
    /// than sampled: the apply loop (and with it the watcher) starts after the gap's
    /// stand-down, so the watcher's first look is guaranteed to be the one that must
    /// yield. It yields because [`super::ResyncGate::require_resync`] recorded the lapse
    /// count it covers, and the flush and origin re-LIST that follow a stand-down cover
    /// every lapse observed before it. Without that watermark the watcher would flush and
    /// re-LIST a second time, on top of a resync already in flight.
    ///
    /// The reverse order is deliberately *not* deduplicated: a gap that lands after the
    /// watcher remediated is independent proof of missed writes and is entitled to its
    /// own resync.
    #[tokio::test]
    async fn a_lapse_a_gap_already_owns_is_not_remediated_twice() {
        let net = Network::new();
        let (a_id, _a_node, a_group) = spawn_node(&net, "dedupe-a", "dedupe-b");
        let (b_id, _b_node, b_group, b_alive) = spawn_killable(&net, "dedupe-b", "dedupe-a");
        let metrics = Arc::new(Metrics::default());
        let (sync_a, cache, resyncs) = leased_reader(a_group, a_id, &metrics);
        let _sync_b = attach(b_group, b_id.clone(), Consistency::Strong);

        sync_a.affirm_resynced(sync_a.resync_gen()).await;
        assert!(sync_a.may_serve_local(), "A takes its serve-lease");
        b_alive.store(false, Ordering::Relaxed);
        eventually(
            || sync_a.lease_lapses() == 1,
            "A's lease to lapse on B's silence",
        )
        .await;

        // The gap arm's stand-down, verbatim — a gap landing on the same lapse.
        let generation = sync_a.resync_gen();
        sync_a.require_resync();
        assert_eq!(
            sync_a.resync_gen(),
            generation + 1,
            "the gap started a generation of its own"
        );

        // Now the watcher starts, with the lapse already counted and already covered.
        let _state = start_watching(&sync_a, &cache, &resyncs, &metrics);
        tokio::time::sleep(lapse_poll(TEST_LEASE) * 5).await;

        assert_eq!(
            resyncs.load(Ordering::Relaxed),
            0,
            "the watcher yielded: the gap's remediation already owns this lapse"
        );
        assert_eq!(
            sync_a.resync_gen(),
            generation + 1,
            "and it started no generation of its own to affirm against"
        );
        assert!(
            metrics
                .prometheus_text()
                .contains("\ns3cache_lease_lapse_resyncs 0\n"),
            "a lapse remediated by the gap is not also counted as the watcher's"
        );
    }

    /// The modes that hold no lease answer the licence questions the way they always
    /// did, and nothing in the lease path can silently change that.
    #[tokio::test]
    async fn an_unleased_mode_keeps_its_own_licence_and_affirms_vacuously() {
        let net = Network::new();
        let (_sync_a, sync_b, _state, _cache) =
            wired_pair_named(&net, ("bnd-a", "bnd-b"), Consistency::Bounded);
        assert!(
            sync_b.may_serve_local(),
            "bounded serves local state always — freshness is the bound"
        );
        // No window to open, so the affirmation must return rather than poll to its
        // deadline. A budget far under AFFIRM_DEADLINE is the assertion.
        tokio::time::timeout(
            Duration::from_millis(500),
            sync_b.affirm_resynced(sync_b.resync_gen()),
        )
        .await
        .expect("an unleased affirmation is immediate");
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
