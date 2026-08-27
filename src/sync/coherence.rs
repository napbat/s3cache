use std::num::NonZeroUsize;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant, SystemTime};

use groupnet::consistency::{
    AckLedger, CAP_ACKS, CAP_LEASE, CoherenceOutcome, Frontier, LeaseConfig, LeaseView, Leases,
    PeerWrite, PeerWrites, RenewalId, WriteFeed, WriteToken, advertised_head, applied_by_selected,
};
use groupnet::core::{NodeId, Status};
use groupnet::runtime::{Group, Node};
use groupnet::transport::udp::UdpTransport;
use s3s::dto::ObjectStorageClass;
use tracing::{info, warn};

use crate::index::{KeyIndex, ObjEntry, apply_del, apply_put, standard_class};
use crate::metrics::Metrics;
use crate::sync::recovery::{Affirmation, LapseWatch, ResyncGate, remediate, watch_lapses};
use crate::sync::wire::{
    IndexEvent, IndexOp, decode_event, encode_event, etag_to_wire, from_micros, parse_token,
    to_micros,
};
use crate::tier::LocalCache;

/// Ring capacity: a peer that falls further behind than this many writes
/// gets a gap (distrust every body + origin resync, see [`remediate`]) instead
/// of per-event application.
pub(super) const FEED_CAPACITY: usize = 4096;

/// How much coherence the cluster pays for.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Consistency {
    /// Indistinguishable from a single S3 node with zero client cooperation,
    /// bought with **coherence leases** (groupnet's T3): a node may answer a
    /// local positive read only while it holds
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
    /// round and nothing above it, with a fully-alive membership view as the
    /// read-side licence. Deprecated on arrival: it
    /// exists so a deployment can pin the pre-lease mechanism for exactly one
    /// release while the lease tier rolls through the fleet. Nothing new
    /// should choose it, and the next release removes it.
    StrongAcks,
    /// The gossip bound only: reads are fresh within ~one push hop, session
    /// tokens still upgrade individual reads to strict, but writes return as
    /// soon as the origin acks and no ledger traffic flows. For large
    /// clusters where per-write cluster acks are unaffordable. A bounded node
    /// never acks, so it declares itself as such (`CAP_BOUNDED`) and strong
    /// writers skip it rather than waiting out an ack timeout per write.
    Bounded,
}

/// The capability a node advertises to declare it does *not* participate in
/// the ack tier — a positive statement of non-participation, since the absence
/// of an advertisement means something else entirely (see [`waits_on`]).
/// Namespaced, per groupnet's convention for consumer-defined capabilities.
pub(super) const CAP_BOUNDED: &str = "s3cache:bounded";

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
    pub(super) fn label(self) -> &'static str {
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
    pub(super) fn leases(self) -> bool {
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
    pub(super) fn capabilities(self) -> &'static [&'static str] {
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

/// The floor under the tuned
/// [`Config::dead_timeout_ms`](groupnet::core::Config::dead_timeout_ms) — see
/// [`WriteSync::new`], which prices what that tuning buys and what it costs.
pub(super) const DEAD_TIMEOUT_FLOOR_MS: u64 = 2_000;

/// How far past one lease duration a leased write waits before abandoning the
/// guarantee. Anything at or below `D` would turn every ordinary lapse into a
/// [`CoherenceOutcome::TimedOut`], which is the one outcome carrying no
/// guarantee at all; the second is slack for scheduling, not for hope.
pub(super) const WRITE_WAIT_SLACK: Duration = Duration::from_secs(1);

/// How often a pending affirmation retries while the lease declines it.
pub(super) const AFFIRM_POLL: Duration = Duration::from_millis(50);

/// How long an affirmation keeps trying before giving up **loudly**, and the same
/// budget the staged recovery's stage 1 gives its granters. Generous on purpose:
/// the two things that decline it — this node's own warm-up window and a frozen
/// confirmation behind an unreaped granter — both clear on their own, the second
/// only at the reap horizon. Giving up leaves the node serving via the origin,
/// which is correct and slow, never stale.
///
/// It is a **fixed** minute against a reap horizon that scales with the lease: `2 ×
/// dead_timeout` past the `Dead` verdict, itself up to one detection window past the
/// silence, and `dead_timeout` is `max(D, 2s)` (see [`WriteSync::new`]). Past roughly
/// `D = 25s` the horizon outruns this, and then a dead granter's reap always arrives
/// after the deadline: the recovery's cheap arm stops existing — every lapse takes
/// the fallback, and every affirmation gives up before the confirmation it is waiting
/// on can come back. That direction is fail-closed (a slow, correct, origin-served
/// node, and a cache thrown away for nothing), but it is silent, so it is stated here
/// rather than discovered. A fleet running a lease that long wants this scaled with
/// it; nothing in the shipped envelope — `D = 2s` by default — comes near.
pub(super) const AFFIRM_DEADLINE: Duration = Duration::from_mins(1);

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
pub(super) fn waits_on(group: &Group, node: &NodeId) -> bool {
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
pub(super) fn waits_on_unleased(group: &Group, node: &NodeId) -> bool {
    waits_on(group, node) && !group.node_has_capability(node, CAP_LEASE)
}

/// Node ids for a log line. `(unnamed)` rather than an empty string because the
/// transitional ack wait answers yes or no and names nobody — an operator reading the
/// line should see that the wait could not say, not a blank where a name should be.
pub(super) fn node_names(nodes: &[NodeId]) -> String {
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

impl WriteWait {
    /// Whether this outcome is an application acknowledgement that makes the
    /// corresponding feed prefix safe to retire.
    ///
    /// `bounded` reports `Applied` without waiting for readers, so its result is not a
    /// retirement watermark. A stalled strong wait carries no guarantee either.
    pub(crate) fn retires_feed(&self, consistency: Consistency) -> bool {
        matches!(consistency, Consistency::Strong | Consistency::StrongAcks)
            && matches!(self, Self::Applied | Self::Lapsed(_))
    }
}

/// The publishing half of the write feed, plus the barrier view.
pub struct WriteSync {
    feed: WriteFeed<IndexEvent>,
    pub(super) group: Group,
    me: NodeId,
    consistency: Consistency,
    /// Set by [`start_apply`](Self::start_apply); the freshness barrier reads it.
    view: OnceLock<groupnet::consistency::FrontierView>,
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

    /// The newest renewal of this node's serve-lease that **every** granter in its
    /// roster has confirmed — a **min** over that roster — or `None` in any mode
    /// holding no lease, and `None` before the first renewal this epoch is confirmed
    /// by everyone (a fresh boot, or a granter that has never granted).
    ///
    /// A mid-life freeze does not read as `None`: a granter that goes silent stops
    /// *advancing* the min, it does not remove itself from it, so this keeps
    /// answering `Some` with the stale value it was frozen at until membership reaps
    /// the granter. Movement, not `Some`-ness, is the signal.
    ///
    /// And movement is a weaker signal than it looks, which is why the staged lapse
    /// recovery does not rest on it alone: a min advances when the member *pinning*
    /// it leaves, so reaping a dead granter moves this while another granter's grant
    /// is still frozen. [`lease_granted_by`](Self::lease_granted_by) is the per-granter
    /// reading that answers the question this one only approximates.
    pub(crate) fn lease_confirmed(&self) -> Option<RenewalId> {
        self.leases.as_ref().and_then(Leases::confirmed)
    }

    /// The newest renewal of **this** node's serve-lease that `granter` advertises
    /// having adopted, read straight off `granter`'s published grant map. `None` in
    /// any mode holding no lease, and for a granter advertising no grant of this
    /// node's lease at all (never granted, or reaped and its entries dropped).
    ///
    /// This is the reading [`lease_confirmed`](Self::lease_confirmed) is a min over,
    /// and the one the staged recovery's stage 1 actually checks: [`RenewalId`] is
    /// epoch-major and ordered, so a value strictly above an earlier one proves *this
    /// granter* has adopted a renewal published after the earlier one — and so has
    /// resolved every wait it was holding against the entry this node lapsed out of.
    /// The min proves that of nobody in particular.
    pub(crate) fn lease_granted_by(&self, granter: &NodeId) -> Option<RenewalId> {
        self.leases
            .as_ref()
            .and_then(|leases| leases.granted_by(granter))
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
    /// reap horizon exactly as it would after a crash. That half is `watch_lapses`'
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
        let outcome: WriteWait = self.wait_cluster_applied(token, timeout).await;
        let retires_feed = outcome.retires_feed(self.consistency);
        match outcome {
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
            }
        }
        // Only a real all-readers watermark may shorten the advertised history.
        // `retire_through` retains the current head as an anchor and re-advertises the
        // compacted window; a dropped advertisement is carried by the next publish.
        if retires_feed {
            self.feed.retire_through(token).await;
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
    /// the local hot body copy; a gap distrusts every local body and triggers
    /// `resync` (an origin re-LIST) since the stale subset is unknowable.
    ///
    /// In `strong` this also spawns [`watch_lapses`], because a gap is not the only
    /// way this node loses its right to serve and the others arrive as no event at
    /// all. Both live here for the same reason: this is where everything a recovery
    /// needs — the local tiers, the applied-write frontier, and the origin re-LIST —
    /// first exists at once.
    ///
    /// Takes `self` as an [`Arc`] because the lapse watch holds a
    /// [`Weak`](std::sync::Weak) back to
    /// it: the recovery has to read this node's own lease confirmation, and a strong
    /// handle in a task this object spawned would be a cycle.
    pub(crate) fn start_apply(
        self: &Arc<Self>,
        local: LocalCache,
        state: Arc<KeyIndex>,
        resync: Arc<dyn Fn() + Send + Sync>,
        metrics: Arc<Metrics>,
    ) {
        let (frontier, view) = Frontier::new();
        let _ = self.view.set(view.clone());
        if let Some(leases) = &self.leases {
            watch_lapses(LapseWatch {
                gate: Arc::clone(&self.resync),
                sync: Arc::downgrade(self),
                local: local.clone(),
                frontier: view,
                group: self.group.clone(),
                me: self.me.clone(),
                resync: Arc::clone(&resync),
                metrics: Arc::clone(&metrics),
                lease: leases.config().duration,
            });
        }
        // Bounded mode publishes no acks (that is its point at scale); both strong
        // spellings do, and say so through their capability advertisement.
        let ledger = self
            .consistency
            .acks()
            .then(|| AckLedger::new(self.group.clone()));
        let mut peers = PeerWrites::new(self.group.clone(), self.me.clone(), decode_event);
        let gate = Arc::clone(&self.resync);
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
                        // The index must move first: a warm body promoted after this hot
                        // eviction decodes suspect and is checked against the new entry
                        // before it can be served. Awaiting moka removal before the ack
                        // closes the stale-hot window without putting disk I/O on the
                        // feed frontier.
                        local.invalidate_hot(&(event.bucket, event.key)).await;
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
                        warn!("write-feed gap from `{peer}`: distrusting bodies, resyncing index");
                        // Stand the licence down, distrust, re-LIST — see
                        // `remediate`, which is also the fallback of a lapse with no
                        // gap behind it. The node stays out of service until the
                        // resync affirms catch-up for *this* generation: a reader
                        // that missed invalidations missed exactly the ones whose
                        // writers proceeded because it had. Acking the gap is
                        // truthful the moment this returns even though the bodies are
                        // still here: `strong` serves none of them while the licence
                        // is down, and the other modes refuse a suspect body in a
                        // bucket the re-LIST has put back to passthrough.
                        remediate(&gate, &local, &resync);
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
