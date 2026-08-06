//! Staged recovery after a coherence-lease lapse.
//!
//! # Why a lapse can end without throwing the cache away
//!
//! A gap is proof that specific events were missed, and the only honest answer
//! is to stop trusting what they might have invalidated. A **lapse** is not
//! that proof: it says this node's licence expired, not that anything actually
//! changed underneath it. The recovery [`watch_lapses`] runs turns that
//! difference into a real argument rather than an optimism.
//!
//! A write `W` by peer `P` completed only after every lease-holder had either
//! applied it or lapsed in `P`'s engine. If we applied it, our
//! [`Frontier`](groupnet::consistency::Frontier) covers it and the apply loop
//! already evicted the key. If it proceeded via *our* lapse, then `W` was in
//! `P`'s ring before `P`'s wait resolved, so `P`'s advertised head is
//! `≥ token(W)` from completion onward. And `P` adopts our post-lapse renewal
//! strictly *after* resolving every wait against our old entry — so `P`'s feed
//! frame containing every lapse-era `W` was authored before the grant our new
//! confirmation rests on. The settle window (`2 ×`
//! [`Config::anti_entropy_interval_ms`](groupnet::core::Config::anti_entropy_interval_ms),
//! the same bound the lease shell's own warm-up uses for entry propagation) is
//! the fabric's bound for that frame reaching us.
//!
//! Hence: a head sampled after the confirmation moves *plus* the settle window
//! dominates every lapse-era completed write, and
//! [`FrontierView::reached`](groupnet::consistency::FrontierView::reached) on
//! that head means the apply loop has evicted exactly the keys that changed.
//! Every other body is provably untouched and keeps its proof — no flush, no
//! re-LIST, no distrust.
//!
//! The argument has two hinges, and both are checked rather than assumed.
//!
//! **The vanished peer.** A peer that was alive when the lapse landed and is
//! **gone from membership** before this recovery affirms took its feed frame
//! with it (a reap drops the entries), so there is no head left to sample and
//! no way to tell whether it wrote. Those force the fallback — the gap's
//! remediation, distrust and re-LIST — unless they were provably non-writers
//! for the whole window, which here means non-`Alive` since before the lapse.
//! The check runs **twice**, before the barrier and again immediately before
//! the affirmation, because the barrier waits and a peer can be reaped inside
//! that wait.
//!
//! **Every granter re-granting.** The frame-ordering step above is licensed by
//! "`P` adopted our post-lapse renewal", so that has to be established for each
//! `P` separately — and
//! [`Leases::confirmed`](groupnet::consistency::Leases::confirmed) cannot
//! establish it. It is a **min** over the roster, and a min advances when the
//! member pinning it *leaves*: reap a dead granter and the confirmation jumps
//! up to the next granter's sequence while that granter's grant is frozen
//! exactly where it was. With two granters those are the same event; with
//! three they are not, and the frozen one is the worst case there is — a
//! partitioned peer, whose advertised head is frozen too, so the barrier
//! passes on it vacuously while its via-lapse writes are precisely the ones we
//! cannot see. So stage 1 reads each granter's grant on its own
//! ([`Leases::granted_by`](groupnet::consistency::Leases::granted_by)) and
//! requires every one of them to advance, or that granter to have left the
//! roster — where a reap hands it straight to the first hinge.
//!
//! Every other unprovable case (a grant that never moves, a head never being
//! reached) lands in the same place, because the fallback is always available
//! and always correct. What none of this bounds is the residual groupnet's own
//! honesty box names, and which this crate inherits rather than fixes: a peer
//! this node **reaps** while it is in fact still writing. [`LapseWatch::exempt`]
//! is where that residual is taken deliberately — a peer non-`Alive` since
//! before the lapse is *assumed* a non-writer for the lapse era, because
//! without the assumption every scale-in would force the expensive arm forever.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use groupnet::consistency::{
    CAP_LEASE, FrontierView, LeaseView, RenewalId, WriteToken, advertised_head,
};
use groupnet::core::{NodeId, Status};
use groupnet::runtime::Group;
use tracing::{info, warn};

use crate::metrics::Metrics;
use crate::sync::coherence::{
    AFFIRM_DEADLINE, AFFIRM_POLL, WRITE_WAIT_SLACK, WriteSync, node_names,
};
use crate::tier::LocalCache;

/// How long this node's view of the cluster must have held still before it trusts its
/// own index enough to answer an authoritative 404: one full failure-detection cycle, as
/// the group itself is configured to run it. Inside this window a peer may be writing
/// keys this node will never hear about while [`WriteSync::cluster_healthy`] still says
/// everything is fine.
///
/// This is the **pre-lease** licence, and it survives only for
/// [`Consistency::StrongAcks`](crate::sync::coherence::Consistency::StrongAcks) and
/// [`Consistency::Bounded`](crate::sync::coherence::Consistency::Bounded). A heuristic is what it
/// is — "the picture has held still, so probably nobody is writing behind my back" —
/// and replacing it with a mechanism a reader holds and a writer can wait out is the
/// whole of the lease migration. In
/// [`Consistency::Strong`](crate::sync::coherence::Consistency::Strong) nothing reads it.
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
pub(super) fn settle_window(group: &Group) -> Duration {
    Duration::from_millis(
        group
            .config()
            .detection_window_ms(group.members().len().max(2)),
    )
}

/// How long this node's picture of the cluster has been unchanged. A membership or
/// status change, a write held past its ack window, or a write-feed gap each mean the
/// picture may be incomplete, and each restarts the clock.
pub(super) struct Stability {
    /// The last observed `(member, status)` view and when it was first seen.
    seen: Mutex<(Vec<(NodeId, Status)>, Instant)>,
}

impl Stability {
    pub(super) fn new() -> Self {
        Self {
            seen: Mutex::new((Vec::new(), Instant::now())),
        }
    }

    /// Note that something happened this node's index may not have caught up with.
    pub(super) fn disturb(&self) {
        self.seen.lock().unwrap().1 = Instant::now();
    }

    /// Whether the view has held still for the whole detection window (see
    /// [`settle_window`], which this re-reads on every call — the window a two-member
    /// cluster needs is not the window it needs after it scales out).
    pub(super) fn settled(&self, group: &Group) -> bool {
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
pub(super) enum Affirmation {
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
    /// lease down. Every lapse counted at or below it is *covered*: the distrust
    /// and the origin re-LIST that follow the stand-down happen after it, so
    /// nothing that lapse could have made stale is served again without proving
    /// itself against that re-LISTed index.
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
pub(super) struct ResyncGate {
    /// `None` in every mode but
    /// [`Consistency::Strong`](crate::sync::coherence::Consistency::Strong); the generation still
    /// turns, so a consumer's bookkeeping needs no mode branch of its own.
    view: Option<LeaseView>,
    state: Mutex<GateState>,
}

impl ResyncGate {
    pub(super) fn new(view: Option<LeaseView>) -> Self {
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
    pub(super) fn require_resync(&self) {
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
    /// mode but [`Consistency::Strong`](crate::sync::coherence::Consistency::Strong): without a
    /// lease there is nothing to
    /// lapse.
    pub(super) fn lapse_uncovered(&self) -> bool {
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
    pub(super) fn generation(&self) -> u64 {
        self.state.lock().unwrap().generation
    }

    /// Affirm catch-up on behalf of `generation` — see [`Affirmation`].
    pub(super) fn affirm(&self, generation: u64) -> Affirmation {
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
/// The licence goes **first**: this node must not answer one more read from
/// local state under a window it no longer deserves. Then every cached body is
/// *distrusted* rather than dropped — the trust generation moves on, so each
/// copy has to prove itself against the LIST index before it is served again —
/// and then `resync`, which re-LISTs that index from the origin and owns the
/// affirmation that puts this node back in service under the generation this
/// stand-down just started (see `CachingProxy::gap_resync_handle`).
///
/// **Distrust, not flush**, and nothing weaker than the flush it replaces. The
/// three modes reach the same closed door by their own routes:
///
/// * `strong` never consults a body at all while this runs. The stand-down is a
///   latch on the read-side licence, so every read is origin-served from the
///   first line of this function until the affirmation lands — the re-LIST has
///   the whole window to itself, exactly as it did behind a flush.
/// * `strong-acks` and `bounded` hold no such latch, and are closed by the
///   *unsynced-bucket* rule in `CachingProxy::validated_get`: `resync` puts
///   every bucket back to passthrough before its first LIST, and a suspect body
///   in a bucket that cannot arbitrate is dropped rather than served. That
///   reproduces the flush's window one key at a time, and only for the keys
///   that are actually read.
///
/// What survives is the difference: once the index is back, a body the index
/// confirms costs one lookup instead of an origin GET, and a body the index
/// contradicts is dropped then — which is what a flush did to *every* copy,
/// correct ones included. [`LocalCache::flush`] stays as the escape hatch for a
/// stale set that cannot be revalidated at all; no path here reaches for it.
///
/// Both triggers run exactly this: a write-feed
/// [`PeerWrite::Gap`](groupnet::consistency::PeerWrite::Gap), and a lease
/// lapse with no gap behind it whose staged recovery could not prove the cache
/// ([`watch_lapses`]). They are the same proof — "writes happened that this node
/// cannot account for" — arriving by different routes, and a difference in
/// remediation between them would be a difference in what a reader may serve
/// afterwards.
pub(super) fn remediate(
    gate: &ResyncGate,
    local: &LocalCache,
    resync: &Arc<dyn Fn() + Send + Sync>,
) {
    gate.require_resync();
    local.distrust_all();
    resync();
}

/// The floor under [`lapse_poll`]: a lease tuned absurdly short must not turn the
/// watch into a spin.
const LAPSE_POLL_FLOOR: Duration = Duration::from_millis(25);

/// How often [`watch_lapses`] looks: a quarter of the lease duration `D`, so a
/// lapse is picked up well inside the lease period it happened in. The watch is
/// two counter reads and no allocation — cadence buys latency here, not load.
pub(super) fn lapse_poll(duration: Duration) -> Duration {
    (duration / 4).max(LAPSE_POLL_FLOOR)
}

/// How many times the barrier will re-run against a moved head before it stops
/// chasing and proceeds. Three is enough for a head that advanced *because* the
/// barrier's own wait let the apply loop catch up; past that the writer is simply
/// writing, and post-confirmation writes need no chasing — the renewal ticker and
/// the apply loop never stopped, so those writes wait on this node's acks exactly
/// as they would on any healthy reader.
const BARRIER_ROUNDS: u32 = 3;

/// What one stage of the staged recovery decided.
enum Step {
    /// The stage held. Carry on.
    Go,
    /// A newer generation owns the recovery now — a gap landed, and its own
    /// remediation covers everything this one was going to prove. Yield without
    /// touching the licence, the cache, or the counters.
    Yield,
    /// The proof is unavailable. Take the fallback: the gap's remediation.
    Fall,
}

/// Everything the lapse watcher needs to run the staged recovery, in one place so
/// the stages read as stages rather than as an argument list.
pub(super) struct LapseWatch {
    pub(super) gate: Arc<ResyncGate>,
    /// **Weak on purpose.** The watch outlives every request and is owned by a task
    /// the [`WriteSync`] itself spawned; a strong handle would be a cycle that keeps
    /// the feed, the lease set and the gossip node alive for the process lifetime.
    /// A failed upgrade is this node shutting down, and every stage treats it as
    /// [`Step::Yield`].
    pub(super) sync: Weak<WriteSync>,
    pub(super) local: LocalCache,
    /// The apply loop's own frontier view — the barrier's whole instrument.
    pub(super) frontier: FrontierView,
    pub(super) group: Group,
    pub(super) me: NodeId,
    pub(super) resync: Arc<dyn Fn() + Send + Sync>,
    pub(super) metrics: Arc<Metrics>,
    /// The lease duration `D`: the watch cadence and the barrier's deadline both
    /// come off it.
    pub(super) lease: Duration,
}

impl LapseWatch {
    /// How often the lapse itself is looked for, and the staleness bound on the
    /// exemption snapshot: a peer non-`Alive` for at least this long was already
    /// non-`Alive` when the previous look happened.
    fn poll(&self) -> Duration {
        lapse_poll(self.lease)
    }

    /// The fabric's bound for one peer's entry to reach this node once gossip is
    /// flowing again — `2 ×` the anti-entropy interval, read off the group's
    /// *effective* config, and the same number the lease shell's own warm-up guard
    /// budgets for entry propagation. See the module docs for why the frame this
    /// waits for was authored before the grant that ends stage 1.
    fn settle(&self) -> Duration {
        Duration::from_millis(
            self.group
                .config()
                .anti_entropy_interval_ms
                .saturating_mul(2),
        )
    }

    /// Whether this recovery still owns the gate. Checked between every stage and
    /// inside the two that wait: a gap arriving mid-recovery starts a generation of
    /// its own, and that generation's remediation supersedes everything here.
    fn owns(&self, generation: u64) -> bool {
        self.gate.generation() == generation
    }

    /// The peers this node currently sees as `Alive` (never itself).
    fn alive(&self) -> HashSet<NodeId> {
        self.group
            .statuses()
            .into_iter()
            .filter(|(peer, status)| *peer != self.me && *status == Status::Alive)
            .map(|(peer, _)| peer)
            .collect()
    }

    /// The peers a vanishing cannot indict: non-`Alive`, and held that way for at
    /// least one watch interval — long enough that they were *already* non-`Alive`
    /// when this watch last looked, which is before the lapse could have been picked
    /// up. A peer this node has not considered live since before the lapse is taken
    /// as a non-writer for the lapse era, and its later reap proves nothing about
    /// this cache.
    ///
    /// This is the recovery's one **policy** decision rather than a deduction, and
    /// it is the same residual groupnet's own honesty box names: a peer this node
    /// reaps while it is in fact still writing is outside what any of this bounds.
    /// Without the exemption every scale-in would force the expensive arm forever,
    /// because a departed peer always eventually vanishes.
    ///
    /// The boundary is drawn at one watch interval and **everything on the near side
    /// of it counts** — a peer whose down-verdict is younger than that was live too
    /// recently to be proved a non-writer, so it stays in the set stage 3 tests
    /// rather than falling into a gap between "alive" and "provably gone". That is
    /// why `seen` is seeded from every member rather than from the `Alive` ones:
    /// membership status at the instant of the snapshot is not the question, and
    /// erring towards an extra fallback is the only direction that is safe.
    fn exempt(&self) -> HashSet<NodeId> {
        let floor = self.poll();
        self.group
            .statuses_held()
            .into_iter()
            .filter(|(peer, status, held)| {
                *peer != self.me && *status != Status::Alive && *held >= floor
            })
            .map(|(peer, _, _)| peer)
            .collect()
    }

    /// Every member this node still knows of, at any status. A reaped peer is
    /// absent from this and *only* from this, which is exactly what stage 3 asks.
    fn present(&self) -> HashSet<NodeId> {
        self.group
            .statuses()
            .into_iter()
            .map(|(peer, _)| peer)
            .collect()
    }

    /// The peers whose grants this node's confirmation is a min over: every member it
    /// still knows of — `Suspect` and `Dead`-but-unreaped included, because either may
    /// still be writing — that advertises [`CAP_LEASE`].
    ///
    /// Derived the same way the lease shell's own ingest derives it, off
    /// [`Group::statuses`] rather than the not-`Dead` set, so that a granter leaving
    /// this set means exactly one thing: it was **reaped**, or it stopped advertising
    /// that it grants at all. Stage 1 needs that distinction, because "left the roster"
    /// is the one way past its per-granter check.
    fn roster(&self) -> HashSet<NodeId> {
        self.group
            .statuses()
            .into_iter()
            .map(|(peer, _)| peer)
            .filter(|peer| *peer != self.me && self.group.node_has_capability(peer, CAP_LEASE))
            .collect()
    }

    /// What `granter` currently advertises having adopted of this node's serve-lease —
    /// the per-granter reading stage 1 checks. `None` while this node is on its way out.
    fn granted_by(&self, granter: &NodeId) -> Option<RenewalId> {
        self.sync.upgrade()?.lease_granted_by(granter)
    }

    /// **Stage 3's question**, asked before the barrier and again after it: the peers
    /// this recovery counted that are gone from membership now, named for the log.
    /// `None` when everything it has to answer for is still there.
    ///
    /// A peer that was live when the lapse landed and has since been reaped took its
    /// feed frame with it, so nothing left can say whether it wrote — see the module
    /// docs. Asked twice because stages 4 and 5 *wait*, and a reap horizon can fall
    /// inside that wait: the pre-barrier answer says nothing about the post-barrier one.
    fn unaccounted(&self, seen: &HashSet<NodeId>, exempt: &HashSet<NodeId>) -> Option<String> {
        let present = self.present();
        let vanished: Vec<NodeId> = seen
            .iter()
            .filter(|peer| !present.contains(*peer) && !exempt.contains(*peer))
            .cloned()
            .collect();
        (!vanished.is_empty()).then(|| node_names(&vanished))
    }

    /// One advertised head per peer this node still knows of — `Alive`, `Suspect`
    /// and `Dead`-but-unreaped alike, which is why this sweeps
    /// [`Group::statuses`] rather than `members()`: a peer whose tombstone is still
    /// standing has an entry, may have written before it went quiet, and is
    /// precisely the one worth barriering on. A peer with no decodable feed has
    /// published nothing and is skipped.
    fn heads(&self) -> HashMap<NodeId, WriteToken> {
        self.group
            .statuses()
            .into_iter()
            .filter(|(peer, _)| *peer != self.me)
            .filter_map(|(peer, _)| advertised_head(&self.group, &peer).map(|head| (peer, head)))
            .collect()
    }

    /// The newest renewal every granter has confirmed, or `None` while the
    /// confirmation is frozen (or this node is on its way out).
    fn confirmed(&self) -> Option<RenewalId> {
        self.sync.upgrade()?.lease_confirmed()
    }

    /// **Stage 1.** Wait until every granter this recovery has to answer for has
    /// adopted a renewal published after the one it had adopted when the recovery
    /// started — the event that proves it has resolved every wait it was holding
    /// against the entry this node lapsed out of.
    ///
    /// **Per granter, not on [`WriteSync::lease_confirmed`] alone.** That is a min
    /// over the roster, and a min advances when the member pinning it *leaves*: reap
    /// a dead granter and it jumps to the next granter's sequence while a partitioned
    /// granter's grant sits frozen exactly where it was, its via-lapse writes still
    /// unaccounted for. The confirmation moving is kept as a coarse gate — this node
    /// has to hold a live window again for any of this to matter — but the proof is
    /// `grants`.
    ///
    /// A granter may **leave the roster** instead of advancing, and that is a
    /// deferral rather than a proof: reaped, it is in `seen` and stage 3 refuses the
    /// cheap arm on its behalf; still a member but no longer advertising
    /// [`CAP_LEASE`], it is still swept by [`heads`](Self::heads) and the barrier
    /// covers it like any other member.
    ///
    /// `seen` and `grants` both grow while this waits. A peer that comes up
    /// mid-recovery is one whose writes the barrier must cover and whose grant this
    /// must therefore watch too — entered at what it advertises *now*, so it has to
    /// re-grant after being noticed. A peer that goes down is removed from neither,
    /// because going down is what stage 3 is looking for.
    async fn await_confirmation(
        &self,
        generation: u64,
        seen: &mut HashSet<NodeId>,
        grants: &mut HashMap<NodeId, Option<RenewalId>>,
        exempt: &HashSet<NodeId>,
    ) -> Step {
        let before = self.confirmed();
        let deadline = Instant::now() + AFFIRM_DEADLINE;
        loop {
            if !self.owns(generation) {
                return Step::Yield;
            }
            let roster = self.roster();
            seen.extend(
                self.alive()
                    .into_iter()
                    .filter(|peer| !exempt.contains(peer)),
            );
            for granter in roster.iter().filter(|peer| !exempt.contains(*peer)) {
                if !grants.contains_key(granter) {
                    grants.insert(granter.clone(), self.granted_by(granter));
                }
            }
            let pending: Vec<NodeId> = grants
                .iter()
                .filter(|(granter, at_start)| {
                    roster.contains(*granter)
                        && !self
                            .granted_by(granter)
                            .is_some_and(|now| at_start.is_none_or(|at_start| now > at_start))
                })
                .map(|(granter, _)| granter.clone())
                .collect();
            if pending.is_empty()
                && self
                    .confirmed()
                    .is_some_and(|now| before.is_none_or(|before| now > before))
            {
                return Step::Go;
            }
            if Instant::now() >= deadline {
                let why = if pending.is_empty() {
                    "the confirmation never moved".to_owned()
                } else {
                    format!("{} never re-granted", node_names(&pending))
                };
                warn!(
                    "coherence lease never re-confirmed after a lapse ({why}); falling back \
                     to the full remediation (a granter may neither publish nor get reaped)"
                );
                return Step::Fall;
            }
            tokio::time::sleep(AFFIRM_POLL).await;
        }
    }

    /// **Stage 4.** Wait until every sampled head has been applied locally, which is
    /// the whole proof: past it, the apply loop has evicted exactly the keys the
    /// lapse era changed and every body still held is provably untouched.
    ///
    /// The deadline is one lease duration plus [`WRITE_WAIT_SLACK`] — the same
    /// budget a leased *write* gets, because it is bounded by the same thing: the
    /// longest a peer can be behind and still be somebody this node has to wait
    /// for. Running out means the proof did not arrive, not that it failed.
    async fn barrier(&self, heads: &HashMap<NodeId, WriteToken>, generation: u64) -> Step {
        let deadline = Instant::now() + self.lease + WRITE_WAIT_SLACK;
        for (peer, head) in heads {
            if !self.owns(generation) {
                return Step::Yield;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            let reached = tokio::time::timeout(remaining, self.frontier.reached(peer, *head)).await;
            // `Err` is out of time; `Ok(false)` is the apply loop being gone, so
            // nothing is being applied at all. Neither yields a proof.
            if !matches!(reached, Ok(true)) {
                warn!(
                    "lapse recovery could not reach `{peer}`'s advertised feed head in \
                     time; falling back to the full remediation"
                );
                return Step::Fall;
            }
        }
        if self.owns(generation) {
            Step::Go
        } else {
            Step::Yield
        }
    }

    /// The expensive arm: exactly the gap's remediation, on the generation stage 0
    /// already started — [`remediate`] starts one of its own, which is right. This
    /// recovery is abandoning its proof, so the affirmation it would have owned must
    /// never land, and `resync` reads the *current* generation for itself (see
    /// `CachingProxy::gap_resync_handle`). One resync, one affirmation, whichever
    /// generation is live when it runs.
    fn fall_back(&self) {
        self.metrics.lapse_barrier_fallback();
        self.metrics.lease_lapse_resync();
        remediate(&self.gate, &self.local, &self.resync);
    }

    /// The staged recovery, stage by stage. See the module docs for why the
    /// barrier is a proof rather than a hope, and [`watch_lapses`] for what it is
    /// recovering from.
    async fn recover(&self) {
        // Stage 0. The licence goes down first and unconditionally: whatever this
        // recovery concludes, it must not conclude it while still serving. Then the
        // accounting: every member this recovery has to answer for (`seen`), the ones
        // it does not (`exempt` — see there for where that line is drawn), and what
        // each granter has adopted of this node's lease so far, which is the baseline
        // stage 1 needs each of them to move off.
        self.gate.require_resync();
        let generation = self.gate.generation();
        let started = Instant::now();
        let exempt = self.exempt();
        let mut seen: HashSet<NodeId> = self
            .present()
            .into_iter()
            .filter(|peer| *peer != self.me && !exempt.contains(peer))
            .collect();
        let mut grants: HashMap<NodeId, Option<RenewalId>> = self
            .roster()
            .into_iter()
            .filter(|granter| !exempt.contains(granter))
            .map(|granter| {
                let at_start = self.granted_by(&granter);
                (granter, at_start)
            })
            .collect();
        warn!(
            "coherence lease lapsed with no write-feed gap (a peer stopped granting): \
             holding reads at the origin while the retention barrier runs"
        );

        // Stage 1: every granter re-grants. Stage 2: the fabric's own bound for the
        // feed frame behind those grants to arrive.
        match self
            .await_confirmation(generation, &mut seen, &mut grants, &exempt)
            .await
        {
            Step::Go => {}
            Step::Yield => return,
            Step::Fall => return self.fall_back(),
        }
        tokio::time::sleep(self.settle()).await;
        if !self.owns(generation) {
            return;
        }

        // Stage 3, the first hinge: a peer that was live when the lapse landed and is
        // now gone from membership took its feed frame with it, so nothing can say
        // whether it wrote.
        if let Some(who) = self.unaccounted(&seen, &exempt) {
            warn!(
                "lapse recovery cannot account for {who} (live at the lapse, gone before it \
                 could affirm); falling back to the full remediation"
            );
            return self.fall_back();
        }

        // Stages 4 and 5: barrier on every advertised head, and re-sample. A head
        // that moved while the barrier waited gets barriered again, up to
        // BARRIER_ROUNDS — then this proceeds, because a head still moving is a
        // writer writing now, and those writes wait on this node's acks normally.
        let mut heads = self.heads();
        for _ in 0..BARRIER_ROUNDS {
            match self.barrier(&heads, generation).await {
                Step::Go => {}
                Step::Yield => return,
                Step::Fall => return self.fall_back(),
            }
            let resampled = self.heads();
            if !advanced(&heads, &resampled) {
                break;
            }
            heads = resampled;
        }

        // Stage 3 again, on the same accounting. Stages 4 and 5 waited — up to a lease
        // duration per round — and a reap horizon can fall inside that wait, so the
        // answer from before the barrier says nothing about the answer now. This is the
        // last look before the licence goes back on.
        if let Some(who) = self.unaccounted(&seen, &exempt) {
            warn!(
                "lapse recovery cannot account for {who} (live at the lapse, gone during the \
                 barrier); falling back to the full remediation"
            );
            return self.fall_back();
        }

        // Stage 6. No flush, no re-LIST, no resync: every body still held was
        // proved untouched by the barrier, and the ones that were not are already
        // gone — the apply loop evicted them on the way past.
        self.metrics.lapse_barrier_retain();
        let elapsed = started.elapsed();
        info!(
            "lapse recovery kept the body cache: every lapse-era write applied, \
             licence re-affirmed after {elapsed:?}"
        );
        if let Some(sync) = self.sync.upgrade() {
            sync.affirm_resynced(generation).await;
        }
    }
}

/// Whether any peer's head moved between two samples — including a peer that had
/// no decodable feed the first time and has one now.
fn advanced(before: &HashMap<NodeId, WriteToken>, after: &HashMap<NodeId, WriteToken>) -> bool {
    after
        .iter()
        .any(|(peer, head)| before.get(peer).is_none_or(|seen| seen < head))
}

/// Watch this node's own serve-lease and recover from a lapse that arrives with
/// **no** write-feed gap behind it. `strong` only — nothing else holds a lease to
/// lapse.
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
/// So the lapse gets a recovery, and it is a **staged** one rather than the gap's:
///
/// 0. Stand the licence down and start a generation, exactly as a gap does. Snapshot
///    which peers were live, which were already down long enough not to count, and
///    what each granter has so far adopted of this node's lease.
/// 1. Wait for **every** granter to adopt a renewal published after that snapshot, so
///    every wait held against the old entry is resolved. Per granter, not on the
///    roster-wide min — see the module docs for what a min cannot tell apart.
/// 2. Settle: `2 × anti_entropy_interval`, the fabric's bound for the feed frames
///    behind those grants to arrive.
/// 3. Refuse the cheap arm if any peer that was live at the lapse has vanished from
///    membership since — its frame went with it.
/// 4. Barrier on every peer's advertised head.
/// 5. Re-sample; a head that moved gets barriered again, a few times over.
/// 6. Re-run stage 3 — the barrier waits, and a peer can be reaped inside that wait —
///    then affirm. No flush, no re-LIST, no resync; see the module docs for why that
///    is a proof and not an optimism.
///
/// Any stage that cannot get its proof falls back to [`remediate`], which is the
/// gap's answer and always available. The fallback is the *only* thing the two
/// triggers still share, and that is the point: a gap knows something was missed,
/// a lapse only knows it stopped being allowed to serve.
///
/// **The interlock**: a gap racing the same lapse must not buy a second remediation.
/// [`ResyncGate::require_resync`] records the lapse count it covers while it stands
/// the lease down, so whichever trigger arrives first owns every lapse observed
/// before it and this watch yields (see [`ResyncGate::lapse_uncovered`]). A gap that
/// lands *during* the staged recovery is handled by the generation checks threaded
/// through every stage: the gap's own remediation is strictly stronger than
/// anything this was about to conclude, so the recovery abandons its proof rather
/// than racing it. The reverse — this watch recovering first and a gap then
/// arriving — is not deduplicated and must not be: a gap is independent proof of
/// missed writes, and it is entitled to its own resync.
///
/// One lapse is one recovery, and that is also the residual: a stand-down latches,
/// so no *further* lapse edge can fire until an affirmation lifts it. If that
/// affirmation gives up at [`AFFIRM_DEADLINE`] — a granter that neither publishes
/// nor gets reaped, the fail-slow shape the lease tier names — this node stays
/// origin-serving until the next gap, exactly as it would after a gap whose
/// affirmation gave up. The remedy for that one is operational, and the warning
/// [`WriteSync::affirm_resynced`] logs is where it is stated.
pub(super) fn watch_lapses(watch: LapseWatch) {
    let poll = watch.poll();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(poll).await;
            if !watch.gate.lapse_uncovered() {
                continue;
            }
            watch.recover().await;
        }
    });
}
