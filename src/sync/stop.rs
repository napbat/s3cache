//! A planned stop, and how the survivors see it.
//!
//! A crash leaves peers a gap: the dead life may have committed origin writes it
//! never published, and only the origin knows which. A planned stop can prove
//! that tail empty. Once the HTTP drain has finished and every PUT tail has
//! published (see [`crate::cache::proxy::CachingProxy::seal_writes`]), this life
//! can publish nothing more, so [`WriteSync::seal`](crate::sync::coherence::WriteSync::seal)
//! says so on the feed and waits
//! for the peers to acknowledge it. A peer that delivered the seal has applied
//! that whole life: its recovery no longer needs the stopped node's head, so the
//! node may leave the roster or come back empty without costing it a scan. It
//! then crosses into the restarted node's next life with a renewal instead of a
//! gap: it keeps its index and bodies, and its donor journal carries the writer
//! across the restart.
//!
//! `WriteSync::announce` is the other end: a starting node advertises its new
//! life at once, so an unsealed restart gaps on every peer immediately rather
//! than at the new life's first write.

use std::time::Duration;

use groupnet::consistency::volatile_recovery::{Mark, Renewal};
use groupnet::consistency::{WriteToken, applied_by_selected};
use groupnet::core::NodeId;
use tracing::{info, warn};

use crate::index::KeyIndex;
use crate::metrics::Metrics;
use crate::sync::coherence::{WriteSync, native_cut, waits_on};

/// How a [`WriteSync::seal`] ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SealOutcome {
    /// Every peer a write waits on acknowledged the seal: each will cross this
    /// node's restart without a gap.
    Observed,
    /// The seal was advertised, but not every peer acknowledged it in time (or,
    /// in `bounded`, no peer acknowledges anything). A peer that saw it still
    /// renews; one that did not takes the ordinary restart gap.
    Unconfirmed,
}

/// What this node delivered of one peer's planned stops since its last gap from
/// that peer: the volatile recovery's evidence that the peer's head leaving a
/// life lost nothing (`groupnet`'s `Peer::renewal` and `Peer::sealed`).
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Crossing {
    /// The peer's latest restart this node crossed through a delivered seal.
    pub(crate) renewal: Option<Renewal>,
    /// The delivered seal of the peer's life this node is still in: set once that
    /// life's every write is applied, cleared when its next life is crossed into.
    pub(crate) sealed: Option<Mark>,
}

impl WriteSync {
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

    /// Advertise this life's feed epoch before any write, so every peer learns of the
    /// restart now: a sealed previous life renews, an unsealed one gaps at once.
    pub(crate) async fn announce(&self) {
        self.feed.republish().await;
    }

    /// Seal this life's write feed and wait up to `wait` for the peers a write waits
    /// on to acknowledge the seal.
    ///
    /// Call it only once nothing can publish again: the HTTP drain has completed and
    /// every PUT tail has finished. The seal promises the peers that this life wrote
    /// nothing after it, which is what lets them skip the restart remediation; a
    /// write published afterwards panics in the feed rather than break that promise.
    pub async fn seal(&self, wait: Duration) -> SealOutcome {
        let token = self.feed.seal().await;
        let observed = self.consistency.acks()
            && applied_by_selected(
                &self.group,
                &self.me,
                token,
                |node| waits_on(&self.group, node),
                wait,
            )
            .await;
        if observed {
            info!(
                epoch = token.epoch,
                seq = token.seq,
                "sealed the write feed; every peer observed it"
            );
            SealOutcome::Observed
        } else {
            warn!(
                epoch = token.epoch,
                seq = token.seq,
                "sealed the write feed, but not every peer confirmed it; \
                 a peer that missed it rescans after the restart"
            );
            SealOutcome::Unconfirmed
        }
    }

    /// A peer sealed the life this node is in, and every write of it is applied:
    /// kept as recovery evidence until this node crosses into the peer's next life.
    pub(super) fn observe_seal(&self, peer: &NodeId, sealed: WriteToken) {
        self.crossings
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(peer.clone())
            .or_default()
            .sealed = Some(Mark {
            epoch: sealed.epoch,
            sequence: sealed.seq,
        });
    }

    /// A peer restarted after a seal this node delivered. Nothing was missed: the index
    /// renews the writer (and an open capture journals the crossing), and the renewal is
    /// kept as recovery evidence. Returns the new life's start, `(epoch, 0)`.
    pub(super) fn observe_renewal(
        &self,
        state: &KeyIndex,
        peer: &NodeId,
        sealed: WriteToken,
        epoch: u64,
        metrics: &Metrics,
    ) -> WriteToken {
        info!("`{peer}` restarted after a sealed stop; continuing into its epoch {epoch}");
        state.renew_native_writer(&native_cut(peer, sealed), epoch);
        self.crossings
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(
                peer.clone(),
                Crossing {
                    renewal: Some(Renewal {
                        sealed: Mark {
                            epoch: sealed.epoch,
                            sequence: sealed.seq,
                        },
                        epoch,
                    }),
                    sealed: None,
                },
            );
        metrics.feed_renewal();
        WriteToken { epoch, seq: 0 }
    }

    /// A gap from `peer` supersedes any seal or renewal it made before.
    pub(super) fn forget_crossing(&self, peer: &NodeId) {
        self.crossings
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(peer);
    }

    /// What this node delivered of `peer`'s planned stops, if no gap has followed.
    pub(crate) fn crossing_of(&self, peer: &NodeId) -> Crossing {
        self.crossings
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(peer)
            .copied()
            .unwrap_or_default()
    }
}
