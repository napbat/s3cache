//! The gossip half of the pod readiness probe: whether a peer this node can see,
//! or a seed it has not met yet, may hold an index this node lacks.

use std::sync::{Arc, Mutex, PoisonError};

use groupnet::core::{NodeId, Status};
use groupnet::runtime::Group;
use tokio::sync::broadcast::error::RecvError;

use crate::sync::advertise::CAP_INDEXED;
use crate::sync::coherence::WriteSync;

/// Configured seeds that have not appeared in this node's roster yet. Until one
/// does, an index holder can be invisible here: the roster starts empty, and the
/// seed resolver registers a seed only once its address resolves.
#[derive(Default)]
pub(crate) struct UnmetSeeds(Mutex<Vec<NodeId>>);

impl UnmetSeeds {
    /// Stop waiting for `seed`: its address did not resolve within the resolver's
    /// first round, so no pod holds it, let alone an index.
    pub(crate) fn give_up(&self, seed: &NodeId) {
        self.lock().retain(|unmet| unmet != seed);
    }

    /// Forget the seeds now in `group`'s roster; whether any are left.
    fn settle(&self, group: &Group) -> bool {
        let mut unmet = self.lock();
        unmet.retain(|seed| group.member_status(seed).is_none());
        !unmet.is_empty()
    }

    fn any(&self) -> bool {
        !self.lock().is_empty()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<NodeId>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl WriteSync {
    /// Count `seeds` as possible index holders until each appears in the roster,
    /// and watch membership for them. Returns the set for the seed resolver, which
    /// gives up on a seed whose address does not resolve.
    ///
    /// # Panics
    /// Outside a Tokio runtime: the watch is a task.
    pub(crate) fn expect_seeds(&self, seeds: impl IntoIterator<Item = NodeId>) -> Arc<UnmetSeeds> {
        self.unmet_seeds.lock().extend(seeds);
        let (group, unmet) = (self.group.clone(), Arc::clone(&self.unmet_seeds));
        tokio::spawn(async move {
            // Subscribed before the first read: a seed that arrives in between
            // still wakes the loop. Events are emitted after the roster they
            // describe is published.
            let mut events = group.events();
            while unmet.settle(&group) {
                if let Err(RecvError::Closed) = events.recv().await {
                    return;
                }
            }
        });
        Arc::clone(&self.unmet_seeds)
    }

    /// Whether a peer that is not known dead may hold an index this node lacks: a
    /// seed not met yet, or a roster member that is `Alive` or `Suspect` and
    /// advertises [`CAP_INDEXED`] or has advertised nothing yet.
    ///
    /// * `Suspect` counts. A suspicion is often a peer that is merely slow, and reading
    ///   it as gone is the direction a readiness probe must not err in: the kubelet marks
    ///   a pod ready on one success, so a momentary misreading could let a rollout stop
    ///   the last index-holding pod. A peer that really died turns `Dead` within the
    ///   membership detection window.
    /// * `Dead` and reaped peers do not count, whatever their last advertisement said.
    /// * An empty set is a peer whose declaration has not arrived (a seed is learned
    ///   before any gossip with it) — unknown, so it may hold an index. Every node
    ///   declares a non-empty set at attach.
    pub(crate) fn peer_may_hold_index(&self) -> bool {
        self.unmet_seeds.any()
            || self.group.statuses().into_iter().any(|(peer, status)| {
                peer != self.me && status != Status::Dead && {
                    let caps = self.group.node_capabilities(&peer);
                    caps.is_empty() || caps.iter().any(|cap| cap == CAP_INDEXED)
                }
            })
    }
}
