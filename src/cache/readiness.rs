//! The pod's readiness: whether the Service may route to this node, and whether
//! a rollout may treat it as available.
//!
//! A cold node forwards every request to the origin, so it is useful while its
//! index warms, and a cold fleet must stay in the Service. But a rollout reads
//! the same readiness to decide when to stop the next pod, and stopping the last
//! index-holding pod while its replacement has none costs a whole origin scan.
//! [`CachingProxy::probe_ready`] holds both: a node is ready once its own index
//! is complete, or while no peer that may hold one is alive.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tracing::info;

use crate::cache::proxy::CachingProxy;

/// How often the index watch re-checks [`CachingProxy::initially_ready`]. Index
/// completion and the serve-lease have no change notification of their own, and
/// a quarter second is well inside the kubelet's probe period.
const INDEX_READY_POLL: Duration = Duration::from_millis(250);

/// The `/index-ready` latch: set once, when the initial index and coherence
/// warm-up first complete.
#[derive(Clone, Default)]
pub(crate) struct IndexReady(Arc<AtomicBool>);

impl CachingProxy {
    /// Whether this node's initial index and coherence warm-up completed (see
    /// [`initially_ready`](Self::initially_ready)). A latch: a later gap or
    /// recovery does not clear it, because the node still holds that index in
    /// memory and rebuilds it rather than losing it.
    #[must_use]
    pub fn index_ready(&self) -> bool {
        self.index_ready.0.load(Ordering::Acquire)
    }

    /// The pod readiness predicate, exactly what `GET /ready` serves: this node is
    /// [`index_ready`](Self::index_ready), or no peer that may hold an index is
    /// alive in its gossip roster.
    ///
    /// * A cold fleet is ready: no peer holds an index, so every node stays in the
    ///   Service and forwards to the origin while one of them builds.
    /// * A replacement beside an index-holding peer is not ready until it installs
    ///   or builds its own index, so a rollout cannot stop that peer first.
    /// * A `Suspect` peer counts: the kubelet marks a pod ready on one success, so
    ///   misreading a slow peer as gone could let a rollout stop the last index
    ///   holder. A dead peer turns `Dead` within the membership detection window.
    /// * A peer whose capability declaration has not arrived yet counts: unknown,
    ///   so it may hold an index. So does a configured gossip seed that has not
    ///   appeared in the roster yet, until its address fails to resolve.
    ///
    /// Without gossip there are no peers, so this is always true.
    #[must_use]
    pub fn probe_ready(&self) -> bool {
        self.index_ready()
            || self
                .sync
                .as_ref()
                .is_none_or(|sync| !sync.peer_may_hold_index())
    }

    /// Latch [`index_ready`](Self::index_ready) once `buckets` are indexed, and
    /// declare it to the peers. Started once per life by whichever path starts
    /// this node's indexing.
    pub(super) fn watch_index_ready(&self, buckets: &[String]) {
        let (proxy, buckets) = (self.clone(), buckets.to_vec());
        tokio::spawn(async move {
            while !proxy.initially_ready(&buckets) {
                tokio::time::sleep(INDEX_READY_POLL).await;
            }
            proxy.index_ready.0.store(true, Ordering::Release);
            if let Some(sync) = &proxy.sync {
                sync.advertise_indexed();
            }
            info!(
                "initial index ready for {} configured buckets",
                buckets.len()
            );
        });
    }
}

impl crate::metrics::Readiness for CachingProxy {
    fn probe_ready(&self) -> bool {
        Self::probe_ready(self)
    }

    fn index_ready(&self) -> bool {
        Self::index_ready(self)
    }
}
