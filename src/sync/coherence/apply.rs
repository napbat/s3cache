use std::sync::Arc;

use groupnet::consistency::{AckLedger, Frontier, LeaseView, PeerWrite, PeerWrites};
use tracing::warn;

use crate::index::KeyIndex;
use crate::metrics::Metrics;
use crate::sync::coherence::{KeyReconcile, WriteSync, apply_feed_index, lapse_poll, native_cut};
use crate::sync::wire::decode_event;
use crate::tier::LocalCache;

impl WriteSync {
    /// Spawn the apply loop: peer events update the LIST index and invalidate
    /// hot bodies. A peer write that describes nothing fences its key, and
    /// `reconcile` resolves that fence from the origin. A gap synchronously
    /// closes Groupnet's local serving gate, discards the index, and schedules
    /// its rebuild. In `strong`, a small watcher reports the lease's monotone
    /// lapse count to that same recovery driver.
    ///
    /// Takes `self` as an [`Arc`] because the lapse watcher holds a weak
    /// reference back to this object, avoiding a task/handle cycle.
    pub(crate) fn start_apply(
        self: &Arc<Self>,
        local: LocalCache,
        state: Arc<KeyIndex>,
        metrics: Arc<Metrics>,
        reconcile: KeyReconcile,
    ) {
        let (frontier, view) = Frontier::new();
        let _ = self.view.set(view);
        // Declare this node's feed writer before any capture can start, at
        // its current position (zero for a feed with no writes yet).
        state.register_native_writer(&self.own_position());
        if let Some(leases) = &self.leases {
            let weak = Arc::downgrade(self);
            let poll = lapse_poll(leases.config().duration);
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(poll).await;
                    let Some(sync) = weak.upgrade() else { return };
                    let Some(recovery) = sync.recovery.get() else {
                        return;
                    };
                    let count = sync.lease_view.as_ref().map_or(0, LeaseView::lapses);
                    if count > recovery.status().state.covered_lapses {
                        let _ = recovery.lease_lapse(count);
                    }
                }
            });
        }
        // Bounded mode publishes no acks (that is its point at scale); both strong
        // spellings do, and say so through their capability advertisement.
        let ledger = self
            .consistency
            .acks()
            .then(|| AckLedger::new(self.group.clone()));
        let mut peers = PeerWrites::new(self.group.clone(), self.me.clone(), decode_event);
        let weak = Arc::downgrade(self);
        let lapse_view = self.lease_view.clone();
        tokio::spawn(async move {
            while let Some(event) = peers.next().await {
                let Some(sync) = weak.upgrade() else { return };
                match event {
                    PeerWrite::Sealed { peer, token } => {
                        state.note_native_seal(native_cut(&peer, token));
                        sync.observe_seal(&peer, token);
                        frontier.advance(&peer, token);
                        if let Some(ledger) = &ledger {
                            ledger.record(&peer, token).await;
                        }
                    }
                    PeerWrite::Renewed {
                        peer,
                        sealed,
                        epoch,
                    } => {
                        let start = sync.observe_renewal(&state, &peer, sealed, epoch, &metrics);
                        frontier.advance(&peer, start);
                        if let Some(ledger) = &ledger {
                            ledger.record(&peer, start).await;
                        }
                    }
                    PeerWrite::Wrote {
                        peer,
                        token,
                        key: event,
                    } => {
                        let cache_key = (event.bucket.clone(), event.key.clone());
                        let mutation = local.fence_mutation(&cache_key).await;
                        let fenced = apply_feed_index(&state, &peer, token, &event);
                        drop(mutation);
                        // Fenced before the acknowledgement below: from then on this
                        // node answers the key from the origin until the reconciliation
                        // resolves it.
                        if let Some(fence) = fenced {
                            reconcile(&event.bucket, &event.key, fence);
                        }
                        // The index must move first: a warm body promoted after this hot
                        // eviction decodes suspect and is checked against the new entry
                        // before it can be served. Awaiting moka removal before the ack
                        // closes the stale-hot window without putting disk I/O on the
                        // feed frontier.
                        local.invalidate_hot(&cache_key).await;
                        frontier.advance(&peer, token);
                        if let Some(ledger) = &ledger {
                            ledger.record(&peer, token).await;
                        }
                        metrics.feed_applied();
                        // The index has moved past any copy this node holds, so the warm
                        // copy can never serve again; left in place it would only crowd
                        // live bodies out of the disk budget until LRU found it. Disk I/O
                        // stays off the frontier: retire it after the acknowledgement.
                        let retire = local.clone();
                        tokio::spawn(async move { retire.invalidate(&cache_key).await });
                    }
                    PeerWrite::Gap {
                        peer,
                        missed_through,
                    } => {
                        warn!("write-feed gap from `{peer}`: distrusting bodies, resyncing index");
                        sync.forget_crossing(&peer);
                        state.discard_for_gap(&native_cut(&peer, missed_through));
                        // The public signal fences serving before returning; the
                        // recovery worker distrusts bodies and guards every origin
                        // scan page with that exact operation's permit. Frontier
                        // progress below means the old local state is unservable,
                        // not that the replacement index has materialized already.
                        if let Some(recovery) = sync.recovery.get() {
                            let lapses = lapse_view.as_ref().map_or(0, LeaseView::lapses);
                            let _ = recovery.feed_gap(lapses);
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
}
