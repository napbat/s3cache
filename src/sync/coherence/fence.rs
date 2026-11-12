//! The pre-write fence. In `strong`, a write is announced to every peer, and the
//! announcement acknowledged (or the peer's lease lapsed), before the write reaches the
//! origin. A writer that crashes after the origin applied the write therefore leaves
//! every peer answering the key from the origin (see [`crate::index::pending`]).

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, SystemTime};

use groupnet::consistency::{PublishError, WriteToken};
use groupnet::core::volatile_bootstrap::journal::NativeCut;

use crate::index::KeyIndex;
use crate::metrics::Metrics;
use crate::sync::coherence::{Consistency, WriteReceipt, WriteSync};
use crate::sync::wire::{IndexEvent, IndexOp, to_micros};

type Open = Arc<Mutex<BTreeSet<u64>>>;

fn lock(open: &Open) -> MutexGuard<'_, BTreeSet<u64>> {
    open.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The feed sequences of this node's pending writes not settled yet.
#[derive(Default)]
pub(super) struct OpenWrites(Open);

/// A write announced to every peer before its origin call. The event reporting its
/// outcome settles it. Dropped unsettled (its task died), it stops holding back the
/// floor the writer's next event carries.
pub(crate) struct PendingWrite {
    seq: u64,
    open: Open,
}

impl Drop for PendingWrite {
    fn drop(&mut self) {
        lock(&self.open).remove(&self.seq);
    }
}

/// The fence did not reach every peer in time. Its announcements are abandoned and
/// the write must not reach the origin.
#[derive(Debug)]
pub(crate) struct FenceStalled;

impl WriteSync {
    /// Stamp an own event with the pending write it settles and the oldest one still
    /// open, and publish it. Runs under the index lock that orders every own event;
    /// `opens` records the event as a new pending write.
    pub(super) fn publish_own(
        &self,
        mut event: IndexEvent,
        settles: Option<&PendingWrite>,
        opens: bool,
    ) -> (
        impl Future<Output = Result<WriteToken, PublishError>> + Send + use<'_>,
        NativeCut,
    ) {
        let mut open = lock(&self.open_writes.0);
        if let Some(pending) = settles {
            open.remove(&pending.seq);
            event.settles = Some(pending.seq);
        }
        event.floor = open.first().copied();
        let publishing = self.feed.publish(&event);
        let position = self.own_position();
        if opens {
            open.insert(position.sequence);
        }
        (publishing, position)
    }

    /// Announce a write of each of `keys` and wait until every peer that could serve
    /// them holds the announcement or has lapsed. Only then may the write reach the
    /// origin, and each returned [`PendingWrite`] is settled by the event that reports
    /// its key's outcome. Only `strong` fences; other modes get `None` for each key.
    ///
    /// # Errors
    /// [`FenceStalled`] when the wait stalled. The announcements are then abandoned.
    pub(crate) async fn fence(
        &self,
        state: &KeyIndex,
        bucket: &str,
        keys: &[String],
        timeout: Duration,
        metrics: &Metrics,
    ) -> Result<Vec<Option<PendingWrite>>, FenceStalled> {
        if self.consistency != Consistency::Strong {
            return Ok(keys.iter().map(|_| None).collect());
        }
        let mut pending = Vec::with_capacity(keys.len());
        let mut newest = None;
        for key in keys {
            let (write, receipt) = self.index_pending(state, bucket, key, metrics).await;
            pending.push(write);
            newest = Some(receipt.token);
        }
        let Some(token) = newest else {
            return Ok(Vec::new());
        };
        let label = match keys {
            [key] => key.as_str(),
            _ => "<batch>",
        };
        let on_stall = "answering 503, the write was not sent to the origin";
        if self
            .acked(token, timeout, bucket, label, metrics, on_stall)
            .await
        {
            return Ok(pending.into_iter().map(Some).collect());
        }
        for (key, write) in keys.iter().zip(pending) {
            self.index_abandoned(state, bucket, key, write, metrics)
                .await;
        }
        Err(FenceStalled)
    }

    async fn index_pending(
        &self,
        state: &KeyIndex,
        bucket: &str,
        key: &str,
        metrics: &Metrics,
    ) -> (PendingWrite, WriteReceipt) {
        let event = IndexEvent {
            op: IndexOp::Pending,
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            ts_us: to_micros(SystemTime::now()),
            settles: None,
            floor: None,
        };
        let (publishing, seq) = state.note_own_native(|| {
            let (publishing, position) = self.publish_own(event, None, true);
            ((publishing, position.sequence), position)
        });
        let write = PendingWrite {
            seq,
            open: Arc::clone(&self.open_writes.0),
        };
        (write, self.advertised(publishing, metrics).await)
    }

    /// Settle a pending write the origin refused, or that was never sent: the key is
    /// as it was, so peers stop fencing it without waiting for anyone.
    pub(crate) async fn index_abandoned(
        &self,
        state: &KeyIndex,
        bucket: &str,
        key: &str,
        write: PendingWrite,
        metrics: &Metrics,
    ) {
        let event = IndexEvent {
            op: IndexOp::Abandoned,
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            ts_us: to_micros(SystemTime::now()),
            settles: None,
            floor: None,
        };
        let publishing =
            state.note_own_native(|| self.publish_own(event, Some(&write), false));
        drop(write);
        self.advertised(publishing, metrics).await;
    }
}
