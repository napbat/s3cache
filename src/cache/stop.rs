//! The write path's half of a planned stop.
//!
//! A mutation's origin call and coherence tail run in a spawned task so that a client
//! hanging up cannot strand an applied write outside the index. That same task
//! outlives the HTTP drain: the connection is gone while the tail may still be
//! about to publish. [`WriteTails`] counts those tasks and, once a planned stop
//! closes it, admits no new one; [`CachingProxy::seal_writes`] seals the feed once
//! none is left — a seal promises the peers that this life publishes nothing more.
//! Only mutation tails publish, so the seal does not wait for reads, uploads of
//! parts, or listings still draining.

use std::sync::Arc;
use std::time::{Duration, Instant};

use s3s::S3Result;
use tokio::sync::watch;
use tracing::{info, warn};

use crate::cache::proxy::CachingProxy;
use crate::sync::stop::SealOutcome;

/// How long a planned stop lets in-flight requests finish.
pub const DRAIN_WAIT: Duration = Duration::from_secs(10);

/// How long a planned stop waits for mutation tails and for peers to acknowledge the
/// feed seal. It runs alongside [`DRAIN_WAIT`], so the stop takes the longer of the two,
/// which must fit inside the pod's termination grace period (the Helm chart's
/// `terminationGracePeriodSeconds`).
pub const SEAL_WAIT: Duration = Duration::from_secs(5);

/// What a planned stop did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Stopped {
    /// Every in-flight request finished within [`DRAIN_WAIT`].
    pub drained: bool,
    /// How the write feed's seal ended; `None` when nothing was sealed.
    pub sealed: Option<SealOutcome>,
}

/// The mutation tails still running on this node, and whether new ones are admitted.
#[derive(Clone)]
pub(crate) struct WriteTails(Arc<watch::Sender<TailCount>>);

#[derive(Default)]
pub(crate) struct TailCount {
    running: usize,
    closed: bool,
}

impl Default for WriteTails {
    fn default() -> Self {
        Self(Arc::new(watch::Sender::new(TailCount::default())))
    }
}

/// One running tail; dropping it (the task finished, panicked, or was aborted)
/// takes it off the count.
pub(crate) struct TailGuard(Arc<watch::Sender<TailCount>>);

impl TailGuard {
    /// Another hold on the same tail, counted even once admission is closed: the tail
    /// it extends was admitted before.
    fn share(&self) -> Self {
        self.0.send_modify(|count| count.running += 1);
        Self(Arc::clone(&self.0))
    }
}

impl Drop for TailGuard {
    fn drop(&mut self) {
        self.0.send_modify(|count| count.running -= 1);
    }
}

impl WriteTails {
    /// Count a tail, or `None` once a planned stop closed admission. Take it before the
    /// task is spawned, so no wait can observe zero between the spawn and the task's
    /// first poll.
    pub(crate) fn track(&self) -> Option<TailGuard> {
        let mut admitted = false;
        self.0.send_if_modified(|count| {
            admitted = !count.closed;
            if admitted {
                count.running += 1;
            }
            admitted
        });
        admitted.then(|| TailGuard(Arc::clone(&self.0)))
    }

    /// Admit no new tail; the check and the count share one lock, so after this returns
    /// the count can only fall.
    fn close(&self) {
        self.0.send_modify(|count| count.closed = true);
    }

    /// Whether every tail finished within `wait`.
    async fn drained(&self, wait: Duration) -> bool {
        let mut running = self.0.subscribe();
        tokio::time::timeout(wait, running.wait_for(|count| count.running == 0))
            .await
            .is_ok_and(|result| result.is_ok())
    }
}

impl CachingProxy {
    /// Run a mutation of `keys` — its origin call and its coherence tail — on a task of
    /// its own, counted for the planned stop's seal, and answer with its result. A
    /// client that hangs up cancels only the wait; the task still records whatever the
    /// origin did. A task that dies cannot say what it did, so every key it touched is
    /// advertised as unknown before the failure is answered. Once a planned stop has
    /// begun, the mutation is refused with a 503 before it reaches the origin.
    pub(super) async fn mutation_tail<T, W>(
        &self,
        bucket: &str,
        keys: &[String],
        work: W,
    ) -> S3Result<T>
    where
        T: Send + 'static,
        W: Future<Output = S3Result<T>> + Send + 'static,
    {
        let Some(guard) = self.tails.track() else {
            return Err(s3s::s3_error!(
                ServiceUnavailable,
                "s3cache: this node is stopping; retry the write on another node"
            ));
        };
        // The announcements of a terminated task publish too, so this wait holds the
        // tail open until they are done.
        let _announcing = guard.share();
        let tail = tokio::spawn(async move {
            let _guard = guard;
            work.await
        });
        match tail.await {
            Ok(result) => result,
            Err(error) => {
                let mut receipt = None;
                for key in keys {
                    receipt = self
                        .announce_unknown(bucket, key, "the mutation task terminated")
                        .await
                        .or(receipt);
                }
                self.settle_cluster(receipt, bucket, "<terminated mutation>")
                    .await;
                Err(s3s::s3_error!(
                    InternalError,
                    "s3cache: mutation task failed: {error}"
                ))
            }
        }
    }

    /// Seal this node's write feed for a planned stop, within `wait` in total.
    ///
    /// Call it only once mutation admission is closed (as [`stop`](Self::stop) does), so
    /// no new write can start. It first waits for every mutation tail to finish, then
    /// seals and waits for the peers to acknowledge the seal (see
    /// [`crate::sync::coherence::WriteSync::seal`]). Returns `None` when nothing was
    /// sealed: no coherence is configured, or a tail was still running at the deadline,
    /// in which case the restart stays an ordinary gap for the peers — the only safe
    /// answer while a write may still publish.
    pub async fn seal_writes(&self, wait: Duration) -> Option<SealOutcome> {
        let sync = self.sync.as_ref()?;
        let started = Instant::now();
        if !self.tails.drained(wait).await {
            warn!(
                "a mutation is still completing at the stop deadline; not sealing the write feed"
            );
            return None;
        }
        Some(sync.seal(wait.saturating_sub(started.elapsed())).await)
    }
}

impl CachingProxy {
    /// The binary's planned stop (`SIGTERM`): retract this node's serve-lease, refuse
    /// new mutations, and then, side by side, let `drain` — the HTTP server's graceful
    /// shutdown — finish within [`DRAIN_WAIT`] and seal the write feed within
    /// [`SEAL_WAIT`]. The seal waits only for mutation tails, so a long read still
    /// draining does not cost the peers their index.
    pub async fn stop(&self, drain: impl Future<Output = ()>) -> Stopped {
        // Announce the departure before draining, not after: the retraction is one gossip
        // entry and the drain can take seconds, and every one of them is a second a peer's
        // next write might spend waiting out a lease this node has already stopped using.
        if let Some(sync) = &self.sync {
            sync.leave();
        }
        // From here every new write is a 503 before it reaches the origin, so once the
        // admitted tails finish nothing can publish after the seal.
        self.tails.close();
        let (drained, sealed) = tokio::join!(
            async { tokio::time::timeout(DRAIN_WAIT, drain).await.is_ok() },
            self.seal_writes(SEAL_WAIT),
        );
        if drained {
            info!("graceful shutdown complete");
        } else {
            info!("shutdown timed out");
        }
        Stopped { drained, sealed }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn tails_drain_only_once_every_guard_is_gone() {
        let tails = WriteTails::default();
        assert!(tails.drained(Duration::ZERO).await, "no tail is running");
        let first = tails.track().expect("admitted");
        let second = tails.track().expect("admitted");
        assert!(!tails.drained(Duration::from_millis(20)).await);
        drop(first);
        assert!(!tails.drained(Duration::from_millis(20)).await);
        let waiting = tokio::spawn({
            let tails = tails.clone();
            async move { tails.drained(Duration::from_secs(5)).await }
        });
        drop(second);
        assert!(waiting.await.unwrap(), "the last guard wakes the waiter");
    }

    #[tokio::test]
    async fn a_closed_count_admits_nothing_but_keeps_admitted_tails() {
        let tails = WriteTails::default();
        let admitted = tails.track().expect("admitted");
        tails.close();
        assert!(
            tails.track().is_none(),
            "a stopping node admits no new tail"
        );
        let announcing = admitted.share();
        drop(admitted);
        assert!(
            !tails.drained(Duration::from_millis(20)).await,
            "a shared hold keeps the tail open"
        );
        drop(announcing);
        assert!(tails.drained(Duration::ZERO).await);
    }
}
