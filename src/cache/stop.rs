//! The write path's half of a planned stop.
//!
//! A mutation's origin call and coherence tail run in a spawned task so that a client
//! hanging up cannot strand an applied write outside the index. That same task
//! outlives the HTTP drain: the connection is gone while the tail may still be
//! about to publish. [`WriteTails`] counts those tasks, and
//! [`CachingProxy::seal_writes`] seals the feed only once none is left — a seal
//! promises the peers that this life publishes nothing more.

use std::sync::Arc;
use std::time::{Duration, Instant};

use s3s::S3Result;
use tokio::sync::watch;
use tracing::{info, warn};

use crate::cache::proxy::CachingProxy;
use crate::sync::stop::SealOutcome;

/// How long a planned stop lets in-flight requests finish.
pub const DRAIN_WAIT: Duration = Duration::from_secs(10);

/// How long a drained stop waits for mutation tails and for peers to acknowledge the feed
/// seal. With [`DRAIN_WAIT`] it must fit inside the pod's termination grace period
/// (the Helm chart's `terminationGracePeriodSeconds`).
pub const SEAL_WAIT: Duration = Duration::from_secs(5);

/// What a planned stop did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Stopped {
    /// Every in-flight request finished within [`DRAIN_WAIT`].
    pub drained: bool,
    /// How the write feed's seal ended; `None` when nothing was sealed.
    pub sealed: Option<SealOutcome>,
}

/// The mutation tails still running on this node.
#[derive(Clone)]
pub(crate) struct WriteTails(Arc<watch::Sender<usize>>);

impl Default for WriteTails {
    fn default() -> Self {
        Self(Arc::new(watch::Sender::new(0)))
    }
}

/// One running tail; dropping it (the task finished, panicked, or was aborted)
/// takes it off the count.
pub(crate) struct TailGuard(Arc<watch::Sender<usize>>);

impl Drop for TailGuard {
    fn drop(&mut self) {
        self.0.send_modify(|running| *running -= 1);
    }
}

impl WriteTails {
    /// Count a tail. Take it before the task is spawned, so no wait can observe
    /// zero between the spawn and the task's first poll.
    pub(crate) fn track(&self) -> TailGuard {
        self.0.send_modify(|running| *running += 1);
        TailGuard(Arc::clone(&self.0))
    }

    /// Whether every tail finished within `wait`.
    async fn drained(&self, wait: Duration) -> bool {
        let mut running = self.0.subscribe();
        tokio::time::timeout(wait, running.wait_for(|running| *running == 0))
            .await
            .is_ok_and(|result| result.is_ok())
    }
}

impl CachingProxy {
    /// Run a mutation of `keys` — its origin call and its coherence tail — on a task of
    /// its own, counted for the planned stop's seal, and answer with its result. A
    /// client that hangs up cancels only the wait; the task still records whatever the
    /// origin did. A task that dies cannot say what it did, so every key it touched is
    /// advertised as unknown before the failure is answered.
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
        let guard = self.tails.track();
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
    /// Call it only after the HTTP drain has completed, so no request can start a
    /// write. It first waits for every mutation tail to finish, then seals and waits for
    /// the peers to acknowledge the seal (see [`crate::sync::coherence::WriteSync::seal`]).
    /// Returns `None` when nothing was sealed: no coherence is configured, or a tail
    /// was still running at the deadline, in which case the restart stays an
    /// ordinary gap for the peers — the only safe answer while a write may still
    /// publish.
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
    /// The binary's planned stop (`SIGTERM`): retract this node's serve-lease, let
    /// `drain` — the HTTP server's graceful shutdown — finish within [`DRAIN_WAIT`],
    /// and seal the write feed only if it did.
    pub async fn stop(&self, drain: impl Future<Output = ()>) -> Stopped {
        // Announce the departure before draining, not after: the retraction is one gossip
        // entry and the drain can take seconds, and every one of them is a second a peer's
        // next write might spend waiting out a lease this node has already stopped using.
        if let Some(sync) = &self.sync {
            sync.leave();
        }
        let drained = tokio::time::timeout(DRAIN_WAIT, drain).await.is_ok();
        // Seal only a drained stop. A request still running could publish after the
        // seal, and the seal would promise peers a tail this life did not end at; the
        // restart then stays an ordinary gap, which is always safe.
        let sealed = if drained {
            info!("graceful shutdown complete");
            self.seal_writes(SEAL_WAIT).await
        } else {
            info!("shutdown timed out");
            warn!("requests were still running at the drain deadline; not sealing the write feed");
            None
        };
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
        let first = tails.track();
        let second = tails.track();
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
}
