//! What this node declares to its peers in its one `~caps` group entry: its
//! coherence participation ([`Consistency::capabilities`]) and, once its initial
//! index is complete, [`CAP_INDEXED`] — the fact a peer's readiness probe reads
//! (see [`crate::sync::readiness`]).

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use groupnet::runtime::Group;
use tracing::warn;

use crate::sync::coherence::{Consistency, WriteSync};

/// The capability a node adds once its initial index and coherence warm-up are
/// complete (the `/index-ready` latch). Namespaced, per groupnet's convention for
/// consumer-defined capabilities.
pub(crate) const CAP_INDEXED: &str = "s3cache:indexed";

/// Attempts to get the capability advertisement enqueued after a rejection. A rejection
/// is a full actor inbox at startup, which drains in milliseconds; the advertisement is
/// state and the last call wins, so re-trying the same set costs nothing.
const ADVERTISE_RETRIES: u32 = 30;

/// How long between those attempts.
const ADVERTISE_RETRY_DELAY: Duration = Duration::from_millis(100);

/// This node's `~caps` declaration, rewritten whole on every change.
pub(crate) struct Advertisement {
    group: Group,
    consistency: Consistency,
    /// Whether [`CAP_INDEXED`] is declared. It only ever turns on. The lock also
    /// orders the offers (see [`offer`](Self::offer)).
    indexed: Mutex<bool>,
}

impl Advertisement {
    /// Declare this node's coherence participation to the group. Every attach path goes
    /// through [`WriteSync::attach`], which is where this is called, so no mode can reach
    /// a group without saying what it is.
    ///
    /// Two things make the call unconditional — including for `bounded`, whose whole point
    /// is *not* participating:
    ///
    /// * The declaration must be **non-empty**. Never-advertised and advertised-empty are
    ///   indistinguishable to a reader (`node_capabilities` answers both with an empty
    ///   set), and the transition rule in
    ///   [`waits_on`](crate::sync::coherence::waits_on) keys on exactly that distinction:
    ///   an empty set means "unknown, assume the old contract and wait". A bounded node
    ///   that advertised nothing would therefore be waited on by every strong writer —
    ///   the timeout-per-write this whole mechanism exists to remove. The readiness probe
    ///   reads an empty set the same way: "unknown, may hold an index".
    /// * The **call itself** must happen regardless of mode. groupnet's restart recovery
    ///   re-adopts un-authored entries from peers' echoes, so a node that comes back
    ///   without authoring `~caps` this life inherits its previous life's set. A pod
    ///   redeployed from strong to bounded would keep advertising `acks` and haunt every
    ///   writer in the cluster until it was reaped, and a restarted pod would keep its
    ///   previous life's [`CAP_INDEXED`]; authoring the entry is what buries the ghost.
    pub(crate) fn declare(group: Group, consistency: Consistency) -> Arc<Self> {
        let advertisement = Arc::new(Self {
            group,
            consistency,
            indexed: Mutex::new(false),
        });
        advertisement.publish();
        advertisement
    }

    /// Add [`CAP_INDEXED`] to the declaration.
    fn mark_indexed(self: &Arc<Self>) {
        *self.indexed.lock().unwrap_or_else(PoisonError::into_inner) = true;
        self.publish();
    }

    fn publish(self: &Arc<Self>) {
        if self.offer() {
            return;
        }
        // Rejection is backpressure, not refusal. Retry off the startup path: serving must
        // not wait on the advertisement, and the advertisement must not be dropped because
        // an inbox was briefly full.
        let advertisement = Arc::clone(self);
        tokio::spawn(async move {
            for _ in 0..ADVERTISE_RETRIES {
                tokio::time::sleep(ADVERTISE_RETRY_DELAY).await;
                if advertisement.offer() {
                    return;
                }
            }
            warn!(
                "could not advertise coherence capabilities; peers will read this node as unknown"
            );
        });
    }

    /// One attempt to enqueue the current declaration. The state is read and enqueued
    /// under one lock, so offers reach the group's inbox in the order they read it: a
    /// startup retry still carrying the set without [`CAP_INDEXED`] cannot land after
    /// the set that has it.
    fn offer(&self) -> bool {
        let indexed = self.indexed.lock().unwrap_or_else(PoisonError::into_inner);
        let caps = self
            .consistency
            .capabilities()
            .iter()
            .copied()
            .chain(indexed.then_some(CAP_INDEXED));
        self.group.advertise_capabilities(caps).is_ok()
    }
}

impl WriteSync {
    /// Tell the peers this node's initial index is complete.
    pub(crate) fn advertise_indexed(&self) {
        self.advertisement.mark_indexed();
    }
}
