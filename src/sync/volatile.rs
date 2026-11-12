//! Application facts and origin-index effects for Groupnet's volatile recovery.

use std::collections::BTreeSet;
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use groupnet::consistency::lease::CAP_LEASE;
use groupnet::consistency::{
    FrontierView, WriteToken, checked_advertised_head,
    volatile_recovery::{
        AdapterError, AdapterErrorKind, Mark, Peer, PeerObservation, PublicationPermit,
        RecoveryAdapter, RecoveryBuilder, RecoveryConfig, RecoveryFallback, RecoveryHandle,
        RecoveryMode, RecoveryOperation, RecoveryRearm, RecoveryStage,
    },
};
use groupnet::core::{NodeId, Status};
use groupnet::runtime::Group;

use crate::index::{KeyIndex, ScanConfig, begin_bucket_resync, sync_bucket_generation_guarded};
use crate::metrics::Metrics;
use crate::sync::coherence::{Consistency, DEFAULT_LEASE_MS, WriteSync};
use crate::tier::LocalCache;

pub(super) mod fleet;

/// The first consumer of the optional reusable volatile-recovery runtime.
/// It reads only existing Groupnet gossip and the S3 origin; no control object
/// or journal is created in the origin bucket.
pub(super) struct CacheRecoveryAdapter {
    sync: Weak<WriteSync>,
    client: aws_sdk_s3::Client,
    state: Arc<KeyIndex>,
    local: LocalCache,
    buckets: Vec<String>,
    scan: ScanConfig,
    group: Group,
    me: NodeId,
    lease: Duration,
    metrics: Arc<Metrics>,
}

static NEXT_SESSION: AtomicU64 = AtomicU64::new(1);
const MAX_RECOVERY_BUCKETS: usize = 4_096;

/// The origin and node-local state one recovery driver guards as a unit.
pub(crate) struct RecoveryInputs {
    pub(crate) client: aws_sdk_s3::Client,
    pub(crate) state: Arc<KeyIndex>,
    pub(crate) local: LocalCache,
    pub(crate) buckets: Vec<String>,
    pub(crate) scan: ScanConfig,
    pub(crate) metrics: Arc<Metrics>,
    pub(crate) config: Option<RecoveryConfig>,
    pub(crate) rearm: Option<RecoveryRearm>,
    pub(crate) fleet: Option<crate::sync::fleet::config::FleetConfig>,
}

impl WriteSync {
    /// Start one Groupnet-owned cold scan and all later volatile-feed recovery.
    /// The session is local correlation only, never a claimed durable cursor.
    pub(crate) fn open_recovery(self: &Arc<Self>, inputs: RecoveryInputs) {
        let prepared = self.prepare_recovery(inputs);
        if prepared.fleet.is_some() {
            tracing::warn!(
                "fleet configured but synchronous coherence startup uses guarded origin recovery; call start_fleet_coherence for peer bootstrap"
            );
        }
        let recovery = prepared.open_origin();
        self.install_recovery(recovery);
    }

    pub(crate) async fn open_fleet_recovery(self: &Arc<Self>, inputs: RecoveryInputs) {
        let prepared = self.prepare_recovery(inputs);
        if let Some((recovery, listener)) = fleet::open_recovery(self, &prepared).await {
            self.install_recovery(recovery);
            self.fleet_listener
                .set(listener)
                .expect("fleet listener already installed");
        } else {
            tracing::warn!("fleet bootstrap unavailable; opening guarded origin recovery");
            self.install_recovery(prepared.open_origin());
        }
    }

    fn prepare_recovery(self: &Arc<Self>, inputs: RecoveryInputs) -> PreparedRecovery {
        let RecoveryInputs {
            client,
            state,
            local,
            buckets,
            scan,
            metrics,
            config,
            rearm,
            fleet,
        } = inputs;
        assert!(
            buckets.len() <= MAX_RECOVERY_BUCKETS,
            "configured recovery bucket count exceeds bound"
        );
        let lease = self
            .leases
            .as_ref()
            .map_or(Duration::from_millis(DEFAULT_LEASE_MS), |set| {
                set.config().duration
            });
        let adapter = Arc::new(CacheRecoveryAdapter {
            sync: Arc::downgrade(self),
            client,
            state,
            local,
            buckets,
            scan,
            group: self.group.clone(),
            me: self.me.clone(),
            lease,
            metrics,
        });
        let mode = if self.consistency == Consistency::Strong {
            RecoveryMode::Leased
        } else {
            RecoveryMode::Unleased
        };
        let settle_ms = u64::try_from(lease.as_millis())
            .unwrap_or(2_000)
            .max(self.recovery_settle_floor_ms());
        let config = config.unwrap_or(RecoveryConfig {
            max_members: const { NonZeroUsize::new(256).unwrap() },
            max_member_bytes: const { NonZeroUsize::new(256).unwrap() },
            max_barrier_rounds: const { NonZeroU32::new(4).unwrap() },
            // Neither bound has to fit a bucket's scan time. Every committed LIST page
            // reports progress, which restarts both, so a scan that keeps committing
            // pages runs to completion in one pass. `attempt_ms` is how long a scan may
            // go without committing a page before it is retried; `total_ms` is how long
            // an episode may go without progress before it ends in `OriginOnly`.
            total_ms: 600_000_u64.max(settle_ms.saturating_mul(2)),
            attempt_ms: 60_000,
            settle_ms,
            poll_ms: 100,
        });
        let session = NEXT_SESSION.fetch_add(1, Ordering::AcqRel);
        PreparedRecovery {
            adapter,
            config,
            mode,
            session,
            rearm,
            fleet,
        }
    }
}

struct PreparedRecovery {
    adapter: Arc<CacheRecoveryAdapter>,
    config: RecoveryConfig,
    mode: RecoveryMode,
    session: u64,
    rearm: Option<RecoveryRearm>,
    fleet: Option<crate::sync::fleet::config::FleetConfig>,
}

impl PreparedRecovery {
    /// The driver's builder with this node's configuration, mode, and rearm policy.
    fn builder(&self) -> RecoveryBuilder<CacheRecoveryAdapter> {
        let builder = RecoveryHandle::builder(
            Arc::clone(&self.adapter),
            self.adapter.me.clone(),
            self.session,
        )
        .config(self.config)
        .mode(self.mode);
        match self.rearm {
            Some(policy) => builder.rearm(policy),
            None => builder,
        }
    }

    fn open_origin(&self) -> RecoveryHandle<CacheRecoveryAdapter> {
        self.builder()
            .open()
            .expect("valid recovery configuration and Tokio runtime")
    }
}

impl CacheRecoveryAdapter {
    fn frontier(&self) -> Option<FrontierView> {
        self.sync.upgrade()?.frontier_view()
    }
}

/// One bounded, fail-closed translation of the observer's current gossip
/// facts. It never treats a malformed present feed as a quiet writer.
fn observe_group(
    group: &Group,
    sync: &WriteSync,
    me: &NodeId,
    lease: Duration,
    limits: RecoveryConfig,
) -> PeerObservation {
    let bound = limits
        .max_members
        .checked_add(1)
        .ok_or(AdapterErrorKind::Capacity)?;
    let roster = group
        .statuses_held_bounded(bound, limits.max_member_bytes)
        .map_err(|error| AdapterError::with_source(AdapterErrorKind::Capacity, error))?;
    let mut peers = Vec::with_capacity(roster.len().saturating_sub(1));
    for (node, status, held) in roster {
        if node == *me {
            continue;
        }
        let head = checked_advertised_head(group, &node)
            .map_err(|_| AdapterError::new(AdapterErrorKind::Malformed))?
            .map(|token| Mark {
                epoch: token.epoch,
                sequence: token.seq,
            });
        let grant = sync.lease_granted_by(&node).map(|grant| Mark {
            epoch: grant.epoch,
            sequence: grant.seq,
        });
        let grants_lease = group.node_has_capability(&node, CAP_LEASE) || grant.is_some();
        let crossing = sync.crossing_of(&node);
        peers.push(Peer {
            alive: status == Status::Alive,
            grants_lease,
            old_nonlive: status != Status::Alive
                && held >= (lease / 4).max(Duration::from_millis(25)),
            renewal: crossing.renewal,
            sealed: crossing.sealed,
            node,
            grant,
            head,
        });
    }
    let confirmed = sync.lease_confirmed().map(|grant| Mark {
        epoch: grant.epoch,
        sequence: grant.seq,
    });
    Ok((peers, confirmed))
}

impl RecoveryAdapter for CacheRecoveryAdapter {
    fn revoke_serving(&self) {
        if let Some(sync) = self.sync.upgrade() {
            sync.require_lease_resync();
        }
    }

    fn operation_failed(&self, stage: RecoveryStage, error: AdapterError) {
        let cause = std::error::Error::source(&error).map(ToString::to_string);
        tracing::warn!(?stage, ?cause, "recovery operation failed: {error}");
    }

    fn fell_back(&self, from: RecoveryStage, reason: RecoveryFallback) {
        self.metrics.recovery_fallback();
        tracing::info!(?from, ?reason, "recovery fell back");
    }

    fn invalidate(
        &self,
        _op: RecoveryOperation,
        distrust_bodies: bool,
        permit: PublicationPermit,
    ) -> impl Future<Output = Result<(), AdapterError>> + Send {
        std::future::ready(
            permit
                .publish(|| {
                    if distrust_bodies {
                        self.local.distrust_all();
                    }
                })
                .ok_or_else(fenced),
        )
    }

    async fn rebuild_origin(
        &self,
        _op: RecoveryOperation,
        permit: PublicationPermit,
    ) -> Result<(), AdapterError> {
        self.metrics.recovery_origin_scan();
        let buckets: BTreeSet<String> = {
            let state = self
                .state
                .read()
                .map_err(|_| AdapterError::new(AdapterErrorKind::Unavailable))?;
            if state.len() > MAX_RECOVERY_BUCKETS
                || state.len().saturating_add(self.buckets.len()) > MAX_RECOVERY_BUCKETS
            {
                return Err(AdapterErrorKind::Capacity.into());
            }
            let mut buckets: BTreeSet<String> = self.buckets.iter().cloned().collect();
            buckets.extend(state.keys().cloned());
            buckets
        };
        let mut generations = Vec::with_capacity(buckets.len());
        for bucket in buckets {
            let generation = permit
                .publish(|| begin_bucket_resync(&self.state, &bucket))
                .ok_or_else(fenced)?;
            generations.push((bucket, generation));
        }
        for (bucket, generation) in generations {
            sync_bucket_generation_guarded(
                &self.client,
                &self.state,
                &bucket,
                generation,
                self.scan,
                permit.clone(),
            )
            .await
            .map_err(|error| AdapterError::with_source(AdapterErrorKind::Unavailable, error))?;
        }
        Ok(())
    }

    fn observe_peers(
        &self,
        _op: RecoveryOperation,
        limits: RecoveryConfig,
    ) -> impl Future<Output = PeerObservation> + Send {
        std::future::ready(match self.sync.upgrade() {
            Some(sync) => observe_group(&self.group, &sync, &self.me, self.lease, limits),
            None => Err(closed()),
        })
    }

    async fn wait_frontiers(
        &self,
        _op: RecoveryOperation,
        heads: Vec<(NodeId, Mark)>,
    ) -> Result<(), AdapterError> {
        let frontier = self.frontier().ok_or_else(closed)?;
        for (node, head) in heads {
            if !frontier
                .reached(
                    &node,
                    WriteToken {
                        epoch: head.epoch,
                        seq: head.sequence,
                    },
                )
                .await
            {
                return Err(AdapterErrorKind::Unavailable.into());
            }
        }
        Ok(())
    }

    fn affirm(&self, _op: RecoveryOperation) -> bool {
        self.sync
            .upgrade()
            .is_some_and(|sync| sync.affirm_lease_now())
    }
}

/// The operation's publication permit is no longer current.
fn fenced() -> AdapterError {
    AdapterError::new(AdapterErrorKind::Fenced)
}

/// The write feed this adapter serves has shut down or never started its apply loop.
fn closed() -> AdapterError {
    AdapterError::new(AdapterErrorKind::Unavailable)
}

#[cfg(test)]
mod tests {
    use super::*;
    use groupnet::consistency::lease::LeaseConfig;
    use groupnet::core::Config;
    use groupnet::network::RouterConfig;
    use groupnet::runtime::Node;
    use groupnet::transport::link::{BoundLink, LinkConfig};
    use groupnet::transport::mem::Network;

    #[tokio::test]
    async fn malformed_present_feed_fails_observation_instead_of_looking_quiet() {
        let net = Network::new();
        let a = NodeId::new("recovery-a");
        let b = NodeId::new("recovery-b");
        let config = Config {
            gossip_interval_ms: 25,
            ..Config::default()
        };
        let routing = RouterConfig {
            announce_interval: Duration::from_millis(10),
            ..RouterConfig::default()
        };
        let node_a = Node::builder(a.clone())
            .link(BoundLink::new(
                net.endpoint(a.clone()),
                LinkConfig::new(vec![b.clone()]),
            ))
            .routing(routing.clone())
            .config(config.clone())
            .start()
            .await
            .expect("A managed node starts");
        let node_b = Node::builder(b.clone())
            .link(BoundLink::new(
                net.endpoint(b.clone()),
                LinkConfig::new(vec![a.clone()]),
            ))
            .routing(routing)
            .config(config)
            .start()
            .await
            .expect("B managed node starts");
        let group_a = node_a.join_group("s3cache");
        let group_b = node_b.join_group("s3cache");
        tokio::time::timeout(Duration::from_secs(3), async {
            while !group_b.members().contains(&a) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("membership converged");
        let sync = WriteSync::attach(
            group_b.clone(),
            b.clone(),
            Consistency::Bounded,
            LeaseConfig::for_duration(Duration::from_millis(300)),
            None,
        );
        let limits = RecoveryConfig {
            max_members: NonZeroUsize::new(4).unwrap(),
            max_member_bytes: NonZeroUsize::new(64).unwrap(),
            max_barrier_rounds: NonZeroU32::new(2).unwrap(),
            total_ms: 2_000,
            attempt_ms: 500,
            settle_ms: 100,
            poll_ms: 25,
        };
        assert!(observe_group(&group_b, &sync, &b, Duration::from_millis(300), limits).is_ok());
        group_a
            .set_entry("~writes", vec![0xFF], None)
            .expect("malformed entry enqueued");
        tokio::time::timeout(Duration::from_secs(3), async {
            while group_b.node_entry(&a, "~writes").is_none() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("malformed entry propagated");
        assert!(observe_group(&group_b, &sync, &b, Duration::from_millis(300), limits).is_err());
    }
}
