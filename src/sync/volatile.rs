//! Application facts and origin-index effects for Groupnet's volatile recovery.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use groupnet::consistency::{
    CAP_LEASE, FrontierView, WriteToken, checked_advertised_head,
    volatile_recovery::{
        AdapterError, BoxRecoveryFuture, Mark, Peer, PeerObservation, PublicationPermit,
        RecoveryAdapter, RecoveryConfig, RecoveryFallback, RecoveryHandle, RecoveryMode,
        RecoveryOperation, RecoveryRearm, RecoveryStage,
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
            max_members: 256,
            max_member_bytes: 256,
            max_barrier_rounds: 4,
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
    fn open_origin(&self) -> RecoveryHandle<CacheRecoveryAdapter> {
        if let Some(policy) = self.rearm {
            RecoveryHandle::open_with_rearm(
                Arc::clone(&self.adapter),
                self.config,
                self.mode,
                self.adapter.me.clone(),
                self.session,
                policy,
            )
        } else {
            RecoveryHandle::open(
                Arc::clone(&self.adapter),
                self.config,
                self.mode,
                self.adapter.me.clone(),
                self.session,
            )
        }
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
    let bound = limits.max_members.checked_add(1).ok_or(AdapterError)?;
    let roster = group
        .statuses_held_bounded(bound, limits.max_member_bytes)
        .map_err(|_| AdapterError)?;
    let mut peers = Vec::with_capacity(roster.len().saturating_sub(1));
    for (node, status, held) in roster {
        if node == *me {
            continue;
        }
        let head = checked_advertised_head(group, &node)
            .map_err(|_| AdapterError)?
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

    fn fell_back(&self, from: RecoveryStage, reason: RecoveryFallback) {
        self.metrics.recovery_fallback();
        tracing::info!(?from, ?reason, "recovery fell back");
    }

    fn invalidate(
        &self,
        _op: RecoveryOperation,
        distrust_bodies: bool,
        permit: PublicationPermit,
    ) -> BoxRecoveryFuture<'_, Result<(), AdapterError>> {
        Box::pin(async move {
            permit
                .publish(|| {
                    if distrust_bodies {
                        self.local.distrust_all();
                    }
                })
                .ok_or(AdapterError)
        })
    }

    fn rebuild_origin(
        &self,
        _op: RecoveryOperation,
        permit: PublicationPermit,
    ) -> BoxRecoveryFuture<'_, Result<(), AdapterError>> {
        Box::pin(async move {
            self.metrics.recovery_origin_scan();
            let buckets: BTreeSet<String> = {
                let state = self.state.read().map_err(|_| AdapterError)?;
                if state.len() > MAX_RECOVERY_BUCKETS
                    || state.len().saturating_add(self.buckets.len()) > MAX_RECOVERY_BUCKETS
                {
                    return Err(AdapterError);
                }
                let mut buckets: BTreeSet<String> = self.buckets.iter().cloned().collect();
                buckets.extend(state.keys().cloned());
                buckets
            };
            let mut generations = Vec::with_capacity(buckets.len());
            for bucket in buckets {
                let generation = permit
                    .publish(|| begin_bucket_resync(&self.state, &bucket))
                    .ok_or(AdapterError)?;
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
                .map_err(|_| AdapterError)?;
            }
            Ok(())
        })
    }

    fn observe_peers(
        &self,
        _op: RecoveryOperation,
        limits: RecoveryConfig,
    ) -> BoxRecoveryFuture<'_, PeerObservation> {
        Box::pin(async move {
            let sync = self.sync.upgrade().ok_or(AdapterError)?;
            observe_group(&self.group, &sync, &self.me, self.lease, limits)
        })
    }

    fn wait_frontiers(
        &self,
        _op: RecoveryOperation,
        heads: Vec<(NodeId, Mark)>,
    ) -> BoxRecoveryFuture<'_, Result<(), AdapterError>> {
        Box::pin(async move {
            let frontier = self.frontier().ok_or(AdapterError)?;
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
                    return Err(AdapterError);
                }
            }
            Ok(())
        })
    }

    fn affirm(&self, _op: RecoveryOperation) -> bool {
        self.sync
            .upgrade()
            .is_some_and(|sync| sync.affirm_lease_now())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use groupnet::consistency::LeaseConfig;
    use groupnet::core::Config;
    use groupnet::runtime::Node;
    use groupnet::transport::mem::Network;

    #[tokio::test]
    async fn malformed_present_feed_fails_observation_instead_of_looking_quiet() {
        let net = Network::new();
        let a = NodeId::new("recovery-a");
        let b = NodeId::new("recovery-b");
        let config = Config {
            gossip_interval_ms: 10,
            anti_entropy_interval_ms: 25,
            ..Config::default()
        };
        let node_a = Node::builder(a.clone(), net.endpoint(a.clone()))
            .seed(b.clone())
            .config(config.clone())
            .spawn();
        let node_b = Node::builder(b.clone(), net.endpoint(b.clone()))
            .seed(a.clone())
            .config(config)
            .spawn();
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
            max_members: 4,
            max_member_bytes: 64,
            max_barrier_rounds: 2,
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
