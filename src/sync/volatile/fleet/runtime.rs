//! One TCP listener and claim/transfer child of the recovery worker.

use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use groupnet::consistency::QueueCapacity;
use groupnet::consistency::volatile_recovery::RecoveryHandle;
use groupnet::consistency::volatile_recovery::bootstrap::admission::{
    AdmissionLimits, ByteAdmission,
};
use groupnet::consistency::volatile_recovery::bootstrap::bulk_adapter::{
    BulkDonorOptions, BulkDonorPort,
};
use groupnet::consistency::volatile_recovery::bootstrap::bulk_wire::{
    BootstrapBulkListener, BulkLimits, PhaseLimits, WireLimits,
};
use groupnet::consistency::volatile_recovery::bootstrap::native_claims::{
    NativeClaimOptions, NativeClaimSource,
};
use groupnet::consistency::volatile_recovery::bootstrap::ports::BootstrapCapabilities;
use groupnet::consistency::volatile_recovery::bootstrap::report::{
    BootstrapDecision, BootstrapObserver,
};
use groupnet::consistency::volatile_recovery::bootstrap::session::{
    BootstrapRuntimeConfig, BootstrapSession,
};
use groupnet::core::NodeId;
use groupnet::core::volatile_bootstrap::transfer::TransferConfig;
use groupnet::core::volatile_bootstrap::{BootstrapConfig, BootstrapScope, PresenceIdentity};
use groupnet::transport::bulk::DataPlane;
use groupnet::transport::tcp::TcpBulkTransport;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::metrics::Metrics;
use crate::sync::coherence::WriteSync;
use crate::sync::fleet::fresh_boot;

use super::capture::bound;
use super::{FleetStatePort, capture};
use crate::sync::volatile::{CacheRecoveryAdapter, PreparedRecovery};

/// Exact bound listener lifetime. Dropping it closes admission promptly;
/// Groupnet's existing worker owns claim withdrawal and candidate cleanup.
#[derive(Debug)]
pub(crate) struct FleetListenerGuard {
    shutdown: watch::Sender<bool>,
    task: JoinHandle<()>,
}

impl Drop for FleetListenerGuard {
    fn drop(&mut self) {
        self.shutdown.send_replace(true);
        self.task.abort();
    }
}

/// Logs each peer-bootstrap decision at info: whether this node waits for a
/// peer's image, why it stopped waiting (which may cost an origin scan, and
/// includes an aborted transfer from a Ready donor), why the session ended
/// without an image, and whether its own finished image was offered to peers.
/// Each Ready recapture started is also counted.
struct DecisionLog(Arc<Metrics>);

impl std::fmt::Debug for DecisionLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DecisionLog")
    }
}

impl BootstrapObserver for DecisionLog {
    fn decided(&self, decision: &BootstrapDecision) {
        match decision {
            BootstrapDecision::Following { builder } => tracing::info!(
                builder = %builder.node,
                attempt = builder.attempt,
                "fleet bootstrap following a peer's build"
            ),
            BootstrapDecision::Released { builder, reason } => tracing::info!(
                builder = %builder.node,
                attempt = builder.attempt,
                ?reason,
                "fleet bootstrap stopped waiting for a peer's image"
            ),
            BootstrapDecision::RecaptureStarted { donor } => {
                self.0.recovery_ready_recapture();
                tracing::info!(attempt = donor.attempt, "fleet Ready recapture started");
            }
            BootstrapDecision::RecaptureDeclined { reason } => {
                tracing::info!(?reason, "fleet Ready recapture declined");
            }
            BootstrapDecision::Declined { reason } => {
                tracing::info!(?reason, "fleet bootstrap declined; recovering from origin");
            }
        }
    }
}

/// Claim policy inside one recovery. `donor_wait_ms` is a stall bound, not a scan
/// budget: the local origin build and a follower's wait for that builder both
/// restart it whenever the build commits a page, so any bucket size fits.
fn claim_config(recovery: &PreparedRecovery) -> Option<BootstrapConfig> {
    let total_ms = recovery.config.total_ms.min(60_000);
    (total_ms >= 5_000).then_some(BootstrapConfig {
        max_members: capture::JOURNAL_CONFIG.max_members,
        max_member_bytes: bound(256),
        max_scope_bytes: capture::JOURNAL_CONFIG.max_scope_bytes,
        // Give a newly observed Ready donor's native TTL claim time to
        // propagate during the existing core settle phase. This remains
        // inside the original recovery budget; origin passthrough is open.
        settle_ms: recovery.config.settle_ms.max(3_000).min(total_ms / 2),
        renew_ms: 500,
        claim_ttl_ms: 3_000,
        observe_ms: 1_000,
        donor_wait_ms: total_ms / 2,
        total_ms,
    })
}

fn transfer_config() -> Option<TransferConfig> {
    Some(TransferConfig {
        expected_schema: NonZeroU32::new(crate::index::fleet::IMAGE_SCHEMA)?,
        max_metadata_bytes: bound(64 << 10),
        max_encoded_bytes: bound(capture::IMAGE_CAPS.bytes),
        max_decoded_bytes: bound(capture::IMAGE_CAPS.decoded_bytes),
        max_chunk_bytes: bound(64 << 10),
        max_chunks: bound(capture::IMAGE_CAPS.bytes / (64 << 10)),
        max_batch_bytes: capture::JOURNAL_CONFIG.max_batch_bytes,
        max_batch_events: capture::JOURNAL_CONFIG.max_batch_events,
        max_replay_events: capture::JOURNAL_CONFIG.max_events,
        max_native_buffer_bytes: bound(1 << 20),
        max_members: capture::JOURNAL_CONFIG.max_members,
        max_cuts: capture::JOURNAL_CONFIG.max_cuts,
        coverage_poll_ms: 100,
    })
}

fn bulk_limits() -> BulkLimits {
    BulkLimits {
        wire: WireLimits {
            max_frame_bytes: bound(256 << 10),
            max_scope_bytes: capture::JOURNAL_CONFIG.max_scope_bytes,
            max_node_bytes: bound(256),
            max_payload_bytes: bound(192 << 10),
        },
        phase: PhaseLimits {
            max_body_bytes: bound(128 << 10),
            max_scope_bytes: capture::JOURNAL_CONFIG.max_scope_bytes,
            max_node_bytes: bound(256),
            max_writer_bytes: capture::JOURNAL_CONFIG.max_cut_bytes,
            max_cuts: capture::JOURNAL_CONFIG.max_cuts,
            max_members: capture::JOURNAL_CONFIG.max_members,
            max_events: capture::JOURNAL_CONFIG.max_batch_events,
            max_identity_bytes: capture::JOURNAL_CONFIG.max_identity_bytes,
            max_effect_bytes: capture::JOURNAL_CONFIG.max_event_bytes,
        },
        server_request: Duration::from_millis(3_000),
    }
}

/// Capture and transfer reserve exactly what the measured image at C needs from
/// this pool; its class limits are the image codec's hard ceilings, only a
/// safety cap. One node is a donor or a follower within a recovery, never both.
fn admission() -> Option<ByteAdmission> {
    ByteAdmission::new(AdmissionLimits {
        max_total_bytes: bound((256 << 20) + (1 << 30) + (80 << 20)),
        max_encoded_bytes: capture::IMAGE_CAPS.bytes,
        max_decoded_bytes: capture::IMAGE_CAPS.decoded_bytes,
        max_suffix_bytes: 32 << 20,
        max_native_overlap_bytes: 16 << 20,
        max_inflight_bytes: 32 << 20,
        max_reservations: bound(512),
    })
    .ok()
}

fn required<T>(stage: &'static str, value: Option<T>) -> Option<T> {
    if value.is_none() {
        tracing::warn!(
            stage,
            "fleet startup declined; using guarded origin recovery"
        );
    }
    value
}

/// Assemble source, bounded TCP exchange, and one child of the existing
/// recovery worker. Any failure drops the partial socket and returns to
/// ordinary origin recovery before feed apply starts.
#[expect(
    clippy::too_many_lines,
    reason = "one bounded bootstrap construction owns and drops every partially built resource"
)]
pub(in crate::sync::volatile) async fn open_recovery(
    sync: &Arc<WriteSync>,
    prepared: &PreparedRecovery,
) -> Option<(RecoveryHandle<CacheRecoveryAdapter>, FleetListenerGuard)> {
    let fleet = prepared.fleet.as_ref()?;
    let scope: BootstrapScope = required("scope", fleet.scope(&prepared.adapter.buckets).ok())?;
    let claim = required("claim configuration", claim_config(prepared))?;
    required("claim policy", claim.validate().ok())?;
    let transfer = required("transfer schema", transfer_config())?;
    required("transfer policy", transfer.validate().ok())?;
    let limits = bulk_limits();
    required("bulk limits", limits.validate().ok())?;
    let budget = required("admission", admission())?;
    let suffix_bytes = groupnet::core::volatile_bootstrap::journal::DonorJournal::storage_bound(
        capture::JOURNAL_CONFIG,
    )
    .ok()?;
    if suffix_bytes.get() > (32 << 20) {
        tracing::warn!("fleet suffix exceeds shared admission cap; using guarded origin recovery");
        return None;
    }
    let boot = required("boot identity", fresh_boot().ok())?;
    let tcp = required(
        "TCP bind",
        TcpBulkTransport::bind(sync.me.clone(), fleet.bind)
            .await
            .ok(),
    )?;
    for (name, address) in &fleet.peers {
        if name != sync.me.as_str() {
            required(
                "TCP peer registration",
                tcp.register_peer_host(NodeId::from(name.as_str()), address.clone())
                    .ok(),
            )?;
        }
    }
    let plane = DataPlane::new(tcp);
    let source = Arc::new(required(
        "native claim source",
        NativeClaimSource::new(
            sync.group.clone(),
            budget.clone(),
            NativeClaimOptions {
                scope: scope.clone(),
                policy: claim,
                max_key_bytes: bound(32 << 10),
                max_value_bytes: bound(64 << 10),
            },
        )
        .ok(),
    )?);
    let listener_alive = Arc::new(AtomicBool::new(true));
    let state = Arc::new(required(
        "index state port",
        FleetStatePort::new(
            Arc::clone(&prepared.adapter),
            Arc::clone(&source),
            scope.clone(),
            fleet.buckets.clone(),
            Arc::clone(&listener_alive),
        )
        .ok(),
    )?);
    let donor = Arc::new(required(
        "bulk donor port",
        BulkDonorPort::new(
            Arc::clone(&state),
            plane.clone(),
            budget.clone(),
            BulkDonorOptions {
                scope: scope.clone(),
                policy: transfer,
                limits,
            },
        )
        .ok(),
    )?);
    let capabilities = BootstrapCapabilities {
        claims: source,
        donor,
        admission: budget.clone(),
    };
    let runtime_config = BootstrapRuntimeConfig {
        claim,
        transfer,
        max_claim_metadata_bytes: bound(64 << 10),
        donor_inbox_capacity: required("donor inbox", QueueCapacity::new(32))?,
        require_participation: true,
    };
    let (child, sender) = required(
        "bootstrap worker",
        BootstrapSession::new(
            capabilities,
            runtime_config,
            scope,
            PresenceIdentity {
                node: sync.me.clone(),
                boot,
                session: prepared.session,
            },
        )
        .ok(),
    )?;
    let listener = required(
        "bulk listener",
        BootstrapBulkListener::new(plane, sender, budget, limits).ok(),
    )?;
    let bootstrap =
        Box::new(child.with_observer(Arc::new(DecisionLog(Arc::clone(&prepared.adapter.metrics)))));
    let handle = prepared.builder().bootstrap(bootstrap).open().ok()?;
    let (shutdown, receiver) = watch::channel(false);
    let task = tokio::spawn(async move {
        if let Err(error) = listener.run(receiver).await {
            listener_alive.store(false, Ordering::Release);
            state.adapter.state.retire_fleet_service();
            tracing::warn!(
                ?error,
                "fleet bulk listener stopped; origin fallback remains available"
            );
        }
    });
    Some((handle, FleetListenerGuard { shutdown, task }))
}
