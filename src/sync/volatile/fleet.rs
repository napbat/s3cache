//! S3 index facts supplied to Groupnet's one optional bootstrap worker.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use groupnet::consistency::volatile_recovery::bootstrap::admission::ByteAdmission;
use groupnet::consistency::volatile_recovery::bootstrap::native_claims::NativeClaimSource;
use groupnet::consistency::volatile_recovery::bootstrap::ports::{
    ClaimObservationLimits, ClaimSource, DonorCapture, LocalCaptureOutcome, LocalCaptureRequest,
    ParticipationSnapshot, ReadyCaptureRequest,
};
use groupnet::core::volatile_bootstrap::{
    BootstrapMemberIdentity, BootstrapOperation, BootstrapScope,
};

use crate::index::fleet::FleetDonorImage;

use super::{AdapterError, CacheRecoveryAdapter, RecoveryAdapter};

mod capture;
mod donor;
mod follower;
mod runtime;
pub(crate) use runtime::FleetListenerGuard;
pub(super) use runtime::open_recovery;

const MAX_ROSTER: usize = 64;
const MAX_ROSTER_ID_BYTES: usize = 256;
const MAX_ROSTER_METADATA: usize = 64 << 10;

/// Consumer index/source evidence. Claim election, bulk retry, and lifetime
/// decisions remain in the existing Groupnet recovery worker.
pub(super) struct FleetStatePort {
    adapter: Arc<CacheRecoveryAdapter>,
    claims: Arc<NativeClaimSource>,
    scope: BootstrapScope,
    universe: Vec<String>,
    listener_alive: Arc<AtomicBool>,
}

impl FleetStatePort {
    /// Native TTL is sampled inside the actor, so age it through callback
    /// transit with the same upward rounding used by Groupnet's worker.
    fn fresh_roster(
        snapshot: &ParticipationSnapshot,
        expected: &[BootstrapMemberIdentity],
    ) -> bool {
        let Some(elapsed) = Instant::now().checked_duration_since(snapshot.sampled_at) else {
            return false;
        };
        let Some(age) = elapsed
            .as_nanos()
            .checked_add(999_999)
            .map(|nanos| nanos / 1_000_000)
            .and_then(|millis| u64::try_from(millis).ok())
            .and_then(|millis| millis.checked_add(1))
        else {
            return false;
        };
        snapshot.roster == expected
            && snapshot.participants.iter().all(|p| p.remaining_ms > age)
            && snapshot
                .roster
                .iter()
                .filter(|m| m.presence.is_some())
                .all(|member| {
                    snapshot
                        .participants
                        .iter()
                        .any(|participant| &participant.member == member)
                })
    }

    pub(super) fn new(
        adapter: Arc<CacheRecoveryAdapter>,
        claims: Arc<NativeClaimSource>,
        scope: BootstrapScope,
        universe: Vec<String>,
        listener_alive: Arc<AtomicBool>,
    ) -> Result<Self, AdapterError> {
        if universe.is_empty()
            || universe.len() > capture::IMAGE_CAPS.buckets
            || universe.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(AdapterError);
        }
        Ok(Self {
            adapter,
            claims,
            scope,
            universe,
            listener_alive,
        })
    }

    fn listener_alive(&self) -> bool {
        self.listener_alive.load(Ordering::Acquire)
    }

    async fn source_matches(
        &self,
        op: BootstrapOperation,
        expected: &[BootstrapMemberIdentity],
        admission: &ByteAdmission,
        deadline: Instant,
    ) -> bool {
        let limits = ClaimObservationLimits {
            max_members: MAX_ROSTER,
            max_member_bytes: MAX_ROSTER_ID_BYTES,
            max_metadata_bytes: MAX_ROSTER_METADATA,
        };
        let observed = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            self.claims.observe_participation(op, limits, admission),
        )
        .await;
        observed.is_ok_and(|result| {
            result.is_ok_and(|snapshot| Self::fresh_roster(snapshot.get(), expected))
        })
    }

    pub(super) async fn initial_capture(
        &self,
        request: LocalCaptureRequest,
        _admission: &ByteAdmission,
    ) -> Result<LocalCaptureOutcome<FleetDonorImage>, AdapterError> {
        self.adapter
            .rebuild_origin(request.recovery, request.permit.clone())
            .await?;
        // The scan is a complete local baseline. Return it before the original
        // build deadline instead of starting another bounded actor observation
        // and large encode that could time out after the successful scan and
        // cause a second origin LIST. Once local recovery reaches Ready, the
        // same Groupnet worker attempts one finite guarded index recapture.
        if !request.permit.valid() {
            return Err(AdapterError);
        }
        Ok(LocalCaptureOutcome::LocalOnly)
    }

    async fn source_matches_pending(
        &self,
        op: BootstrapOperation,
        pending: &crate::index::fleet::PendingFleetCapture,
        admission: &ByteAdmission,
        deadline: Instant,
    ) -> bool {
        let limits = ClaimObservationLimits {
            max_members: MAX_ROSTER,
            max_member_bytes: MAX_ROSTER_ID_BYTES,
            max_metadata_bytes: MAX_ROSTER_METADATA,
        };
        let observed = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            self.claims.observe_participation(op, limits, admission),
        )
        .await;
        observed.is_ok_and(|result| {
            result.is_ok_and(|snapshot| {
                self.listener_alive()
                    && Self::fresh_roster(snapshot.get(), &snapshot.get().roster)
                    && capture::exact_roster(pending, &snapshot.get().roster)
            })
        })
    }

    pub(super) async fn ready_capture(
        &self,
        request: ReadyCaptureRequest,
        admission: &ByteAdmission,
    ) -> Result<DonorCapture<FleetDonorImage>, AdapterError> {
        let sync = self.adapter.sync.upgrade().ok_or(AdapterError)?;
        if !self.listener_alive()
            || !self
                .source_matches(
                    request.operation,
                    &request.members,
                    admission,
                    request.deadline,
                )
                .await
        {
            tracing::debug!("fleet Ready recapture declined: source roster changed");
            return Err(AdapterError);
        }
        let started = Instant::now();
        let id = capture::next_id(
            self.scope.clone(),
            request.selected.clone(),
            request.recovery_generation,
        )?;
        let prepared = capture::prepare(admission, id)?;
        // Measuring and cloning C hold the Ready guard's publication fence and
        // the index write lock for as long as the bucket makes the clone
        // (about 300 ms at 800k rows in release). The blocking pool runs that
        // same critical section without stalling a runtime worker. A dropped
        // waiter detaches it, and Drop of its result unlinks C again.
        let guard = request.guard.clone();
        let index = Arc::clone(&self.adapter.state);
        let budget = admission.clone();
        let universe = self.universe.clone();
        let listener = Arc::clone(&self.listener_alive);
        let local = Arc::clone(&sync);
        let (generation, members, clock, wake) = (
            request.recovery_generation,
            request.members,
            request.clock,
            request.wake,
        );
        let pending = tokio::task::spawn_blocking(move || {
            guard
                .capture(|current| {
                    if current != generation
                        || !local.mode_allows_local()
                        || !listener.load(Ordering::Acquire)
                    {
                        return Err(AdapterError);
                    }
                    prepared.attach(&index, &budget, members, &universe, clock, wake)
                })
                .ok_or(AdapterError)
                .flatten()
        })
        .await
        .map_err(|_| AdapterError)
        .flatten()
        .inspect_err(|_| {
            tracing::debug!(
                "fleet Ready recapture declined at C: stale guard, local reads \
                 closed, or the measured image is over a ceiling or admission"
            );
        })?;
        let size = pending.size();
        let pending = capture::encode(pending, self.universe.clone()).await?;
        if !self
            .source_matches_pending(request.operation, &pending, admission, request.deadline)
            .await
        {
            tracing::debug!("fleet Ready recapture declined: roster changed while encoding");
            return Err(AdapterError);
        }
        let captured = request
            .guard
            .capture(|generation| {
                if generation != request.recovery_generation
                    || !sync.mode_allows_local()
                    || !self.listener_alive()
                {
                    return Err(AdapterError);
                }
                capture::finish(pending)
            })
            .ok_or(AdapterError)
            .flatten();
        match &captured {
            Ok(_) => tracing::info!(
                rows = size.rows,
                encoded_bytes = size.encoded_bytes,
                decoded_charge = size.decoded_bytes,
                capture_ms = started.elapsed().as_millis(),
                "fleet donor image captured"
            ),
            Err(_) => tracing::debug!("fleet Ready recapture declined at its guarded finish"),
        }
        captured
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use groupnet::consistency::volatile_recovery::bootstrap::ports::TimedParticipant;
    use groupnet::core::volatile_bootstrap::{BootId, BootstrapMember, PresenceIdentity};
    use groupnet::core::{NodeId, Status};

    use super::*;

    #[test]
    fn final_install_rejects_roster_change_or_ttl_expiry_after_valid_barrier() {
        let presence = PresenceIdentity {
            node: NodeId::from("donor"),
            boot: BootId(1),
            session: 2,
        };
        let member = BootstrapMemberIdentity {
            node: presence.node.clone(),
            presence: Some(presence),
            member_incarnation: 3,
            status: Status::Alive,
        };
        let barrier = ParticipationSnapshot {
            sampled_at: Instant::now(),
            members: vec![BootstrapMember {
                node: member.node.clone(),
                eligible: true,
            }],
            roster: vec![member.clone()],
            participants: vec![TimedParticipant {
                member: member.clone(),
                renewal: 1,
                remaining_ms: 2_000,
            }],
            claims: Vec::new(),
        };
        // B accepts a live, exact native cut. InstallCandidate must obtain a
        // *new* cut, not reuse that successful B observation.
        assert!(FleetStatePort::fresh_roster(
            &barrier,
            std::slice::from_ref(&member)
        ));

        let mut changed = barrier.clone();
        changed.roster[0].member_incarnation += 1;
        assert!(!FleetStatePort::fresh_roster(
            &changed,
            std::slice::from_ref(&member)
        ));

        let mut expired = barrier;
        expired.sampled_at -= Duration::from_millis(2_001);
        assert!(!FleetStatePort::fresh_roster(&expired, &[member]));
    }
}
