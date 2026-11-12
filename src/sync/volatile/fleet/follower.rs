//! Private follower staging and one guarded native-feed/index handoff.

use std::mem::size_of;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use groupnet::consistency::volatile_recovery::bootstrap::admission::{
    AdmissionClass, Admitted, ByteAdmission, Reservation,
};
use groupnet::consistency::volatile_recovery::bootstrap::ports::{
    EffectContext, StageResources, TransferResources,
};
use groupnet::consistency::volatile_recovery::{AdapterError, AdapterErrorKind};
use groupnet::core::volatile_bootstrap::journal::AttachToken;
use groupnet::core::volatile_bootstrap::transfer::{
    NativeCoverageReceipt, NativeHandoffReceipt, TransferEffect, TransferEvent,
};

use crate::index::fleet::{FleetStage, IMAGE_SCHEMA, InstallRefusal};
use crate::sync::volatile::fenced;

use super::{FleetStatePort, capture};

static NEXT_APPLIER: AtomicU64 = AtomicU64::new(1);

impl FleetStatePort {
    fn event_charge(
        admission: &ByteAdmission,
        metadata: bool,
    ) -> Result<Reservation, AdapterError> {
        let bytes = if metadata {
            // B, proven cuts, membership, and handoff metadata coexist with
            // the original effect and retained stage. Charge this physical
            // result copy separately from Groupnet's core event envelope.
            NonZeroUsize::new(64_usize << 10)
                .and_then(|n| n.checked_mul(NonZeroUsize::new(4)?))
                .and_then(|n| n.checked_add(size_of::<TransferEvent>()))
                .ok_or(AdapterErrorKind::Capacity)?
        } else {
            NonZeroUsize::new(size_of::<TransferEvent>()).unwrap_or(NonZeroUsize::MIN)
        };
        admission
            .reserve(AdmissionClass::Inflight, bytes)
            .map_err(|error| AdapterError::with_source(AdapterErrorKind::Capacity, error))
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one typed effect at a time under the existing Groupnet worker"
    )]
    pub(super) async fn execute_follower(
        &self,
        effect: TransferEffect,
        resources: &mut TransferResources<FleetStage, AttachToken, ()>,
        context: EffectContext<'_>,
    ) -> Result<Option<Admitted<TransferEvent>>, AdapterError> {
        let EffectContext {
            transfer: context,
            admission,
            permit,
            deadline,
            ..
        } = context;
        if Instant::now() >= deadline {
            return Err(AdapterErrorKind::Expired.into());
        }
        match effect {
            TransferEffect::ReserveStage {
                op,
                image_cut,
                chunks,
                encoded_bytes,
                decoded_bytes,
            } => {
                if resources.stage.is_some()
                    || encoded_bytes.get() > capture::IMAGE_CAPS.bytes
                    || decoded_bytes.get() > capture::IMAGE_CAPS.decoded_bytes
                {
                    tracing::debug!(
                        encoded_bytes,
                        decoded_bytes,
                        "fleet peer image declined: over the image codec ceilings"
                    );
                    return Err(AdapterErrorKind::Capacity.into());
                }
                let reserved = admission
                    .reserve(AdmissionClass::Encoded, encoded_bytes)
                    .and_then(|encoded| {
                        admission
                            .reserve(AdmissionClass::Decoded, decoded_bytes)
                            .map(|decoded| (encoded, decoded))
                    });
                let (encoded, decoded) = reserved.map_err(|error| {
                    tracing::debug!(
                        encoded_bytes,
                        decoded_bytes,
                        "fleet peer image declined: memory admission"
                    );
                    AdapterError::with_source(AdapterErrorKind::Capacity, error)
                })?;
                let stage = FleetStage::new(
                    encoded_bytes.get(),
                    chunks.get(),
                    image_cut,
                    decoded_bytes.get(),
                    capture::IMAGE_CAPS,
                    capture::JOURNAL_CONFIG.max_event_bytes.get(),
                )
                .map_err(|_| AdapterError::new(AdapterErrorKind::InvalidInput))?;
                resources.stage = Some(StageResources::new(stage, encoded, decoded)?);
                Ok(Some(
                    Self::event_charge(admission, false)?.hold(TransferEvent::StageReserved { op }),
                ))
            }
            TransferEffect::VerifyImage { op, commitment } => {
                let mut stage = resources.stage.take().ok_or_else(fenced)?;
                // Hash and full map decode are bounded but potentially large.
                // The blocking task owns the stage and both permits until it
                // exits, even if the recovery worker is cancelled.
                let verified = tokio::task::spawn_blocking(move || {
                    stage
                        .stage_mut()
                        .verify(commitment)
                        .map_err(|_| AdapterError::new(AdapterErrorKind::Malformed))?;
                    Ok::<_, AdapterError>(stage)
                })
                .await
                .map_err(|error| AdapterError::with_source(AdapterErrorKind::Unavailable, error))
                .flatten()
                .inspect_err(|_| {
                    tracing::debug!("fleet peer image declined: commitment or decode failed");
                })?;
                resources.stage = Some(verified);
                Ok(Some(
                    Self::event_charge(admission, false)?
                        .hold(TransferEvent::ImageVerified { op, commitment }),
                ))
            }
            TransferEffect::CheckNativeCoverage { op, receipt, .. } => {
                let stage_matches = resources
                    .stage
                    .as_mut()
                    .is_some_and(|stage| stage.stage_mut().through() == &receipt.cursor);
                if !stage_matches
                    || receipt.reservation.follower != context.follower
                    || !self
                        .source_matches(op, &receipt.members, admission, deadline)
                        .await
                {
                    return Err(fenced());
                }
                let stage = resources.stage.as_mut().ok_or_else(fenced)?;
                match stage.stage_mut().check_coverage(
                    &self.adapter.state,
                    &receipt.covered_cuts,
                    &self.universe,
                ) {
                    Ok(()) => {}
                    // Either side may still be applying the same feeds;
                    // Groupnet samples a later barrier for this stage.
                    Err(InstallRefusal::Pending(cut)) => {
                        tracing::debug!(?cut, "fleet peer image pending native cuts");
                        return Ok(Some(
                            Self::event_charge(admission, false)?
                                .hold(TransferEvent::NativePending { op }),
                        ));
                    }
                    Err(InstallRefusal::Incompatible(why)) => {
                        tracing::info!(
                            refusal = %why,
                            "fleet peer image declined: live index incompatible"
                        );
                        return Err(AdapterErrorKind::InvalidInput.into());
                    }
                }
                let charge = Self::event_charge(admission, true)?;
                let coverage = NativeCoverageReceipt {
                    parent: context.parent,
                    staged_through: receipt.cursor.clone(),
                    proven_cuts: receipt.covered_cuts.clone(),
                    members: receipt.members.clone(),
                    buffered_bytes: 0,
                    barrier: receipt,
                };
                Ok(Some(
                    charge.hold(TransferEvent::NativeCovered { op, coverage }),
                ))
            }
            TransferEffect::InstallCandidate { op, coverage } => {
                let permit = permit.ok_or_else(fenced)?.restricted_to(deadline);
                let attachment = resources.attachment.as_ref().ok_or_else(fenced)?;
                if coverage.parent != context.parent
                    || coverage.barrier.reservation != attachment.reservation
                    || coverage.barrier.attach_operation != attachment.operation
                    || coverage.staged_through != coverage.barrier.cursor
                    || coverage.buffered_bytes != 0
                {
                    return Err(AdapterErrorKind::InvalidInput.into());
                }
                // Membership can change after the B coverage observation even
                // when the local index publication serial stays unchanged.
                // Reobserve before entering the non-awaiting publication guard.
                if !self
                    .source_matches(op, &coverage.members, admission, deadline)
                    .await
                {
                    return Err(fenced());
                }
                let charge = Self::event_charge(admission, true)?;
                let continued_cuts = coverage.proven_cuts.clone();
                let attachment = attachment.clone();
                let applier_generation = NEXT_APPLIER
                    .try_update(Ordering::AcqRel, Ordering::Acquire, |old| {
                        old.checked_add(1)
                    })
                    .map_err(|_| AdapterError::new(AdapterErrorKind::Capacity))?;
                if applier_generation == 0 {
                    return Err(AdapterErrorKind::Capacity.into());
                }
                let stage = resources.stage.as_mut().ok_or_else(fenced)?;
                let installed = permit
                    .publish(|| {
                        stage.stage_mut().install_into(
                            &self.adapter.state,
                            &coverage.proven_cuts,
                            &self.universe,
                        )
                    })
                    .ok_or_else(fenced)?;
                match installed {
                    // The image moved into the live index; release the
                    // emptied stage and its private memory permits.
                    Ok(()) => {
                        if let Some(mut emptied) = resources.stage.take() {
                            let stage = emptied.stage_mut();
                            let (rows, encoded_bytes) = stage.image();
                            tracing::info!(
                                rows,
                                encoded_bytes,
                                transfer_ms = stage.elapsed().as_millis(),
                                "fleet peer image installed"
                            );
                        }
                    }
                    // A live effect landed after the coverage check. Keep the
                    // stage; Groupnet samples a later barrier.
                    Err(InstallRefusal::Pending(cut)) => {
                        tracing::debug!(?cut, "fleet peer image install pending native cuts");
                        return Ok(Some(
                            Self::event_charge(admission, false)?
                                .hold(TransferEvent::NativePending { op }),
                        ));
                    }
                    Err(InstallRefusal::Incompatible(why)) => {
                        tracing::info!(
                            refusal = %why,
                            "fleet peer image declined at install: live index incompatible"
                        );
                        return Err(AdapterErrorKind::InvalidInput.into());
                    }
                }
                let handoff = NativeHandoffReceipt {
                    recovery: permit.operation(),
                    install: op,
                    coverage: *coverage,
                    attachment,
                    schema: IMAGE_SCHEMA,
                    applier_generation,
                    continued_cuts,
                    buffered_bytes: 0,
                };
                Ok(Some(charge.hold(TransferEvent::Installed {
                    op,
                    handoff: Box::new(handoff),
                })))
            }
            TransferEffect::DiscardStage { .. } => {
                resources.stage.take();
                resources.native_overlap.take();
                Ok(None)
            }
            _ => Err(AdapterErrorKind::Unsupported.into()),
        }
    }
}
