//! Groupnet donor callbacks: complete guarded C and immutable bounded chunks.

use std::mem::size_of;
use std::time::Instant;

use groupnet::consistency::volatile_recovery::bootstrap::admission::{
    AdmissionClass, Admitted, ByteAdmission,
};
use groupnet::consistency::volatile_recovery::bootstrap::bulk_adapter::BootstrapStatePort;
use groupnet::consistency::volatile_recovery::bootstrap::ports::{
    DonorCapture, LocalCaptureOutcome, LocalCaptureRequest, ReadyCaptureRequest, TransferContext,
    TransferResources,
};
use groupnet::consistency::volatile_recovery::{
    AdapterError, BoxRecoveryFuture, PublicationPermit,
};
use groupnet::core::volatile_bootstrap::journal::{AttachToken, JournalBatch, JournalCursor};
use groupnet::core::volatile_bootstrap::transfer::{TransferEffect, TransferEvent, TransferOffer};

use crate::index::fleet::{FleetDonorImage, FleetStage};

use super::{FleetStatePort, capture};

const CHUNK_BYTES: usize = 64 << 10;

impl BootstrapStatePort for FleetStatePort {
    type Image = FleetDonorImage;
    type Stage = FleetStage;
    // The native applier already publishes every accepted feed event under
    // KeyIndex's coordinator. This first slice waits for exact B cuts and
    // refuses any concurrent final effect at the guarded install.
    type NativeBuffer = ();

    fn build_local_capture<'a>(
        &'a self,
        request: LocalCaptureRequest,
        admission: &'a ByteAdmission,
    ) -> BoxRecoveryFuture<'a, Result<LocalCaptureOutcome<Self::Image>, AdapterError>> {
        Box::pin(self.initial_capture(request, admission))
    }

    fn recapture_current_index<'a>(
        &'a self,
        request: ReadyCaptureRequest,
        admission: &'a ByteAdmission,
    ) -> BoxRecoveryFuture<'a, Result<DonorCapture<Self::Image>, AdapterError>> {
        Box::pin(self.ready_capture(request, admission))
    }

    fn retire_local_capture(&self, capture: &DonorCapture<Self::Image>) {
        capture::retire(&self.adapter.state, capture.ingress());
    }

    fn image_offer(
        &self,
        capture: &DonorCapture<Self::Image>,
        max_metadata_bytes: usize,
        admission: &ByteAdmission,
    ) -> Result<Admitted<TransferOffer>, AdapterError> {
        if !capture.is_active() || !capture::still_attached(&self.adapter.state, capture) {
            return Err(AdapterError);
        }
        let bytes = capture.image().as_bytes().len();
        if bytes == 0 || bytes > capture::IMAGE_CAPS.bytes || max_metadata_bytes == 0 {
            return Err(AdapterError);
        }
        let chunks = bytes.div_ceil(CHUNK_BYTES);
        // Charge the complete retained DTO, both scope copies, cut vectors,
        // member vector, and conservative cloned-capacity headroom *before*
        // entering the journal and allocating any response-owned metadata.
        let headers = size_of::<TransferOffer>()
            .checked_add(
                capture::JOURNAL_CONFIG.max_members
                    * size_of::<groupnet::core::volatile_bootstrap::BootstrapMemberIdentity>(),
            )
            .and_then(|n| {
                n.checked_add(
                    capture::JOURNAL_CONFIG.max_cuts
                        * size_of::<groupnet::core::volatile_bootstrap::journal::NativeCut>(),
                )
            })
            .ok_or(AdapterError)?;
        let charge = max_metadata_bytes
            .checked_mul(4)
            .and_then(|n| n.checked_add(headers))
            .ok_or(AdapterError)?;
        let permit = admission
            .reserve(AdmissionClass::Inflight, charge)
            .map_err(|_| AdapterError)?;
        let offer = capture.ingress().with_journal(|journal| {
            let id = journal.id().clone();
            let members = journal.image_members().to_vec();
            let cuts = journal.image_cuts().to_vec();
            TransferOffer {
                image_cut: JournalCursor {
                    capture: id.clone(),
                    position: 0,
                },
                capture: id,
                schema: crate::index::fleet::IMAGE_SCHEMA,
                encoded_bytes: bytes,
                decoded_bytes: capture::IMAGE_CAPS.decoded_bytes,
                chunks,
                commitment: capture.image().commitment(),
                members,
                cuts,
            }
        });
        let variable = offer
            .capture
            .scope
            .domain
            .len()
            .checked_add(offer.capture.scope.partition.len())
            .and_then(|n| {
                offer
                    .members
                    .iter()
                    .try_fold(n, |sum, member| sum.checked_add(member.node.as_str().len()))
            })
            .and_then(|n| {
                offer
                    .cuts
                    .iter()
                    .try_fold(n, |sum, cut| sum.checked_add(cut.writer.len()))
            })
            .ok_or(AdapterError)?;
        if variable > max_metadata_bytes || !capture.is_active() {
            return Err(AdapterError);
        }
        Ok(permit.hold(offer))
    }

    fn image_chunk(
        &self,
        capture: &DonorCapture<Self::Image>,
        sequence: usize,
        max_bytes: usize,
        admission: &ByteAdmission,
    ) -> Result<Admitted<Vec<u8>>, AdapterError> {
        if !capture.is_active()
            || !capture::still_attached(&self.adapter.state, capture)
            || max_bytes == 0
            || max_bytes > CHUNK_BYTES
        {
            return Err(AdapterError);
        }
        let start = sequence.checked_mul(CHUNK_BYTES).ok_or(AdapterError)?;
        let end = start
            .checked_add(CHUNK_BYTES)
            .ok_or(AdapterError)?
            .min(capture.image().as_bytes().len());
        let slice = capture
            .image()
            .as_bytes()
            .get(start..end)
            .ok_or(AdapterError)?;
        if slice.is_empty() || slice.len() > max_bytes {
            return Err(AdapterError);
        }
        let permit = admission
            .reserve(AdmissionClass::Encoded, max_bytes)
            .map_err(|_| AdapterError)?;
        let mut copy = Vec::new();
        copy.try_reserve_exact(slice.len())
            .map_err(|_| AdapterError)?;
        if copy.capacity() > max_bytes {
            return Err(AdapterError);
        }
        copy.extend_from_slice(slice);
        Ok(permit.hold(copy))
    }

    fn execute_local<'a>(
        &'a self,
        context: &'a TransferContext,
        effect: TransferEffect,
        resources: &'a mut TransferResources<Self::Stage, AttachToken, Self::NativeBuffer>,
        admission: &'a ByteAdmission,
        permit: Option<PublicationPermit>,
        deadline: Instant,
    ) -> BoxRecoveryFuture<'a, Result<Option<Admitted<TransferEvent>>, AdapterError>> {
        Box::pin(self.execute_follower(context, effect, resources, admission, permit, deadline))
    }

    fn store_chunk<'a>(
        &'a self,
        sequence: usize,
        chunk: &'a Admitted<Vec<u8>>,
        resources: &'a mut TransferResources<Self::Stage, AttachToken, Self::NativeBuffer>,
    ) -> BoxRecoveryFuture<'a, Result<usize, AdapterError>> {
        Box::pin(async move {
            let stage = resources.stage.as_mut().ok_or(AdapterError)?;
            stage
                .stage_mut()
                .store_chunk(sequence, chunk.get())
                .map_err(|_| AdapterError)?;
            Ok(stage.reserved_bytes().1)
        })
    }

    fn stage_batch<'a>(
        &'a self,
        batch: &'a Admitted<JournalBatch>,
        resources: &'a mut TransferResources<Self::Stage, AttachToken, Self::NativeBuffer>,
    ) -> BoxRecoveryFuture<'a, Result<(), AdapterError>> {
        Box::pin(async move {
            resources
                .stage
                .as_mut()
                .ok_or(AdapterError)?
                .stage_mut()
                .stage_batch(batch.get())
                .map_err(|_| AdapterError)
        })
    }

    fn detach<'a>(
        &'a self,
        token: AttachToken,
        resources: &'a mut TransferResources<Self::Stage, AttachToken, Self::NativeBuffer>,
    ) -> BoxRecoveryFuture<'a, Result<(), AdapterError>> {
        Box::pin(async move {
            if resources.attachment.as_ref() != Some(&token) {
                return Err(AdapterError);
            }
            resources.attachment.take();
            Ok(())
        })
    }
}
