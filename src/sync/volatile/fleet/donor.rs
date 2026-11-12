//! Groupnet donor callbacks: complete guarded C and immutable bounded chunks.

use std::mem::size_of;
use std::num::NonZeroUsize;

use groupnet::consistency::volatile_recovery::bootstrap::admission::{
    AdmissionClass, Admitted, ByteAdmission,
};
use groupnet::consistency::volatile_recovery::bootstrap::bulk_adapter::BootstrapStatePort;
use groupnet::consistency::volatile_recovery::bootstrap::ports::{
    DonorCapture, EffectContext, LocalCaptureOutcome, LocalCaptureRequest, ReadyCaptureRequest,
    TransferResources,
};
use groupnet::consistency::volatile_recovery::{AdapterError, AdapterErrorKind};
use groupnet::core::volatile_bootstrap::journal::{AttachToken, JournalBatch, JournalCursor};
use groupnet::core::volatile_bootstrap::transfer::{TransferEffect, TransferEvent, TransferOffer};

use crate::index::fleet::{FleetDonorImage, FleetStage};
use crate::sync::volatile::fenced;

use super::{FleetStatePort, capture};

const CHUNK_BYTES: NonZeroUsize = NonZeroUsize::new(64 << 10).unwrap();

/// A request or argument outside this donor's bounds.
fn invalid() -> AdapterError {
    AdapterError::new(AdapterErrorKind::InvalidInput)
}

/// An arithmetic bound or memory admission refused the work.
fn capacity() -> AdapterError {
    AdapterError::new(AdapterErrorKind::Capacity)
}

impl BootstrapStatePort for FleetStatePort {
    type Image = FleetDonorImage;
    type Stage = FleetStage;
    // The native applier already publishes every accepted feed event under
    // KeyIndex's coordinator. This first slice waits for exact B cuts and
    // refuses any concurrent final effect at the guarded install.
    type NativeBuffer = ();

    async fn build_local_capture(
        &self,
        request: LocalCaptureRequest,
        admission: &ByteAdmission,
    ) -> Result<LocalCaptureOutcome<Self::Image>, AdapterError> {
        self.initial_capture(request, admission).await
    }

    async fn recapture_current_index(
        &self,
        request: ReadyCaptureRequest,
        admission: &ByteAdmission,
    ) -> Result<DonorCapture<Self::Image>, AdapterError> {
        self.ready_capture(request, admission).await
    }

    fn retire_local_capture(&self, capture: &DonorCapture<Self::Image>) {
        capture::retire(&self.adapter.state, capture.ingress());
    }

    fn image_offer(
        &self,
        capture: &DonorCapture<Self::Image>,
        max_metadata_bytes: NonZeroUsize,
        admission: &ByteAdmission,
    ) -> Result<Admitted<TransferOffer>, AdapterError> {
        if !capture.is_active() || !capture::still_attached(&self.adapter.state, capture) {
            return Err(fenced());
        }
        let bytes = NonZeroUsize::new(capture.image().as_bytes().len()).ok_or_else(invalid)?;
        if bytes.get() > capture::IMAGE_CAPS.bytes {
            return Err(capacity());
        }
        let chunks = bytes.div_ceil(CHUNK_BYTES);
        // Charge the complete retained DTO, both scope copies, cut vectors,
        // member vector, and conservative cloned-capacity headroom *before*
        // entering the journal and allocating any response-owned metadata.
        let headers = size_of::<TransferOffer>()
            .checked_add(
                capture::JOURNAL_CONFIG.max_members.get()
                    * size_of::<groupnet::core::volatile_bootstrap::BootstrapMemberIdentity>(),
            )
            .and_then(|n| {
                n.checked_add(
                    capture::JOURNAL_CONFIG.max_cuts.get()
                        * size_of::<groupnet::core::volatile_bootstrap::journal::NativeCut>(),
                )
            })
            .ok_or_else(capacity)?;
        let charge = max_metadata_bytes
            .checked_mul(NonZeroUsize::new(4).ok_or_else(capacity)?)
            .and_then(|n| n.checked_add(headers))
            .ok_or_else(capacity)?;
        let permit = admission
            .reserve(AdmissionClass::Inflight, charge)
            .map_err(|error| AdapterError::with_source(AdapterErrorKind::Capacity, error))?;
        // The decoded charge finished at C is the measured image charge the
        // follower must reserve and decode under.
        let offer = capture.ingress().with_journal(|journal| {
            let decoded_bytes = journal.image_charge()?.image.decoded_bytes;
            let id = journal.id().clone();
            let members = journal.image_members().to_vec();
            let cuts = journal.image_cuts().to_vec();
            Some(TransferOffer {
                image_cut: JournalCursor {
                    capture: id.clone(),
                    position: 0,
                },
                capture: id,
                schema: crate::index::fleet::IMAGE_SCHEMA,
                encoded_bytes: bytes,
                decoded_bytes,
                chunks,
                commitment: capture.image().commitment(),
                members,
                cuts,
            })
        });
        let offer = offer.ok_or_else(fenced)?;
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
            .ok_or_else(capacity)?;
        if variable > max_metadata_bytes.get() {
            return Err(capacity());
        }
        if !capture.is_active() {
            return Err(fenced());
        }
        Ok(permit.hold(offer))
    }

    fn image_chunk(
        &self,
        capture: &DonorCapture<Self::Image>,
        sequence: usize,
        max_bytes: NonZeroUsize,
        admission: &ByteAdmission,
    ) -> Result<Admitted<Vec<u8>>, AdapterError> {
        if !capture.is_active() || !capture::still_attached(&self.adapter.state, capture) {
            return Err(fenced());
        }
        if max_bytes > CHUNK_BYTES {
            return Err(invalid());
        }
        let start = sequence
            .checked_mul(CHUNK_BYTES.get())
            .ok_or_else(invalid)?;
        let end = start
            .checked_add(CHUNK_BYTES.get())
            .ok_or_else(invalid)?
            .min(capture.image().as_bytes().len());
        let slice = capture
            .image()
            .as_bytes()
            .get(start..end)
            .ok_or_else(invalid)?;
        if slice.is_empty() || slice.len() > max_bytes.get() {
            return Err(invalid());
        }
        let permit = admission
            .reserve(AdmissionClass::Encoded, max_bytes)
            .map_err(|error| AdapterError::with_source(AdapterErrorKind::Capacity, error))?;
        let mut copy = Vec::new();
        copy.try_reserve_exact(slice.len())
            .map_err(|error| AdapterError::with_source(AdapterErrorKind::Capacity, error))?;
        if copy.capacity() > max_bytes.get() {
            return Err(capacity());
        }
        copy.extend_from_slice(slice);
        Ok(permit.hold(copy))
    }

    async fn execute_local(
        &self,
        effect: TransferEffect,
        resources: &mut TransferResources<Self::Stage, AttachToken, Self::NativeBuffer>,
        context: EffectContext<'_>,
    ) -> Result<Option<Admitted<TransferEvent>>, AdapterError> {
        self.execute_follower(effect, resources, context).await
    }

    fn store_chunk(
        &self,
        sequence: usize,
        chunk: &Admitted<Vec<u8>>,
        resources: &mut TransferResources<Self::Stage, AttachToken, Self::NativeBuffer>,
    ) -> impl Future<Output = Result<usize, AdapterError>> + Send {
        std::future::ready(
            resources
                .stage
                .as_mut()
                .ok_or_else(fenced)
                .and_then(|stage| {
                    stage
                        .stage_mut()
                        .store_chunk(sequence, chunk.get())
                        .map_err(|_| AdapterError::new(AdapterErrorKind::Malformed))?;
                    Ok(stage.reserved_bytes().1)
                }),
        )
    }

    fn stage_batch(
        &self,
        batch: &Admitted<JournalBatch>,
        resources: &mut TransferResources<Self::Stage, AttachToken, Self::NativeBuffer>,
    ) -> impl Future<Output = Result<(), AdapterError>> + Send {
        std::future::ready(
            resources
                .stage
                .as_mut()
                .ok_or_else(fenced)
                .and_then(|stage| {
                    stage
                        .stage_mut()
                        .stage_batch(batch.get())
                        .map_err(|_| AdapterError::new(AdapterErrorKind::Malformed))
                }),
        )
    }

    fn detach(
        &self,
        token: AttachToken,
        resources: &mut TransferResources<Self::Stage, AttachToken, Self::NativeBuffer>,
    ) -> impl Future<Output = Result<(), AdapterError>> + Send {
        std::future::ready(if resources.attachment.as_ref() == Some(&token) {
            resources.attachment.take();
            Ok(())
        } else {
            Err(fenced())
        })
    }
}
