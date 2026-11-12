//! Finite donor C capture, with all large encoding outside the index lock.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use groupnet::consistency::volatile_recovery::bootstrap::admission::{
    AdmissionClass, ByteAdmission, Reservation,
};
use groupnet::consistency::volatile_recovery::bootstrap::ports::{
    DonorCapture, JournalIngress, LogicalClock,
};
use groupnet::core::volatile_bootstrap::journal::{CaptureId, DonorJournal, JournalConfig};
use groupnet::core::volatile_bootstrap::{BootstrapMemberIdentity, BootstrapScope, ClaimIdentity};
use tokio::sync::Notify;

use crate::index::KeyIndex;
use crate::index::fleet::{FleetDonorImage, ImageCaps, PendingFleetCapture};

use super::super::{AdapterError, AdapterErrorKind, fenced};

static NEXT_CAPTURE: AtomicU64 = AtomicU64::new(1);

/// The image and suffix policy, sized for a real bucket. A production docres row
/// encodes to about 135 bytes and is charged about 650 decoded bytes (measured:
/// 800k rows, 108 MB encoded, 512 MiB charged; C held the index lock for
/// microseconds and the snapshot measured and encoded off-lock in about 200 ms
/// in release), so these caps (the codec's ceilings) offer an index of up to
/// about 1.5M rows; the 798k-row production index fits with headroom. A larger
/// index declines peer transfer and continues guarded origin recovery.
pub(super) const IMAGE_CAPS: ImageCaps = ImageCaps {
    bytes: 256 << 20,
    decoded_bytes: 1 << 30,
    buckets: 256,
    rows: 2_000_000,
    name_bytes: 1_024,
};

/// `n` as a bound Groupnet accepts; a zero bound fails to compile.
pub(super) const fn bound(n: usize) -> NonZeroUsize {
    NonZeroUsize::new(n).expect("journal bounds are nonzero")
}

pub(super) const JOURNAL_CONFIG: JournalConfig = JournalConfig {
    max_encoded_bytes: bound(IMAGE_CAPS.bytes),
    max_decoded_bytes: bound(IMAGE_CAPS.decoded_bytes),
    max_events: bound(1_024),
    max_suffix_bytes: bound(8 << 20),
    max_event_bytes: bound(4_096),
    max_identity_bytes: bound(256),
    max_followers: bound(2),
    max_follower_id_bytes: bound(256),
    max_cuts: bound(64),
    max_cut_bytes: bound(4_096),
    max_members: bound(64),
    max_membership_bytes: bound(16 << 10),
    max_scope_bytes: bound(8_192),
    max_batch_events: bound(32),
    max_batch_bytes: bound(128 << 10),
    max_inflight_bytes: bound(256 << 10),
    max_total_ms: 600_000,
    // A 108 MB production image is about 1,700 sequential 64 KiB chunks; give a
    // follower on a loaded node minutes, not one, before its reservation expires.
    max_follower_ms: 300_000,
};

pub(super) fn next_id(
    scope: BootstrapScope,
    donor: ClaimIdentity,
    generation: u64,
) -> Result<CaptureId, AdapterError> {
    let serial = NEXT_CAPTURE
        .try_update(Ordering::AcqRel, Ordering::Acquire, |old| {
            old.checked_add(1)
        })
        .map_err(|_| AdapterError::new(AdapterErrorKind::Capacity))?;
    if serial == 0 || generation == 0 {
        return Err(AdapterErrorKind::InvalidInput.into());
    }
    Ok(CaptureId {
        scope,
        donor,
        recovery_generation: generation,
        serial,
    })
}

pub(super) struct PreparedCapture {
    journal: DonorJournal,
    suffix: Reservation,
}

/// Reserve the journal suffix before entering recovery Control or the index
/// coordinator. The image itself is measured and reserved at C by `attach`;
/// a refusal at either step cannot leave a partly attached candidate.
pub(super) fn prepare(
    budget: &ByteAdmission,
    id: CaptureId,
) -> Result<PreparedCapture, AdapterError> {
    let suffix_bytes = DonorJournal::storage_bound(JOURNAL_CONFIG)
        .map_err(|error| AdapterError::with_source(AdapterErrorKind::InvalidInput, error))?;
    let suffix = budget
        .reserve(AdmissionClass::Suffix, suffix_bytes)
        .map_err(|error| AdapterError::with_source(AdapterErrorKind::Capacity, error))?;
    let journal = DonorJournal::new(JOURNAL_CONFIG, id)
        .map_err(|error| AdapterError::with_source(AdapterErrorKind::InvalidInput, error))?;
    Ok(PreparedCapture { journal, suffix })
}

impl PreparedCapture {
    /// Attach C under one current Control-to-index publication callback,
    /// reserving exactly the image measured there from `budget`.
    pub(super) fn attach(
        self,
        index: &Arc<KeyIndex>,
        budget: &ByteAdmission,
        members: Vec<BootstrapMemberIdentity>,
        universe: &[String],
        clock: LogicalClock,
        wake: Arc<Notify>,
    ) -> Result<PendingFleetCapture, AdapterError> {
        index
            .begin_fleet_capture(
                self.journal,
                self.suffix,
                budget,
                wake,
                members,
                universe,
                IMAGE_CAPS,
                clock,
            )
            .map_err(|_| AdapterError::new(AdapterErrorKind::Capacity))
    }
}

/// The pending owner and its permits move to the blocking pool together. A
/// cancelled async waiter merely detaches the task; Drop unlinks C only when
/// actual encoding work has finished and the charged buffers are retired.
pub(super) async fn encode(
    mut pending: PendingFleetCapture,
    universe: Vec<String>,
) -> Result<PendingFleetCapture, AdapterError> {
    tokio::task::spawn_blocking(move || {
        pending
            .encode(&universe, IMAGE_CAPS)
            .map_err(|_| AdapterError::new(AdapterErrorKind::Capacity))?;
        Ok(pending)
    })
    .await
    .map_err(|error| AdapterError::with_source(AdapterErrorKind::Unavailable, error))?
}

pub(super) fn finish(
    pending: PendingFleetCapture,
) -> Result<DonorCapture<FleetDonorImage>, AdapterError> {
    pending.finish().map_err(|_| fenced())
}

pub(super) fn same_roster(
    pending: &PendingFleetCapture,
    observed: &[BootstrapMemberIdentity],
) -> bool {
    pending.same_membership_as_image(observed)
}

pub(super) fn still_attached(index: &KeyIndex, capture: &DonorCapture<FleetDonorImage>) -> bool {
    index.has_fleet_capture(capture.ingress())
}

pub(super) fn retire(index: &KeyIndex, ingress: &JournalIngress) {
    index.retire_fleet_capture(ingress);
}
