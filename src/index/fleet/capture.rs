//! Exact C publication under the live index lock; large encoding runs off-lock.

use std::sync::Arc;

use groupnet::consistency::volatile_recovery::bootstrap::admission::{AdmissionClass, Reservation};
use groupnet::consistency::volatile_recovery::bootstrap::ports::{
    DonorCapture, JournalIngress, LogicalClock,
};
use groupnet::core::volatile_bootstrap::BootstrapMemberIdentity;
use groupnet::core::volatile_bootstrap::journal::{
    CutAlignment, DonorJournal, Invalidation, NativeCut, align_cuts,
};
use tokio::sync::Notify;

use crate::index::{KeyIndex, KeyIndexState};

use super::{ImageCaps, ImageError, IndexCapture, clone_state, encode};

/// Exact pending C clone and all admission owners. Move this whole value into
/// blocking encoding work; cancellation leaves the charge and ingress alive
/// until the job exits, then Drop unlinks only this candidate.
pub(crate) struct PendingFleetCapture {
    index: Option<Arc<KeyIndex>>,
    ingress: JournalIngress,
    private: KeyIndexState,
    encoded_image: Option<Vec<u8>>,
    commitment: Option<[u8; 32]>,
    encoded: Option<Reservation>,
    decoded: Option<Reservation>,
}

/// Encoded immutable donor image. Unexpected worker retirement still unlinks
/// the exact live ingress before its storage charge can be reused.
pub(crate) struct FleetDonorImage {
    bytes: Vec<u8>,
    commitment: [u8; 32],
    index: Arc<KeyIndex>,
    ingress: JournalIngress,
}

impl FleetDonorImage {
    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub(crate) fn commitment(&self) -> [u8; 32] {
        self.commitment
    }
}

impl Drop for FleetDonorImage {
    fn drop(&mut self) {
        self.index.retire_fleet_capture(&self.ingress);
    }
}

impl PendingFleetCapture {
    /// Compare the fresh source cut with the exact C roster before a guarded
    /// finish. This borrows the already charged journal copy.
    pub(crate) fn image_members_equal(&self, observed: &[BootstrapMemberIdentity]) -> bool {
        self.ingress
            .with_journal(|journal| journal.image_members() == observed)
    }

    /// Encode away from the live publication lock, retaining both image permits.
    pub(crate) fn encode(
        &mut self,
        universe: &[String],
        caps: ImageCaps,
    ) -> Result<(), ImageError> {
        if self.encoded_image.is_some() {
            return Err(ImageError::Corrupt);
        }
        let image = encode(&self.private, universe, caps)?;
        self.commitment = Some(*blake3::hash(&image).as_bytes());
        self.encoded_image = Some(image);
        Ok(())
    }

    /// Finish C only from within a current Groupnet publication/Ready guard.
    /// The guard must check source roster, recovery generation, scope, and
    /// lease/feed evidence again before invoking this method.
    pub(crate) fn finish(mut self) -> Result<DonorCapture<FleetDonorImage>, ImageError> {
        let image = self.encoded_image.take().ok_or(ImageError::Incomplete)?;
        let private_size = self.private_size();
        {
            let live = self
                .index
                .as_ref()
                .ok_or(ImageError::Corrupt)?
                .inner
                .write()
                .map_err(|_| ImageError::Corrupt)?;
            let capture = live
                .capture
                .as_ref()
                .filter(|capture| capture.same_ingress(&self.ingress))
                .ok_or(ImageError::Incomplete)?;
            // The clock every local effect was journaled on, so finishing can never
            // land behind a write that arrived while the image was encoding.
            let now = capture.clock().now();
            self.ingress
                .with_journal(|journal| journal.finish_capture(now, image.len(), private_size))
                .map_err(|_| ImageError::Incomplete)?;
        }
        let encoded = self.encoded.take().ok_or(ImageError::Corrupt)?;
        let decoded = self.decoded.take().ok_or(ImageError::Corrupt)?;
        let image = FleetDonorImage {
            bytes: image,
            commitment: self.commitment.take().ok_or(ImageError::Corrupt)?,
            index: Arc::clone(self.index.as_ref().ok_or(ImageError::Corrupt)?),
            ingress: self.ingress.clone(),
        };
        let donor = DonorCapture::new(image, self.ingress.clone(), encoded, decoded)
            .map_err(|_| ImageError::Incomplete)?;
        self.index.take();
        Ok(donor)
    }

    fn private_size(&self) -> usize {
        self.decoded.as_ref().map_or(0, Reservation::bytes)
    }
}

impl Drop for PendingFleetCapture {
    fn drop(&mut self) {
        if let Some(index) = &self.index {
            index.retire_fleet_capture(&self.ingress);
        }
    }
}

/// Why a verified private stage cannot replace the live index now.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum InstallRefusal {
    /// Live native positions differ from the barrier in either direction.
    /// The stage is retained and a later barrier may align it.
    Pending,
    /// A structural or version difference that no later barrier of this
    /// capture can repair, such as an origin-validated local repair.
    Incompatible,
}

/// Whether `candidate`, staged exactly through the barrier `cuts`, may
/// replace `live` without losing or reordering any live effect.
fn check_candidate(
    live: &KeyIndexState,
    candidate: &KeyIndexState,
    cuts: &[NativeCut],
    universe: &[String],
    max_rows: usize,
) -> Result<(), InstallRefusal> {
    use InstallRefusal::{Incompatible, Pending};
    if universe.is_empty()
        || universe.windows(2).any(|pair| pair[0] >= pair[1])
        || candidate.buckets.len() != universe.len()
        || live
            .buckets
            .keys()
            .any(|name| universe.binary_search(name).is_err())
        || live
            .buckets
            .values()
            .any(|bucket| bucket.rebuild_generation.is_some() || !bucket.uncertain_keys.is_empty())
    {
        return Err(Incompatible);
    }
    match align_cuts(
        live.native_cuts
            .iter()
            .map(|(writer, (epoch, sequence))| (writer.as_slice(), *epoch, *sequence)),
        cuts,
    ) {
        CutAlignment::Exact => {}
        CutAlignment::Pending => return Err(Pending),
        CutAlignment::Conflict => return Err(Incompatible),
    }
    for name in universe {
        let bucket = candidate.buckets.get(name).ok_or(Incompatible)?;
        if !bucket.synced || !bucket.uncertain_keys.is_empty() {
            return Err(Incompatible);
        }
    }
    // A locally origin-validated repair is ordered by no native writer cut.
    // Accept only a version-identical candidate; an incomparable write,
    // delete, or missing row declines peer installation.
    let mut rows = 0usize;
    for (name, bucket) in &live.buckets {
        let incoming = candidate.buckets.get(name).ok_or(Incompatible)?;
        rows = rows
            .checked_add(bucket.keys.len())
            .and_then(|rows| rows.checked_add(bucket.gone.len()))
            .ok_or(Incompatible)?;
        if rows > max_rows {
            return Err(Incompatible);
        }
        for (key, entry) in &bucket.keys {
            let peer = incoming.keys.get(key).ok_or(Incompatible)?;
            if peer.last_modified != entry.last_modified
                || peer.etag != entry.etag
                || peer.size != entry.size
                || peer.storage_class != entry.storage_class
                || incoming
                    .gone
                    .get(key)
                    .is_some_and(|time| *time >= entry.last_modified)
            {
                return Err(Incompatible);
            }
        }
        for (key, deleted_at) in &bucket.gone {
            // A donor's complete absence is not the native writer proof
            // needed to retire a local tombstone: delayed local repairs
            // or replay may still consult it after this swap.
            if incoming.keys.contains_key(key) || incoming.gone.get(key) != Some(deleted_at) {
                return Err(Incompatible);
            }
        }
    }
    Ok(())
}

impl KeyIndex {
    /// Pre-check a staged candidate against the live index at one barrier.
    /// [`Self::install_fleet_candidate`] repeats the same check under the
    /// write lock, so a live effect in between can only refuse the swap.
    pub(in crate::index::fleet) fn fleet_install_check(
        &self,
        candidate: &KeyIndexState,
        cuts: &[NativeCut],
        universe: &[String],
        max_rows: usize,
    ) -> Result<(), InstallRefusal> {
        let live = self
            .inner
            .read()
            .map_err(|_| InstallRefusal::Incompatible)?;
        check_candidate(&live, candidate, cuts, universe, max_rows)
    }

    /// Swap a verified private stage into the live index at one guarded
    /// publication point, checking it under the same write lock. The caller
    /// holds the exact Groupnet install permit; no source callback can await
    /// here. A refusal leaves `stage` untouched for a later barrier.
    pub(in crate::index::fleet) fn install_fleet_candidate(
        &self,
        stage: &mut Option<KeyIndex>,
        cuts: &[NativeCut],
        universe: &[String],
        max_rows: usize,
    ) -> Result<(), InstallRefusal> {
        let mut live = self
            .inner
            .write()
            .map_err(|_| InstallRefusal::Incompatible)?;
        {
            let staged = stage.as_ref().ok_or(InstallRefusal::Incompatible)?;
            let candidate = staged
                .inner
                .read()
                .map_err(|_| InstallRefusal::Incompatible)?;
            check_candidate(&live, &candidate, cuts, universe, max_rows)?;
        }
        let mut candidate = stage
            .take()
            .ok_or(InstallRefusal::Incompatible)?
            .inner
            .into_inner()
            .map_err(|_| InstallRefusal::Incompatible)?;
        for name in universe {
            let bucket = candidate
                .buckets
                .get_mut(name)
                .ok_or(InstallRefusal::Incompatible)?;
            let old = live.buckets.get(name);
            bucket.sync_generation = old
                .map_or(0, |prior| prior.sync_generation)
                .checked_add(1)
                .ok_or(InstallRefusal::Incompatible)?;
            bucket.uncertainty_epoch = old.map_or(0, |prior| prior.uncertainty_epoch);
        }
        if let Some(capture) = &live.capture {
            capture.invalidate(Invalidation::Rebuild);
        }
        candidate.native_cuts = std::mem::take(&mut live.native_cuts);
        *live = candidate;
        Ok(())
    }

    /// Capture exactly the state and native cuts at C. The caller must invoke
    /// this inside a current recovery publication guard (Control -> `KeyIndex`),
    /// with encoded, decoded and suffix reservations acquired beforehand.
    /// A later source signal cannot pass the guard until this bounded clone
    /// and ingress attachment finish. No network or origin I/O runs here.
    #[expect(
        clippy::too_many_arguments,
        reason = "one atomic C capture owns the journal, three charges, roster, and index cut"
    )]
    pub(crate) fn begin_fleet_capture(
        self: &Arc<Self>,
        mut journal: DonorJournal,
        suffix: Reservation,
        encoded: Reservation,
        decoded: Reservation,
        changed: Arc<Notify>,
        members: Vec<BootstrapMemberIdentity>,
        universe: &[String],
        caps: ImageCaps,
        clock: LogicalClock,
    ) -> Result<PendingFleetCapture, ImageError> {
        if encoded.class() != AdmissionClass::Encoded
            || encoded.bytes() < caps.bytes
            || decoded.class() != AdmissionClass::Decoded
            || decoded.bytes() < caps.decoded_bytes
            || suffix.class() != AdmissionClass::Suffix
        {
            return Err(ImageError::Capacity);
        }
        let config = journal.config();
        let generation = journal.id().recovery_generation;
        let mut live = self.inner.write().map_err(|_| ImageError::Corrupt)?;
        // clone_state validates exact configured+indexed bucket coverage,
        // complete sync, no uncertainty, and decoded admission before copies.
        let private = clone_state(&live, universe, caps)?;
        let mut cuts = Vec::new();
        if live.native_cuts.len() > config.max_cuts {
            return Err(ImageError::Capacity);
        }
        cuts.try_reserve_exact(live.native_cuts.len())
            .map_err(|_| ImageError::Capacity)?;
        if cuts.capacity() > config.max_cuts.saturating_mul(2).max(4) {
            return Err(ImageError::Capacity);
        }
        let mut cut_bytes = 0usize;
        for (writer, (epoch, sequence)) in &live.native_cuts {
            cut_bytes = cut_bytes
                .checked_add(writer.len())
                .ok_or(ImageError::Capacity)?;
            if writer.is_empty() || cut_bytes > config.max_cut_bytes {
                return Err(ImageError::Capacity);
            }
            let mut identity = Vec::new();
            identity
                .try_reserve_exact(writer.len())
                .map_err(|_| ImageError::Capacity)?;
            if identity.capacity() > config.max_cut_bytes {
                return Err(ImageError::Capacity);
            }
            identity.extend_from_slice(writer);
            cuts.push(NativeCut {
                writer: identity,
                epoch: *epoch,
                sequence: *sequence,
            });
        }
        journal
            .begin_capture(clock.now(), caps.bytes, caps.decoded_bytes, members, cuts)
            .map_err(|_| ImageError::Incomplete)?;
        let ingress =
            JournalIngress::new(journal, suffix, changed).map_err(|_| ImageError::Capacity)?;
        if let Some(previous) = &live.capture {
            previous.invalidate(Invalidation::DonorLost);
        }
        live.capture = Some(IndexCapture::new(
            ingress.clone(),
            generation,
            clock,
            config.max_event_bytes,
            caps.name_bytes,
        ));
        Ok(PendingFleetCapture {
            index: Some(Arc::clone(self)),
            ingress,
            private,
            encoded_image: None,
            commitment: None,
            encoded: Some(encoded),
            decoded: Some(decoded),
        })
    }

    /// Remove only this candidate; delayed cleanup cannot unlink a replacement.
    pub(crate) fn retire_fleet_capture(&self, ingress: &JournalIngress) {
        let mut live = self
            .inner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if live
            .capture
            .as_ref()
            .is_some_and(|capture| capture.same_ingress(ingress))
            && let Some(capture) = live.capture.take()
        {
            capture.invalidate(Invalidation::DonorLost);
        }
    }

    /// Stop donating the current image when the TCP listener can no longer
    /// serve it. Local index facts and their read permission are unchanged.
    pub(crate) fn retire_fleet_service(&self) {
        let mut live = self
            .inner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(capture) = live.capture.take() {
            capture.invalidate(Invalidation::DonorLost);
        }
    }

    /// Whether this exact ingress is still the synchronous publication sink.
    pub(crate) fn has_fleet_capture(&self, ingress: &JournalIngress) -> bool {
        self.inner.read().is_ok_and(|live| {
            live.capture
                .as_ref()
                .is_some_and(|capture| capture.same_ingress(ingress))
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};
    use std::time::{Duration, UNIX_EPOCH};

    use groupnet::consistency::volatile_recovery::bootstrap::admission::{
        AdmissionClass, AdmissionLimits, ByteAdmission,
    };
    use groupnet::core::volatile_bootstrap::journal::{CaptureId, JournalConfig, JournalState};
    use groupnet::core::volatile_bootstrap::{
        BootId, BootstrapMemberIdentity, BootstrapScope, ClaimIdentity, PresenceIdentity,
    };
    use groupnet::core::{NodeId, Status};
    use s3s::dto::ObjectStorageClass;

    use crate::index::{ObjEntry, apply_del, apply_observed_put};
    use crate::sync::wire::{from_micros, to_micros, wire_stamp};

    use super::*;

    fn config() -> JournalConfig {
        JournalConfig {
            max_encoded_bytes: 2_048,
            max_decoded_bytes: 8_192,
            max_events: 4,
            max_suffix_bytes: 1_024,
            max_event_bytes: 128,
            max_identity_bytes: 64,
            max_followers: 1,
            max_follower_id_bytes: 64,
            max_cuts: 2,
            max_cut_bytes: 128,
            max_members: 2,
            max_membership_bytes: 256,
            max_scope_bytes: 128,
            max_batch_events: 1,
            max_batch_bytes: 128,
            max_inflight_bytes: 256,
            max_total_ms: 100_000,
            max_follower_ms: 10_000,
        }
    }

    fn capture_id(serial: u64) -> CaptureId {
        CaptureId {
            scope: BootstrapScope {
                domain: "origin".to_owned(),
                partition: "bucket".to_owned(),
            },
            donor: ClaimIdentity {
                node: NodeId::from("donor"),
                incarnation: BootId(3),
                session: 2,
                attempt: 1,
            },
            recovery_generation: 1,
            serial,
        }
    }

    fn member() -> BootstrapMemberIdentity {
        BootstrapMemberIdentity {
            node: NodeId::from("donor"),
            presence: Some(PresenceIdentity {
                node: NodeId::from("donor"),
                boot: BootId(3),
                session: 2,
            }),
            member_incarnation: 1,
            status: Status::Alive,
        }
    }

    fn admission() -> ByteAdmission {
        ByteAdmission::new(AdmissionLimits {
            max_total_bytes: 1_000_000,
            max_encoded_bytes: 16_384,
            max_decoded_bytes: 65_536,
            max_suffix_bytes: 900_000,
            max_native_overlap_bytes: 1,
            max_inflight_bytes: 1,
            max_reservations: 16,
        })
        .unwrap()
    }

    fn object(time: u64) -> ObjEntry {
        ObjEntry {
            size: Some(4),
            last_modified: UNIX_EPOCH + Duration::from_secs(time),
            etag: None,
            storage_class: ObjectStorageClass::from("STANDARD".to_owned()),
            content_type: None,
            meta: None,
        }
    }

    fn pending(index: &Arc<KeyIndex>, budget: &ByteAdmission, serial: u64) -> PendingFleetCapture {
        pending_on(index, budget, serial, LogicalClock::start())
    }

    fn pending_on(
        index: &Arc<KeyIndex>,
        budget: &ByteAdmission,
        serial: u64,
        clock: LogicalClock,
    ) -> PendingFleetCapture {
        let config = config();
        let journal = DonorJournal::new(config, capture_id(serial)).unwrap();
        let suffix = budget
            .reserve(
                AdmissionClass::Suffix,
                DonorJournal::storage_bound(config).unwrap(),
            )
            .unwrap();
        let encoded = budget.reserve(AdmissionClass::Encoded, 2_048).unwrap();
        let decoded = budget.reserve(AdmissionClass::Decoded, 8_192).unwrap();
        index
            .begin_fleet_capture(
                journal,
                suffix,
                encoded,
                decoded,
                Arc::new(Notify::new()),
                vec![member()],
                &["bucket".to_owned()],
                ImageCaps {
                    bytes: 2_048,
                    decoded_bytes: 8_192,
                    buckets: 1,
                    rows: 1,
                    name_bytes: 64,
                },
                clock,
            )
            .unwrap()
    }

    /// The worker ticks the capture on the session clock while the index
    /// journals effects on it too. A write landing mid-millisecond must never
    /// put the journal ahead of the worker's next tick, which would refuse it as
    /// backward time and fail the capture.
    #[test]
    fn index_effects_and_worker_ticks_share_one_timeline() {
        let index = Arc::new(KeyIndex::default());
        index.mark_bucket_synced("bucket");
        let budget = admission();
        let clock = LogicalClock::start();
        let mut pending = pending_on(&index, &budget, 1, clock);
        let caps = ImageCaps {
            bytes: 2_048,
            decoded_bytes: 8_192,
            buckets: 1,
            rows: 1,
            name_bytes: 64,
        };
        pending.encode(&["bucket".to_owned()], caps).unwrap();
        let donor = pending.finish().unwrap();
        for (n, key) in ["a", "b", "c"].into_iter().enumerate() {
            std::thread::sleep(Duration::from_micros(300));
            assert!(apply_observed_put(
                &index,
                "bucket",
                key,
                object(n as u64 + 2)
            ));
            donor
                .tick(clock.now())
                .expect("the worker is never behind the index's journal time");
        }
        assert!(donor.is_active());
    }

    #[test]
    fn effects_after_c_stay_in_suffix_until_private_image_finishes() {
        let index = Arc::new(KeyIndex::default());
        index.mark_bucket_synced("bucket");
        let budget = admission();
        let mut pending = pending(&index, &budget, 1);
        let caps = ImageCaps {
            bytes: 2_048,
            decoded_bytes: 8_192,
            buckets: 1,
            rows: 1,
            name_bytes: 64,
        };
        let universe = ["bucket".to_owned()];
        pending.encode(&universe, caps).unwrap();
        // The write lands milliseconds into the capture, so its journal time is past
        // any time fixed when C began; finishing must not land behind it.
        std::thread::sleep(std::time::Duration::from_millis(3));
        assert!(apply_observed_put(&index, "bucket", "later", object(2)));
        assert_eq!(
            pending.ingress.with_journal(|journal| journal.state()),
            JournalState::Capturing
        );
        let donor = pending.finish().unwrap();
        let at_c = super::super::decode(donor.image().as_bytes(), caps).unwrap();
        assert!(at_c.buckets["bucket"].keys.is_empty());
        assert_eq!(
            donor.ingress().with_journal(|journal| {
                assert!(journal.image_cuts().is_empty());
                journal.current_cursor().unwrap().position
            }),
            1
        );
        drop(donor);
        assert_eq!(budget.usage().0, 0);
    }

    fn staged(state: KeyIndexState) -> KeyIndex {
        KeyIndex {
            inner: std::sync::RwLock::new(state),
        }
    }

    #[test]
    fn final_swap_refuses_repair_after_coverage_without_losing_the_repair() {
        let index = Arc::new(KeyIndex::default());
        index.mark_bucket_synced("bucket");
        let caps = ImageCaps {
            bytes: 2_048,
            decoded_bytes: 8_192,
            buckets: 1,
            rows: 1,
            name_bytes: 64,
        };
        let universe = ["bucket".to_owned()];
        let donor = KeyIndex::default();
        donor.mark_bucket_synced("bucket");
        apply_del(
            &donor,
            "bucket",
            "repaired",
            UNIX_EPOCH + Duration::from_secs(1),
        );
        let candidate = clone_state(&donor.inner.read().unwrap(), &universe, caps).unwrap();
        assert_eq!(
            index.fleet_install_check(&candidate, &[], &universe, caps.rows),
            Ok(())
        );
        let mut stage = Some(staged(candidate));
        assert!(apply_observed_put(&index, "bucket", "repaired", object(2)));
        assert_eq!(
            index.install_fleet_candidate(&mut stage, &[], &universe, caps.rows),
            Err(InstallRefusal::Incompatible)
        );
        assert!(stage.is_some(), "a refused swap keeps the private stage");
        assert!(
            index.read().unwrap()["bucket"]
                .keys
                .contains_key("repaired")
        );
    }

    #[test]
    fn origin_repair_before_coverage_refuses_incomparable_donor_delete() {
        let local = KeyIndex::default();
        local.mark_bucket_synced("bucket");
        assert!(apply_observed_put(&local, "bucket", "key", object(5)));
        let donor = KeyIndex::default();
        donor.mark_bucket_synced("bucket");
        apply_del(&donor, "bucket", "key", UNIX_EPOCH + Duration::from_secs(6));
        let caps = ImageCaps {
            bytes: 2_048,
            decoded_bytes: 8_192,
            buckets: 1,
            rows: 2,
            name_bytes: 64,
        };
        let universe = ["bucket".to_owned()];
        let candidate = clone_state(&donor.inner.read().unwrap(), &universe, caps).unwrap();
        assert_eq!(
            local.fleet_install_check(&candidate, &[], &universe, caps.rows),
            Err(InstallRefusal::Incompatible)
        );
        assert!(local.read().unwrap()["bucket"].keys.contains_key("key"));
    }

    #[test]
    fn matching_origin_repair_before_coverage_keeps_peer_path_available() {
        let local = KeyIndex::default();
        local.mark_bucket_synced("bucket");
        let donor = KeyIndex::default();
        donor.mark_bucket_synced("bucket");
        assert!(apply_observed_put(&local, "bucket", "key", object(5)));
        assert!(apply_observed_put(&donor, "bucket", "key", object(5)));
        let caps = ImageCaps {
            bytes: 2_048,
            decoded_bytes: 8_192,
            buckets: 1,
            rows: 2,
            name_bytes: 64,
        };
        let universe = ["bucket".to_owned()];
        let mut stage = Some(staged(
            clone_state(&donor.inner.read().unwrap(), &universe, caps).unwrap(),
        ));
        local
            .install_fleet_candidate(&mut stage, &[], &universe, caps.rows)
            .unwrap();
        assert!(stage.is_none());
        assert!(local.read().unwrap()["bucket"].keys.contains_key("key"));
    }

    #[test]
    fn local_delete_tombstone_missing_from_donor_refuses_before_swap() {
        let local = KeyIndex::default();
        local.mark_bucket_synced("bucket");
        apply_del(
            &local,
            "bucket",
            "deleted",
            UNIX_EPOCH + Duration::from_secs(8),
        );
        let donor = KeyIndex::default();
        donor.mark_bucket_synced("bucket");
        let caps = ImageCaps {
            bytes: 2_048,
            decoded_bytes: 8_192,
            buckets: 1,
            rows: 1,
            name_bytes: 64,
        };
        let universe = ["bucket".to_owned()];
        let candidate = clone_state(&donor.inner.read().unwrap(), &universe, caps).unwrap();
        assert_eq!(
            local.fleet_install_check(&candidate, &[], &universe, caps.rows),
            Err(InstallRefusal::Incompatible)
        );
        assert!(local.read().unwrap()["bucket"].gone.contains_key("deleted"));
    }

    #[test]
    fn same_delete_uses_identical_wire_precision_on_donor_and_follower() {
        let local = KeyIndex::default();
        local.mark_bucket_synced("bucket");
        let donor = KeyIndex::default();
        donor.mark_bucket_synced("bucket");
        let raw = UNIX_EPOCH + Duration::new(8, 123_456_789);
        let local_stamp = wire_stamp(raw);
        let peer_stamp = from_micros(to_micros(raw));
        assert_eq!(local_stamp, peer_stamp);
        apply_del(&local, "bucket", "deleted", local_stamp);
        apply_del(&donor, "bucket", "deleted", peer_stamp);
        let caps = ImageCaps {
            bytes: 2_048,
            decoded_bytes: 8_192,
            buckets: 1,
            rows: 1,
            name_bytes: 64,
        };
        let universe = ["bucket".to_owned()];
        let mut stage = Some(staged(
            clone_state(&donor.inner.read().unwrap(), &universe, caps).unwrap(),
        ));
        local
            .install_fleet_candidate(&mut stage, &[], &universe, caps.rows)
            .expect("the same deletion stays comparable across the wire");
        assert_eq!(local.read().unwrap()["bucket"].gone["deleted"], local_stamp);
    }

    #[test]
    fn quiet_current_stage_swaps_and_advances_local_bucket_generation() {
        let index = Arc::new(KeyIndex::default());
        index.mark_bucket_synced("bucket");
        let caps = ImageCaps {
            bytes: 2_048,
            decoded_bytes: 8_192,
            buckets: 1,
            rows: 1,
            name_bytes: 64,
        };
        let universe = ["bucket".to_owned()];
        let mut stage = Some(staged({
            let live = index.inner.read().unwrap();
            clone_state(&live, &universe, caps).unwrap()
        }));
        index
            .install_fleet_candidate(&mut stage, &[], &universe, caps.rows)
            .unwrap();
        let current = index.inner.read().unwrap();
        assert!(current.buckets["bucket"].synced);
        assert_eq!(current.buckets["bucket"].sync_generation, 1);
        assert!(current.native_cuts.is_empty());
    }

    /// A follower whose live feed is behind or ahead of the barrier keeps its
    /// stage for a later barrier; only the exact position installs, and the
    /// follower keeps its own writer positions across the swap.
    #[test]
    fn misaligned_native_positions_pend_without_consuming_the_stage() {
        let index = Arc::new(KeyIndex::default());
        index.mark_bucket_synced("bucket");
        let writer = |sequence| NativeCut {
            writer: b"donor".to_vec(),
            epoch: 4,
            sequence,
        };
        index.register_native_writer(&writer(2));
        let caps = ImageCaps {
            bytes: 2_048,
            decoded_bytes: 8_192,
            buckets: 1,
            rows: 1,
            name_bytes: 64,
        };
        let universe = ["bucket".to_owned()];
        let mut stage = Some(staged({
            let live = index.inner.read().unwrap();
            clone_state(&live, &universe, caps).unwrap()
        }));
        for barrier in [writer(1), writer(3)] {
            assert_eq!(
                index.install_fleet_candidate(
                    &mut stage,
                    std::slice::from_ref(&barrier),
                    &universe,
                    caps.rows
                ),
                Err(InstallRefusal::Pending)
            );
            assert!(stage.is_some());
        }
        let changed = NativeCut {
            epoch: 5,
            ..writer(2)
        };
        assert_eq!(
            index.install_fleet_candidate(&mut stage, &[changed], &universe, caps.rows),
            Err(InstallRefusal::Incompatible)
        );
        index
            .install_fleet_candidate(&mut stage, &[writer(2)], &universe, caps.rows)
            .unwrap();
        assert_eq!(
            index.inner.read().unwrap().native_cuts[b"donor".as_slice()],
            (4, 2)
        );
    }

    /// This node's own writes are assigned their feed positions under the
    /// index lock, so an open capture journals them as contiguous native
    /// effects of its own writer and the barrier covers them exactly.
    #[test]
    fn own_writes_enter_the_capture_as_contiguous_native_effects() {
        let index = Arc::new(KeyIndex::default());
        index.mark_bucket_synced("bucket");
        let own = |sequence| NativeCut {
            writer: b"donor".to_vec(),
            epoch: 9,
            sequence,
        };
        index.register_native_writer(&own(0));
        let budget = admission();
        let pending = pending(&index, &budget, 1);
        for (sequence, time) in [(1, 2), (2, 3)] {
            let (changed, ()) =
                crate::index::apply_own_put(&index, "bucket", "own", object(time), || {
                    ((), own(sequence))
                });
            assert!(changed);
        }
        pending.ingress.with_journal(|journal| {
            assert_eq!(journal.image_cuts(), &[own(0)]);
            assert_eq!(journal.covered_cuts(), &[own(2)]);
            assert_eq!(journal.state(), JournalState::Capturing);
        });
    }

    #[test]
    fn cancelled_capture_cannot_finish_or_retain_a_publication_sink() {
        let index = Arc::new(KeyIndex::default());
        index.mark_bucket_synced("bucket");
        let budget = admission();
        let mut pending = pending(&index, &budget, 1);
        pending
            .encode(
                &["bucket".to_owned()],
                ImageCaps {
                    bytes: 2_048,
                    decoded_bytes: 8_192,
                    buckets: 1,
                    rows: 1,
                    name_bytes: 64,
                },
            )
            .unwrap();
        let ingress = pending.ingress.clone();
        ingress.with_journal(|journal| journal.invalidate(Invalidation::DonorLost));
        assert!(matches!(pending.finish(), Err(ImageError::Incomplete)));
        assert!(!index.has_fleet_capture(&ingress));
        drop(ingress);
        assert_eq!(budget.usage().0, 0);
    }

    #[test]
    fn failed_listener_retires_donor_capture_without_revoking_local_index() {
        let index = Arc::new(KeyIndex::default());
        index.mark_bucket_synced("bucket");
        assert!(apply_observed_put(&index, "bucket", "cached", object(2)));
        let budget = admission();
        let mut pending = pending(&index, &budget, 1);
        pending
            .encode(
                &["bucket".to_owned()],
                ImageCaps {
                    bytes: 2_048,
                    decoded_bytes: 8_192,
                    buckets: 1,
                    rows: 1,
                    name_bytes: 64,
                },
            )
            .unwrap();
        let donor = pending.finish().unwrap();
        let ingress = donor.ingress().clone();
        assert_eq!(
            ingress.with_journal(|journal| journal.state()),
            JournalState::Active
        );
        index.retire_fleet_service();
        assert!(!index.has_fleet_capture(&ingress));
        assert_ne!(
            ingress.with_journal(|journal| journal.state()),
            JournalState::Active
        );
        assert!(!donor.is_active());
        let live = index.inner.read().unwrap();
        assert!(live.buckets["bucket"].synced);
        assert!(live.buckets["bucket"].keys.contains_key("cached"));
        drop(live);
        drop(donor);
        drop(ingress);
        assert_eq!(budget.usage().0, 0);
    }

    #[test]
    fn cancelled_old_encode_releases_only_old_charge_and_never_unlinks_replacement() {
        let index = Arc::new(KeyIndex::default());
        index.mark_bucket_synced("bucket");
        let budget = admission();
        let old = pending(&index, &budget, 1);
        let old_ingress = old.ingress.clone();
        assert_eq!(
            old_ingress.with_journal(|journal| journal.state()),
            JournalState::Capturing
        );
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let worker = {
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            std::thread::spawn(move || {
                entered.wait();
                release.wait();
                drop(old);
            })
        };
        entered.wait();
        let newer = pending(&index, &budget, 2);
        assert!(index.has_fleet_capture(&newer.ingress));
        let before = budget.usage().0;
        release.wait();
        worker.join().unwrap();
        assert!(budget.usage().0 < before);
        assert!(index.has_fleet_capture(&newer.ingress));
        drop(old_ingress);
        drop(newer);
        assert_eq!(budget.usage().0, 0);
    }
}
