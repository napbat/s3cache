//! Exact C publication under the live index lock in O(buckets); measuring and
//! encoding the image run off-lock, from the snapshot taken at C.

use std::sync::Arc;
use std::time::{Duration, Instant};

use groupnet::consistency::volatile_recovery::bootstrap::admission::{
    AdmissionClass, ByteAdmission, Reservation,
};
use groupnet::consistency::volatile_recovery::bootstrap::ports::{
    DonorCapture, JournalIngress, LogicalClock,
};
use groupnet::core::volatile_bootstrap::journal::{DonorJournal, Invalidation, NativeCut};
use groupnet::core::volatile_bootstrap::{BootstrapMemberIdentity, same_membership};
use tokio::sync::Notify;

use crate::index::{KeyIndex, KeyIndexState};

use super::{
    ImageCaps, ImageError, ImageSize, IndexCapture, encode, measure_image, snapshot_state,
    tallied_size,
};

/// Exact pending C snapshot and all admission owners. Move this whole value
/// into blocking encoding work; cancellation leaves the charge and ingress
/// alive until the job exits, then Drop unlinks only this candidate.
pub(crate) struct PendingFleetCapture {
    index: Option<Arc<KeyIndex>>,
    ingress: JournalIngress,
    /// The index at C, sharing its nodes with the live index until encoded.
    snapshot: Option<KeyIndexState>,
    size: ImageSize,
    lock_held: Duration,
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
    /// Whether the fresh source cut binds the same membership as the C
    /// roster, before a guarded finish: the same members with the same
    /// presence, whatever their SWIM status or incarnation. This borrows the
    /// already charged journal copy.
    pub(crate) fn same_membership_as_image(&self, observed: &[BootstrapMemberIdentity]) -> bool {
        self.ingress
            .with_journal(|journal| same_membership(journal.image_members(), observed))
    }

    /// The image's size, taken at C.
    pub(crate) fn size(&self) -> ImageSize {
        self.size
    }

    /// How long C held the index write lock.
    pub(crate) fn lock_held(&self) -> Duration {
        self.lock_held
    }

    /// Measure every row of the snapshot and encode it, away from the live
    /// publication lock, into exactly the size taken at C, retaining both
    /// image permits. C sized the image from the rows' tallies; a snapshot
    /// that measures any other size, or holds a row the codec refuses,
    /// encodes nothing. The snapshot is released here, with any nodes live
    /// writes copied away from it since C.
    pub(crate) fn encode(
        &mut self,
        universe: &[String],
        caps: ImageCaps,
    ) -> Result<(), ImageError> {
        let snapshot = self.snapshot.take().ok_or(ImageError::Corrupt)?;
        if measure_image(&snapshot, universe, caps)? != self.size {
            return Err(ImageError::Corrupt);
        }
        let image = encode(&snapshot, universe, caps, self.size)?;
        drop(snapshot);
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

impl KeyIndex {
    /// Capture exactly the state and native cuts at C. The caller must invoke
    /// this inside a current recovery publication guard (Control -> `KeyIndex`),
    /// with the suffix reservation acquired beforehand. Under the write lock
    /// this sizes the exact image at C from the rows' tallies and reserves
    /// exactly its encoded and decoded size from `admission` before taking
    /// the snapshot or attaching the ingress; a refusal attaches nothing.
    /// Everything here is O(buckets + native writers): no row is visited or
    /// copied, so the guard and the lock are held for microseconds however
    /// large the index is. No network or origin I/O runs here.
    #[expect(
        clippy::too_many_arguments,
        reason = "one atomic C capture owns the journal, its charges, roster, and index cut"
    )]
    pub(crate) fn begin_fleet_capture(
        self: &Arc<Self>,
        mut journal: DonorJournal,
        suffix: Reservation,
        admission: &ByteAdmission,
        changed: Arc<Notify>,
        members: Vec<BootstrapMemberIdentity>,
        universe: &[String],
        caps: ImageCaps,
        clock: LogicalClock,
    ) -> Result<PendingFleetCapture, ImageError> {
        if suffix.class() != AdmissionClass::Suffix {
            return Err(ImageError::Capacity);
        }
        let config = journal.config();
        let generation = journal.id().recovery_generation;
        let mut live = self.inner.write().map_err(|_| ImageError::Corrupt)?;
        let locked = Instant::now();
        // tallied_size validates exact configured+indexed bucket coverage,
        // complete sync, no uncertainty, and the ceilings before anything is
        // reserved; the rows themselves are measured off-lock at encode.
        let size = tallied_size(&live, universe, caps)?;
        let encoded = admission
            .reserve(AdmissionClass::Encoded, size.encoded_bytes)
            .map_err(|_| ImageError::Capacity)?;
        let decoded = admission
            .reserve(AdmissionClass::Decoded, size.decoded_bytes)
            .map_err(|_| ImageError::Capacity)?;
        let snapshot = snapshot_state(&live, universe)?;
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
            .begin_capture(
                clock.now(),
                size.encoded_bytes,
                size.decoded_bytes,
                members,
                cuts,
            )
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
        let lock_held = locked.elapsed();
        drop(live);
        Ok(PendingFleetCapture {
            index: Some(Arc::clone(self)),
            ingress,
            snapshot: Some(snapshot),
            size,
            lock_held,
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
        index
            .begin_fleet_capture(
                journal,
                suffix,
                budget,
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

    #[test]
    fn capture_reserves_exactly_the_image_measured_at_c() {
        let index = Arc::new(KeyIndex::default());
        index.mark_bucket_synced("bucket");
        assert!(apply_observed_put(&index, "bucket", "cached", object(2)));
        let caps = ImageCaps {
            bytes: 2_048,
            decoded_bytes: 8_192,
            buckets: 1,
            rows: 1,
            name_bytes: 64,
        };
        let universe = ["bucket".to_owned()];
        let size = measure_image(&index.inner.read().unwrap(), &universe, caps).unwrap();
        let budget = admission();
        let mut pending = pending(&index, &budget, 1);
        assert_eq!(pending.size, size);
        assert_eq!(
            pending.encoded.as_ref().map(Reservation::bytes),
            Some(size.encoded_bytes)
        );
        assert_eq!(
            pending.decoded.as_ref().map(Reservation::bytes),
            Some(size.decoded_bytes)
        );
        pending.encode(&universe, caps).unwrap();
        let donor = pending.finish().unwrap();
        assert_eq!(donor.image().as_bytes().len(), size.encoded_bytes);
        assert_eq!(
            donor
                .ingress()
                .with_journal(|journal| journal.image_charge())
                .map(|charge| (charge.encoded_bytes, charge.decoded_bytes)),
            Some((size.encoded_bytes, size.decoded_bytes))
        );
    }

    #[test]
    fn index_over_a_ceiling_reserves_and_attaches_nothing() {
        let index = Arc::new(KeyIndex::default());
        index.mark_bucket_synced("bucket");
        assert!(apply_observed_put(&index, "bucket", "a", object(2)));
        assert!(apply_observed_put(&index, "bucket", "b", object(3)));
        let budget = admission();
        let config = config();
        let suffix_bytes = DonorJournal::storage_bound(config).unwrap();
        let fits = ImageCaps {
            bytes: 2_048,
            decoded_bytes: 8_192,
            buckets: 1,
            rows: 2,
            name_bytes: 64,
        };
        let universe = ["bucket".to_owned()];
        let size = measure_image(&index.inner.read().unwrap(), &universe, fits).unwrap();
        for caps in [
            ImageCaps { rows: 1, ..fits },
            ImageCaps {
                bytes: size.encoded_bytes - 1,
                ..fits
            },
            ImageCaps {
                decoded_bytes: size.decoded_bytes - 1,
                ..fits
            },
        ] {
            let journal = DonorJournal::new(config, capture_id(1)).unwrap();
            let suffix = budget
                .reserve(AdmissionClass::Suffix, suffix_bytes)
                .unwrap();
            let refused = index.begin_fleet_capture(
                journal,
                suffix,
                &budget,
                Arc::new(Notify::new()),
                vec![member()],
                &universe,
                caps,
                LogicalClock::start(),
            );
            assert!(matches!(refused, Err(ImageError::Capacity)));
            assert!(index.inner.read().unwrap().capture.is_none());
            assert_eq!(budget.usage().0, 0);
        }
    }

    #[test]
    fn refused_image_reservation_attaches_nothing() {
        let index = Arc::new(KeyIndex::default());
        index.mark_bucket_synced("bucket");
        let budget = admission();
        let config = config();
        let journal = DonorJournal::new(config, capture_id(1)).unwrap();
        let suffix = budget
            .reserve(
                AdmissionClass::Suffix,
                DonorJournal::storage_bound(config).unwrap(),
            )
            .unwrap();
        let held = budget.reserve(AdmissionClass::Decoded, 65_536).unwrap();
        let before = budget.usage().0;
        let refused = index.begin_fleet_capture(
            journal,
            suffix,
            &budget,
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
            LogicalClock::start(),
        );
        assert!(matches!(refused, Err(ImageError::Capacity)));
        assert!(index.inner.read().unwrap().capture.is_none());
        assert!(budget.usage().0 < before, "the suffix is returned too");
        drop(held);
        assert_eq!(budget.usage().0, 0);
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
        assert_eq!(at_c.buckets["bucket"].keys.len(), 0);
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

    fn peer_at(epoch: u64, sequence: u64) -> NativeCut {
        NativeCut {
            writer: b"peer".to_vec(),
            epoch,
            sequence,
        }
    }

    /// A peer's delivered seal and its renewal into the next life reach an open
    /// capture as the exact continuation of the peer's cut, so the capture
    /// stays a candidate and the new life's writes continue it.
    #[test]
    fn a_delivered_seal_and_renewal_carry_a_peer_across_its_restart() {
        let index = Arc::new(KeyIndex::default());
        index.mark_bucket_synced("bucket");
        index.register_native_writer(&peer_at(3, 1));
        let budget = admission();
        let pending = pending(&index, &budget, 1);
        index.note_native_seal(peer_at(3, 2));
        index.renew_native_writer(&peer_at(3, 2), 4);
        crate::index::apply_put_native(&index, "bucket", "new-life", object(2), peer_at(4, 1));
        pending.ingress.with_journal(|journal| {
            assert_eq!(journal.image_cuts(), &[peer_at(3, 1)]);
            assert_eq!(journal.covered_cuts(), &[peer_at(4, 1)]);
            assert_eq!(journal.invalidation(), None);
            assert_eq!(journal.state(), JournalState::Capturing);
        });
        assert_eq!(
            index.inner.read().unwrap().native_cuts[b"peer".as_slice()],
            (4, 1)
        );
    }

    /// A renewal that does not continue the cut from its delivered seal is a
    /// restart the capture cannot cover: it withdraws, while the live index
    /// still moves the writer into its new life so later writes stay contiguous.
    #[test]
    fn a_renewal_without_its_seal_withdraws_the_capture() {
        let index = Arc::new(KeyIndex::default());
        index.mark_bucket_synced("bucket");
        index.register_native_writer(&peer_at(3, 1));
        let budget = admission();
        let pending = pending(&index, &budget, 1);
        index.renew_native_writer(&peer_at(3, 2), 4);
        pending.ingress.with_journal(|journal| {
            assert_eq!(journal.invalidation(), Some(Invalidation::Gap));
        });
        assert_eq!(
            index.inner.read().unwrap().native_cuts[b"peer".as_slice()],
            (4, 0)
        );
    }

    /// A gap over a peer's crashed life moves its cut to where the missed
    /// span ends, so an image that covers the peer's new life from there
    /// aligns; the open capture, which cannot carry the missed writes,
    /// withdraws. A stale gap behind the cut moves nothing.
    #[test]
    fn a_gap_moves_the_cut_past_the_missed_life_and_withdraws_the_capture() {
        let index = Arc::new(KeyIndex::default());
        index.mark_bucket_synced("bucket");
        index.register_native_writer(&peer_at(3, 4));
        let budget = admission();
        let pending = pending(&index, &budget, 1);
        index.discard_for_gap(&peer_at(5, 0));
        pending.ingress.with_journal(|journal| {
            assert_eq!(journal.invalidation(), Some(Invalidation::Gap));
        });
        index.discard_for_gap(&peer_at(3, 9));
        assert_eq!(
            index.inner.read().unwrap().native_cuts[b"peer".as_slice()],
            (5, 0)
        );
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

    #[test]
    fn expired_tombstones_stay_while_captured_and_are_swept_a_step_at_a_time_after() {
        use crate::index::fleet::IndexDelta;
        use crate::index::rows::SWEEP_STEP;
        use crate::index::{TOMBSTONE_PRUNE_LEN, TOMBSTONE_TTL};

        let index = Arc::new(KeyIndex::default());
        index.mark_bucket_synced("bucket");
        let dead = UNIX_EPOCH + Duration::from_secs(1);
        let late = dead + TOMBSTONE_TTL * 2;
        let held = TOMBSTONE_PRUNE_LEN + 1;
        {
            let mut live = index.inner.write().unwrap();
            let gone = &mut live.buckets.get_mut("bucket").unwrap().gone;
            for n in 0..held {
                gone.insert(format!("dead/{n:06}"), dead);
            }
        }
        let universe = ["bucket".to_owned()];
        let caps = ImageCaps {
            bytes: 1 << 24,
            decoded_bytes: 1 << 26,
            buckets: 1,
            rows: held + 2,
            name_bytes: 64,
        };
        let config = JournalConfig {
            max_encoded_bytes: caps.bytes,
            max_decoded_bytes: caps.decoded_bytes,
            ..config()
        };
        let budget = ByteAdmission::new(AdmissionLimits {
            max_total_bytes: 1 << 28,
            max_encoded_bytes: caps.bytes,
            max_decoded_bytes: caps.decoded_bytes,
            max_suffix_bytes: 900_000,
            max_native_overlap_bytes: 1,
            max_inflight_bytes: 1,
            max_reservations: 16,
        })
        .unwrap();
        let suffix = budget
            .reserve(
                AdmissionClass::Suffix,
                DonorJournal::storage_bound(config).unwrap(),
            )
            .unwrap();
        let mut pending = index
            .begin_fleet_capture(
                DonorJournal::new(config, capture_id(1)).unwrap(),
                suffix,
                &budget,
                Arc::new(Notify::new()),
                vec![member()],
                &universe,
                caps,
                LogicalClock::start(),
            )
            .unwrap();
        pending.encode(&universe, caps).unwrap();
        let gone_len = |index: &KeyIndex| index.inner.read().unwrap().buckets["bucket"].gone.len();

        // Every held tombstone has expired, yet none is forgotten under the
        // capture: its image plus suffix stays exactly the live rows.
        apply_del(&index, "bucket", "fresh", late);
        assert_eq!(gone_len(&index), held + 1);
        assert_eq!(
            pending
                .ingress
                .with_journal(|journal| journal.invalidation()),
            None
        );
        let donor = pending.finish().unwrap();
        assert_eq!(
            donor
                .ingress()
                .with_journal(|journal| journal.current_cursor().unwrap().position),
            1,
            "the delete is the capture's only effect"
        );
        // Its follower replays that delete without sweeping, into the same rows.
        let stage = staged(super::super::decode(donor.image().as_bytes(), caps).unwrap());
        IndexDelta::Delete {
            bucket: "bucket".to_owned(),
            key: "fresh".to_owned(),
            deleted_at: late,
        }
        .apply(&stage);
        {
            let staged = stage.inner.read().unwrap();
            let live = index.inner.read().unwrap();
            assert!(
                staged.buckets["bucket"]
                    .gone
                    .iter()
                    .eq(live.buckets["bucket"].gone.iter())
            );
        }
        drop(donor);

        // Uncaptured, a delete forgets one bounded step of expired tombstones,
        // and none once the bucket is back under the sweep threshold.
        apply_del(&index, "bucket", "later/0", late);
        assert_eq!(gone_len(&index), held + 2 - SWEEP_STEP);
        apply_del(&index, "bucket", "later/1", late);
        assert_eq!(gone_len(&index), held + 3 - SWEEP_STEP);
    }
}
