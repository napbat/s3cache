//! The follower's guarded swap of a verified private stage into the live
//! index, and the coverage check that decides whether it may happen.

use std::fmt;

use groupnet::core::volatile_bootstrap::journal::{
    CutAlignment, Invalidation, NativeCut, align_cuts,
};

use crate::index::{KeyIndex, KeyIndexState, ObjEntry};

/// Why a verified private stage cannot replace the live index now.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum InstallRefusal {
    /// Live native positions differ from the barrier in either direction.
    /// The stage is retained and a later barrier may align it.
    Pending,
    /// A structural or version difference that no later barrier of this
    /// capture can repair.
    Incompatible(Incompatibility),
}

impl InstallRefusal {
    /// The live index or the stage is poisoned, or the stage is gone.
    pub(super) fn unavailable() -> Self {
        Incompatibility::whole(Clause::Unavailable)
    }
}

/// The coverage clause that refused a stage, and the bucket and key it
/// failed at where one is involved.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Incompatibility {
    pub(crate) clause: Clause,
    pub(crate) bucket: Option<String>,
    pub(crate) key: Option<String>,
}

impl Incompatibility {
    fn at(clause: Clause, bucket: &str, key: &str) -> InstallRefusal {
        InstallRefusal::Incompatible(Self {
            clause,
            bucket: Some(bucket.to_owned()),
            key: Some(key.to_owned()),
        })
    }

    fn in_bucket(clause: Clause, bucket: &str) -> InstallRefusal {
        InstallRefusal::Incompatible(Self {
            clause,
            bucket: Some(bucket.to_owned()),
            key: None,
        })
    }

    fn whole(clause: Clause) -> InstallRefusal {
        InstallRefusal::Incompatible(Self {
            clause,
            bucket: None,
            key: None,
        })
    }
}

/// Each way a stage can be incompatible with the live index.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Clause {
    /// The universe is empty or unsorted, the candidate's buckets are not
    /// exactly it, or the live index holds a bucket outside it.
    UniverseMismatch,
    /// A live bucket is being rebuilt by an origin scan.
    LiveRebuilding,
    /// A live key's last mutation outcome is not yet reconciled.
    LiveUncertain,
    /// A live native writer's incarnation differs from the barrier's.
    CutConflict,
    /// A candidate bucket is not complete, or holds an unresolved key.
    CandidateUnsynced,
    /// The live index holds more rows than a coverage walk may visit.
    RowLimit,
    /// A live row and the candidate's describe different versions; the
    /// field is the first that differs, identity before time.
    RowMismatch(RowField),
    /// The candidate holds no row for a live key.
    MissingRow,
    /// A live tombstone and the candidate's state of that key disagree, or
    /// the candidate deleted a live row.
    TombstoneMismatch,
    /// The live index or the stage is poisoned, or the stage is gone.
    Unavailable,
}

/// The version field of a row that differs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RowField {
    ETag,
    Size,
    StorageClass,
    LastModified,
}

impl fmt::Display for Incompatibility {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.clause)?;
        match (&self.bucket, &self.key) {
            (Some(bucket), Some(key)) => write!(f, " at `{bucket}/{key}`"),
            (Some(bucket), None) => write!(f, " in `{bucket}`"),
            _ => Ok(()),
        }
    }
}

/// The first version field in which `live` and `peer` differ.
fn differing_field(live: &ObjEntry, peer: &ObjEntry) -> Option<RowField> {
    if peer.etag != live.etag {
        Some(RowField::ETag)
    } else if peer.size != live.size {
        Some(RowField::Size)
    } else if peer.storage_class != live.storage_class {
        Some(RowField::StorageClass)
    } else if peer.last_modified != live.last_modified {
        Some(RowField::LastModified)
    } else {
        None
    }
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
    if universe.is_empty()
        || universe.windows(2).any(|pair| pair[0] >= pair[1])
        || candidate.buckets.len() != universe.len()
    {
        return Err(Incompatibility::whole(Clause::UniverseMismatch));
    }
    for (name, bucket) in &live.buckets {
        if universe.binary_search(name).is_err() {
            return Err(Incompatibility::in_bucket(Clause::UniverseMismatch, name));
        }
        if bucket.rebuild_generation.is_some() {
            return Err(Incompatibility::in_bucket(Clause::LiveRebuilding, name));
        }
        if let Some(key) = bucket.uncertain_keys.keys().next() {
            return Err(Incompatibility::at(Clause::LiveUncertain, name, key));
        }
    }
    match align_cuts(
        live.native_cuts
            .iter()
            .map(|(writer, (epoch, sequence))| (writer.as_slice(), *epoch, *sequence)),
        cuts,
    ) {
        CutAlignment::Exact => {}
        CutAlignment::Pending => return Err(InstallRefusal::Pending),
        CutAlignment::Conflict => return Err(Incompatibility::whole(Clause::CutConflict)),
    }
    for name in universe {
        let bucket = candidate
            .buckets
            .get(name)
            .ok_or_else(|| Incompatibility::in_bucket(Clause::UniverseMismatch, name))?;
        if !bucket.synced || !bucket.uncertain_keys.is_empty() {
            return Err(Incompatibility::in_bucket(Clause::CandidateUnsynced, name));
        }
    }
    // A locally origin-validated repair is ordered by no native writer cut.
    // Accept only a version-identical candidate; an incomparable write,
    // delete, or missing row declines peer installation.
    let mut rows = 0usize;
    for (name, bucket) in &live.buckets {
        let incoming = candidate
            .buckets
            .get(name)
            .ok_or_else(|| Incompatibility::in_bucket(Clause::UniverseMismatch, name))?;
        rows = rows
            .checked_add(bucket.keys.len())
            .and_then(|rows| rows.checked_add(bucket.gone.len()))
            .filter(|rows| *rows <= max_rows)
            .ok_or_else(|| Incompatibility::in_bucket(Clause::RowLimit, name))?;
        for (key, entry) in &bucket.keys {
            let peer = incoming
                .keys
                .get(key)
                .ok_or_else(|| Incompatibility::at(Clause::MissingRow, name, key))?;
            if let Some(field) = differing_field(entry, peer) {
                return Err(Incompatibility::at(Clause::RowMismatch(field), name, key));
            }
            if incoming
                .gone
                .get(key)
                .is_some_and(|time| *time >= entry.last_modified)
            {
                return Err(Incompatibility::at(Clause::TombstoneMismatch, name, key));
            }
        }
        for (key, deleted_at) in &bucket.gone {
            // A donor's complete absence is not the native writer proof
            // needed to retire a local tombstone: delayed local repairs
            // or replay may still consult it after this swap.
            if incoming.keys.contains_key(key) || incoming.gone.get(key) != Some(deleted_at) {
                return Err(Incompatibility::at(Clause::TombstoneMismatch, name, key));
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
            .map_err(|_| InstallRefusal::unavailable())?;
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
        let unavailable = InstallRefusal::unavailable;
        let mut live = self.inner.write().map_err(|_| unavailable())?;
        {
            let staged = stage.as_ref().ok_or_else(unavailable)?;
            let candidate = staged.inner.read().map_err(|_| unavailable())?;
            check_candidate(&live, &candidate, cuts, universe, max_rows)?;
        }
        let mut candidate = stage
            .take()
            .ok_or_else(unavailable)?
            .inner
            .into_inner()
            .map_err(|_| unavailable())?;
        for name in universe {
            let bucket = candidate
                .buckets
                .get_mut(name)
                .ok_or_else(|| Incompatibility::in_bucket(Clause::UniverseMismatch, name))?;
            let old = live.buckets.get(name);
            bucket.sync_generation = old
                .map_or(0, |prior| prior.sync_generation)
                .checked_add(1)
                .ok_or_else(unavailable)?;
            bucket.uncertainty_epoch = old.map_or(0, |prior| prior.uncertainty_epoch);
        }
        if let Some(capture) = &live.capture {
            capture.invalidate(Invalidation::Rebuild);
        }
        candidate.native_cuts = std::mem::take(&mut live.native_cuts);
        *live = candidate;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, UNIX_EPOCH};

    use s3s::dto::ObjectStorageClass;

    use crate::index::fleet::{ImageCaps, ImageError, encode, snapshot_state, tallied_size};
    use crate::index::{ObjEntry, apply_del, apply_observed_put};
    use crate::sync::wire::{from_micros, to_micros, wire_stamp};

    use super::*;

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

    /// The image a follower stages from this index at C: sized and
    /// snapshotted as the donor does under the write lock, then encoded and
    /// decoded.
    fn staged_at_c(
        index: &KeyIndexState,
        universe: &[String],
        caps: ImageCaps,
    ) -> Result<KeyIndexState, ImageError> {
        let size = tallied_size(index, universe, caps)?;
        let snapshot = snapshot_state(index, universe)?;
        crate::index::fleet::decode(&encode(&snapshot, universe, caps, size)?, caps)
    }

    fn staged(state: KeyIndexState) -> KeyIndex {
        KeyIndex {
            inner: std::sync::RwLock::new(state),
        }
    }

    fn refused(clause: Clause, key: &str) -> Result<(), InstallRefusal> {
        Err(InstallRefusal::Incompatible(Incompatibility {
            clause,
            bucket: Some("bucket".to_owned()),
            key: Some(key.to_owned()),
        }))
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
        let candidate = staged_at_c(&donor.inner.read().unwrap(), &universe, caps).unwrap();
        assert_eq!(
            index.fleet_install_check(&candidate, &[], &universe, caps.rows),
            Ok(())
        );
        let mut stage = Some(staged(candidate));
        assert!(apply_observed_put(&index, "bucket", "repaired", object(2)));
        assert_eq!(
            index.install_fleet_candidate(&mut stage, &[], &universe, caps.rows),
            refused(Clause::MissingRow, "repaired")
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
        let candidate = staged_at_c(&donor.inner.read().unwrap(), &universe, caps).unwrap();
        assert_eq!(
            local.fleet_install_check(&candidate, &[], &universe, caps.rows),
            refused(Clause::MissingRow, "key")
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
            staged_at_c(&donor.inner.read().unwrap(), &universe, caps).unwrap(),
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
        let candidate = staged_at_c(&donor.inner.read().unwrap(), &universe, caps).unwrap();
        assert_eq!(
            local.fleet_install_check(&candidate, &[], &universe, caps.rows),
            refused(Clause::TombstoneMismatch, "deleted")
        );
        assert!(
            local.read().unwrap()["bucket"]
                .gone
                .get("deleted")
                .is_some()
        );
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
            staged_at_c(&donor.inner.read().unwrap(), &universe, caps).unwrap(),
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
            staged_at_c(&live, &universe, caps).unwrap()
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
            staged_at_c(&live, &universe, caps).unwrap()
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
            Err(Incompatibility::whole(Clause::CutConflict))
        );
        index
            .install_fleet_candidate(&mut stage, &[writer(2)], &universe, caps.rows)
            .unwrap();
        assert_eq!(
            index.inner.read().unwrap().native_cuts[b"donor".as_slice()],
            (4, 2)
        );
    }
}
