//! The follower's guarded swap of a verified private stage into the live
//! index, and the coverage check that decides whether it may happen.
//!
//! Exactly aligned native cuts prove that the stage and the live index
//! applied the same native effects. What else the live index knows came from
//! this node alone: origin-validated GET/HEAD observations, reconciled
//! uncertain writes, and open reconciliations. A live row is covered by a
//! candidate row of the same origin version, whatever clock stamped either;
//! otherwise each live row or tombstone is covered by a provably later
//! candidate effect for its key, carried into the candidate when it is
//! provably later than everything the candidate holds, and refuses the swap
//! when the two cannot be ordered. Open reconciliations are carried with
//! their tokens. See `docs/fleet-bootstrap.md`.

use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

use groupnet::core::volatile_bootstrap::journal::{
    CutAlignment, CutDifference, Invalidation, NativeCut, align_cuts,
};

use crate::index::{IndexStats, KeyIndex, KeyIndexState, ObjEntry, account_replacement};

/// Why a verified private stage cannot replace the live index now.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum InstallRefusal {
    /// Live native positions differ from the barrier in either direction,
    /// first at the named writer. The stage is retained and a later barrier
    /// may align it.
    Pending(Option<Box<CutPair>>),
    /// A structural difference, or a pair of versions no later barrier of
    /// this capture can order.
    Incompatible(Incompatibility),
}

impl InstallRefusal {
    /// The live index or the stage is poisoned, or the stage is gone.
    pub(super) fn unavailable() -> Self {
        Incompatibility::whole(Clause::Unavailable)
    }
}

/// One native writer's live and barrier positions, each `(epoch,
/// sequence)` or `None` where that side holds no cut for it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CutPair {
    pub(crate) writer: String,
    pub(crate) live: Option<(u64, u64)>,
    pub(crate) barrier: Option<(u64, u64)>,
}

impl From<CutDifference<'_>> for CutPair {
    fn from(difference: CutDifference<'_>) -> Self {
        Self {
            writer: String::from_utf8_lossy(difference.writer).into_owned(),
            live: difference.live,
            barrier: difference.covered,
        }
    }
}

impl fmt::Display for CutPair {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let side = |f: &mut fmt::Formatter<'_>, position: Option<(u64, u64)>| match position {
            Some((epoch, sequence)) => write!(f, "epoch {epoch} sequence {sequence}"),
            None => write!(f, "no cut"),
        };
        write!(f, "writer `{}`: live ", self.writer)?;
        side(f, self.live)?;
        write!(f, ", barrier ")?;
        side(f, self.barrier)
    }
}

/// The coverage clause that refused a stage, and the bucket and key, or the
/// native writer, it failed at where one is involved.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Incompatibility {
    pub(crate) clause: Clause,
    pub(crate) bucket: Option<String>,
    pub(crate) key: Option<String>,
    pub(crate) cut: Option<Box<CutPair>>,
}

impl Incompatibility {
    fn at(clause: Clause, bucket: &str, key: &str) -> InstallRefusal {
        InstallRefusal::Incompatible(Self {
            clause,
            bucket: Some(bucket.to_owned()),
            key: Some(key.to_owned()),
            cut: None,
        })
    }

    fn in_bucket(clause: Clause, bucket: &str) -> InstallRefusal {
        InstallRefusal::Incompatible(Self {
            clause,
            bucket: Some(bucket.to_owned()),
            key: None,
            cut: None,
        })
    }

    fn whole(clause: Clause) -> InstallRefusal {
        InstallRefusal::Incompatible(Self {
            clause,
            bucket: None,
            key: None,
            cut: None,
        })
    }

    fn at_writer(clause: Clause, cut: Option<Box<CutPair>>) -> InstallRefusal {
        InstallRefusal::Incompatible(Self {
            clause,
            bucket: None,
            key: None,
            cut,
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
    /// A live native writer's position cannot align with the barrier's, or
    /// a cut list is not sorted by writer.
    CutConflict,
    /// A candidate bucket is not complete, or holds an unresolved key.
    CandidateUnsynced,
    /// The live index holds more rows than a coverage walk may visit.
    RowLimit,
    /// A live row and the candidate's are different versions whose times
    /// fall in one whole second, so neither is provably later. The field is
    /// the first that tells them apart, identity before time.
    RowMismatch(RowField),
    /// A row and a delete of one key fall within one whole second, the row
    /// not provably after the delete: the live row against the candidate's
    /// tombstone, or the live tombstone against the candidate's row.
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
            (Some(bucket), Some(key)) => write!(f, " at `{bucket}/{key}`")?,
            (Some(bucket), None) => write!(f, " in `{bucket}`")?,
            _ => {}
        }
        match &self.cut {
            Some(cut) => write!(f, " for {cut}"),
            None => Ok(()),
        }
    }
}

/// Whole seconds since the epoch: the unit in which every index time can be
/// ordered. An origin time (LIST, GET, HEAD) enters the index rounded down
/// to whole seconds, so it never runs ahead of its version's true time and
/// runs behind it by less than a second; a write or feed stamp keeps the
/// writer's microseconds.
fn whole_seconds(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_secs())
}

/// The first field telling apart the versions two rows describe, or `None`
/// when they describe the same one. The `ETag`, size, and storage class are
/// the origin's version identity and compare exactly; the time plays no part
/// in it. One version can carry different times: a row indexed from a write
/// or a peer's feed event holds the writer's clock, stamped after the origin
/// answered, while a row from LIST, GET, or HEAD holds the origin's own
/// mtime. A row without an `ETag` has no identity beyond its exact time.
fn version_difference(live: &ObjEntry, peer: &ObjEntry) -> Option<RowField> {
    if peer.etag != live.etag {
        Some(RowField::ETag)
    } else if peer.size != live.size {
        Some(RowField::Size)
    } else if peer.storage_class != live.storage_class {
        Some(RowField::StorageClass)
    } else if live.etag.is_none() && peer.last_modified != live.last_modified {
        Some(RowField::LastModified)
    } else {
        None
    }
}

/// A live effect the candidate does not already reflect.
enum Carried<'a> {
    /// A live row provably later than everything the candidate holds for
    /// its key.
    Row(&'a ObjEntry),
    /// A live tombstone later than the candidate's tombstone, and provably
    /// later than its row, for its key.
    Delete(SystemTime),
}

/// The live effects an install applies to the candidate before the swap,
/// each with its bucket and key. Empty, it allocates nothing.
type Carry<'a> = Vec<(&'a str, &'a str, Carried<'a>)>;

/// Whether `candidate` covers the live `row`: `Ok(false)` when it holds the
/// same version or a provably later row or delete, `Ok(true)` when the live
/// row is provably later and must be carried.
fn row_carried(
    row: &ObjEntry,
    peer: Option<&ObjEntry>,
    peer_deleted: Option<SystemTime>,
) -> Result<bool, Clause> {
    let difference = match peer.map(|peer| version_difference(row, peer)) {
        Some(None) => return Ok(false),
        Some(Some(field)) => Some(field),
        None => None,
    };
    let second = whole_seconds(row.last_modified);
    if peer_deleted.is_some_and(|at| whole_seconds(at) > second)
        || peer.is_some_and(|peer| whole_seconds(peer.last_modified) > second)
    {
        return Ok(false);
    }
    // A tombstone's time is exact; the row's may be rounded down, so only an
    // exact time past the delete proves the row later.
    if peer_deleted.is_some_and(|at| row.last_modified <= at) {
        return Err(Clause::TombstoneMismatch);
    }
    if let (Some(peer), Some(field)) = (peer, difference)
        && whole_seconds(peer.last_modified) == second
    {
        return Err(Clause::RowMismatch(field));
    }
    Ok(true)
}

/// Whether `candidate` covers the live tombstone `deleted_at`: `Ok(false)`
/// when it holds a delete at least as late or a row provably written after
/// it, `Ok(true)` when the live delete is provably later and must be carried.
fn tombstone_carried(
    deleted_at: SystemTime,
    peer: Option<&ObjEntry>,
    peer_deleted: Option<SystemTime>,
) -> Result<bool, Clause> {
    if peer_deleted.is_some_and(|at| at >= deleted_at) {
        return Ok(false);
    }
    if let Some(peer) = peer {
        if peer.last_modified > deleted_at {
            return Ok(false);
        }
        if whole_seconds(peer.last_modified) == whole_seconds(deleted_at) {
            return Err(Clause::TombstoneMismatch);
        }
    }
    Ok(true)
}

/// Whether `candidate`, staged exactly through the barrier `cuts`, may
/// replace `live` without losing or reordering any live effect, and which
/// live effects the swap must carry into it.
fn check_candidate<'a>(
    live: &'a KeyIndexState,
    candidate: &KeyIndexState,
    cuts: &[NativeCut],
    universe: &[String],
    max_rows: usize,
) -> Result<Carry<'a>, InstallRefusal> {
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
    }
    let alignment = align_cuts(
        live.native_cuts
            .iter()
            .map(|(writer, (epoch, sequence))| (writer.as_slice(), *epoch, *sequence)),
        cuts,
    );
    match alignment.verdict {
        CutAlignment::Exact => {}
        CutAlignment::Pending => {
            return Err(InstallRefusal::Pending(
                alignment.deciding.map(|cut| Box::new(CutPair::from(cut))),
            ));
        }
        CutAlignment::Conflict => {
            return Err(Incompatibility::at_writer(
                Clause::CutConflict,
                alignment.deciding.map(|cut| Box::new(CutPair::from(cut))),
            ));
        }
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
    // Aligned cuts leave only this node's own effects unordered against the
    // candidate: observations, reconciled uncertain writes, and a delete of
    // a key the donor never held. Each is covered, carried, or refuses.
    let mut carry = Carry::new();
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
            let carried = row_carried(
                entry,
                incoming.keys.get(key),
                incoming.gone.get(key).copied(),
            )
            .map_err(|clause| Incompatibility::at(clause, name, key))?;
            if carried {
                carry.push((name, key, Carried::Row(entry)));
            }
        }
        for (key, deleted_at) in &bucket.gone {
            let carried = tombstone_carried(
                *deleted_at,
                incoming.keys.get(key),
                incoming.gone.get(key).copied(),
            )
            .map_err(|clause| Incompatibility::at(clause, name, key))?;
            if carried {
                carry.push((name, key, Carried::Delete(*deleted_at)));
            }
        }
    }
    Ok(carry)
}

/// Apply the carried live effects to `candidate`. Deletes go first: a live
/// key holding both a carried tombstone and a carried row holds the row
/// later, as the live index does.
fn apply_carry(
    candidate: &mut KeyIndexState,
    carry: &[(&str, &str, Carried<'_>)],
) -> Result<(), InstallRefusal> {
    let KeyIndexState { buckets, stats, .. } = candidate;
    let deletes = carry
        .iter()
        .filter(|(_, _, effect)| matches!(effect, Carried::Delete(_)));
    let rows = carry
        .iter()
        .filter(|(_, _, effect)| matches!(effect, Carried::Row(_)));
    for (name, key, effect) in deletes.chain(rows) {
        let bucket = buckets
            .get_mut(*name)
            .ok_or_else(|| Incompatibility::in_bucket(Clause::UniverseMismatch, name))?;
        match effect {
            // Coverage proved any candidate row older than this delete.
            Carried::Delete(deleted_at) => {
                bucket.gone.raise(key, *deleted_at);
                if let Some(previous) = bucket.keys.remove(key) {
                    account_replacement(
                        bucket,
                        stats,
                        IndexStats::for_entry(&previous),
                        IndexStats::default(),
                    );
                }
            }
            Carried::Row(entry) => {
                let previous = bucket
                    .keys
                    .insert((*key).to_owned(), (*entry).clone())
                    .as_ref()
                    .map_or_else(IndexStats::default, IndexStats::for_entry);
                account_replacement(bucket, stats, previous, IndexStats::for_entry(entry));
            }
        }
    }
    Ok(())
}

impl KeyIndex {
    /// Pre-check a staged candidate against the live index at one barrier.
    /// [`Self::install_fleet_candidate`] repeats the same check under the
    /// write lock, so a live effect in between is classified again there.
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
        check_candidate(&live, candidate, cuts, universe, max_rows).map(drop)
    }

    /// Swap a verified private stage into the live index at one guarded
    /// publication point, checking it under the same write lock and
    /// carrying the live effects it does not cover, open reconciliations
    /// included. The caller holds the exact Groupnet install permit; no
    /// source callback can await here. A refusal leaves `stage` untouched
    /// for a later barrier.
    pub(in crate::index::fleet) fn install_fleet_candidate(
        &self,
        stage: &mut Option<KeyIndex>,
        cuts: &[NativeCut],
        universe: &[String],
        max_rows: usize,
    ) -> Result<(), InstallRefusal> {
        let unavailable = InstallRefusal::unavailable;
        let mut live = self.inner.write().map_err(|_| unavailable())?;
        let carry = {
            let staged = stage.as_ref().ok_or_else(unavailable)?;
            let candidate = staged.inner.read().map_err(|_| unavailable())?;
            check_candidate(&live, &candidate, cuts, universe, max_rows)?
        };
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
            // An open reconciliation keeps its exact token, and later fences
            // keep drawing from the same epoch: its origin HEAD still
            // resolves the key in the installed index, and no later fence
            // reuses its token.
            if let Some(prior) = old {
                bucket.uncertainty_epoch = prior.uncertainty_epoch;
                bucket.uncertain_keys.clone_from(&prior.uncertain_keys);
            }
        }
        apply_carry(&mut candidate, &carry)?;
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
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use s3s::dto::{ETag, ObjectStorageClass};

    use crate::index::fleet::{ImageCaps, ImageError, encode, snapshot_state, tallied_size};
    use crate::index::{
        AuthoritativeKeyState, BucketState, apply_del, apply_observed_put, apply_put,
        begin_bucket_resync, fence_uncertain_key, resolve_uncertain_key,
    };
    use crate::sync::wire::{from_micros, to_micros, wire_stamp};

    use super::*;

    const UNIVERSE: [&str; 1] = ["bucket"];

    fn universe() -> [String; 1] {
        UNIVERSE.map(str::to_owned)
    }

    fn caps(rows: usize) -> ImageCaps {
        ImageCaps {
            bytes: 4_096,
            decoded_bytes: 16_384,
            buckets: 1,
            rows,
            name_bytes: 64,
        }
    }

    fn at(secs: u64, millis: u32) -> SystemTime {
        UNIX_EPOCH + Duration::new(secs, millis * 1_000_000)
    }

    /// A row of `etag` at `time`, the shape every indexing path produces.
    fn row(etag: Option<&str>, time: SystemTime) -> ObjEntry {
        ObjEntry {
            size: Some(4),
            last_modified: time,
            etag: etag.map(|etag| ETag::Strong(etag.to_owned())),
            storage_class: ObjectStorageClass::from("STANDARD".to_owned()),
            content_type: None,
            meta: None,
        }
    }

    fn synced() -> KeyIndex {
        let index = KeyIndex::default();
        index.mark_bucket_synced("bucket");
        index
    }

    /// The image a follower stages from this index at C: sized and
    /// snapshotted as the donor does under the write lock, then encoded and
    /// decoded.
    fn staged_at_c(index: &KeyIndexState, caps: ImageCaps) -> Result<KeyIndexState, ImageError> {
        let universe = universe();
        let size = tallied_size(index, &universe, caps)?;
        let snapshot = snapshot_state(index, &universe)?;
        crate::index::fleet::decode(&encode(&snapshot, &universe, caps, size)?, caps)
    }

    fn staged(state: KeyIndexState) -> KeyIndex {
        KeyIndex {
            inner: std::sync::RwLock::new(state),
        }
    }

    fn stage_of(donor: &KeyIndex) -> KeyIndex {
        staged(staged_at_c(&donor.inner.read().unwrap(), caps(8)).unwrap())
    }

    /// Install `donor`'s image into `local` at an empty barrier.
    fn install(local: &KeyIndex, donor: &KeyIndex) -> Result<(), InstallRefusal> {
        local.install_fleet_candidate(&mut Some(stage_of(donor)), &[], &universe(), 8)
    }

    fn refused(clause: Clause, key: &str) -> Result<(), InstallRefusal> {
        Err(Incompatibility::at(clause, "bucket", key))
    }

    fn live_row(index: &KeyIndex, key: &str) -> Option<ObjEntry> {
        index.read().unwrap()["bucket"].keys.get(key).cloned()
    }

    fn live_tombstone(index: &KeyIndex, key: &str) -> Option<SystemTime> {
        index.read().unwrap()["bucket"].gone.get(key).copied()
    }

    /// A fleet-written key the follower later read from the origin: the
    /// donor's row holds the writer's clock, stamped after the origin
    /// answered and here past a second boundary, while the follower's holds
    /// the origin's whole-second mtime. One version by identity, it installs
    /// the donor's row, whichever time is later.
    #[test]
    fn same_version_covers_whatever_clock_stamped_it() {
        for donor_time in [UNIX_EPOCH + Duration::new(11, 50_000), at(9, 990)] {
            let local = synced();
            assert!(apply_observed_put(
                &local,
                "bucket",
                "doc",
                row(Some("v1"), at(10, 0))
            ));
            let donor = synced();
            assert!(apply_put(
                &donor,
                "bucket",
                "doc",
                row(Some("v1"), donor_time)
            ));
            install(&local, &donor).unwrap();
            assert_eq!(
                live_row(&local, "doc").map(|row| row.last_modified),
                Some(donor_time)
            );
        }
    }

    /// A donor row or delete in a later second supersedes a stale local
    /// observation, which the swap drops.
    #[test]
    fn later_donor_version_or_delete_supersedes_a_stale_observation() {
        let local = synced();
        for key in ["rewritten", "deleted"] {
            assert!(apply_observed_put(
                &local,
                "bucket",
                key,
                row(Some("old"), at(10, 0))
            ));
        }
        let donor = synced();
        assert!(apply_put(
            &donor,
            "bucket",
            "rewritten",
            row(Some("new"), at(11, 200))
        ));
        apply_del(&donor, "bucket", "deleted", at(11, 500));
        install(&local, &donor).unwrap();
        assert_eq!(
            live_row(&local, "rewritten").and_then(|row| row.etag),
            Some(ETag::Strong("new".to_owned()))
        );
        assert!(live_row(&local, "deleted").is_none());
        assert_eq!(live_tombstone(&local, "deleted"), Some(at(11, 500)));
    }

    /// A live row in a later second than anything the donor holds for its
    /// key is carried: a reconciled uncertain write the donor never saw, and
    /// a key the donor lacks.
    #[test]
    fn later_live_row_is_carried_across_the_install() {
        let local = synced();
        let token = fence_uncertain_key(&local, "bucket", "reconciled");
        assert!(resolve_uncertain_key(
            &local,
            "bucket",
            "reconciled",
            token,
            AuthoritativeKeyState::Present(row(Some("committed"), at(20, 0)))
        ));
        assert!(apply_observed_put(
            &local,
            "bucket",
            "unknown",
            row(Some("fresh"), at(21, 0))
        ));
        let donor = synced();
        assert!(apply_put(
            &donor,
            "bucket",
            "reconciled",
            row(Some("previous"), at(12, 400))
        ));
        assert!(apply_put(
            &donor,
            "bucket",
            "donor-only",
            row(Some("kept"), at(12, 0))
        ));
        install(&local, &donor).unwrap();
        assert_eq!(
            live_row(&local, "reconciled").and_then(|row| row.etag),
            Some(ETag::Strong("committed".to_owned()))
        );
        assert_eq!(
            live_row(&local, "unknown").and_then(|row| row.etag),
            Some(ETag::Strong("fresh".to_owned()))
        );
        assert!(live_row(&local, "donor-only").is_some());
        assert_eq!(local.stats().objects, 3);
        assert_eq!(local.stats().logical_bytes, 12);
        // The carried row keeps ordering later effects as it did live.
        assert!(!apply_put(
            &local,
            "bucket",
            "reconciled",
            row(Some("late"), at(15, 0))
        ));
    }

    /// A live delete later than the donor's row is carried and removes that
    /// row; one the donor never saw keeps shielding its key.
    #[test]
    fn later_live_delete_is_carried_and_removes_the_older_donor_row() {
        let local = synced();
        let token = fence_uncertain_key(&local, "bucket", "absent");
        assert!(resolve_uncertain_key(
            &local,
            "bucket",
            "absent",
            token,
            AuthoritativeKeyState::Absent
        ));
        let absent_at = live_tombstone(&local, "absent").unwrap();
        apply_del(&local, "bucket", "unseen", at(30, 250));
        let donor = synced();
        assert!(apply_put(
            &donor,
            "bucket",
            "absent",
            row(Some("stale"), at(12, 0))
        ));
        install(&local, &donor).unwrap();
        assert!(live_row(&local, "absent").is_none());
        assert_eq!(live_tombstone(&local, "absent"), Some(absent_at));
        assert_eq!(live_tombstone(&local, "unseen"), Some(at(30, 250)));
        assert_eq!(local.stats().objects, 0);
        assert!(
            !apply_put(&local, "bucket", "unseen", row(Some("late"), at(30, 100))),
            "the carried tombstone still rejects an older put"
        );
    }

    /// A feed gap means the follower missed native effects it can no longer
    /// order its rows against. Here the missed span deleted `deleted`, and the
    /// donor then rebuilt from the origin, so its image holds neither the key
    /// nor a tombstone: the follower's pre-gap row must not cross the install
    /// as if it were this node's own later effect.
    #[test]
    fn a_gap_leaves_no_pre_gap_row_for_an_install_to_carry() {
        let peer = |sequence| NativeCut {
            writer: b"peer".to_vec(),
            epoch: 7,
            sequence,
        };
        let local = synced();
        assert!(crate::index::apply_put_native(
            &local,
            "bucket",
            "deleted",
            row(Some("v1"), at(10, 0)),
            peer(1)
        ));
        local.discard_for_gap(&peer(5));
        let donor = synced();
        assert!(apply_put(
            &donor,
            "bucket",
            "kept",
            row(Some("k1"), at(12, 0))
        ));
        local
            .install_fleet_candidate(&mut Some(stage_of(&donor)), &[peer(5)], &universe(), 8)
            .unwrap();
        assert!(
            live_row(&local, "deleted").is_none(),
            "a row the gap may have deleted was carried into the install"
        );
        assert!(live_row(&local, "kept").is_some());
        assert_eq!(local.stats().objects, 1);
    }

    /// An open reconciliation crosses the swap with its token: its HEAD
    /// resolves the key in the installed index, and later fences never
    /// reuse the token.
    #[test]
    fn open_reconciliation_is_carried_with_its_token() {
        let local = synced();
        assert!(apply_observed_put(
            &local,
            "bucket",
            "pending",
            row(Some("before"), at(10, 0))
        ));
        let token = fence_uncertain_key(&local, "bucket", "pending");
        let donor = synced();
        assert!(apply_observed_put(
            &donor,
            "bucket",
            "pending",
            row(Some("before"), at(10, 420))
        ));
        install(&local, &donor).unwrap();
        assert_eq!(
            local.read().unwrap()["bucket"]
                .uncertain_keys
                .get("pending"),
            Some(&token)
        );
        assert!(resolve_uncertain_key(
            &local,
            "bucket",
            "pending",
            token,
            AuthoritativeKeyState::Present(row(Some("after"), at(40, 0)))
        ));
        assert!(local.read().unwrap()["bucket"].uncertain_keys.is_empty());
        assert_eq!(
            live_row(&local, "pending").and_then(|row| row.etag),
            Some(ETag::Strong("after".to_owned()))
        );
        assert!(fence_uncertain_key(&local, "bucket", "pending") > token);
    }

    /// The swap classifies the live index again under its write lock: a
    /// repair landing after the coverage pre-check is carried, not lost.
    #[test]
    fn final_swap_carries_a_repair_that_landed_after_coverage() {
        let local = synced();
        let donor = synced();
        apply_del(&donor, "bucket", "repaired", at(1, 0));
        let candidate = staged_at_c(&donor.inner.read().unwrap(), caps(1)).unwrap();
        assert_eq!(
            local.fleet_install_check(&candidate, &[], &universe(), 1),
            Ok(())
        );
        let mut stage = Some(staged(candidate));
        assert!(apply_observed_put(
            &local,
            "bucket",
            "repaired",
            row(Some("repair"), at(2, 0))
        ));
        local
            .install_fleet_candidate(&mut stage, &[], &universe(), 1)
            .unwrap();
        assert!(live_row(&local, "repaired").is_some());
        assert_eq!(live_tombstone(&local, "repaired"), Some(at(1, 0)));
    }

    #[test]
    fn live_bucket_outside_the_universe_refuses_as_universe_mismatch() {
        let local = synced();
        assert!(apply_observed_put(
            &local,
            "other",
            "key",
            row(Some("v"), at(10, 0))
        ));
        assert_eq!(
            install(&local, &synced()),
            Err(Incompatibility::in_bucket(
                Clause::UniverseMismatch,
                "other"
            ))
        );
    }

    #[test]
    fn live_origin_rebuild_refuses_as_live_rebuilding() {
        let local = synced();
        begin_bucket_resync(&local, "bucket");
        assert_eq!(
            install(&local, &synced()),
            Err(Incompatibility::in_bucket(Clause::LiveRebuilding, "bucket"))
        );
    }

    #[test]
    fn incomplete_candidate_bucket_refuses_as_candidate_unsynced() {
        let candidate = KeyIndexState {
            buckets: HashMap::from([("bucket".to_owned(), BucketState::default())]),
            ..KeyIndexState::default()
        };
        assert_eq!(
            synced().fleet_install_check(&candidate, &[], &universe(), 8),
            Err(Incompatibility::in_bucket(
                Clause::CandidateUnsynced,
                "bucket"
            ))
        );
    }

    #[test]
    fn live_rows_past_the_walk_bound_refuse_as_row_limit() {
        let local = synced();
        for key in ["a", "b"] {
            assert!(apply_observed_put(
                &local,
                "bucket",
                key,
                row(Some("v"), at(10, 0))
            ));
        }
        let donor = synced();
        assert_eq!(
            local.install_fleet_candidate(&mut Some(stage_of(&donor)), &[], &universe(), 1),
            Err(Incompatibility::in_bucket(Clause::RowLimit, "bucket"))
        );
    }

    /// Two versions within one whole second have no provable order.
    #[test]
    fn different_versions_in_one_second_refuse_as_row_mismatch() {
        let local = synced();
        assert!(apply_observed_put(
            &local,
            "bucket",
            "etag",
            row(Some("mine"), at(10, 0))
        ));
        let donor = synced();
        assert!(apply_put(
            &donor,
            "bucket",
            "etag",
            row(Some("theirs"), at(10, 400))
        ));
        assert_eq!(
            install(&local, &donor),
            refused(Clause::RowMismatch(RowField::ETag), "etag")
        );

        let local = synced();
        assert!(apply_put(
            &local,
            "bucket",
            "untagged",
            row(None, at(10, 100))
        ));
        let donor = synced();
        assert!(apply_put(
            &donor,
            "bucket",
            "untagged",
            row(None, at(10, 600))
        ));
        assert_eq!(
            install(&local, &donor),
            refused(Clause::RowMismatch(RowField::LastModified), "untagged")
        );
    }

    /// A row and a delete within one whole second, the row not provably
    /// after the delete, have no provable order in either direction.
    #[test]
    fn row_and_delete_in_one_second_refuse_as_tombstone_mismatch() {
        let local = synced();
        assert!(apply_observed_put(
            &local,
            "bucket",
            "observed",
            row(Some("v"), at(10, 0))
        ));
        let donor = synced();
        apply_del(&donor, "bucket", "observed", at(10, 500));
        assert_eq!(
            install(&local, &donor),
            refused(Clause::TombstoneMismatch, "observed")
        );

        let local = synced();
        apply_del(&local, "bucket", "deleted", at(10, 200));
        let donor = synced();
        assert!(apply_observed_put(
            &donor,
            "bucket",
            "deleted",
            row(Some("v"), at(10, 0))
        ));
        assert_eq!(
            install(&local, &donor),
            refused(Clause::TombstoneMismatch, "deleted")
        );
    }

    #[test]
    fn consumed_stage_refuses_as_unavailable() {
        assert_eq!(
            synced().install_fleet_candidate(&mut None, &[], &universe(), 8),
            Err(InstallRefusal::unavailable())
        );
    }

    #[test]
    fn same_delete_uses_identical_wire_precision_on_donor_and_follower() {
        let local = synced();
        let donor = synced();
        let raw = UNIX_EPOCH + Duration::new(8, 123_456_789);
        let local_stamp = wire_stamp(raw);
        let peer_stamp = from_micros(to_micros(raw));
        assert_eq!(local_stamp, peer_stamp);
        apply_del(&local, "bucket", "deleted", local_stamp);
        apply_del(&donor, "bucket", "deleted", peer_stamp);
        install(&local, &donor).expect("the same deletion stays comparable across the wire");
        assert_eq!(live_tombstone(&local, "deleted"), Some(local_stamp));
    }

    #[test]
    fn quiet_current_stage_swaps_and_advances_local_bucket_generation() {
        let index = Arc::new(synced());
        let mut stage = Some(stage_of(&index));
        index
            .install_fleet_candidate(&mut stage, &[], &universe(), 1)
            .unwrap();
        let current = index.inner.read().unwrap();
        assert!(current.buckets["bucket"].synced);
        assert_eq!(current.buckets["bucket"].sync_generation, 1);
        assert!(current.native_cuts.is_empty());
    }

    /// A follower whose live feed is behind or ahead of the barrier keeps its
    /// stage for a later barrier; only the exact position installs, and the
    /// follower keeps its own writer positions across the swap. Positions order
    /// epoch-major, so a barrier in another writer life pends too: one side
    /// still has to cross that restart, by a sealed renewal or by a gap.
    #[test]
    fn misaligned_native_positions_pend_without_consuming_the_stage() {
        let index = Arc::new(synced());
        let writer = |sequence| NativeCut {
            writer: b"donor".to_vec(),
            epoch: 4,
            sequence,
        };
        index.register_native_writer(&writer(2));
        let mut stage = Some(stage_of(&index));
        let other_life = |epoch| NativeCut { epoch, ..writer(2) };
        for barrier in [writer(1), writer(3), other_life(3), other_life(5)] {
            assert_eq!(
                index.install_fleet_candidate(
                    &mut stage,
                    std::slice::from_ref(&barrier),
                    &universe(),
                    1
                ),
                Err(InstallRefusal::Pending(Some(Box::new(CutPair {
                    writer: "donor".to_owned(),
                    live: Some((4, 2)),
                    barrier: Some((barrier.epoch, barrier.sequence)),
                }))))
            );
            assert!(stage.is_some());
        }
        index
            .install_fleet_candidate(&mut stage, &[writer(2)], &universe(), 1)
            .unwrap();
        assert_eq!(
            index.inner.read().unwrap().native_cuts[b"donor".as_slice()],
            (4, 2)
        );
    }
}
