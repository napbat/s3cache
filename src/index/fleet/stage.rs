//! Charged private image and suffix replay before the guarded live swap.

use std::sync::RwLock;

use groupnet::core::volatile_bootstrap::journal::{JournalBatch, JournalCursor, NativeCut};

use crate::index::KeyIndex;

use super::{ImageCaps, ImageError, InstallRefusal, decode, delta::decode_delta};

/// Private, non-serving candidate. Its owner holds the encoded and decoded
/// `StageResources` permits through verification, replay, and publication.
pub(crate) struct FleetStage {
    encoded: Vec<u8>,
    expected_bytes: usize,
    expected_chunks: usize,
    next_chunk: usize,
    caps: ImageCaps,
    index: Option<KeyIndex>,
    rows: usize,
    through: JournalCursor,
    max_event_bytes: usize,
}

impl FleetStage {
    pub(crate) fn new(
        expected_bytes: usize,
        expected_chunks: usize,
        cut: JournalCursor,
        caps: ImageCaps,
        max_event_bytes: usize,
    ) -> Result<Self, ImageError> {
        if expected_bytes == 0
            || expected_bytes > caps.bytes
            || expected_chunks == 0
            || max_event_bytes == 0
        {
            return Err(ImageError::Capacity);
        }
        let mut encoded = Vec::new();
        encoded
            .try_reserve_exact(expected_bytes)
            .map_err(|_| ImageError::Capacity)?;
        if encoded.capacity() > expected_bytes {
            return Err(ImageError::Capacity);
        }
        Ok(Self {
            encoded,
            expected_bytes,
            expected_chunks,
            next_chunk: 0,
            caps,
            index: None,
            rows: 0,
            through: cut,
            max_event_bytes,
        })
    }

    /// Append one exact nonempty chunk to the pre-admitted buffer.
    pub(crate) fn store_chunk(&mut self, sequence: usize, chunk: &[u8]) -> Result<(), ImageError> {
        if sequence != self.next_chunk || chunk.is_empty() || sequence >= self.expected_chunks {
            return Err(ImageError::Corrupt);
        }
        let end = self
            .encoded
            .len()
            .checked_add(chunk.len())
            .ok_or(ImageError::Capacity)?;
        if end > self.expected_bytes {
            return Err(ImageError::Capacity);
        }
        self.encoded.extend_from_slice(chunk);
        self.next_chunk += 1;
        Ok(())
    }

    /// Verify the complete encoded image and decode a non-serving private
    /// index under the caller's decoded-state reservation.
    pub(crate) fn verify(&mut self, commitment: [u8; 32]) -> Result<(), ImageError> {
        if self.index.is_some()
            || self.next_chunk != self.expected_chunks
            || self.encoded.len() != self.expected_bytes
        {
            return Err(ImageError::Incomplete);
        }
        if *blake3::hash(&self.encoded).as_bytes() != commitment {
            return Err(ImageError::Corrupt);
        }
        let state = decode(&self.encoded, self.caps)?;
        self.rows = state
            .buckets
            .values()
            .map(|bucket| bucket.keys.len() + bucket.gone.len())
            .sum();
        self.index = Some(KeyIndex {
            inner: RwLock::new(state),
        });
        Ok(())
    }

    /// Apply one exactly contiguous donor-local batch to the private image.
    /// A failed decode leaves only private state; the caller discards it.
    pub(crate) fn stage_batch(&mut self, batch: &JournalBatch) -> Result<(), ImageError> {
        let index = self.index.as_ref().ok_or(ImageError::Incomplete)?;
        if batch.from != self.through
            || batch.through.capture != self.through.capture
            || batch.deltas.is_empty()
        {
            return Err(ImageError::Corrupt);
        }
        let mut expected = self.through.position;
        for delta in &batch.deltas {
            expected = expected.checked_add(1).ok_or(ImageError::Capacity)?;
            if delta.position != expected {
                return Err(ImageError::Corrupt);
            }
            let effect = decode_delta(
                &delta.effect,
                self.max_event_bytes,
                self.caps.name_bytes,
                self.caps.decoded_bytes,
            )?;
            let (before, may_grow) = match &effect {
                super::IndexDelta::Put { bucket, key, .. } => {
                    let state = index.inner.read().unwrap();
                    let bucket = state.buckets.get(bucket).ok_or(ImageError::Incomplete)?;
                    (
                        bucket.keys.len() + bucket.gone.len(),
                        !bucket.keys.contains_key(key),
                    )
                }
                super::IndexDelta::Delete { bucket, .. } => {
                    let state = index.inner.read().unwrap();
                    let bucket = state.buckets.get(bucket).ok_or(ImageError::Incomplete)?;
                    (bucket.keys.len() + bucket.gone.len(), true)
                }
                super::IndexDelta::Noop => (0, false),
            };
            if may_grow && self.rows >= self.caps.rows {
                // Refuse before a tree insertion can exceed the decoded
                // reservation. A DELETE may leave an older tombstone next
                // to a newer live entry, so treat it as potential growth.
                return Err(ImageError::Capacity);
            }
            let (_, bucket_name) = effect.apply(index);
            if let Some(bucket_name) = bucket_name {
                let index = index.inner.read().unwrap();
                let bucket = index
                    .buckets
                    .get(&bucket_name)
                    .ok_or(ImageError::Incomplete)?;
                let after = bucket.keys.len() + bucket.gone.len();
                self.rows = self
                    .rows
                    .checked_sub(before)
                    .and_then(|rows| rows.checked_add(after))
                    .ok_or(ImageError::Capacity)?;
                if self.rows > self.caps.rows {
                    return Err(ImageError::Capacity);
                }
            }
        }
        if expected != batch.through.position {
            return Err(ImageError::Corrupt);
        }
        self.through = batch.through.clone();
        Ok(())
    }

    /// Exact donor-local cut reached by this private stage.
    pub(crate) fn through(&self) -> &JournalCursor {
        &self.through
    }

    /// Pre-check that the live index sits exactly at the barrier `cuts` with
    /// version-identical rows, so the guarded install would not lose a live
    /// effect. [`Self::install_into`] repeats the check under the write lock.
    pub(crate) fn check_coverage(
        &self,
        live: &KeyIndex,
        cuts: &[NativeCut],
        universe: &[String],
    ) -> Result<(), InstallRefusal> {
        let index = self.index.as_ref().ok_or(InstallRefusal::Incompatible)?;
        let candidate = index
            .inner
            .read()
            .map_err(|_| InstallRefusal::Incompatible)?;
        live.fleet_install_check(&candidate, cuts, universe, self.caps.rows)
    }

    /// Publish only under the caller's exact Groupnet install permit. The
    /// live index repeats the coverage check under its publication lock and
    /// swaps the image in only when it still holds; a refusal keeps this
    /// stage for a later barrier. This never grants reads by itself.
    pub(crate) fn install_into(
        &mut self,
        live: &KeyIndex,
        cuts: &[NativeCut],
        universe: &[String],
    ) -> Result<(), InstallRefusal> {
        live.install_fleet_candidate(&mut self.index, cuts, universe, self.caps.rows)
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, UNIX_EPOCH};

    use groupnet::core::NodeId;
    use groupnet::core::volatile_bootstrap::journal::{
        CaptureId, DeltaIdentity, JournalDelta, ReservationId,
    };
    use groupnet::core::volatile_bootstrap::{BootId, BootstrapScope, ClaimIdentity};
    use s3s::dto::ObjectStorageClass;

    use crate::index::{ObjEntry, apply_put};

    use super::*;
    use crate::index::fleet::{IndexDelta, encode, encode_delta};

    #[test]
    fn chunks_verify_then_older_delete_replays_without_resurrecting_or_erasing() {
        let caps = ImageCaps {
            bytes: 1024,
            decoded_bytes: 16_384,
            buckets: 1,
            rows: 2,
            name_bytes: 64,
        };
        let source = KeyIndex::default();
        source.mark_bucket_synced("bucket");
        assert!(apply_put(
            &source,
            "bucket",
            "key",
            ObjEntry {
                size: Some(5),
                last_modified: UNIX_EPOCH + Duration::from_secs(19),
                etag: None,
                storage_class: ObjectStorageClass::from("STANDARD".to_owned()),
                content_type: None,
                meta: None,
            },
        ));
        let bytes = encode(&source.inner.read().unwrap(), &["bucket".to_owned()], caps).unwrap();
        let donor = ClaimIdentity {
            node: NodeId::from("donor"),
            incarnation: BootId(1),
            session: 1,
            attempt: 1,
        };
        let follower = ClaimIdentity {
            node: NodeId::from("follower"),
            incarnation: BootId(2),
            session: 2,
            attempt: 1,
        };
        let capture = CaptureId {
            scope: BootstrapScope {
                domain: "origin".to_owned(),
                partition: "bucket".to_owned(),
            },
            donor,
            recovery_generation: 1,
            serial: 1,
        };
        let c = JournalCursor {
            capture: capture.clone(),
            position: 0,
        };
        let mut stage = FleetStage::new(bytes.len(), 2, c.clone(), caps, 256).unwrap();
        let split = bytes.len() / 2;
        assert!(stage.store_chunk(1, &bytes[split..]).is_err());
        stage.store_chunk(0, &bytes[..split]).unwrap();
        stage.store_chunk(1, &bytes[split..]).unwrap();
        stage.verify(*blake3::hash(&bytes).as_bytes()).unwrap();
        let deleted = IndexDelta::Delete {
            bucket: "bucket".to_owned(),
            key: "key".to_owned(),
            deleted_at: UNIX_EPOCH + Duration::from_secs(18),
        };
        let effect = encode_delta(&deleted, 256, 64).unwrap();
        let through = JournalCursor {
            capture,
            position: 1,
        };
        let batch = JournalBatch {
            reservation: ReservationId {
                capture: c.capture.clone(),
                follower,
                serial: 1,
            },
            operation: 1,
            from: c,
            through: through.clone(),
            deltas: vec![JournalDelta {
                position: 1,
                identity: DeltaIdentity::Local(b"repair".to_vec()),
                effect,
            }],
            bytes: 42,
        };
        stage.stage_batch(&batch).unwrap();
        assert_eq!(stage.through(), &through);
        let image = stage.index.as_ref().unwrap().inner.read().unwrap();
        let bucket = &image.buckets["bucket"];
        assert!(bucket.keys.contains_key("key"));
        assert_eq!(bucket.gone["key"], UNIX_EPOCH + Duration::from_secs(18));
    }
}
