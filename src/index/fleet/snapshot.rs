//! The donor's image at C: sized and snapshotted in O(buckets) under the live
//! index publication lock, then measured row by row off-lock.

use std::collections::{BTreeMap, HashMap};

use crate::index::{BucketState, IndexStats, KeyIndexState};

use super::{
    DecodeBudget, HEADER_BYTES, ImageCaps, ImageError, ImageSize, bucket_bytes, bucket_charge,
    bucket_slots, entry_bytes, entry_charge, etag_text_len, gone_bytes, gone_charge, state_charge,
    sum,
};

/// One universe bucket, named in strictly ascending order, completely synced
/// and free of uncertainty: the bucket-level refusals shared by the size at C
/// and the measurement off-lock.
fn image_bucket<'a>(
    index: &'a KeyIndexState,
    name: &'a str,
    previous: &mut Option<&'a str>,
    caps: ImageCaps,
) -> Result<&'a BucketState, ImageError> {
    if name.is_empty() || name.len() > caps.name_bytes || previous.is_some_and(|last| last >= name)
    {
        return Err(ImageError::Corrupt);
    }
    *previous = Some(name);
    let bucket = index.buckets.get(name).ok_or(ImageError::Incomplete)?;
    if !bucket.synced || !bucket.uncertain_keys.is_empty() || bucket.rebuild_generation.is_some() {
        return Err(ImageError::Incomplete);
    }
    Ok(bucket)
}

/// The rows so far plus `bucket`'s, within the row ceiling.
fn add_rows(rows: usize, bucket: &BucketState, caps: ImageCaps) -> Result<usize, ImageError> {
    let rows = rows
        .checked_add(bucket.keys.len())
        .and_then(|count| count.checked_add(bucket.gone.len()))
        .ok_or(ImageError::Capacity)?;
    if rows > caps.rows {
        return Err(ImageError::Capacity);
    }
    Ok(rows)
}

/// The exact image size at C, from the tallies every bucket's rows keep:
/// O(buckets), so the index lock and the recovery fence cover no row. Bucket
/// coverage, completeness and every ceiling refuse here, before anything is
/// reserved or attached. The rows themselves are validated off-lock by
/// [`measure_image`] of the snapshot, which must agree with this size exactly
/// before a byte is encoded.
pub(crate) fn tallied_size(
    index: &KeyIndexState,
    universe: &[String],
    caps: ImageCaps,
) -> Result<ImageSize, ImageError> {
    let caps = caps.validate()?;
    if universe.len() > caps.buckets || index.buckets.len() != universe.len() {
        return Err(ImageError::Incomplete);
    }
    let mut budget = DecodeBudget::new(caps.decoded_bytes)?;
    budget.take(state_charge(universe.len())?)?;
    let mut encoded = HEADER_BYTES;
    let mut rows = 0usize;
    let mut previous = None::<&str>;
    for name in universe {
        let bucket = image_bucket(index, name, &mut previous, caps)?;
        rows = add_rows(rows, bucket, caps)?;
        let (keys, gone) = (bucket.keys.image(), bucket.gone.image());
        budget.take(bucket_charge(name.len())?)?;
        budget.take(keys.decoded)?;
        budget.take(gone.decoded)?;
        encoded = sum(&[
            encoded,
            bucket_bytes(name.len())?,
            keys.encoded,
            gone.encoded,
        ])?;
    }
    if encoded > caps.bytes {
        return Err(ImageError::Capacity);
    }
    Ok(ImageSize {
        encoded_bytes: encoded,
        decoded_bytes: budget.used,
        rows,
    })
}

/// The transferable index at C: every universe bucket's live and deleted
/// rows, sharing every node with the live maps. O(buckets): no row is copied.
/// A live write after C copies only the nodes on its own path, and only while
/// this snapshot lives. Stripping to the skeletal rows a peer receives is
/// [`encode`](super::encode)'s: it writes no `Content-Type` or metadata.
pub(crate) fn snapshot_state(
    index: &KeyIndexState,
    universe: &[String],
) -> Result<KeyIndexState, ImageError> {
    let mut buckets = HashMap::new();
    buckets
        .try_reserve(universe.len())
        .map_err(|_| ImageError::Capacity)?;
    if buckets.capacity() > bucket_slots(universe.len())? {
        return Err(ImageError::Capacity);
    }
    let mut total = IndexStats::default();
    for name in universe {
        let source = index.buckets.get(name).ok_or(ImageError::Incomplete)?;
        total.replace(IndexStats::default(), source.stats);
        buckets.insert(
            name.clone(),
            BucketState {
                synced: true,
                keys: source.keys.clone(),
                gone: source.gone.clone(),
                stats: source.stats,
                ..BucketState::default()
            },
        );
    }
    Ok(KeyIndexState {
        buckets,
        stats: total,
        capture: None,
        native_cuts: BTreeMap::new(),
    })
}

/// Measure the exact image of the transferable index fields row by row: the
/// bytes `encode` writes, the charge `decode` takes, and the rows. Refuses
/// anything incomplete, malformed, or above `caps`. The capture runs it
/// off-lock on the snapshot taken at C.
pub(crate) fn measure_image(
    index: &KeyIndexState,
    universe: &[String],
    caps: ImageCaps,
) -> Result<ImageSize, ImageError> {
    let caps = caps.validate()?;
    if universe.len() > caps.buckets || index.buckets.len() != universe.len() {
        return Err(ImageError::Incomplete);
    }
    let mut budget = DecodeBudget::new(caps.decoded_bytes)?;
    budget.take(state_charge(universe.len())?)?;
    let mut encoded = HEADER_BYTES;
    let mut rows = 0usize;
    let mut previous = None::<&str>;
    for name in universe {
        let bucket = image_bucket(index, name, &mut previous, caps)?;
        budget.take(bucket_charge(name.len())?)?;
        encoded = sum(&[encoded, bucket_bytes(name.len())?])?;
        rows = add_rows(rows, bucket, caps)?;
        for (key, entry) in &bucket.keys {
            if key.is_empty() || key.len() > caps.name_bytes {
                return Err(ImageError::Capacity);
            }
            let class = entry.storage_class.as_str();
            if class.is_empty() || class.len() > caps.name_bytes {
                return Err(ImageError::Capacity);
            }
            let etag = entry.etag.as_ref().map(etag_text_len).transpose()?;
            if etag.is_some_and(|len| len > caps.name_bytes) {
                return Err(ImageError::Capacity);
            }
            if entry.size.is_some_and(|size| size < 0) {
                return Err(ImageError::Corrupt);
            }
            budget.take(entry_charge(key.len(), etag, class.len())?)?;
            encoded = sum(&[encoded, entry_bytes(key.len(), entry)?])?;
        }
        for (key, deleted_at) in &bucket.gone {
            if key.is_empty() || key.len() > caps.name_bytes {
                return Err(ImageError::Capacity);
            }
            if bucket
                .keys
                .get(key)
                .is_some_and(|entry| *deleted_at >= entry.last_modified)
            {
                return Err(ImageError::Corrupt);
            }
            budget.take(gone_charge(key.len())?)?;
            encoded = sum(&[encoded, gone_bytes(key.len())?])?;
        }
    }
    if encoded > caps.bytes {
        return Err(ImageError::Capacity);
    }
    Ok(ImageSize {
        encoded_bytes: encoded,
        decoded_bytes: budget.used,
        rows,
    })
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, UNIX_EPOCH};

    use s3s::dto::{ETag, ObjectStorageClass};

    use super::super::{decode, encode};
    use super::*;
    use crate::index::{
        AuthoritativeKeyState, EntryFill, KeyIndex, ObjEntry, ObjMeta, apply_del,
        apply_observed_put, apply_put, complete_entry, fence_uncertain_key, resolve_uncertain_key,
    };

    fn caps() -> ImageCaps {
        ImageCaps {
            bytes: 4096,
            decoded_bytes: 16_384,
            buckets: 2,
            rows: 8,
            name_bytes: 64,
        }
    }

    fn entry(etag: Option<ETag>, size: Option<i64>) -> ObjEntry {
        ObjEntry {
            size,
            last_modified: UNIX_EPOCH + Duration::from_secs(5),
            etag,
            storage_class: ObjectStorageClass::from("STANDARD".to_owned()),
            content_type: Some("text/plain".to_owned()),
            meta: None,
        }
    }

    /// Two buckets with a strong, a weak, and no `ETag`, plus tombstones.
    fn mixed() -> (KeyIndexState, Vec<String>) {
        let mut source = KeyIndexState::default();
        let mut first = BucketState {
            synced: true,
            ..BucketState::default()
        };
        first.keys.insert(
            "strong".to_owned(),
            entry(Some(ETag::Strong("abc".to_owned())), Some(3)),
        );
        first.keys.insert(
            "weak".to_owned(),
            entry(Some(ETag::Weak("defgh".to_owned())), None),
        );
        first
            .gone
            .insert("dead".to_owned(), UNIX_EPOCH + Duration::from_secs(9));
        let mut second = BucketState {
            synced: true,
            ..BucketState::default()
        };
        second.keys.insert("plain".to_owned(), entry(None, Some(0)));
        second
            .gone
            .insert("gone-key".to_owned(), UNIX_EPOCH + Duration::from_secs(1));
        source.buckets.insert("alpha".to_owned(), first);
        source.buckets.insert("beta".to_owned(), second);
        (source, vec!["alpha".to_owned(), "beta".to_owned()])
    }

    /// The snapshot at C is the index at C: writes to the live index after
    /// it change neither its rows nor its measured image, and it encodes
    /// exactly the size the live tallies gave at C.
    #[test]
    fn snapshot_holds_c_while_the_live_index_moves_on() {
        let (mut source, universe) = mixed();
        let at_c = tallied_size(&source, &universe, caps()).unwrap();
        let snapshot = snapshot_state(&source, &universe).unwrap();
        assert_eq!(snapshot.buckets["alpha"].sync_generation, 0);

        let alpha = source.buckets.get_mut("alpha").unwrap();
        alpha.keys.remove("strong");
        alpha.keys.insert(
            "later".to_owned(),
            entry(Some(ETag::Strong("0123456789".to_owned())), Some(9)),
        );
        alpha
            .gone
            .insert("strong".to_owned(), UNIX_EPOCH + Duration::from_secs(7));
        assert_ne!(tallied_size(&source, &universe, caps()).unwrap(), at_c);

        assert_eq!(measure_image(&snapshot, &universe, caps()), Ok(at_c));
        let alpha = &snapshot.buckets["alpha"];
        assert!(alpha.keys.contains_key("strong") && !alpha.keys.contains_key("later"));
        assert!(alpha.gone.get("strong").is_none());
        let bytes = encode(&snapshot, &universe, caps(), at_c).unwrap();
        let decoded = decode(&bytes, caps()).unwrap();
        assert!(
            decoded.buckets["alpha"].keys["strong"]
                .content_type
                .is_none()
        );
        assert_eq!(decoded.buckets["alpha"].keys.len(), 2);
    }

    /// Uncertainty, an unsynced bucket or a running rebuild refuse at C, in
    /// O(buckets), exactly as the row-by-row measurement does.
    #[test]
    fn tallied_size_refuses_an_incomplete_bucket_before_anything_is_reserved() {
        let (mut source, universe) = mixed();
        source
            .buckets
            .get_mut("alpha")
            .unwrap()
            .uncertain_keys
            .insert("strong".to_owned(), 1);
        assert_eq!(
            tallied_size(&source, &universe, caps()),
            Err(ImageError::Incomplete)
        );
        assert_eq!(
            measure_image(&source, &universe, caps()),
            Err(ImageError::Incomplete)
        );
        let (mut source, universe) = mixed();
        source.buckets.get_mut("beta").unwrap().synced = false;
        assert_eq!(
            tallied_size(&source, &universe, caps()),
            Err(ImageError::Incomplete)
        );
        let (source, _) = mixed();
        assert_eq!(
            tallied_size(&source, &["alpha".to_owned()], caps()),
            Err(ImageError::Incomplete)
        );
    }

    /// The donor reserves and offers the measured size; a follower reserves
    /// and decodes under exactly that. Any drift between the measurement and
    /// the codec would refuse every real transfer or under-reserve it.
    #[test]
    fn measured_size_is_exactly_what_encode_writes_and_decode_charges() {
        let (source, universe) = mixed();
        let size = measure_image(&source, &universe, caps()).unwrap();
        assert_eq!(size.rows, 5);
        assert_eq!(tallied_size(&source, &universe, caps()), Ok(size));
        let snapshot = snapshot_state(&source, &universe).unwrap();
        let bytes = encode(&snapshot, &universe, caps(), size).unwrap();
        assert_eq!(bytes.len(), size.encoded_bytes);

        let exact = ImageCaps {
            bytes: size.encoded_bytes,
            decoded_bytes: size.decoded_bytes,
            ..caps()
        };
        let decoded = decode(&bytes, exact).unwrap();
        assert_eq!(
            decoded.buckets["alpha"].keys["weak"].etag,
            Some(ETag::Weak("defgh".to_owned()))
        );
        assert_eq!(tallied_size(&decoded, &universe, exact), Ok(size));
        let short = ImageCaps {
            decoded_bytes: size.decoded_bytes - 1,
            ..exact
        };
        assert_eq!(decode(&bytes, short).err(), Some(ImageError::Capacity));

        let mut undersized = size;
        undersized.encoded_bytes -= 1;
        assert_eq!(
            encode(&snapshot, &universe, caps(), undersized).err(),
            Some(ImageError::Capacity)
        );
        let mut oversized = size;
        oversized.encoded_bytes += 1;
        assert_eq!(
            encode(&snapshot, &universe, caps(), oversized).err(),
            Some(ImageError::Corrupt)
        );
    }

    #[test]
    fn measurement_refuses_an_image_above_any_ceiling() {
        let (source, universe) = mixed();
        let size = measure_image(&source, &universe, caps()).unwrap();
        for limited in [
            ImageCaps {
                rows: size.rows - 1,
                ..caps()
            },
            ImageCaps {
                bytes: size.encoded_bytes - 1,
                ..caps()
            },
            ImageCaps {
                decoded_bytes: size.decoded_bytes - 1,
                ..caps()
            },
        ] {
            assert_eq!(
                measure_image(&source, &universe, limited),
                Err(ImageError::Capacity)
            );
            assert_eq!(
                tallied_size(&source, &universe, limited),
                Err(ImageError::Capacity)
            );
        }
        let at_ceiling = ImageCaps {
            bytes: size.encoded_bytes,
            decoded_bytes: size.decoded_bytes,
            rows: size.rows,
            ..caps()
        };
        assert_eq!(measure_image(&source, &universe, at_ceiling), Ok(size));
        assert_eq!(tallied_size(&source, &universe, at_ceiling), Ok(size));
    }

    /// Every path that writes a row keeps its bucket's tally exact: after a
    /// long mixed history of puts, overwrites that change the size and
    /// `ETag`, deletes, tombstone raises, completions and reconciliations,
    /// the O(buckets) size at C is exactly the row-by-row measurement.
    #[test]
    fn every_row_mutation_keeps_the_image_tally_exact() {
        let index = KeyIndex::default();
        index.mark_bucket_synced("bucket");
        let universe = ["bucket".to_owned()];
        let caps = ImageCaps {
            bytes: 1 << 20,
            decoded_bytes: 1 << 24,
            buckets: 1,
            rows: 10_000,
            name_bytes: 64,
        };
        let at = |secs: u64| UNIX_EPOCH + Duration::from_secs(secs);
        let row = |n: u64, secs: u64| ObjEntry {
            size: (!n.is_multiple_of(3)).then_some(i64::try_from(n % 17).unwrap()),
            last_modified: at(secs),
            etag: match n % 4 {
                0 => None,
                1 => Some(ETag::Weak("w".repeat(usize::try_from(n % 9).unwrap()))),
                _ => Some(ETag::Strong("s".repeat(usize::try_from(n % 13).unwrap()))),
            },
            storage_class: ObjectStorageClass::from(
                if n.is_multiple_of(5) {
                    "GLACIER"
                } else {
                    "STANDARD"
                }
                .to_owned(),
            ),
            content_type: None,
            meta: None,
        };
        let mut seed = 0x2545_f491_4f6c_dd1d_u64;
        let mut next = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for step in 1..4_000_u64 {
            let pick = next();
            let key = format!(
                "key-{:0width$}",
                pick % 97,
                width = usize::try_from(pick % 7).unwrap()
            );
            let secs = 10 + step;
            match next() % 6 {
                0 | 1 => {
                    apply_put(&index, "bucket", &key, row(next(), secs));
                }
                2 => {
                    apply_observed_put(&index, "bucket", &key, row(next(), secs));
                }
                3 => {
                    apply_del(&index, "bucket", &key, at(secs));
                }
                4 => {
                    let n = next();
                    complete_entry(
                        &index,
                        "bucket",
                        &key,
                        EntryFill {
                            size: Some(i64::try_from(n % 1_000).unwrap()),
                            etag: Some(ETag::Strong("c".repeat(usize::try_from(n % 11).unwrap()))),
                            content_type: Some("text/plain".to_owned()),
                            meta: ObjMeta::default(),
                        },
                    );
                }
                _ => {
                    let token = fence_uncertain_key(&index, "bucket", &key);
                    let outcome = if next() % 2 == 0 {
                        AuthoritativeKeyState::Present(row(next(), secs))
                    } else {
                        AuthoritativeKeyState::Absent
                    };
                    assert!(resolve_uncertain_key(
                        &index, "bucket", &key, token, outcome
                    ));
                }
            }
            let live = index.inner.read().unwrap();
            assert_eq!(
                tallied_size(&live, &universe, caps),
                measure_image(&live, &universe, caps),
                "step {step}"
            );
        }
        let live = index.inner.read().unwrap();
        assert!(
            live.buckets["bucket"].gone.len() > 10,
            "the history deletes"
        );
    }
}
