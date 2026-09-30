//! Bounded private C-state clone under the live index publication lock.

use std::collections::{BTreeMap, HashMap};

use crate::index::{BucketState, IndexStats, KeyIndexState, ObjEntry};

use super::{
    DecodeBudget, HEADER_BYTES, ImageCaps, ImageError, ImageSize, bucket_bytes, bucket_charge,
    bucket_slots, entry_bytes, entry_charge, etag_text_len, gone_bytes, gone_charge, state_charge,
    sum,
};

/// Measure the exact image of the transferable index fields under the live
/// publication lock, before any row is allocated: the bytes `encode` writes,
/// the charge `decode` takes, and the rows. Refuses anything incomplete,
/// malformed, or above `caps` so no admission is taken for it.
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
        if name.is_empty()
            || name.len() > caps.name_bytes
            || previous.is_some_and(|last| last >= name.as_str())
        {
            return Err(ImageError::Corrupt);
        }
        previous = Some(name);
        let bucket = index.buckets.get(name).ok_or(ImageError::Incomplete)?;
        if !bucket.synced
            || !bucket.uncertain_keys.is_empty()
            || bucket.rebuild_generation.is_some()
        {
            return Err(ImageError::Incomplete);
        }
        budget.take(bucket_charge(name.len())?)?;
        encoded = sum(&[encoded, bucket_bytes(name.len())?])?;
        rows = rows
            .checked_add(bucket.keys.len())
            .and_then(|count| count.checked_add(bucket.gone.len()))
            .ok_or(ImageError::Capacity)?;
        if rows > caps.rows {
            return Err(ImageError::Capacity);
        }
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

/// Copy only index fields transferable to a peer. The caller has measured
/// this exact state with [`measure_image`], reserved that size, and holds the
/// publication lock from C/journal attachment through this bounded copy.
/// Encoding and hashing of the returned private value can then run off-lock.
pub(crate) fn clone_state(
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
        let mut keys = BTreeMap::new();
        let mut stats = IndexStats::default();
        for (key, entry) in &source.keys {
            let private = ObjEntry {
                size: entry.size,
                last_modified: entry.last_modified,
                etag: entry.etag.clone(),
                storage_class: entry.storage_class.clone(),
                content_type: None,
                meta: None,
            };
            stats.replace(IndexStats::default(), IndexStats::for_entry(&private));
            keys.insert(key.clone(), private);
        }
        total.replace(IndexStats::default(), stats);
        buckets.insert(
            name.clone(),
            BucketState {
                synced: true,
                keys,
                gone: source.gone.clone(),
                stats,
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

#[cfg(test)]
mod tests {
    use std::time::{Duration, UNIX_EPOCH};

    use s3s::dto::{ETag, ObjectStorageClass};

    use super::super::{decode, encode};
    use super::*;

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

    #[test]
    fn private_clone_is_skeletal_and_rejects_uncertainty_before_allocation() {
        let (mut source, universe) = mixed();
        let size = measure_image(&source, &universe, caps()).unwrap();
        let cloned = clone_state(&source, &universe).unwrap();
        assert!(
            cloned.buckets["alpha"].keys["strong"]
                .content_type
                .is_none()
        );
        assert_eq!(cloned.buckets["alpha"].sync_generation, 0);
        assert_eq!(measure_image(&cloned, &universe, caps()), Ok(size));

        source
            .buckets
            .get_mut("alpha")
            .unwrap()
            .uncertain_keys
            .insert("strong".to_owned(), 1);
        assert!(matches!(
            measure_image(&source, &universe, caps()),
            Err(ImageError::Incomplete)
        ));
    }

    /// The donor reserves and offers the measured size; a follower reserves
    /// and decodes under exactly that. Any drift between the measurement and
    /// the codec would refuse every real transfer or under-reserve it.
    #[test]
    fn measured_size_is_exactly_what_encode_writes_and_decode_charges() {
        let (source, universe) = mixed();
        let size = measure_image(&source, &universe, caps()).unwrap();
        assert_eq!(size.rows, 5);
        let cloned = clone_state(&source, &universe).unwrap();
        let bytes = encode(&cloned, &universe, caps(), size).unwrap();
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
        let short = ImageCaps {
            decoded_bytes: size.decoded_bytes - 1,
            ..exact
        };
        assert_eq!(decode(&bytes, short).err(), Some(ImageError::Capacity));

        let mut undersized = size;
        undersized.encoded_bytes -= 1;
        assert_eq!(
            encode(&cloned, &universe, caps(), undersized).err(),
            Some(ImageError::Capacity)
        );
        let mut oversized = size;
        oversized.encoded_bytes += 1;
        assert_eq!(
            encode(&cloned, &universe, caps(), oversized).err(),
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
        }
        let at_ceiling = ImageCaps {
            bytes: size.encoded_bytes,
            decoded_bytes: size.decoded_bytes,
            rows: size.rows,
            ..caps()
        };
        assert_eq!(measure_image(&source, &universe, at_ceiling), Ok(size));
    }
}
