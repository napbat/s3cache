//! Bounded private C-state clone under the live index publication lock.

use std::collections::{BTreeMap, HashMap};
use std::mem::size_of;

use crate::index::{BucketState, IndexStats, KeyIndexState, ObjEntry};

use super::{
    DecodeBudget, HASH_MIN_SLOTS, HASH_SLOTS_PER_BUCKET, ImageCaps, ImageError, map_entry_charge,
};

/// Copy only index fields transferable to a peer. The caller has already
/// reserved the full decoded candidate budget and holds the publication lock
/// from C/journal attachment through this bounded copy. Encoding and hashing
/// of the returned private value can then run off-lock on a blocking worker.
#[expect(
    clippy::too_many_lines,
    reason = "one bounded preflight and private clone share the same publication cut"
)]
pub(crate) fn clone_state(
    index: &KeyIndexState,
    universe: &[String],
    caps: ImageCaps,
) -> Result<KeyIndexState, ImageError> {
    let caps = caps.validate()?;
    if universe.len() > caps.buckets || index.buckets.len() != universe.len() {
        return Err(ImageError::Incomplete);
    }
    let mut budget = DecodeBudget::new(caps.decoded_bytes)?;
    budget.take(size_of::<KeyIndexState>())?;
    let bucket_slots = universe
        .len()
        .checked_mul(HASH_SLOTS_PER_BUCKET)
        .and_then(|count| count.checked_add(HASH_MIN_SLOTS))
        .ok_or(ImageError::Capacity)?;
    budget.take(
        bucket_slots
            .checked_mul(size_of::<(String, BucketState)>() + 16)
            .ok_or(ImageError::Capacity)?,
    )?;

    // Price the entire private clone before allocating any row. The bounds
    // include conservative map-node headroom but are not byte-exact RSS.
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
        budget.take(name.len() + size_of::<String>())?;
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
            budget.take(key.len())?;
            budget.take(map_entry_charge::<(String, ObjEntry)>(1)?)?;
            let class = entry.storage_class.as_str();
            if class.is_empty() || class.len() > caps.name_bytes {
                return Err(ImageError::Capacity);
            }
            budget.take(class.len() + size_of::<s3s::dto::ObjectStorageClass>())?;
            if let Some(etag) = &entry.etag {
                let len = etag.value().len();
                if len.saturating_add(4) > caps.name_bytes {
                    return Err(ImageError::Capacity);
                }
                budget.take(
                    len.checked_mul(2)
                        .and_then(|bytes| bytes.checked_add(size_of::<s3s::dto::ETag>()))
                        .ok_or(ImageError::Capacity)?,
                )?;
            }
            if entry.size.is_some_and(|size| size < 0) {
                return Err(ImageError::Corrupt);
            }
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
            budget.take(key.len())?;
            budget.take(map_entry_charge::<(String, std::time::SystemTime)>(1)?)?;
        }
    }

    let mut buckets = HashMap::new();
    buckets
        .try_reserve(universe.len())
        .map_err(|_| ImageError::Capacity)?;
    if buckets.capacity() > bucket_slots {
        return Err(ImageError::Capacity);
    }
    let mut total = IndexStats::default();
    for name in universe {
        let source = &index.buckets[name];
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

    use s3s::dto::ObjectStorageClass;

    use super::*;

    #[test]
    fn private_clone_is_skeletal_and_rejects_uncertainty_before_allocation() {
        let caps = ImageCaps {
            bytes: 4096,
            decoded_bytes: 8192,
            buckets: 1,
            rows: 2,
            name_bytes: 64,
        };
        let mut source = KeyIndexState::default();
        let mut bucket = BucketState {
            synced: true,
            ..BucketState::default()
        };
        bucket.keys.insert(
            "key".to_owned(),
            ObjEntry {
                size: Some(5),
                last_modified: UNIX_EPOCH + Duration::from_secs(2),
                etag: None,
                storage_class: ObjectStorageClass::from("STANDARD".to_owned()),
                content_type: Some("text/plain".to_owned()),
                meta: None,
            },
        );
        source.buckets.insert("bucket".to_owned(), bucket);
        let universe = vec!["bucket".to_owned()];
        let cloned = clone_state(&source, &universe, caps).unwrap();
        assert!(cloned.buckets["bucket"].keys["key"].content_type.is_none());
        assert_eq!(cloned.buckets["bucket"].sync_generation, 0);
        let encoded = super::super::encode(&cloned, &universe, caps).unwrap();
        assert!(super::super::decode(&encoded, caps).is_ok());

        source
            .buckets
            .get_mut("bucket")
            .unwrap()
            .uncertain_keys
            .insert("key".to_owned(), 1);
        assert!(matches!(
            clone_state(&source, &universe, caps),
            Err(ImageError::Incomplete)
        ));
        let mut too_small = caps;
        too_small.decoded_bytes = 1;
        assert!(matches!(
            clone_state(&cloned, &universe, too_small),
            Err(ImageError::Capacity)
        ));
    }
}
