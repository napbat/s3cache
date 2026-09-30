//! One final, capture-local index effect carried in the bounded donor suffix.

use std::time::SystemTime;

use crate::index::{KeyIndex, ObjEntry, apply_del, apply_put};

use super::{
    DecodeBudget, ImageError, Reader, Writer, read_entry, read_timestamp, timestamp, write_entry,
};

const VERSION: u8 = 1;
const PUT: u8 = 1;
const DELETE: u8 = 2;
const NOOP: u8 = 3;

/// Final live-index effect, including an explicit native no-op that advances
/// source coverage without changing the private image.
pub(crate) enum IndexDelta {
    Put {
        bucket: String,
        key: String,
        entry: ObjEntry,
    },
    Delete {
        bucket: String,
        key: String,
        deleted_at: SystemTime,
    },
    Noop,
}

impl IndexDelta {
    /// Apply exactly one final donor effect to a private stage. The stage has
    /// no live journal ingress and cannot grant local read authority.
    pub(crate) fn apply(self, stage: &KeyIndex) -> (bool, Option<String>) {
        match self {
            Self::Put { bucket, key, entry } => {
                let changed = apply_put(stage, &bucket, &key, entry);
                (changed, Some(bucket))
            }
            Self::Delete {
                bucket,
                key,
                deleted_at,
            } => {
                let changed = apply_del(stage, &bucket, &key, deleted_at);
                (changed, Some(bucket))
            }
            Self::Noop => (false, None),
        }
    }
}

/// Encode one index effect after it won local conflict resolution. The caller
/// owns an event-byte admission before constructing this output.
pub(crate) fn encode_delta(
    delta: &IndexDelta,
    max_event_bytes: usize,
    name_bytes: usize,
) -> Result<Vec<u8>, ImageError> {
    if name_bytes == 0 || max_event_bytes == 0 {
        return Err(ImageError::Capacity);
    }
    match delta {
        IndexDelta::Put { bucket, key, entry } => {
            encode_put(bucket, key, entry, max_event_bytes, name_bytes)
        }
        IndexDelta::Delete {
            bucket,
            key,
            deleted_at,
        } => encode_delete(bucket, key, *deleted_at, max_event_bytes, name_bytes),
        IndexDelta::Noop => {
            let mut writer = Writer::new(max_event_bytes)?;
            writer.u8(VERSION)?;
            writer.u8(NOOP)?;
            Ok(writer.bytes)
        }
    }
}

/// Encode a final put from borrowed live index fields without cloning its
/// names or HEAD metadata under the publication lock.
pub(crate) fn encode_put(
    bucket: &str,
    key: &str,
    entry: &ObjEntry,
    max_event_bytes: usize,
    name_bytes: usize,
) -> Result<Vec<u8>, ImageError> {
    let mut writer = Writer::new(max_event_bytes)?;
    writer.u8(VERSION)?;
    writer.u8(PUT)?;
    writer.text(bucket, name_bytes)?;
    write_entry(&mut writer, key, entry, name_bytes)?;
    Ok(writer.bytes)
}

/// Encode a final tombstone from borrowed names.
pub(crate) fn encode_delete(
    bucket: &str,
    key: &str,
    deleted_at: SystemTime,
    max_event_bytes: usize,
    name_bytes: usize,
) -> Result<Vec<u8>, ImageError> {
    let mut writer = Writer::new(max_event_bytes)?;
    writer.u8(VERSION)?;
    writer.u8(DELETE)?;
    writer.text(bucket, name_bytes)?;
    writer.text(key, name_bytes)?;
    timestamp(&mut writer, deleted_at)?;
    Ok(writer.bytes)
}

/// Decode only one bounded effect. Malformed or trailing bytes cannot become
/// a no-op, and no decoded name is allocated before charging it.
pub(crate) fn decode_delta(
    bytes: &[u8],
    max_event_bytes: usize,
    name_bytes: usize,
    max_decoded_bytes: usize,
) -> Result<IndexDelta, ImageError> {
    if bytes.is_empty() || bytes.len() > max_event_bytes || name_bytes == 0 {
        return Err(ImageError::Capacity);
    }
    let mut reader = Reader { bytes, offset: 0 };
    if reader.u8()? != VERSION {
        return Err(ImageError::Schema);
    }
    let mut budget = DecodeBudget::new(max_decoded_bytes)?;
    let delta = match reader.u8()? {
        PUT => {
            let bucket = reader.text(name_bytes)?;
            budget.take(bucket.len())?;
            let (key, entry) = read_entry(&mut reader, &mut budget, name_bytes)?;
            IndexDelta::Put {
                bucket: bucket.to_owned(),
                key: key.to_owned(),
                entry,
            }
        }
        DELETE => {
            let bucket = reader.text(name_bytes)?;
            budget.take(bucket.len())?;
            let key = reader.text(name_bytes)?;
            budget.take(key.len())?;
            IndexDelta::Delete {
                bucket: bucket.to_owned(),
                key: key.to_owned(),
                deleted_at: read_timestamp(&mut reader)?,
            }
        }
        NOOP => IndexDelta::Noop,
        _ => return Err(ImageError::Schema),
    };
    if reader.offset != bytes.len() {
        return Err(ImageError::Corrupt);
    }
    Ok(delta)
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, UNIX_EPOCH};

    use s3s::dto::ObjectStorageClass;

    use super::*;

    #[test]
    fn final_put_delete_and_native_noop_roundtrip() {
        let entry = ObjEntry {
            size: Some(7),
            last_modified: UNIX_EPOCH + Duration::from_secs(12),
            etag: Some("\"etag\"".parse().unwrap()),
            storage_class: ObjectStorageClass::from("STANDARD".to_owned()),
            content_type: Some("text/plain".to_owned()),
            meta: None,
        };
        let put = IndexDelta::Put {
            bucket: "b".to_owned(),
            key: "k".to_owned(),
            entry,
        };
        let wire = encode_delta(&put, 512, 64).unwrap();
        let IndexDelta::Put { bucket, key, entry } = decode_delta(&wire, 512, 64, 8_192).unwrap()
        else {
            panic!("put")
        };
        assert_eq!((bucket.as_str(), key.as_str()), ("b", "k"));
        assert_eq!(entry.size, Some(7));
        assert!(entry.content_type.is_none(), "peer entries remain skeletal");

        let delete = IndexDelta::Delete {
            bucket: "b".to_owned(),
            key: "k".to_owned(),
            deleted_at: UNIX_EPOCH + Duration::from_secs(13),
        };
        let wire = encode_delta(&delete, 512, 64).unwrap();
        assert!(matches!(
            decode_delta(&wire, 512, 64, 8_192),
            Ok(IndexDelta::Delete { .. })
        ));
        let wire = encode_delta(&IndexDelta::Noop, 512, 64).unwrap();
        assert!(matches!(
            decode_delta(&wire, 512, 64, 8_192),
            Ok(IndexDelta::Noop)
        ));
    }

    #[test]
    fn malformed_and_oversized_effects_fail_closed() {
        let wire = encode_delta(&IndexDelta::Noop, 32, 16).unwrap();
        let mut trailing = wire.clone();
        trailing.push(0);
        assert!(matches!(
            decode_delta(&trailing, 32, 16, 128),
            Err(ImageError::Corrupt)
        ));
        assert!(matches!(
            decode_delta(&wire, 1, 16, 128),
            Err(ImageError::Capacity)
        ));
        let mut unknown = wire;
        unknown[1] = 99;
        assert!(matches!(
            decode_delta(&unknown, 32, 16, 128),
            Err(ImageError::Schema)
        ));
    }

    #[test]
    fn older_delete_replay_preserves_newer_live_row_and_tombstone() {
        let stage = KeyIndex::default();
        stage.mark_bucket_synced("bucket");
        let entry = ObjEntry {
            size: Some(7),
            last_modified: UNIX_EPOCH + Duration::from_secs(19),
            etag: None,
            storage_class: ObjectStorageClass::from("STANDARD".to_owned()),
            content_type: None,
            meta: None,
        };
        assert!(apply_put(&stage, "bucket", "key", entry));
        let old_delete = IndexDelta::Delete {
            bucket: "bucket".to_owned(),
            key: "key".to_owned(),
            deleted_at: UNIX_EPOCH + Duration::from_secs(18),
        };
        let bytes = encode_delta(&old_delete, 256, 64).unwrap();
        let decoded = decode_delta(&bytes, 256, 64, 256).unwrap();
        assert!(
            !decoded.apply(&stage).0,
            "older delete cannot remove live row"
        );

        let caps = super::super::ImageCaps {
            bytes: 1024,
            decoded_bytes: 8192,
            buckets: 1,
            rows: 2,
            name_bytes: 64,
        };
        let encoded =
            super::super::encode(&stage.inner.read().unwrap(), &["bucket".to_owned()], caps)
                .unwrap();
        let restored = super::super::decode(&encoded, caps).unwrap();
        let bucket = &restored.buckets["bucket"];
        assert_eq!(
            bucket.keys["key"].last_modified,
            UNIX_EPOCH + Duration::from_secs(19)
        );
        assert_eq!(bucket.gone["key"], UNIX_EPOCH + Duration::from_secs(18));
    }
}
