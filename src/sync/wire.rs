use std::time::{Duration, SystemTime, UNIX_EPOCH};

use s3s::dto::ETag;
use serde::{Deserialize, Serialize};

/// Splits `writer:epoch:seq` from the right, so writer names may contain
/// colons. `None` for anything else.
pub(super) fn parse_token(value: &str) -> Option<(&str, u64, u64)> {
    let (rest, seq) = value.rsplit_once(':')?;
    let (writer, epoch) = rest.rsplit_once(':')?;
    Some((writer, epoch.parse().ok()?, seq.parse().ok()?))
}

/// Prefix identifying the current event envelope. Bytes without it are not this
/// protocol and are rejected rather than interpreted as another shape.
pub(super) const WIRE_MAGIC: u8 = 0xFF;

/// What one durable write did, as advertised to peers.
#[derive(Serialize, Deserialize)]
pub(crate) enum IndexOp {
    /// The key now holds an object.
    Put {
        /// Object size, for the LIST index. `None` when the writer could not learn it —
        /// never fabricated (see [`crate::index::ObjEntry::size`]).
        size: Option<i64>,
        /// The origin's entity tag, in its header spelling (`"v"` / `W/"v"`), so a peer
        /// can report an `ETag` for the key without an origin round-trip.
        etag: Option<String>,
        /// The object's `Content-Type`. A peer still cannot answer a HEAD from this (no
        /// user metadata rides the feed — see [`crate::index`]), but the entry carries
        /// the one response header clients branch on, and keeps it when a later HEAD
        /// completes the entry.
        content_type: Option<String>,
        /// The object's storage class, so a peer's LIST reports the writer's class
        /// rather than assuming the default.
        storage_class: Option<String>,
    },
    /// The key was deleted.
    Del,
}

/// One durable write: the operation, its `(bucket, key)`, and the writer's wall-clock
/// timestamp — the cross-writer LWW tiebreak, in **microseconds** so it is not coarser
/// than the clock a local write is stamped with (see [`wire_stamp`]).
#[derive(Serialize, Deserialize)]
pub(crate) struct IndexEvent {
    pub(crate) op: IndexOp,
    pub(crate) bucket: String,
    pub(crate) key: String,
    pub(crate) ts_us: u64,
}

pub(super) fn to_micros(ts: SystemTime) -> u64 {
    ts.duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_micros()).unwrap_or(u64::MAX))
}

pub(super) fn from_micros(us: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_micros(us)
}

/// A timestamp truncated to the precision the write feed carries. Local writes are
/// stamped through this before they enter the index, so a local entry and a peer's event
/// describing the same instant compare equal instead of the finer local clock silently
/// winning every last-writer-wins tie.
#[must_use]
pub(crate) fn wire_stamp(ts: SystemTime) -> SystemTime {
    from_micros(to_micros(ts))
}

/// The `ETag` in its header spelling, which [`ETag`]'s own parser round-trips exactly —
/// a string rather than the DTO's serde shape, so the wire format does not move when the
/// DTO does.
pub(super) fn etag_to_wire(tag: &ETag) -> String {
    match tag {
        ETag::Strong(value) => format!("\"{value}\""),
        ETag::Weak(value) => format!("W/\"{value}\""),
    }
}

pub(super) fn encode_event(event: &IndexEvent) -> Vec<u8> {
    let mut out = vec![WIRE_MAGIC];
    match bincode::serialize(event) {
        Ok(body) => out.extend_from_slice(&body),
        Err(_) => return Vec::new(),
    }
    out
}

/// Decode the current event envelope. Unprefixed or malformed bytes are rejected.
pub(super) fn decode_event(bytes: &[u8]) -> Option<IndexEvent> {
    match bytes.split_first() {
        Some((&WIRE_MAGIC, body)) => bincode::deserialize(body).ok(),
        _ => None,
    }
}
