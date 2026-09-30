//! Finite skeletal index images for optional volatile peer bootstrap.
//!
//! A donor serializes only a complete, uncertainty-free index. The receiver
//! rebuilds local generations at install; donor generation numbers never
//! authorize a delayed local LIST or HEAD callback.

use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use groupnet::consistency::volatile_recovery::bootstrap::ports::{JournalIngress, LogicalClock};
use groupnet::core::volatile_bootstrap::journal::{DeltaIdentity, Invalidation, NativeCut};

use s3s::dto::{ETag, ObjectStorageClass};

use super::{BucketState, GoneRows, IndexStats, KeyIndexState, KeyRows, ObjEntry};

const VERSION: u8 = 1;
pub(crate) const IMAGE_SCHEMA: u32 = 1;
const MAX_NATIVE_WRITERS: usize = 256;
const MAX_NATIVE_ID_BYTES: usize = 256;

mod capture;
mod delta;
mod install;
mod snapshot;
mod stage;
pub(crate) use capture::{FleetDonorImage, PendingFleetCapture};
pub(super) use delta::{IndexDelta, encode_delete, encode_delta, encode_put};
pub(crate) use install::InstallRefusal;
pub(super) use snapshot::{measure_image, snapshot_state, tallied_size};
pub(crate) use stage::FleetStage;

/// Exact donor candidate attached to the live index's publication lock.
/// Every final index mutation appends under that lock, sampling the Groupnet
/// session's own clock afresh each time: a paused worker cannot extend an
/// expired candidate by leaving its prior Tick time unchanged, and no effect
/// can land on a time the worker has not yet reached or already passed.
pub(super) struct IndexCapture {
    ingress: JournalIngress,
    generation: u64,
    clock: LogicalClock,
    next_local: u64,
    max_event_bytes: usize,
    max_name_bytes: usize,
}

impl IndexCapture {
    pub(super) fn new(
        ingress: JournalIngress,
        generation: u64,
        clock: LogicalClock,
        max_event_bytes: usize,
        max_name_bytes: usize,
    ) -> Self {
        Self {
            ingress,
            generation,
            clock,
            next_local: 0,
            max_event_bytes,
            max_name_bytes,
        }
    }

    pub(super) fn same_ingress(&self, other: &JournalIngress) -> bool {
        self.ingress.same_candidate(other)
    }

    /// The journal's clock, for work that must finish on the same timeline.
    pub(super) fn clock(&self) -> LogicalClock {
        self.clock
    }

    pub(super) fn invalidate(&self, reason: Invalidation) {
        self.ingress
            .with_journal(|journal| journal.invalidate(reason));
    }

    fn append_bytes(&mut self, identity: DeltaIdentity, effect: Vec<u8>) {
        let now = self.clock.now();
        let _ = self
            .ingress
            .with_journal(|journal| journal.append(now, self.generation, identity, effect));
    }

    fn append_local(&mut self, effect: Vec<u8>) {
        let Some(next) = self.next_local.checked_add(1) else {
            self.invalidate(Invalidation::Capacity);
            return;
        };
        self.next_local = next;
        self.append_bytes(DeltaIdentity::Local(next.to_le_bytes().to_vec()), effect);
    }

    pub(super) fn record_put(
        &mut self,
        identity: Option<DeltaIdentity>,
        bucket: &str,
        key: &str,
        entry: &ObjEntry,
    ) {
        if bucket.len() > self.max_name_bytes || key.len() > self.max_name_bytes {
            self.invalidate(Invalidation::Capacity);
            return;
        }
        let Ok(effect) = encode_put(
            bucket,
            key,
            entry,
            self.max_event_bytes,
            self.max_name_bytes,
        ) else {
            self.invalidate(Invalidation::Capacity);
            return;
        };
        if let Some(identity) = identity {
            self.append_bytes(identity, effect);
        } else {
            self.append_local(effect);
        }
    }

    pub(super) fn record_delete(
        &mut self,
        identity: Option<DeltaIdentity>,
        bucket: &str,
        key: &str,
        deleted_at: SystemTime,
    ) {
        if bucket.len() > self.max_name_bytes || key.len() > self.max_name_bytes {
            self.invalidate(Invalidation::Capacity);
            return;
        }
        let Ok(effect) = encode_delete(
            bucket,
            key,
            deleted_at,
            self.max_event_bytes,
            self.max_name_bytes,
        ) else {
            self.invalidate(Invalidation::Capacity);
            return;
        };
        if let Some(identity) = identity {
            self.append_bytes(identity, effect);
        } else {
            self.append_local(effect);
        }
    }

    pub(super) fn record_native_noop(&mut self, identity: DeltaIdentity) {
        let Ok(effect) = encode_delta(&IndexDelta::Noop, self.max_event_bytes, self.max_name_bytes)
        else {
            self.invalidate(Invalidation::Capacity);
            return;
        };
        self.append_bytes(identity, effect);
    }
}

impl KeyIndexState {
    /// A whole-bucket rebuild, uncertainty fence, or bucket removal cannot
    /// be represented as one final key effect. Withdraw the donor image under
    /// the publication lock while ordinary origin recovery continues.
    pub(super) fn invalidate_capture_for_rebuild(&self) {
        if let Some(capture) = &self.capture {
            capture.invalidate(Invalidation::Rebuild);
        }
    }

    /// Record native coverage under the same lock that will publish the
    /// corresponding index effect. Only a known duplicate suppresses the
    /// index mutation. Malformed or over-capacity cut bookkeeping closes the
    /// donor candidate, but must not drop the ordinary live feed update.
    pub(super) fn note_native_cut(&mut self, cut: &NativeCut) -> bool {
        if cut.writer.is_empty()
            || cut.writer.len() > MAX_NATIVE_ID_BYTES
            || cut.epoch == 0
            || cut.sequence == 0
        {
            if let Some(capture) = &self.capture {
                capture.invalidate(Invalidation::Gap);
            }
            return true;
        }
        let Some((old_epoch, old_sequence)) = self.native_cuts.get_mut(&cut.writer) else {
            if self.native_cuts.len() >= MAX_NATIVE_WRITERS {
                if let Some(capture) = &self.capture {
                    capture.invalidate(Invalidation::Capacity);
                }
                return true;
            }
            self.native_cuts
                .insert(cut.writer.clone(), (cut.epoch, cut.sequence));
            if let Some(capture) = &self.capture {
                capture.invalidate(Invalidation::Membership);
            }
            return true;
        };
        if cut.epoch == *old_epoch && cut.sequence <= *old_sequence {
            return false;
        }
        if let Some(capture) = &self.capture {
            if cut.epoch != *old_epoch {
                capture.invalidate(Invalidation::Membership);
            } else if cut.sequence != old_sequence.saturating_add(1) {
                capture.invalidate(Invalidation::Gap);
            }
        }
        *old_epoch = cut.epoch;
        *old_sequence = cut.sequence;
        true
    }

    /// Declare a native writer at its current position before any of its
    /// effects arrive. A capture taken afterwards covers the writer from
    /// that position; one already open cannot, and withdraws.
    fn register_native_writer(&mut self, cut: &NativeCut) {
        if cut.writer.is_empty()
            || cut.writer.len() > MAX_NATIVE_ID_BYTES
            || cut.epoch == 0
            || self.native_cuts.contains_key(&cut.writer)
        {
            return;
        }
        if self.native_cuts.len() >= MAX_NATIVE_WRITERS {
            if let Some(capture) = &self.capture {
                capture.invalidate(Invalidation::Capacity);
            }
            return;
        }
        self.native_cuts
            .insert(cut.writer.clone(), (cut.epoch, cut.sequence));
        if let Some(capture) = &self.capture {
            capture.invalidate(Invalidation::Membership);
        }
    }
}

impl super::KeyIndex {
    /// Declare this node's own feed as a native writer at its current
    /// position (zero before its first write). Its later writes are indexed
    /// under the same lock that assigns their feed positions, so they reach
    /// the donor journal as contiguous native effects of that writer.
    pub(crate) fn register_native_writer(&self, cut: &NativeCut) {
        self.inner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .register_native_writer(cut);
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ImageCaps {
    pub(crate) bytes: usize,
    pub(crate) decoded_bytes: usize,
    pub(crate) buckets: usize,
    pub(crate) rows: usize,
    pub(crate) name_bytes: usize,
}

impl ImageCaps {
    fn validate(self) -> Result<Self, ImageError> {
        if self.bytes == 0
            || self.bytes > 256 << 20
            || self.decoded_bytes == 0
            || self.decoded_bytes > 1 << 30
            || self.buckets == 0
            || self.buckets > 4_096
            || self.rows == 0
            || self.rows > 2_000_000
            || self.name_bytes == 0
            || self.name_bytes > 16_384
        {
            return Err(ImageError::Capacity);
        }
        Ok(self)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ImageError {
    Capacity,
    Incomplete,
    Corrupt,
    Schema,
}

struct Writer {
    bytes: Vec<u8>,
    limit: usize,
}

impl Writer {
    fn new(limit: usize) -> Result<Self, ImageError> {
        if limit == 0 {
            return Err(ImageError::Capacity);
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(limit)
            .map_err(|_| ImageError::Capacity)?;
        if bytes.capacity() > limit {
            return Err(ImageError::Capacity);
        }
        Ok(Self { bytes, limit })
    }

    fn put(&mut self, value: &[u8]) -> Result<(), ImageError> {
        if self
            .bytes
            .len()
            .checked_add(value.len())
            .is_none_or(|end| end > self.limit)
        {
            return Err(ImageError::Capacity);
        }
        self.bytes.extend_from_slice(value);
        Ok(())
    }

    fn u8(&mut self, value: u8) -> Result<(), ImageError> {
        self.put(&[value])
    }

    fn u32(&mut self, value: usize) -> Result<(), ImageError> {
        let value = u32::try_from(value).map_err(|_| ImageError::Capacity)?;
        self.put(&value.to_le_bytes())
    }

    fn u64(&mut self, value: u64) -> Result<(), ImageError> {
        self.put(&value.to_le_bytes())
    }

    fn i64(&mut self, value: i64) -> Result<(), ImageError> {
        self.put(&value.to_le_bytes())
    }

    fn text(&mut self, value: &str, cap: usize) -> Result<(), ImageError> {
        if value.is_empty() || value.len() > cap {
            return Err(ImageError::Capacity);
        }
        self.u32(value.len())?;
        self.put(value.as_bytes())
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, count: usize) -> Result<&'a [u8], ImageError> {
        let end = self.offset.checked_add(count).ok_or(ImageError::Corrupt)?;
        let part = self
            .bytes
            .get(self.offset..end)
            .ok_or(ImageError::Corrupt)?;
        self.offset = end;
        Ok(part)
    }

    fn u8(&mut self) -> Result<u8, ImageError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<usize, ImageError> {
        let raw: [u8; 4] = self.take(4)?.try_into().map_err(|_| ImageError::Corrupt)?;
        Ok(u32::from_le_bytes(raw) as usize)
    }

    fn u64(&mut self) -> Result<u64, ImageError> {
        let raw: [u8; 8] = self.take(8)?.try_into().map_err(|_| ImageError::Corrupt)?;
        Ok(u64::from_le_bytes(raw))
    }

    fn i64(&mut self) -> Result<i64, ImageError> {
        let raw: [u8; 8] = self.take(8)?.try_into().map_err(|_| ImageError::Corrupt)?;
        Ok(i64::from_le_bytes(raw))
    }

    fn text(&mut self, cap: usize) -> Result<&'a str, ImageError> {
        let size = self.u32()?;
        if size == 0 || size > cap {
            return Err(ImageError::Capacity);
        }
        let raw = self.take(size)?;
        let text = std::str::from_utf8(raw).map_err(|_| ImageError::Corrupt)?;
        Ok(text)
    }
}

struct DecodeBudget {
    used: usize,
    limit: usize,
}

// The charge is a finite ownership bound for the private image, not a claim
// about byte-exact allocator RSS or allocator bookkeeping outside the value.
// Hash tables are charged well above the standard library's layout. The rows
// live in `imbl`'s B+tree: a leaf holds up to 16 rows inline, and `decode`
// inserts in key order, which leaves every leaf but the last at least half
// full, so one row owns at most 2 of its slots plus a sixteenth of the leaf's
// header, and its share of one separator key and child edge in each branch
// above (a branch holds at least 8 children). A stage's bounded replay moves
// only as many rows as its journal holds. 3 slots and 64 bytes a row bound that
// with margin; measured, an 800k-row production index holds about 450 bytes a
// row, key, `ETag` and class text included, against a charge of about 650.
// (The previous 32 slots charged about 4.5 KiB per production row, so a
// 100k-row image already needed 448 MiB and the real 798k-row index could never
// be offered.)
const HASH_SLOTS_PER_BUCKET: usize = 4;
const HASH_MIN_SLOTS: usize = 8;
const TREE_SLOTS_PER_ROW: usize = 3;
const TREE_BYTES_PER_ROW: usize = 64;

fn map_entry_charge<T>(count: usize) -> Result<usize, ImageError> {
    count
        .checked_mul(TREE_SLOTS_PER_ROW)
        .and_then(|slots| slots.checked_mul(std::mem::size_of::<T>()))
        .and_then(|bytes| bytes.checked_add(count.checked_mul(TREE_BYTES_PER_ROW)?))
        .ok_or(ImageError::Capacity)
}

/// Exact size of one image: the bytes [`encode`] writes for it, the charge
/// [`decode`] takes for it, and its rows. Measured at C from the live index.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ImageSize {
    pub(crate) encoded_bytes: usize,
    pub(crate) decoded_bytes: usize,
    pub(crate) rows: usize,
}

// One accounting shared by the donor's measurement and the follower's decode,
// so a follower decoding under the donor's measured charge cannot drift.
const TIMESTAMP_BYTES: usize = 12;

fn sum(parts: &[usize]) -> Result<usize, ImageError> {
    parts
        .iter()
        .try_fold(0usize, |total, part| total.checked_add(*part))
        .ok_or(ImageError::Capacity)
}

fn text_bytes(len: usize) -> Result<usize, ImageError> {
    sum(&[4, len])
}

fn bucket_slots(count: usize) -> Result<usize, ImageError> {
    count
        .checked_mul(HASH_SLOTS_PER_BUCKET)
        .and_then(|slots| slots.checked_add(HASH_MIN_SLOTS))
        .ok_or(ImageError::Capacity)
}

/// Charge for the index value and its bucket table.
fn state_charge(buckets: usize) -> Result<usize, ImageError> {
    let table = bucket_slots(buckets)?
        .checked_mul(std::mem::size_of::<(String, BucketState)>() + 16)
        .ok_or(ImageError::Capacity)?;
    sum(&[std::mem::size_of::<KeyIndexState>(), table])
}

fn bucket_charge(name: usize) -> Result<usize, ImageError> {
    sum(&[name, std::mem::size_of::<String>()])
}

/// Charge for one live row; `etag` is the quoted wire text length.
fn entry_charge(key: usize, etag: Option<usize>, class: usize) -> Result<usize, ImageError> {
    let etag = match etag {
        Some(len) => len
            .checked_mul(2)
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<ETag>()))
            .ok_or(ImageError::Capacity)?,
        None => 0,
    };
    sum(&[
        key,
        map_entry_charge::<(String, ObjEntry)>(1)?,
        etag,
        class,
        std::mem::size_of::<ObjectStorageClass>(),
    ])
}

fn gone_charge(key: usize) -> Result<usize, ImageError> {
    sum(&[key, map_entry_charge::<(String, SystemTime)>(1)?])
}

fn etag_wire(value: &ETag) -> (&'static str, &str) {
    match value {
        ETag::Strong(raw) => ("\"", raw.as_str()),
        ETag::Weak(raw) => ("W/\"", raw.as_str()),
    }
}

/// Length of the quoted `ETag` text on the wire (`"x"` or `W/"x"`).
fn etag_text_len(value: &ETag) -> Result<usize, ImageError> {
    let (prefix, raw) = etag_wire(value);
    sum(&[prefix.len(), raw.len(), 1])
}

/// Encoded bytes of one live row as [`write_entry`] writes it.
fn entry_bytes(key: usize, entry: &ObjEntry) -> Result<usize, ImageError> {
    let size = if entry.size.is_some() { 1 + 8 } else { 1 };
    let etag = match &entry.etag {
        Some(etag) => 1 + text_bytes(etag_text_len(etag)?)?,
        None => 1,
    };
    sum(&[
        text_bytes(key)?,
        size,
        TIMESTAMP_BYTES,
        etag,
        text_bytes(entry.storage_class.as_str().len())?,
    ])
}

/// Encoded bytes of one tombstone.
fn gone_bytes(key: usize) -> Result<usize, ImageError> {
    sum(&[text_bytes(key)?, TIMESTAMP_BYTES])
}

/// Encoded bytes of one bucket header (name and both row counts).
fn bucket_bytes(name: usize) -> Result<usize, ImageError> {
    sum(&[text_bytes(name)?, 4, 4])
}

/// One bucket's share of the image: the bytes [`encode`] writes for its rows
/// and the charge [`decode`] takes for them. Every row mutation keeps it
/// current, so the capture at C sizes the whole image in O(buckets) instead
/// of walking every row under the index lock. The sums wrap, so a row counted
/// in and later out cancels exactly; a row whose own size overflows counts as
/// `usize::MAX`, and the off-lock [`measure_image`] of the snapshot refuses it
/// (or any other disagreement) before a byte is encoded.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ImageTally {
    encoded: usize,
    decoded: usize,
}

impl ImageTally {
    /// One live row under a key of `key` bytes.
    pub(crate) fn entry(key: usize, entry: &ObjEntry) -> Self {
        let decoded = entry
            .etag
            .as_ref()
            .map(etag_text_len)
            .transpose()
            .and_then(|etag| entry_charge(key, etag, entry.storage_class.as_str().len()));
        Self {
            encoded: entry_bytes(key, entry).unwrap_or(usize::MAX),
            decoded: decoded.unwrap_or(usize::MAX),
        }
    }

    /// One tombstone under a key of `key` bytes.
    pub(crate) fn tombstone(key: usize) -> Self {
        Self {
            encoded: gone_bytes(key).unwrap_or(usize::MAX),
            decoded: gone_charge(key).unwrap_or(usize::MAX),
        }
    }

    pub(crate) fn add(&mut self, row: Self) {
        self.encoded = self.encoded.wrapping_add(row.encoded);
        self.decoded = self.decoded.wrapping_add(row.decoded);
    }

    pub(crate) fn sub(&mut self, row: Self) {
        self.encoded = self.encoded.wrapping_sub(row.encoded);
        self.decoded = self.decoded.wrapping_sub(row.decoded);
    }
}

/// Encoded bytes of the image header (version and bucket count).
const HEADER_BYTES: usize = 1 + 4;

impl DecodeBudget {
    fn new(limit: usize) -> Result<Self, ImageError> {
        if limit == 0 {
            return Err(ImageError::Capacity);
        }
        Ok(Self { used: 0, limit })
    }

    fn take(&mut self, bytes: usize) -> Result<(), ImageError> {
        self.used = self.used.checked_add(bytes).ok_or(ImageError::Capacity)?;
        if self.used > self.limit {
            return Err(ImageError::Capacity);
        }
        Ok(())
    }
}

fn timestamp(writer: &mut Writer, value: SystemTime) -> Result<(), ImageError> {
    let duration = value
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ImageError::Corrupt)?;
    writer.u64(duration.as_secs())?;
    writer.u32(duration.subsec_nanos() as usize)
}

fn read_timestamp(reader: &mut Reader<'_>) -> Result<SystemTime, ImageError> {
    let seconds = reader.u64()?;
    let nanos = reader.u32()?;
    if nanos >= 1_000_000_000 {
        return Err(ImageError::Corrupt);
    }
    UNIX_EPOCH
        .checked_add(Duration::new(
            seconds,
            u32::try_from(nanos).map_err(|_| ImageError::Corrupt)?,
        ))
        .ok_or(ImageError::Corrupt)
}

fn write_etag(writer: &mut Writer, value: &ETag, cap: usize) -> Result<(), ImageError> {
    let (prefix, raw) = etag_wire(value);
    let len = etag_text_len(value)?;
    if len > cap {
        return Err(ImageError::Capacity);
    }
    writer.u32(len)?;
    writer.put(prefix.as_bytes())?;
    writer.put(raw.as_bytes())?;
    writer.put(b"\"")
}

fn write_entry(
    writer: &mut Writer,
    key: &str,
    entry: &ObjEntry,
    name_bytes: usize,
) -> Result<(), ImageError> {
    writer.text(key, name_bytes)?;
    match entry.size {
        Some(size) if size >= 0 => {
            writer.u8(1)?;
            writer.i64(size)?;
        }
        None => writer.u8(0)?,
        Some(_) => return Err(ImageError::Corrupt),
    }
    timestamp(writer, entry.last_modified)?;
    if let Some(etag) = &entry.etag {
        writer.u8(1)?;
        write_etag(writer, etag, name_bytes)?;
    } else {
        writer.u8(0)?;
    }
    writer.text(entry.storage_class.as_str(), name_bytes)
}

/// Encode a complete exact bucket universe from the snapshot taken at C, off
/// the live index lock, into exactly the `size` measured for it at C.
pub(super) fn encode(
    index: &KeyIndexState,
    universe: &[String],
    caps: ImageCaps,
    size: ImageSize,
) -> Result<Vec<u8>, ImageError> {
    let caps = caps.validate()?;
    if universe.len() > caps.buckets || index.buckets.len() != universe.len() {
        return Err(ImageError::Incomplete);
    }
    if size.encoded_bytes > caps.bytes {
        return Err(ImageError::Capacity);
    }
    let mut writer = Writer::new(size.encoded_bytes)?;
    writer.u8(VERSION)?;
    writer.u32(universe.len())?;
    let mut rows = 0usize;
    let mut previous: Option<&str> = None;
    for name in universe {
        if previous.is_some_and(|last| last >= name.as_str()) {
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
        writer.text(name, caps.name_bytes)?;
        rows = rows
            .checked_add(bucket.keys.len())
            .and_then(|n| n.checked_add(bucket.gone.len()))
            .ok_or(ImageError::Capacity)?;
        if rows > caps.rows {
            return Err(ImageError::Capacity);
        }
        writer.u32(bucket.keys.len())?;
        for (key, entry) in &bucket.keys {
            write_entry(&mut writer, key, entry, caps.name_bytes)?;
        }
        writer.u32(bucket.gone.len())?;
        for (key, deleted_at) in &bucket.gone {
            if bucket
                .keys
                .get(key)
                .is_some_and(|entry| *deleted_at >= entry.last_modified)
            {
                return Err(ImageError::Corrupt);
            }
            writer.text(key, caps.name_bytes)?;
            timestamp(&mut writer, *deleted_at)?;
        }
    }
    if writer.bytes.len() != size.encoded_bytes {
        return Err(ImageError::Corrupt);
    }
    Ok(writer.bytes)
}

fn read_entry<'a>(
    reader: &mut Reader<'a>,
    budget: &mut DecodeBudget,
    name_bytes: usize,
) -> Result<(&'a str, ObjEntry), ImageError> {
    let key = reader.text(name_bytes)?;
    let size = match reader.u8()? {
        0 => None,
        1 => {
            let size = reader.i64()?;
            if size < 0 {
                return Err(ImageError::Corrupt);
            }
            Some(size)
        }
        _ => return Err(ImageError::Corrupt),
    };
    let last_modified = read_timestamp(reader)?;
    let etag = match reader.u8()? {
        0 => None,
        1 => Some(reader.text(name_bytes)?),
        _ => return Err(ImageError::Corrupt),
    };
    let class = reader.text(name_bytes)?;
    budget.take(entry_charge(key.len(), etag.map(str::len), class.len())?)?;
    let etag = etag
        .map(|raw| raw.parse().map_err(|_| ImageError::Corrupt))
        .transpose()?;
    Ok((
        key,
        ObjEntry {
            size,
            last_modified,
            etag,
            storage_class: ObjectStorageClass::from(class.to_owned()),
            content_type: None,
            meta: None,
        },
    ))
}

/// Decode a private skeletal image. The caller holds a decoded-state permit;
/// this function never publishes the result or treats donor generations as
/// local callback fences.
pub(super) fn decode(bytes: &[u8], caps: ImageCaps) -> Result<KeyIndexState, ImageError> {
    let caps = caps.validate()?;
    if bytes.is_empty() || bytes.len() > caps.bytes {
        return Err(ImageError::Capacity);
    }
    let mut reader = Reader { bytes, offset: 0 };
    if reader.u8()? != VERSION {
        return Err(ImageError::Schema);
    }
    let count = reader.u32()?;
    if count > caps.buckets {
        return Err(ImageError::Capacity);
    }
    let mut budget = DecodeBudget::new(caps.decoded_bytes)?;
    budget.take(state_charge(count)?)?;
    let mut buckets = HashMap::new();
    buckets
        .try_reserve(count)
        .map_err(|_| ImageError::Capacity)?;
    let mut total = IndexStats::default();
    let mut rows = 0usize;
    let mut previous_bucket = None::<&str>;
    for _ in 0..count {
        let name = reader.text(caps.name_bytes)?;
        if previous_bucket.is_some_and(|last| last >= name) {
            return Err(ImageError::Corrupt);
        }
        previous_bucket = Some(name);
        budget.take(bucket_charge(name.len())?)?;
        let key_count = reader.u32()?;
        rows = rows.checked_add(key_count).ok_or(ImageError::Capacity)?;
        if rows > caps.rows {
            return Err(ImageError::Capacity);
        }
        let mut keys = KeyRows::default();
        let mut stats = IndexStats::default();
        let mut previous_key = None::<&str>;
        for _ in 0..key_count {
            let (key, entry) = read_entry(&mut reader, &mut budget, caps.name_bytes)?;
            if previous_key.is_some_and(|last| last >= key) {
                return Err(ImageError::Corrupt);
            }
            previous_key = Some(key);
            stats.replace(IndexStats::default(), IndexStats::for_entry(&entry));
            keys.insert(key.to_owned(), entry);
        }
        let gone_count = reader.u32()?;
        rows = rows.checked_add(gone_count).ok_or(ImageError::Capacity)?;
        if rows > caps.rows {
            return Err(ImageError::Capacity);
        }
        let mut gone = GoneRows::default();
        let mut previous_gone = None::<&str>;
        for _ in 0..gone_count {
            let key = reader.text(caps.name_bytes)?;
            if previous_gone.is_some_and(|last| last >= key) {
                return Err(ImageError::Corrupt);
            }
            previous_gone = Some(key);
            budget.take(gone_charge(key.len())?)?;
            let deleted_at = read_timestamp(&mut reader)?;
            if keys
                .get(key)
                .is_some_and(|entry: &ObjEntry| deleted_at >= entry.last_modified)
            {
                return Err(ImageError::Corrupt);
            }
            gone.insert(key.to_owned(), deleted_at);
        }
        total.replace(IndexStats::default(), stats);
        buckets.insert(
            name.to_owned(),
            BucketState {
                synced: true,
                keys,
                gone,
                stats,
                ..BucketState::default()
            },
        );
    }
    if reader.offset != bytes.len() {
        return Err(ImageError::Corrupt);
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
    use super::*;

    /// Measure then encode, as a donor does at C.
    fn encode(
        index: &KeyIndexState,
        universe: &[String],
        caps: ImageCaps,
    ) -> Result<Vec<u8>, ImageError> {
        super::encode(index, universe, caps, measure_image(index, universe, caps)?)
    }

    fn caps() -> ImageCaps {
        ImageCaps {
            bytes: 4_096,
            decoded_bytes: 16_384,
            buckets: 2,
            rows: 8,
            name_bytes: 128,
        }
    }

    fn image() -> KeyIndexState {
        let entry = ObjEntry {
            size: Some(17),
            last_modified: UNIX_EPOCH + Duration::from_secs(19),
            etag: Some("\"etag\"".parse().unwrap()),
            storage_class: ObjectStorageClass::from("STANDARD".to_owned()),
            content_type: Some("application/octet-stream".to_owned()),
            meta: None,
        };
        let mut keys = KeyRows::default();
        keys.insert("present".to_owned(), entry);
        let mut gone = GoneRows::default();
        gone.insert("deleted".to_owned(), UNIX_EPOCH + Duration::from_secs(20));
        let stats = IndexStats {
            objects: 1,
            logical_bytes: 17,
        };
        let bucket = BucketState {
            synced: true,
            keys,
            gone,
            stats,
            sync_generation: 900,
            ..BucketState::default()
        };
        KeyIndexState {
            buckets: HashMap::from([("bucket".to_owned(), bucket)]),
            stats,
            capture: None,
            native_cuts: BTreeMap::new(),
        }
    }

    #[test]
    fn roundtrip_keeps_list_fields_and_tombstones_but_drops_head_metadata_and_donor_generation() {
        let encoded = encode(&image(), &["bucket".to_owned()], caps()).unwrap();
        let decoded = decode(&encoded, caps()).unwrap();
        let bucket = decoded.buckets.get("bucket").unwrap();
        assert!(bucket.synced);
        assert_eq!(bucket.sync_generation, 0);
        assert_eq!(bucket.keys["present"].size, Some(17));
        assert_eq!(bucket.keys["present"].content_type, None);
        assert_eq!(bucket.gone["deleted"], UNIX_EPOCH + Duration::from_secs(20));
        assert_eq!(decoded.stats.objects, 1);
    }

    #[test]
    fn empty_bucket_universe_is_a_complete_empty_image() {
        let empty = KeyIndexState::default();
        let encoded = encode(&empty, &[], caps()).unwrap();
        let decoded = decode(&encoded, caps()).unwrap();
        assert!(decoded.buckets.is_empty());
        assert_eq!(decoded.stats, IndexStats::default());
    }

    #[test]
    fn sparse_map_backing_capacity_is_charged_before_decode() {
        let mut sparse = KeyIndexState::default();
        sparse.buckets.insert(
            "empty".to_owned(),
            BucketState {
                synced: true,
                ..BucketState::default()
            },
        );
        let encoded = encode(&sparse, &["empty".to_owned()], caps()).unwrap();
        let mut limited = caps();
        limited.decoded_bytes =
            std::mem::size_of::<KeyIndexState>() + std::mem::size_of::<(String, BucketState)>();
        assert!(matches!(
            decode(&encoded, limited),
            Err(ImageError::Capacity)
        ));
        assert!(decode(&encoded, caps()).is_ok());
    }

    #[test]
    fn incomplete_uncertain_wrong_universe_and_small_cap_fail_closed() {
        let mut source = image();
        source.buckets.get_mut("bucket").unwrap().synced = false;
        assert!(matches!(
            encode(&source, &["bucket".to_owned()], caps()),
            Err(ImageError::Incomplete)
        ));
        source.buckets.get_mut("bucket").unwrap().synced = true;
        source
            .buckets
            .get_mut("bucket")
            .unwrap()
            .uncertain_keys
            .insert("present".to_owned(), 1);
        assert!(matches!(
            encode(&source, &["bucket".to_owned()], caps()),
            Err(ImageError::Incomplete)
        ));
        assert!(matches!(
            encode(&image(), &["other".to_owned()], caps()),
            Err(ImageError::Incomplete)
        ));
        let mut limited = caps();
        limited.bytes = 8;
        assert!(matches!(
            encode(&image(), &["bucket".to_owned()], limited),
            Err(ImageError::Capacity)
        ));

        let mut contradictory = image();
        contradictory
            .buckets
            .get_mut("bucket")
            .unwrap()
            .gone
            .insert("present".to_owned(), UNIX_EPOCH + Duration::from_secs(21));
        assert!(matches!(
            encode(&contradictory, &["bucket".to_owned()], caps()),
            Err(ImageError::Corrupt)
        ));

        let mut long_etag = image();
        long_etag
            .buckets
            .get_mut("bucket")
            .unwrap()
            .keys
            .update("present", |entry| {
                entry.etag = Some(ETag::Strong("x".repeat(caps().name_bytes)));
            })
            .unwrap();
        assert!(matches!(
            encode(&long_etag, &["bucket".to_owned()], caps()),
            Err(ImageError::Capacity)
        ));
    }

    #[test]
    fn version_trailing_bytes_and_decoded_budget_are_checked() {
        let mut encoded = encode(&image(), &["bucket".to_owned()], caps()).unwrap();
        encoded[0] = 99;
        assert!(matches!(decode(&encoded, caps()), Err(ImageError::Schema)));
        encoded[0] = VERSION;
        encoded.push(0);
        assert!(matches!(decode(&encoded, caps()), Err(ImageError::Corrupt)));
        encoded.pop();
        let mut limited = caps();
        limited.decoded_bytes = 1;
        assert!(matches!(
            decode(&encoded, limited),
            Err(ImageError::Capacity)
        ));

        for end in 0..encoded.len() {
            assert!(decode(&encoded[..end], caps()).is_err(), "prefix {end}");
        }
        encoded[1..5].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(matches!(
            decode(&encoded, caps()),
            Err(ImageError::Capacity)
        ));
    }

    #[test]
    fn overlap_requires_tombstone_older_than_live_entry() {
        let mut encoded = encode(&image(), &["bucket".to_owned()], caps()).unwrap();
        let deleted = encoded
            .windows(b"deleted".len())
            .position(|part| part == b"deleted")
            .unwrap();
        encoded[deleted..deleted + b"present".len()].copy_from_slice(b"present");
        assert!(matches!(decode(&encoded, caps()), Err(ImageError::Corrupt)));

        let mut stale_tombstone = image();
        stale_tombstone
            .buckets
            .get_mut("bucket")
            .unwrap()
            .gone
            .insert("present".to_owned(), UNIX_EPOCH + Duration::from_secs(18));
        let encoded = encode(&stale_tombstone, &["bucket".to_owned()], caps()).unwrap();
        let decoded = decode(&encoded, caps()).unwrap();
        let bucket = &decoded.buckets["bucket"];
        assert_eq!(bucket.gone["present"], UNIX_EPOCH + Duration::from_secs(18));
        assert_eq!(
            bucket.keys["present"].last_modified,
            UNIX_EPOCH + Duration::from_secs(19)
        );
    }

    #[test]
    fn cut_bookkeeping_failure_never_discards_a_live_feed_mutation() {
        let state = crate::index::KeyIndex::default();
        let entry = |seconds| ObjEntry {
            size: Some(7),
            last_modified: UNIX_EPOCH + Duration::from_secs(seconds),
            etag: None,
            storage_class: ObjectStorageClass::from("STANDARD".to_owned()),
            content_type: None,
            meta: None,
        };
        assert!(crate::index::apply_put_native(
            &state,
            "bucket",
            "invalid-cut",
            entry(1),
            NativeCut {
                writer: Vec::new(),
                epoch: 0,
                sequence: 0,
            },
        ));
        {
            let mut index = state.inner.write().unwrap();
            for writer in 0..MAX_NATIVE_WRITERS {
                index
                    .native_cuts
                    .insert(writer.to_le_bytes().to_vec(), (1, 1));
            }
        }
        assert!(crate::index::apply_put_native(
            &state,
            "bucket",
            "capacity-cut",
            entry(2),
            NativeCut {
                writer: b"new-writer".to_vec(),
                epoch: 1,
                sequence: 1,
            },
        ));
        let index = state.inner.read().unwrap();
        assert!(index.buckets["bucket"].keys.contains_key("invalid-cut"));
        assert!(index.buckets["bucket"].keys.contains_key("capacity-cut"));
    }
}
