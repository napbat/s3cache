//! A follower decodes a peer's image as its chunks arrive, one whole item at a
//! time. Groupnet's worker refreshes the donor's claim only between its steps,
//! so decoding a production-sized image in one step after the last chunk let
//! that claim lapse on the follower while the donor was live.

use std::collections::{BTreeMap, HashMap};

use crate::index::fleet::{
    DecodeBudget, ImageCaps, ImageError, Reader, VERSION, bucket_charge, gone_charge, read_entry,
    read_timestamp, state_charge,
};
use crate::index::pending::PendingWrites;
use crate::index::{BucketState, IndexStats, KeyIndexState};

/// What the next bytes of an image hold.
enum Next {
    Header,
    Bucket,
    Keys(usize),
    GoneCount,
    Gone(usize),
    Done,
}

/// Decodes a private skeletal image from a growing prefix of its bytes. The
/// caller holds a decoded-state permit, and checks the whole image's
/// commitment before it uses the result.
pub(in crate::index) struct ImageDecoder {
    caps: ImageCaps,
    budget: DecodeBudget,
    offset: usize,
    next: Next,
    buckets_left: usize,
    rows: usize,
    buckets: HashMap<String, BucketState>,
    stats: IndexStats,
    /// The bucket being decoded, or the last one decoded.
    name: Option<String>,
    bucket: BucketState,
}

impl ImageDecoder {
    pub(in crate::index) fn new(caps: ImageCaps) -> Result<Self, ImageError> {
        let caps = caps.validate()?;
        Ok(Self {
            budget: DecodeBudget::new(caps.decoded_bytes)?,
            caps,
            offset: 0,
            next: Next::Header,
            buckets_left: 0,
            rows: 0,
            buckets: HashMap::new(),
            stats: IndexStats::default(),
            name: None,
            bucket: BucketState::default(),
        })
    }

    /// Decode every whole item of `bytes`, the image received so far, past
    /// what earlier calls decoded.
    pub(in crate::index) fn feed(&mut self, bytes: &[u8]) -> Result<(), ImageError> {
        if bytes.len() > self.caps.bytes {
            return Err(ImageError::Capacity);
        }
        while !matches!(self.next, Next::Done) {
            let mut reader = Reader {
                bytes,
                offset: self.offset,
            };
            match self.item(&mut reader) {
                Ok(()) => self.offset = reader.offset,
                Err(ImageError::Truncated) => return Ok(()),
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    /// The decoded image, once `bytes` is all of it.
    pub(in crate::index) fn finish(mut self, bytes: &[u8]) -> Result<KeyIndexState, ImageError> {
        self.feed(bytes)?;
        if !matches!(self.next, Next::Done) || self.offset != bytes.len() {
            return Err(ImageError::Corrupt);
        }
        Ok(KeyIndexState {
            buckets: self.buckets,
            stats: self.stats,
            capture: None,
            native_cuts: BTreeMap::new(),
            pending: PendingWrites::default(),
        })
    }

    /// Decode one item. Every read precedes every charge and insert, so an
    /// item cut short leaves nothing to undo.
    fn item(&mut self, reader: &mut Reader<'_>) -> Result<(), ImageError> {
        match self.next {
            Next::Header => {
                if reader.u8()? != VERSION {
                    return Err(ImageError::Schema);
                }
                let count = reader.u32()?;
                if count > self.caps.buckets {
                    return Err(ImageError::Capacity);
                }
                self.budget.take(state_charge(count)?)?;
                self.buckets
                    .try_reserve(count)
                    .map_err(|_| ImageError::Capacity)?;
                self.buckets_left = count;
                self.next = if count == 0 { Next::Done } else { Next::Bucket };
            }
            Next::Bucket => {
                let name = reader.text(self.caps.name_bytes)?;
                let keys = reader.u32()?;
                if self.name.as_deref().is_some_and(|last| last >= name) {
                    return Err(ImageError::Corrupt);
                }
                self.budget.take(bucket_charge(name.len())?)?;
                self.add_rows(keys)?;
                self.name = Some(name.to_owned());
                self.next = if keys == 0 {
                    Next::GoneCount
                } else {
                    Next::Keys(keys)
                };
            }
            Next::Keys(left) => {
                let (key, entry) = read_entry(reader, &mut self.budget, self.caps.name_bytes)?;
                if self.bucket.keys.last_key().is_some_and(|last| last >= key) {
                    return Err(ImageError::Corrupt);
                }
                self.bucket
                    .stats
                    .replace(IndexStats::default(), IndexStats::for_entry(&entry));
                self.bucket.keys.insert(key.to_owned(), entry);
                self.next = if left > 1 {
                    Next::Keys(left - 1)
                } else {
                    Next::GoneCount
                };
            }
            Next::GoneCount => {
                let gone = reader.u32()?;
                self.add_rows(gone)?;
                if gone == 0 {
                    self.end_bucket();
                } else {
                    self.next = Next::Gone(gone);
                }
            }
            Next::Gone(left) => {
                let key = reader.text(self.caps.name_bytes)?;
                let deleted_at = read_timestamp(reader)?;
                if self.bucket.gone.last_key().is_some_and(|last| last >= key) {
                    return Err(ImageError::Corrupt);
                }
                self.budget.take(gone_charge(key.len())?)?;
                if self
                    .bucket
                    .keys
                    .get(key)
                    .is_some_and(|entry| deleted_at >= entry.last_modified)
                {
                    return Err(ImageError::Corrupt);
                }
                self.bucket.gone.insert(key.to_owned(), deleted_at);
                if left > 1 {
                    self.next = Next::Gone(left - 1);
                } else {
                    self.end_bucket();
                }
            }
            Next::Done => {}
        }
        Ok(())
    }

    fn add_rows(&mut self, rows: usize) -> Result<(), ImageError> {
        self.rows = self.rows.checked_add(rows).ok_or(ImageError::Capacity)?;
        if self.rows > self.caps.rows {
            return Err(ImageError::Capacity);
        }
        Ok(())
    }

    fn end_bucket(&mut self) {
        let bucket = std::mem::take(&mut self.bucket);
        self.stats.replace(IndexStats::default(), bucket.stats);
        let name = self.name.clone().unwrap_or_default();
        self.buckets.insert(
            name,
            BucketState {
                synced: true,
                ..bucket
            },
        );
        self.buckets_left -= 1;
        self.next = if self.buckets_left == 0 {
            Next::Done
        } else {
            Next::Bucket
        };
    }
}
