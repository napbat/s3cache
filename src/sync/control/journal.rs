use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};

use super::codec;
use super::{Cursor, PutVerdict, Record, SlotStore, SourceId, StoreError};

/// Hard resource bounds. Retained bytes cannot exceed
/// `max_slots * max_record_bytes` for a single bucket history.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Limits {
    /// Maximum bytes in one encoded manifest or record.
    pub max_record_bytes: usize,
    /// Maximum retained slots for one bucket, with no retirement in this slice.
    pub max_slots: u64,
    /// Maximum records in one scan page.
    pub max_scan_events: usize,
    /// Maximum encoded bytes in one scan page.
    pub max_scan_bytes: usize,
    /// Maximum occupied slots an append may probe before backpressure.
    pub max_append_probes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_record_bytes: 16 * 1024,
            max_slots: 1_000_000,
            max_scan_events: 4096,
            max_scan_bytes: 8 * 1024 * 1024,
            max_append_probes: 64,
        }
    }
}

/// Supplied identity and admission envelope for opening a control source.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct JournalConfig {
    /// Persistent source identity. Do not invent a new value on restart.
    pub source: SourceId,
    /// Supported source-admission contract version.
    pub admission_version: u32,
    /// Largest admitted local serve duration in milliseconds.
    pub max_admission_ms: u64,
    /// Durable and per-operation bounds.
    pub limits: Limits,
}

/// Journal error; none of these grants an application serving permission.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JournalError {
    /// A store operation could not prove created, occupied, or absent.
    Store(StoreError),
    /// The proposed slot may have been committed but readback was inconclusive.
    UnknownWrite {
        /// Exact slot whose conditional PUT may have committed.
        slot: u64,
        /// Original cursor to use when retrying the identical append.
        retry_from: Cursor,
    },
    /// Manifest conditional PUT may have committed; reopen with same config.
    UnknownManifest,
    /// A configured storage or record limit was reached; no later slot may skip it.
    Capacity,
    /// Scan byte budget exhausted before one event could be returned.
    Backpressured,
    /// Occupied append probes exhausted; this cursor follows a verified slot.
    AppendBackpressured { resume: Cursor },
    /// The stored bytes or record sequence violate the source contract.
    Corrupt(&'static str),
    /// Source identity, generation, scope, or admission configuration differs.
    ConfigMismatch,
}

impl std::fmt::Display for JournalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for JournalError {}

impl From<StoreError> for JournalError {
    fn from(error: StoreError) -> Self {
        Self::Store(error)
    }
}

/// Exact result of an append, including its successor cursor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AppendResult {
    /// The conditional write returned success.
    Created(Cursor),
    /// Exact readback proved an ambiguous or retried write committed.
    Recovered(Cursor),
}

/// Bounded contiguous page. `at_first_absent` is a prefix observation, not a
/// source-freshness or read-authority proof.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScanPage {
    /// Validated records in source order.
    pub records: Vec<Record>,
    /// Position immediately following these records.
    pub cursor: Cursor,
    /// Whether the next exact slot was absent at the time of its read.
    pub at_first_absent: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct SlotEnvelope {
    source: SourceId,
    bucket: String,
    slot: u64,
    record: Record,
}

/// Immutable, conditional slot journal over a configured store.
#[derive(Debug)]
pub struct Journal<S> {
    store: S,
    config: JournalConfig,
}

impl<S: SlotStore> Journal<S> {
    /// Open or create a manifest without changing an existing source identity.
    ///
    /// # Errors
    /// Returns a store, format, capacity, or configuration error.
    pub async fn open(store: S, config: JournalConfig) -> Result<Self, JournalError> {
        validate_config(&config)?;
        let key = manifest_key(&config.source);
        let expected = codec::encode(&config, config.limits.max_record_bytes)?;
        match store.put_if_absent(&key, expected.clone()).await? {
            PutVerdict::Created => {}
            PutVerdict::Occupied | PutVerdict::Unknown => {
                let actual = store
                    .get(&key)
                    .await?
                    .ok_or(JournalError::UnknownManifest)?;
                let stored: JournalConfig = codec::decode(&actual, config.limits.max_record_bytes)?;
                if stored != config || actual != expected {
                    return Err(JournalError::ConfigMismatch);
                }
            }
        }
        Ok(Self { store, config })
    }

    /// Initial cursor of a bucket's immutable history.
    #[must_use]
    pub fn start(&self, bucket: impl Into<String>) -> Cursor {
        Cursor {
            source: self.config.source.clone(),
            bucket: bucket.into(),
            next_slot: 0,
        }
    }

    /// Append at `from` or a later occupied slot, without skipping an absent
    /// slot. An ambiguous write returns the original retry cursor; occupied
    /// probe exhaustion returns a verified continuation cursor.
    ///
    /// # Errors
    /// Returns capacity, backpressure, unknown-write, store, or format error.
    pub async fn append(
        &self,
        from: &Cursor,
        record: &Record,
    ) -> Result<AppendResult, JournalError> {
        self.check_cursor(from)?;
        self.validate_record(record)?;
        self.validate_predecessor(from).await?;
        let mut slot = from.next_slot;
        for _ in 0..self.config.limits.max_append_probes {
            if slot >= self.config.limits.max_slots {
                return Err(JournalError::Capacity);
            }
            let key = slot_key(&self.config.source, &from.bucket, slot);
            let bytes = codec::encode(
                &SlotEnvelope {
                    source: self.config.source.clone(),
                    bucket: from.bucket.clone(),
                    slot,
                    record: record.clone(),
                },
                self.config.limits.max_record_bytes,
            )?;
            if let Some(actual) = self.store.get(&key).await? {
                if self.occupied(&actual, &bytes, from, slot, record)? {
                    return Ok(AppendResult::Recovered(Self::after(from, slot)?));
                }
                slot = slot.checked_add(1).ok_or(JournalError::Capacity)?;
                continue;
            }
            self.validate_outcome_binding(from, slot, record).await?;
            let put = self.store.put_if_absent(&key, bytes.clone()).await?;
            if put == PutVerdict::Created {
                return Ok(AppendResult::Created(Self::after(from, slot)?));
            }
            match self.store.get(&key).await? {
                Some(actual) => {
                    if self.occupied(&actual, &bytes, from, slot, record)? {
                        return Ok(AppendResult::Recovered(Self::after(from, slot)?));
                    }
                    slot = slot.checked_add(1).ok_or(JournalError::Capacity)?;
                }
                None => {
                    return Err(JournalError::UnknownWrite {
                        slot,
                        retry_from: from.clone(),
                    });
                }
            }
        }
        if slot >= self.config.limits.max_slots {
            return Err(JournalError::Capacity);
        }
        Err(JournalError::AppendBackpressured {
            resume: Cursor {
                source: from.source.clone(),
                bucket: from.bucket.clone(),
                next_slot: slot,
            },
        })
    }

    /// Read consecutive slots, stopping at the first absent slot or a page
    /// budget. An absent slot is not a promise that the source will stay idle.
    ///
    /// # Errors
    /// Returns corruption, capacity, backpressure, configuration, or store error.
    pub async fn scan(&self, from: &Cursor) -> Result<ScanPage, JournalError> {
        self.check_cursor(from)?;
        self.validate_predecessor(from).await?;
        let mut records = Vec::new();
        let mut cursor = from.clone();
        let mut bytes_read = 0usize;
        for _ in 0..self.config.limits.max_scan_events {
            if cursor.next_slot >= self.config.limits.max_slots {
                if records.is_empty() {
                    return Err(JournalError::Capacity);
                }
                return Ok(ScanPage {
                    records,
                    cursor,
                    at_first_absent: false,
                });
            }
            let key = slot_key(&self.config.source, &cursor.bucket, cursor.next_slot);
            let Some(bytes) = self.store.get(&key).await? else {
                return Ok(ScanPage {
                    records,
                    cursor,
                    at_first_absent: true,
                });
            };
            bytes_read = bytes_read
                .checked_add(bytes.len())
                .ok_or(JournalError::Backpressured)?;
            if bytes_read > self.config.limits.max_scan_bytes {
                if records.is_empty() {
                    return Err(JournalError::Backpressured);
                }
                return Ok(ScanPage {
                    records,
                    cursor,
                    at_first_absent: false,
                });
            }
            let envelope = self.decode_slot(&bytes, &cursor, cursor.next_slot)?;
            records.push(envelope.record);
            cursor.next_slot += 1;
        }
        Ok(ScanPage {
            records,
            cursor,
            at_first_absent: false,
        })
    }

    fn check_cursor(&self, cursor: &Cursor) -> Result<(), JournalError> {
        if cursor.source != self.config.source || cursor.bucket.is_empty() {
            return Err(JournalError::ConfigMismatch);
        }
        Ok(())
    }

    async fn validate_predecessor(&self, cursor: &Cursor) -> Result<(), JournalError> {
        let Some(prior) = cursor.next_slot.checked_sub(1) else {
            return Ok(());
        };
        if prior >= self.config.limits.max_slots {
            return Err(JournalError::Capacity);
        }
        let key = slot_key(&self.config.source, &cursor.bucket, prior);
        let bytes = self
            .store
            .get(&key)
            .await?
            .ok_or(JournalError::Corrupt("cursor predecessor absent"))?;
        self.decode_slot(&bytes, cursor, prior)?;
        Ok(())
    }

    async fn validate_outcome_binding(
        &self,
        cursor: &Cursor,
        slot: u64,
        record: &Record,
    ) -> Result<(), JournalError> {
        let Record::Outcome {
            op_id,
            intent_slot,
            key,
            ..
        } = record
        else {
            return Ok(());
        };
        if *intent_slot >= slot {
            return Err(JournalError::Corrupt("outcome precedes intent"));
        }
        let intent_key = slot_key(&self.config.source, &cursor.bucket, *intent_slot);
        let bytes = self
            .store
            .get(&intent_key)
            .await?
            .ok_or(JournalError::Corrupt("outcome intent absent"))?;
        let envelope = self.decode_slot(&bytes, cursor, *intent_slot)?;
        match envelope.record {
            Record::Intent {
                op_id: original,
                key: original_key,
                ..
            } if original == *op_id && original_key == *key => Ok(()),
            _ => Err(JournalError::Corrupt("outcome intent mismatch")),
        }
    }

    fn after(from: &Cursor, slot: u64) -> Result<Cursor, JournalError> {
        Ok(Cursor {
            source: from.source.clone(),
            bucket: from.bucket.clone(),
            next_slot: slot.checked_add(1).ok_or(JournalError::Capacity)?,
        })
    }

    fn decode_slot(
        &self,
        bytes: &[u8],
        cursor: &Cursor,
        slot: u64,
    ) -> Result<SlotEnvelope, JournalError> {
        let envelope: SlotEnvelope = codec::decode(bytes, self.config.limits.max_record_bytes)?;
        if envelope.source != cursor.source
            || envelope.bucket != cursor.bucket
            || envelope.slot != slot
        {
            return Err(JournalError::Corrupt("slot identity mismatch"));
        }
        self.validate_record(&envelope.record)?;
        Ok(envelope)
    }

    fn occupied(
        &self,
        actual: &[u8],
        expected: &[u8],
        cursor: &Cursor,
        slot: u64,
        record: &Record,
    ) -> Result<bool, JournalError> {
        let stored = self.decode_slot(actual, cursor, slot)?.record;
        if actual == expected {
            return Ok(true);
        }
        let same_identity = match (&stored, record) {
            (Record::Intent { op_id: a, .. }, Record::Intent { op_id: b, .. }) => a == b,
            (
                Record::Outcome {
                    op_id: a,
                    intent_slot: a_slot,
                    ..
                },
                Record::Outcome {
                    op_id: b,
                    intent_slot: b_slot,
                    ..
                },
            ) => a == b && a_slot == b_slot,
            (
                Record::ReaderAdmission {
                    reader_id: a,
                    incarnation: a_inc,
                    ..
                },
                Record::ReaderAdmission {
                    reader_id: b,
                    incarnation: b_inc,
                    ..
                },
            ) => a == b && a_inc == b_inc,
            _ => false,
        };
        if same_identity {
            return Err(JournalError::Corrupt("logical record identity conflict"));
        }
        Ok(false)
    }

    fn validate_record(&self, record: &Record) -> Result<(), JournalError> {
        match record {
            Record::Intent { op_id, key, .. } | Record::Outcome { op_id, key, .. }
                if op_id.is_empty() || key.is_empty() =>
            {
                Err(JournalError::Corrupt("empty operation ID or key"))
            }
            Record::ReaderAdmission {
                reader_id,
                incarnation,
                max_serve_ms,
                config_version,
            } if reader_id.is_empty()
                || *incarnation == 0
                || *max_serve_ms == 0
                || *max_serve_ms > self.config.max_admission_ms
                || *config_version != self.config.admission_version =>
            {
                Err(JournalError::ConfigMismatch)
            }
            _ => Ok(()),
        }
    }
}

fn validate_config(config: &JournalConfig) -> Result<(), JournalError> {
    let l = config.limits;
    if config.source.name.is_empty()
        || config.admission_version == 0
        || config.max_admission_ms == 0
        || l.max_record_bytes < 128
        || l.max_slots == 0
        || l.max_scan_events == 0
        || l.max_scan_bytes < l.max_record_bytes
        || l.max_append_probes == 0
        || l.max_slots.checked_mul(l.max_record_bytes as u64).is_none()
    {
        return Err(JournalError::ConfigMismatch);
    }
    Ok(())
}

fn manifest_key(source: &SourceId) -> String {
    format!(
        "s3cache-control/v1/{}/manifest",
        URL_SAFE_NO_PAD.encode(source.name.as_bytes())
    )
}

fn slot_key(source: &SourceId, bucket: &str, slot: u64) -> String {
    format!(
        "s3cache-control/v1/{}/{}/bucket/{}/slot/{slot:020}",
        URL_SAFE_NO_PAD.encode(source.name.as_bytes()),
        source.generation,
        URL_SAFE_NO_PAD.encode(bucket.as_bytes())
    )
}
