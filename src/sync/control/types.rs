use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// Stable identity of one durable control history, supplied by deployment.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SourceId {
    /// Stable name, reused after a process restart.
    pub name: String,
    /// Explicit history generation; changing it requires migration.
    pub generation: u64,
}

/// Native position of the next slot to read or attempt to append.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Cursor {
    /// Control source that owns the slot chain.
    pub source: SourceId,
    /// Application bucket scoped by this chain.
    pub bucket: String,
    /// The next contiguous slot, starting at zero.
    pub next_slot: u64,
}

/// Mutation category; every intent names one affected key.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Mutation {
    /// Put or overwrite an object.
    Put,
    /// Delete an object or one version.
    Delete,
    /// Copy into the named destination key.
    Copy,
    /// Complete an upload into the named destination key.
    MultipartComplete,
}

/// A definitive outcome; ambiguity is represented by *no* outcome record.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum FinalOutcome {
    /// Origin definitely committed the mutation.
    Committed,
    /// Origin definitely rejected without mutating the key.
    RejectedWithoutMutation,
    /// The origin call provably never began.
    AbortedBeforeDispatch,
}

/// One immutable source record.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Record {
    /// Durable, per-key intent written before an origin mutation.
    Intent {
        /// Stable ID across ambiguous append retries.
        op_id: String,
        /// Affected key (the cursor supplies its bucket).
        key: String,
        /// Intended mutation category.
        mutation: Mutation,
    },
    /// Definitive result for one matching intent.
    Outcome {
        /// ID of the intent being closed.
        op_id: String,
        /// Slot that holds the exact intent.
        intent_slot: u64,
        /// Key of that intent, repeated to detect mismatch.
        key: String,
        /// Definitive result only; an uncertain origin call has no outcome.
        result: FinalOutcome,
    },
    /// Source-ordered, finite read admission for one reader incarnation.
    ReaderAdmission {
        /// Stable reader identity.
        reader_id: String,
        /// Unique incarnation, including across restarts and renewals.
        incarnation: u64,
        /// Maximum local serve duration; deadline starts before append.
        max_serve_ms: u64,
        /// Version of the fleet's admission contract.
        config_version: u32,
    },
}

/// Conditional-create result from a control store.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PutVerdict {
    /// The requested bytes were created.
    Created,
    /// Another object already occupies this exact key.
    Occupied,
    /// The request may have committed, so exact-key readback is required.
    Unknown,
}

/// Store I/O failure that has no truthful committed/absent verdict.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoreError(pub String);

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for StoreError {}

/// Exact-key, conditional object storage. `get` must be authoritative enough
/// to distinguish an absent slot from one already committed before this read.
#[async_trait]
pub trait SlotStore: Send + Sync {
    /// Create `key` only when absent, without overwriting a prior value.
    ///
    /// # Errors
    /// Returns an I/O error only when no truthful write verdict is available.
    async fn put_if_absent(&self, key: &str, bytes: Vec<u8>) -> Result<PutVerdict, StoreError>;

    /// Read one exact key. Absence is `None`; uncertainty is an error.
    ///
    /// # Errors
    /// Returns a store or body-read error that cannot be treated as absence.
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError>;
}
