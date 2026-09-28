//! Bounded, immutable S3 control-source slots.
//!
//! This storage layer proves a contiguous source prefix and tracks unresolved
//! operations. It does not grant local-read authority or dispatch mutations.

mod codec;
mod journal;
mod pending;
mod s3;
mod types;

pub use journal::{AppendResult, Journal, JournalConfig, JournalError, Limits, ScanPage};
pub use pending::PendingSet;
pub use s3::S3SlotStore;
pub use types::{
    Cursor, FinalOutcome, Mutation, PutVerdict, Record, SlotStore, SourceId, StoreError,
};

#[cfg(test)]
mod tests;
