use std::collections::{BTreeMap, BTreeSet};

use super::journal::JournalError;
use super::{Cursor, Record, SourceId};

/// Projection of unresolved mutations from one contiguous scoped history.
/// An outcome removes an unresolved operation, but does **not** prove that an
/// existing LIST index has applied the mutation or may serve local reads.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PendingSet {
    scope: Option<(SourceId, String)>,
    next_slot: u64,
    seen_ops: BTreeSet<String>,
    by_op: BTreeMap<String, (String, u64)>,
    by_key: BTreeMap<String, BTreeSet<String>>,
}

impl PendingSet {
    /// Apply one record at its exact contiguous source position. Operation IDs
    /// are never reused within this bounded, unretired history.
    ///
    /// # Errors
    /// Returns an error for wrong scope/order, reused ID, or an outcome that
    /// does not bind the original intent slot and key. Rejection changes nothing.
    pub fn apply_at(&mut self, cursor: &Cursor, record: &Record) -> Result<(), JournalError> {
        if cursor.next_slot != self.next_slot
            || self.scope.as_ref().is_some_and(|(source, bucket)| {
                source != &cursor.source || bucket != &cursor.bucket
            })
            || (self.scope.is_none() && self.next_slot != 0)
        {
            return Err(JournalError::ConfigMismatch);
        }
        let next = self
            .next_slot
            .checked_add(1)
            .ok_or(JournalError::Capacity)?;
        match record {
            Record::Intent { op_id, key, .. } => {
                if op_id.is_empty() || key.is_empty() {
                    return Err(JournalError::Corrupt("empty operation ID or key"));
                }
                if self.seen_ops.contains(op_id) {
                    return Err(JournalError::Corrupt("reused operation ID"));
                }
                self.seen_ops.insert(op_id.clone());
                self.by_op
                    .insert(op_id.clone(), (key.clone(), cursor.next_slot));
                self.by_key
                    .entry(key.clone())
                    .or_default()
                    .insert(op_id.clone());
            }
            Record::Outcome {
                op_id,
                intent_slot,
                key,
                ..
            } => {
                if self.by_op.get(op_id) != Some(&(key.clone(), *intent_slot)) {
                    return Err(JournalError::Corrupt("outcome has no matching intent"));
                }
                let Some(ops) = self.by_key.get_mut(key) else {
                    return Err(JournalError::Corrupt("missing pending key"));
                };
                if !ops.contains(op_id) {
                    return Err(JournalError::Corrupt("missing pending key"));
                }
                ops.remove(op_id);
                let empty = ops.is_empty();
                self.by_op.remove(op_id);
                if empty {
                    self.by_key.remove(key);
                }
            }
            Record::ReaderAdmission { .. } => {}
        }
        self.scope
            .get_or_insert_with(|| (cursor.source.clone(), cursor.bucket.clone()));
        self.next_slot = next;
        Ok(())
    }

    /// The next contiguous position required by this projection.
    #[must_use]
    pub fn next_slot(&self) -> u64 {
        self.next_slot
    }

    /// Operation IDs currently fencing `key`.
    #[must_use]
    pub fn operations_for(&self, key: &str) -> Option<&BTreeSet<String>> {
        self.by_key.get(key)
    }

    /// Whether every recorded intent has a definitive outcome.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_op.is_empty()
    }
}
