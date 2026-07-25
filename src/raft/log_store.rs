//! An in-memory Raft log store (the openraft v2 `RaftLogStorage` split).
//!
//! The LIST index is a rebuildable cache — S3 is the source of truth — so the Raft log does
//! not need to outlive the process: a restarted node re-bootstraps from S3 and rejoins the
//! cluster. Keeping the log in RAM avoids a disk dependency now; a durable log store can slot
//! in behind the same trait later without touching the rest of the crate.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::ops::RangeBounds;
use std::sync::{Arc, Mutex};

use openraft::storage::{LogFlushed, LogState, RaftLogReader, RaftLogStorage};
use openraft::{Entry, LogId, OptionalSend, StorageError, Vote};

use super::{NodeId, TypeConfig};

/// The mutable log state, guarded by a single mutex. Every method holds it only for the
/// in-memory operation and never across an await, so a plain `std` mutex is correct.
#[derive(Default)]
struct Inner {
    /// The persisted vote (Raft's "when": which leader in which term this node voted for).
    vote: Option<Vote<NodeId>>,
    /// The log entries by index. A `BTreeMap` gives ordered range reads and cheap truncation.
    log: BTreeMap<u64, Entry<TypeConfig>>,
    /// The last committed log id, saved so a restart doesn't regress the commit pointer.
    committed: Option<LogId<NodeId>>,
    /// The greatest purged (compacted-away) log id, reported as the log's lower bound.
    last_purged: Option<LogId<NodeId>>,
}

/// A cloneable handle to the in-memory Raft log; clones share one log behind an `Arc<Mutex>`.
#[derive(Clone, Default)]
pub(crate) struct LogStore {
    inner: Arc<Mutex<Inner>>,
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        let inner = self.inner.lock().unwrap();
        Ok(inner
            .log
            .range(range)
            .map(|(_, entry)| entry.clone())
            .collect())
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let inner = self.inner.lock().unwrap();
        // The last log id is the last present entry, or the purge boundary if the log is empty.
        let last_log_id = inner
            .log
            .values()
            .next_back()
            .map_or(inner.last_purged, |e| Some(e.log_id));
        Ok(LogState {
            last_purged_log_id: inner.last_purged,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.inner.lock().unwrap().vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        Ok(self.inner.lock().unwrap().vote)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        self.inner.lock().unwrap().committed = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        Ok(self.inner.lock().unwrap().committed)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        {
            let mut inner = self.inner.lock().unwrap();
            for entry in entries {
                inner.log.insert(entry.log_id.index, entry);
            }
        }
        // In-memory writes are durable the instant they return, so signal completion at once.
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        // Remove `[log_id.index, +oo)`: split_off keeps the lower part in place, drops the rest.
        let mut inner = self.inner.lock().unwrap();
        inner.log.split_off(&log_id.index);
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        // Remove `(-oo, log_id.index]`: keep everything strictly above it as the new log.
        let mut inner = self.inner.lock().unwrap();
        inner.last_purged = Some(log_id);
        inner.log = inner.log.split_off(&(log_id.index + 1));
        Ok(())
    }
}
