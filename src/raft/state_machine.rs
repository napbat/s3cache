//! The Raft state machine: the LIST index itself.
//!
//! Applying a committed [`IndexWrite`] mutates the shared per-bucket index and invalidates
//! the node-local object cache — exactly what the old Valkey-log consumer did, but now
//! driven by consensus so every node applies the same writes in the same order. The proxy
//! reads LIST results straight from the same shared `index` (a fast in-process read), and a
//! snapshot serializes the whole index so a joining or lagging node catches up in one shot.

use std::collections::HashMap;
use std::io::Cursor;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, UNIX_EPOCH};

use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine, Snapshot};
use openraft::{BasicNode, Entry, EntryPayload, LogId, SnapshotMeta, StorageError, StorageIOError, StoredMembership};
use serde::{Deserialize, Serialize};

use crate::index::{BucketState, ObjEntry};
use crate::tier::LocalCache;

use super::{IndexWrite, IndexWriteResponse, NodeId, TypeConfig};

/// The shared per-bucket LIST index: written by `apply` (committed writes) and read directly
/// by the proxy's LIST path. Handing the proxy this `Arc` keeps reads in-process.
pub(crate) type SharedIndex = Arc<RwLock<HashMap<String, BucketState>>>;

/// Raft bookkeeping stored alongside the index — the last applied log id and the last
/// membership config, both required by `applied_state` and by snapshots.
#[derive(Default)]
struct Meta {
    last_applied: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, BasicNode>,
}

/// The serializable snapshot payload: the raft bookkeeping plus the entire index.
#[derive(Serialize, Deserialize, Default)]
struct SnapshotView {
    last_applied: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, BasicNode>,
    index: HashMap<String, BucketState>,
}

/// A snapshot retained in memory: its metadata and the serialized [`SnapshotView`] bytes.
struct StoredSnapshot {
    meta: SnapshotMeta<NodeId, BasicNode>,
    data: Vec<u8>,
}

/// The Raft state machine backing the LIST index. The `index` is shared with the proxy; the
/// `local` cache handle (attached after the proxy is built) lets a peer's committed write
/// invalidate this node's hot + disk copies.
pub(crate) struct StateMachineStore {
    meta: Mutex<Meta>,
    index: SharedIndex,
    local: Mutex<Option<LocalCache>>,
    snapshot_idx: AtomicU64,
    current_snapshot: Mutex<Option<StoredSnapshot>>,
}

impl Default for StateMachineStore {
    fn default() -> Self {
        Self {
            meta: Mutex::new(Meta::default()),
            index: Arc::new(RwLock::new(HashMap::new())),
            local: Mutex::new(None),
            snapshot_idx: AtomicU64::new(0),
            current_snapshot: Mutex::new(None),
        }
    }
}

impl StateMachineStore {
    /// The shared index handle the proxy serves LIST from.
    pub(crate) fn index(&self) -> SharedIndex {
        self.index.clone()
    }

    /// Attach the node-local cache so an applied peer write invalidates its hot + disk copies.
    pub(crate) fn set_local(&self, local: LocalCache) {
        *self.local.lock().unwrap() = Some(local);
    }
}

/// Apply one committed write to the index map. Pure and lock-free — the caller owns the lock
/// and the cache invalidation, so this stays trivially testable.
fn apply_write(index: &mut HashMap<String, BucketState>, write: &IndexWrite) {
    match write {
        IndexWrite::Put { bucket, key, size, ts_ms } => {
            let last_modified = UNIX_EPOCH + Duration::from_millis(*ts_ms);
            index
                .entry(bucket.clone())
                .or_default()
                .keys
                .insert(key.clone(), ObjEntry { size: *size, last_modified });
        }
        IndexWrite::Del { bucket, key } => {
            if let Some(b) = index.get_mut(bucket) {
                b.keys.remove(key);
            }
        }
    }
}

impl RaftSnapshotBuilder<TypeConfig> for Arc<StateMachineStore> {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        // Serialize a consistent view (bookkeeping + index) under the locks; no await here.
        let (last_log_id, last_membership, data) = {
            let meta = self.meta.lock().unwrap();
            let index = self.index.read().unwrap();
            let view = SnapshotView {
                last_applied: meta.last_applied,
                last_membership: meta.last_membership.clone(),
                index: index.clone(),
            };
            let data = serde_json::to_vec(&view).map_err(|e| StorageIOError::read_state_machine(&e))?;
            (meta.last_applied, meta.last_membership.clone(), data)
        };

        let snapshot_idx = self.snapshot_idx.fetch_add(1, Ordering::Relaxed) + 1;
        let snapshot_id = last_log_id.map_or_else(
            || format!("--{snapshot_idx}"),
            |last| format!("{}-{}-{}", last.leader_id, last.index, snapshot_idx),
        );

        let meta = SnapshotMeta { last_log_id, last_membership, snapshot_id };
        *self.current_snapshot.lock().unwrap() = Some(StoredSnapshot { meta: meta.clone(), data: data.clone() });
        Ok(Snapshot { meta, snapshot: Box::new(Cursor::new(data)) })
    }
}

impl RaftStateMachine<TypeConfig> for Arc<StateMachineStore> {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), StorageError<NodeId>> {
        let meta = self.meta.lock().unwrap();
        Ok((meta.last_applied, meta.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<IndexWriteResponse>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + openraft::OptionalSend,
        I::IntoIter: openraft::OptionalSend,
    {
        let mut responses = Vec::new();
        let mut invalidate: Vec<(String, String)> = Vec::new();
        {
            let mut meta = self.meta.lock().unwrap();
            let mut index = self.index.write().unwrap();
            for entry in entries {
                meta.last_applied = Some(entry.log_id);
                match entry.payload {
                    EntryPayload::Blank => {}
                    EntryPayload::Normal(ref write) => {
                        apply_write(&mut index, write);
                        let (IndexWrite::Put { bucket, key, .. } | IndexWrite::Del { bucket, key }) = write;
                        invalidate.push((bucket.clone(), key.clone()));
                    }
                    EntryPayload::Membership(ref mem) => {
                        meta.last_membership = StoredMembership::new(Some(entry.log_id), mem.clone());
                    }
                }
                responses.push(IndexWriteResponse);
            }
        }
        // Invalidate the node-local cache *after* dropping the locks, so no lock is held across
        // the await. A clone lets us release the `local` guard before awaiting too.
        let local = self.local.lock().unwrap().clone();
        if let Some(local) = local {
            for key in invalidate {
                local.invalidate(&key).await;
            }
        }
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        let bytes = snapshot.into_inner();
        let view: SnapshotView =
            serde_json::from_slice(&bytes).map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), &e))?;
        {
            let mut m = self.meta.lock().unwrap();
            m.last_applied = view.last_applied;
            m.last_membership = view.last_membership;
            *self.index.write().unwrap() = view.index;
        }
        *self.current_snapshot.lock().unwrap() = Some(StoredSnapshot { meta: meta.clone(), data: bytes });
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        match &*self.current_snapshot.lock().unwrap() {
            Some(s) => Ok(Some(Snapshot { meta: s.meta.clone(), snapshot: Box::new(Cursor::new(s.data.clone())) })),
            None => Ok(None),
        }
    }
}
