//! Embedded Raft consensus for the cross-node LIST index — the replacement for the Valkey
//! commit log. The Raft *state machine is the index*: a write is proposed through consensus
//! and applied identically on every node, and a linearizable read barrier makes cross-node
//! reads strongly consistent. There is no external coordinator and no configured leader —
//! any node can be elected (Raft's "inherited role"), so the cluster is self-organizing and
//! survives the loss of any minority.
//!
//! Correctness leans on one property this proxy already has: S3 is the source of truth and
//! the LIST index is a rebuildable cache. So the Raft log is kept in memory ([`log_store`]) —
//! a restarted node re-bootstraps from S3 and rejoins rather than depending on a durable
//! on-disk log.
//!
//! Milestone 1 (this module) is the consensus core plus an in-process transport
//! ([`network`]), validated by [`tests`] against a real multi-node cluster. Wiring it into
//! the proxy write/read paths and a cross-pod HTTP transport come next.

mod log_store;
mod network;
mod state_machine;

#[cfg(test)]
mod tests;

use std::io::Cursor;

use serde::{Deserialize, Serialize};

pub(crate) use log_store::LogStore;
pub(crate) use network::Loopback;
pub(crate) use state_machine::StateMachineStore;

/// A cluster node id. Small and cheap; assigned per replica (e.g. from the pod ordinal).
pub(crate) type NodeId = u64;

openraft::declare_raft_types!(
    /// The concrete openraft type configuration for the index cluster. `SnapshotData`,
    /// `Node`, `Entry`, and the runtime take their defaults (`Cursor<Vec<u8>>`, `BasicNode`,
    /// `Entry<Self>`, Tokio).
    pub(crate) TypeConfig:
        D = IndexWrite,
        R = IndexWriteResponse,
);

/// One committed index mutation — the application payload replicated through the log. `size`
/// and `ts_ms` carry the LIST metadata so a peer applying the event reconstructs the same
/// `ObjEntry` without a round-trip to S3.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum IndexWrite {
    /// A key was written (or overwritten): insert/update it in the index.
    Put { bucket: String, key: String, size: i64, ts_ms: u64 },
    /// A key was deleted: drop it from the index.
    Del { bucket: String, key: String },
}

/// The state machine's reply to an applied [`IndexWrite`]. The index carries no per-write
/// return value, so this is empty — it exists only to satisfy openraft's `AppDataResponse`.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct IndexWriteResponse;
