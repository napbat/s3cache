//! An in-process Raft transport: an RPC to a peer is delivered by calling that peer's local
//! `Raft` handle directly, through a shared registry of node handles.
//!
//! This drives a full multi-node cluster inside a single process — used by the tests, and the
//! seam a cross-pod HTTP transport will later sit behind (same `RaftNetwork` trait, a real
//! socket instead of a map lookup).

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use openraft::error::{InstallSnapshotError, RPCError, RaftError, RemoteError, Unreachable};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse, VoteRequest,
    VoteResponse,
};
use openraft::{BasicNode, Raft, RaftNetwork, RaftNetworkFactory};

use super::{NodeId, TypeConfig};

/// A shared registry of the in-process peers' `Raft` handles. Cloneable — every connection
/// holds the same registry — and doubles as the `RaftNetworkFactory`.
#[derive(Clone, Default)]
pub(crate) struct Loopback {
    peers: Arc<Mutex<BTreeMap<NodeId, Raft<TypeConfig>>>>,
}

impl Loopback {
    /// Register a node's `Raft` handle so peers can route RPCs to it. Called once per node
    /// after it is constructed and before the cluster is initialized.
    pub(crate) fn register(&self, id: NodeId, raft: Raft<TypeConfig>) {
        self.peers.lock().unwrap().insert(id, raft);
    }

    fn peer(&self, target: NodeId) -> Option<Raft<TypeConfig>> {
        self.peers.lock().unwrap().get(&target).cloned()
    }
}

/// A connection to one target node: the shared registry plus the target's id.
pub(crate) struct LoopbackConn {
    reg: Loopback,
    target: NodeId,
}

impl LoopbackConn {
    /// The target's handle, or `Unreachable` — a not-yet-registered peer is treated exactly
    /// like an unreachable socket, so Raft backs off and retries. Returns the small
    /// `Unreachable` (not the large `RPCError`); each call site lifts it into its own
    /// `RPCError<_, _, E>`, which keeps this reusable across the different RPC error types.
    fn handle(&self) -> Result<Raft<TypeConfig>, Unreachable> {
        self.reg.peer(self.target).ok_or_else(|| {
            let io =
                std::io::Error::new(std::io::ErrorKind::NotConnected, format!("peer {} not registered", self.target));
            Unreachable::new(&io)
        })
    }
}

impl RaftNetworkFactory<TypeConfig> for Loopback {
    type Network = LoopbackConn;

    async fn new_client(&mut self, target: NodeId, _node: &BasicNode) -> Self::Network {
        LoopbackConn { reg: self.clone(), target }
    }
}

impl RaftNetwork<TypeConfig> for LoopbackConn {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let raft = self.handle().map_err(RPCError::Unreachable)?;
        raft.append_entries(rpc).await.map_err(|e| RemoteError::new(self.target, e).into())
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let raft = self.handle().map_err(RPCError::Unreachable)?;
        raft.vote(rpc).await.map_err(|e| RemoteError::new(self.target, e).into())
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<InstallSnapshotResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>>
    {
        let raft = self.handle().map_err(RPCError::Unreachable)?;
        raft.install_snapshot(rpc).await.map_err(|e| RemoteError::new(self.target, e).into())
    }
}
