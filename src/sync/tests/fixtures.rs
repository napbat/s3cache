use std::sync::Arc;
use std::time::Duration;

use groupnet::core::NodeId;
use groupnet::network::RouterConfig;
use groupnet::runtime::{Group, Node};
use groupnet::transport::link::{BoundLink, LinkConfig};
use groupnet::transport::mem::Network;

use crate::index::KeyIndex;
use crate::metrics::Metrics;
use crate::sync::coherence::{Consistency, WriteSync};
use crate::tier::TieredCache;

use super::{Index, brisk, no_reconcile, test_lease};

pub(super) async fn spawn_node(net: &Network, id: &str, peer: &str) -> (NodeId, Node, Group) {
    spawn_neighbors(net, id, &[peer]).await
}

pub(super) async fn spawn_neighbors(
    net: &Network,
    id: &str,
    peers: &[&str],
) -> (NodeId, Node, Group) {
    let me = NodeId::new(id);
    let peers = peers.iter().map(|peer| NodeId::new(*peer)).collect();
    let node = Node::builder(me.clone())
        .link(BoundLink::new(
            net.endpoint(me.clone()),
            LinkConfig::new(peers),
        ))
        .routing(RouterConfig {
            announce_interval: Duration::from_millis(10),
            ..RouterConfig::default()
        })
        .config(brisk())
        .start()
        .await
        .expect("managed memory node starts");
    let group = node.join_group("s3cache");
    (me, node, group)
}

/// A feed whose caller retains the managed node separately.
pub(super) fn attach(group: Group, me: NodeId, consistency: Consistency) -> WriteSync {
    WriteSync::attach(group, me, consistency, test_lease(), None)
}

/// A fully-wired pair: A publishes, B applies into its local state and cache.
pub(super) async fn wired_pair(net: &Network) -> (WriteSync, Arc<WriteSync>, Index, TieredCache) {
    wired_pair_named(net, ("sync-a", "sync-b"), Consistency::Strong).await
}

/// Each returned feed owns its node, including the managed network lifetime.
pub(super) async fn wired_pair_named(
    net: &Network,
    ids: (&str, &str),
    consistency: Consistency,
) -> (WriteSync, Arc<WriteSync>, Index, TieredCache) {
    let (a_id, a_node, a_group) = spawn_node(net, ids.0, ids.1).await;
    let (b_id, b_node, b_group) = spawn_node(net, ids.1, ids.0).await;
    let metrics = Arc::new(Metrics::default());
    let cache = TieredCache::new(1024 * 1024, None, metrics.clone());
    let state = Arc::new(KeyIndex::default());
    let sync_b = Arc::new(WriteSync::attach(
        b_group,
        b_id,
        consistency,
        test_lease(),
        Some(b_node),
    ));
    sync_b.start_apply(cache.local(), state.clone(), metrics, no_reconcile());
    let sync_a = WriteSync::attach(a_group, a_id, consistency, test_lease(), Some(a_node));
    (sync_a, sync_b, state, cache)
}
