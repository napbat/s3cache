//! The pod readiness probe, [`CachingProxy::probe_ready`], on a two-node fleet:
//! gossip on the in-memory transport, each node on a one-worker pod runtime of
//! its own with the binary's lease, recovery and claim configuration, over a
//! small origin.
//!
//! * a cold pair is ready, and both nodes forward to the origin while one builds;
//! * a replacement beside an index-holding peer is not ready until it has an
//!   index of its own;
//! * a node left alone is ready, whatever its dead peer last advertised;
//! * a live peer without an index does not hold this node back.

use crate::common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::fleet::{Counts, Pod, free_tcp_port, mem_pod_node};
use common::{Origin, get, list};
use groupnet::core::{NodeId, Status};
use groupnet::transport::mem::Network;
use s3cache::cache::proxy::CachingProxy;
use s3cache::metrics::Metrics;
use s3cache::sync::coherence::{DEFAULT_LEASE_MS, WriteSync};

/// The capability a node declares once its initial index is complete.
const CAP_INDEXED: &str = "s3cache:indexed";
/// Room for a fleet install, a small origin scan, or a dead peer's reap.
const DEADLINE: Duration = Duration::from_secs(30);
/// How often a wait re-reads the probe.
const POLL: Duration = Duration::from_millis(25);
/// The one real object, which a cold node forwards to the origin.
const KEY: &str = "body";
const BODY: &[u8] = b"origin body";

/// Releases a held origin LIST however the test ends.
struct HeldList(Arc<Origin>);

impl Drop for HeldList {
    fn drop(&mut self) {
        self.0.release_paused_list();
    }
}

/// The pair's names, bulk ports and in-memory gossip network.
struct Fleet {
    net: Network,
    origin: Arc<Origin>,
    bucket: String,
    names: [&'static str; 2],
    binds: [u16; 2],
}

impl Fleet {
    async fn new(names: [&'static str; 2]) -> Self {
        let origin = Origin::start(names[0]).await;
        origin.seed(KEY, BODY).await;
        let bucket = origin.bucket().to_owned();
        Self {
            net: Network::new(),
            origin,
            bucket,
            names,
            binds: [free_tcp_port(), free_tcp_port()],
        }
    }

    /// Node `index` on a pod of its own: a fresh life with no index.
    async fn node(&self, index: usize) -> Node {
        let pod = Pod::new(self.names[index]);
        let metrics = Arc::new(Metrics::default());
        let (sync, proxy) = mem_pod_node(
            &pod,
            &self.net,
            &self.origin,
            (self.names[index], self.binds[index]),
            self.names[1 - index],
            [
                (self.names[0], self.binds[0]),
                (self.names[1], self.binds[1]),
            ],
            &metrics,
        )
        .await;
        Node { pod, sync, proxy }
    }

    /// Hold the next origin LIST until the guard drops, and start `node`, whose
    /// startup scan is that LIST.
    async fn start_held(&self, node: &Node) -> HeldList {
        self.origin.pause_next_list();
        let held = HeldList(Arc::clone(&self.origin));
        node.start(&self.bucket).await;
        tokio::time::timeout(DEADLINE, self.origin.wait_for_paused_list())
            .await
            .expect("the builder's scan reached the origin");
        held
    }
}

struct Node {
    pod: Pod,
    sync: Arc<WriteSync>,
    proxy: CachingProxy,
}

impl Node {
    /// Start fleet coherence inside the node's pod, as the binary does.
    async fn start(&self, bucket: &str) {
        let (node, bucket) = (self.proxy.clone(), bucket.to_owned());
        self.pod
            .handle()
            .spawn(async move {
                node.start_fleet_coherence(std::slice::from_ref(&bucket))
                    .await;
            })
            .await
            .expect("fleet coherence started");
    }

    fn ready(&self) -> bool {
        self.proxy.probe_ready()
    }

    fn indexed(&self) -> bool {
        self.proxy.index_ready()
    }

    /// The membership status this node holds for `peer`; `None` once reaped.
    fn status_of(&self, peer: &str) -> Option<Status> {
        self.sync.group().member_status(&NodeId::new(peer))
    }

    /// Whether this node holds `peer`'s index-ready declaration.
    fn reads_indexed(&self, peer: &str) -> bool {
        self.sync
            .group()
            .node_has_capability(&NodeId::new(peer), CAP_INDEXED)
    }
}

async fn until(what: &str, mut done: impl FnMut() -> bool) {
    tokio::time::timeout(DEADLINE, async {
        while !done() {
            tokio::time::sleep(POLL).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{what} within {DEADLINE:?}"));
}

/// Assert `holds` at every poll for one lease duration: long enough for the
/// peer's renewals, gossip rounds and claims to reach the node.
async fn holds_for_a_lease(what: &str, mut holds: impl FnMut() -> bool) {
    let end = Instant::now() + Duration::from_millis(DEFAULT_LEASE_MS);
    while Instant::now() < end {
        assert!(holds(), "{what}");
        tokio::time::sleep(POLL).await;
    }
}

/// Wait until `node` has an index, asserting at every poll that it is not
/// ready before it has one.
async fn not_ready_until_indexed(node: &Node) {
    until("the node has an index", || {
        // The probe first: the index latch only turns on, so a probe that read
        // the latch on is followed by a latch read that is on too.
        let ready = node.ready();
        let indexed = node.indexed();
        assert!(
            !ready || indexed,
            "ready without an index beside an index-holding peer"
        );
        indexed
    })
    .await;
}

/// Both nodes start empty. While the first one's origin scan is held and the
/// other follows it, both are ready and both forward reads to the origin; once
/// both hold the index, both are still ready.
#[tokio::test(flavor = "multi_thread")]
async fn a_cold_pair_is_ready_and_forwards_while_one_builds() {
    let fleet = Fleet::new(["ready-cold-a", "ready-cold-b"]).await;
    let nodes = [fleet.node(0).await, fleet.node(1).await];
    until("both cold nodes are ready", || {
        nodes.iter().all(Node::ready)
    })
    .await;

    let held = fleet.start_held(&nodes[0]).await;
    nodes[1].start(&fleet.bucket).await;
    let before = Counts::take(&fleet.origin);
    for node in &nodes {
        assert!(node.ready() && !node.indexed(), "cold and ready");
        assert_eq!(get(&node.proxy, &fleet.bucket, KEY).await.as_ref(), BODY);
        assert_eq!(list(&node.proxy, &fleet.bucket).await, [KEY]);
    }
    let forwarded = Counts::take(&fleet.origin).since(before);
    assert!(
        forwarded.get >= 2 && forwarded.list >= 2,
        "both cold nodes forwarded to the origin: {forwarded:?}"
    );
    holds_for_a_lease("both nodes stay ready while one builds", || {
        nodes.iter().all(|node| node.ready() && !node.indexed())
    })
    .await;

    drop(held);
    until("both nodes hold the index", || {
        nodes.iter().all(Node::indexed)
    })
    .await;
    assert!(nodes.iter().all(Node::ready));
}

/// The rolling update's case: one pod is replaced while its peer holds the
/// index. The replacement is not ready until it has installed an index of its
/// own, so the rollout cannot stop the peer first; the peer stays ready.
#[tokio::test(flavor = "multi_thread")]
async fn a_replacement_is_not_ready_beside_an_indexed_peer() {
    let fleet = Fleet::new(["ready-roll-a", "ready-roll-b"]).await;
    let [survivor, stopped] = [fleet.node(0).await, fleet.node(1).await];
    survivor.start(&fleet.bucket).await;
    stopped.start(&fleet.bucket).await;
    until("the pair holds the index", || {
        survivor.indexed() && stopped.indexed()
    })
    .await;

    drop(stopped);
    until("the survivor reaps the stopped pod", || {
        survivor.status_of(fleet.names[1]).is_none()
    })
    .await;
    assert!(survivor.ready(), "an index holder left alone is ready");

    let replacement = fleet.node(1).await;
    // No relearning by hand: the survivor keeps contacting its seed, so the
    // replacement rejoins on its own.
    assert!(!replacement.ready(), "a fresh life beside an index holder");
    until("the replacement reads its peer's index", || {
        assert!(!replacement.ready(), "ready before it has an index");
        replacement.reads_indexed(fleet.names[0])
    })
    .await;
    holds_for_a_lease("the replacement stays not ready", || !replacement.ready()).await;

    replacement.start(&fleet.bucket).await;
    not_ready_until_indexed(&replacement).await;
    assert!(replacement.ready() && survivor.ready());
}

/// The node left alone is ready once its index-holding peer is dead, though
/// it never built an index and still holds the dead peer's last declaration.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_left_alone_is_ready() {
    let fleet = Fleet::new(["ready-alone-a", "ready-alone-b"]).await;
    let [donor, alone] = [fleet.node(0).await, fleet.node(1).await];
    donor.start(&fleet.bucket).await;
    until("the donor holds the index", || donor.indexed()).await;
    until("the other node reads the donor's index", || {
        assert!(!alone.ready(), "ready beside an index holder");
        alone.reads_indexed(fleet.names[0])
    })
    .await;

    drop(donor);
    until("the donor is dead", || {
        alone.status_of(fleet.names[0]) == Some(Status::Dead)
    })
    .await;
    assert!(
        alone.reads_indexed(fleet.names[0]),
        "the dead donor's declaration is still held"
    );
    assert!(alone.ready(), "a dead peer's declaration does not count");
    until("the dead donor is reaped", || {
        alone.status_of(fleet.names[0]).is_none()
    })
    .await;
    assert!(alone.ready() && !alone.indexed());
}

/// A peer that is alive and building, without an index yet, keeps nobody out
/// of the Service; once it declares its index, the node without one is not
/// ready.
#[tokio::test(flavor = "multi_thread")]
async fn a_live_peer_without_an_index_keeps_this_node_ready() {
    let fleet = Fleet::new(["ready-live-a", "ready-live-b"]).await;
    let [builder, other] = [fleet.node(0).await, fleet.node(1).await];
    until("both cold nodes are ready", || {
        builder.ready() && other.ready()
    })
    .await;

    let held = fleet.start_held(&builder).await;
    holds_for_a_lease("ready beside a live peer that is still building", || {
        other.status_of(fleet.names[0]) == Some(Status::Alive)
            && !other.reads_indexed(fleet.names[0])
            && other.ready()
    })
    .await;

    drop(held);
    until("the builder holds the index", || builder.indexed()).await;
    until("the other node reads the builder's index", || {
        !other.ready()
    })
    .await;
    assert!(other.reads_indexed(fleet.names[0]) && !other.indexed());
}
