//! A real multi-node Raft cluster, in one process over the in-process transport. These are
//! the Milestone-1 proof that consensus replicates index writes and that the strong-read
//! barrier works — the Raft analogue of the old Valkey-log coherence tests.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use openraft::{BasicNode, Config, Raft};
use s3s::dto::GetObjectOutput;

use super::{IndexWrite, LogStore, Loopback, NodeId, StateMachineStore, TypeConfig};
use crate::tier::{CachedObject, TieredCache};

/// One cluster node: its id, its `Raft` handle, and its state machine (to read the index).
type Node = (NodeId, Raft<TypeConfig>, Arc<StateMachineStore>);

/// Short timers so a leader is elected within a test's patience.
fn test_config() -> Arc<Config> {
    let config = Config {
        heartbeat_interval: 100,
        election_timeout_min: 200,
        election_timeout_max: 400,
        ..Default::default()
    };
    Arc::new(config.validate().unwrap())
}

/// Build an initialized `n`-voter cluster wired over one shared in-process transport.
async fn cluster(n: NodeId) -> Vec<Node> {
    let net = Loopback::default();
    let mut sms = Vec::new();
    let mut rafts = Vec::new();
    for id in 1..=n {
        let sm = Arc::new(StateMachineStore::default());
        let raft = Raft::new(
            id,
            test_config(),
            net.clone(),
            LogStore::default(),
            sm.clone(),
        )
        .await
        .unwrap();
        // Register before initialize: uninitialized nodes send no RPCs, so every peer is
        // present by the time the cluster forms — no election race.
        net.register(id, raft.clone());
        sms.push(sm);
        rafts.push((id, raft));
    }
    let members: BTreeMap<NodeId, BasicNode> =
        (1..=n).map(|id| (id, BasicNode::default())).collect();
    rafts[0].1.initialize(members).await.unwrap();
    rafts
        .into_iter()
        .zip(sms)
        .map(|((id, raft), sm)| (id, raft, sm))
        .collect()
}

/// This node's currently-indexed size for `bucket/key`, or `None` if absent.
fn indexed(sm: &StateMachineStore, bucket: &str, key: &str) -> Option<i64> {
    sm.index()
        .read()
        .unwrap()
        .get(bucket)
        .and_then(|b| b.keys.get(key))
        .map(|e| e.size)
}

/// Propose a write, retrying across nodes until the current leader accepts it (a follower
/// returns `ForwardToLeader`, so we just try the next node until election settles).
async fn propose(nodes: &[Node], write: &IndexWrite) {
    for _ in 0..100 {
        for (_, raft, _) in nodes {
            if raft.client_write(write.clone()).await.is_ok() {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("no leader accepted the write within the deadline");
}

/// Wait until every node's index agrees on `bucket/key == expect`.
async fn converged(nodes: &[Node], bucket: &str, key: &str, expect: Option<i64>) -> bool {
    for _ in 0..100 {
        if nodes
            .iter()
            .all(|(_, _, sm)| indexed(sm, bucket, key) == expect)
        {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

async fn shutdown(nodes: Vec<Node>) {
    for (_, raft, _) in nodes {
        let _ = raft.shutdown().await;
    }
}

fn put(bucket: &str, key: &str, size: i64, ts_ms: u64) -> IndexWrite {
    IndexWrite::Put {
        bucket: bucket.to_owned(),
        key: key.to_owned(),
        size,
        ts_ms,
    }
}

fn del(bucket: &str, key: &str) -> IndexWrite {
    IndexWrite::Del {
        bucket: bucket.to_owned(),
        key: key.to_owned(),
    }
}

#[tokio::test]
async fn cluster_replicates_writes_to_every_node() {
    let nodes = cluster(3).await;

    propose(&nodes, &put("b", "k1", 42, 1)).await;
    assert!(
        converged(&nodes, "b", "k1", Some(42)).await,
        "a put on the leader reaches every node"
    );

    propose(&nodes, &put("b", "k1", 7, 2)).await;
    assert!(
        converged(&nodes, "b", "k1", Some(7)).await,
        "an overwrite converges everywhere"
    );

    propose(&nodes, &del("b", "k1")).await;
    assert!(
        converged(&nodes, "b", "k1", None).await,
        "a delete converges everywhere"
    );

    // A burst of distinct keys must all replicate (ordering + throughput, not just one key).
    for i in 0..20_i64 {
        propose(&nodes, &put("b", &format!("m{i}"), i, 1)).await;
    }
    let all_present = |nodes: &[Node]| {
        (0..20_i64).all(|i| {
            nodes
                .iter()
                .all(|(_, _, sm)| indexed(sm, "b", &format!("m{i}")) == Some(i))
        })
    };
    let mut ok = false;
    for _ in 0..100 {
        if all_present(&nodes) {
            ok = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(ok, "all 20 keys converge on every node");

    shutdown(nodes).await;
}

#[tokio::test]
async fn linearizable_read_sees_the_latest_write() {
    let nodes = cluster(3).await;
    propose(&nodes, &put("b", "x", 5, 1)).await;

    // The read barrier: on the leader, `ensure_linearizable` guarantees the local applied
    // state reflects everything committed as of the call — so the subsequent read is fresh.
    let mut observed = None;
    for _ in 0..100 {
        let mut found = false;
        for (_, raft, sm) in &nodes {
            if raft.ensure_linearizable().await.is_ok() {
                observed = indexed(sm, "b", "x");
                found = true;
                break;
            }
        }
        if found {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        observed,
        Some(5),
        "a linearizable read must observe the just-committed write"
    );

    shutdown(nodes).await;
}

#[tokio::test]
async fn applied_write_invalidates_local_object_cache() {
    // Applying a committed write must drop the key from the node-local object cache — the
    // anti-stale-read guarantee the old Valkey-log consumer provided, now driven by apply.
    let nodes = cluster(1).await;

    let hot = TieredCache::new(
        1024 * 1024,
        None,
        Arc::new(crate::metrics::Metrics::default()),
    );
    let ck = ("b".to_owned(), "k".to_owned());
    let stale = Arc::new(CachedObject::from_get(
        &GetObjectOutput::default(),
        Bytes::from_static(b"stale"),
    ));
    hot.insert(ck.clone(), stale).await;
    assert!(hot.get(&ck).await.is_some(), "seeded a stale hot copy");
    nodes[0].2.set_local(hot.local());

    propose(&nodes, &put("b", "k", 5, 1)).await;

    let mut gone = false;
    for _ in 0..100 {
        if hot.get(&ck).await.is_none() {
            gone = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        gone,
        "an applied write must invalidate the node-local hot cache"
    );

    shutdown(nodes).await;
}
