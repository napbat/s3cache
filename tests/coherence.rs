//! Two proxies, one real S3 origin (`MinIO`), real gossip between them: the cross-node
//! coherence claims checked as a client would experience them.
//!
//! Both nodes run the production wiring — their own index, their own body cache, a
//! [`WriteSync`](s3cache::sync::WriteSync) over loopback UDP seeded with the other, in
//! strong mode — and every request that reaches the origin is counted, so "node B knew
//! without asking" is a fact rather than a hope.

mod common;

use common::{
    Origin, delete, get, gossip_pair, head, list, list_entry, proxy_over, put, put_conditional,
    put_typed, wait_for_index,
};
use s3cache::cache::CachingProxy;
use s3s::S3ErrorCode;
use s3s::dto::{ETag, ETagCondition};

/// Nothing here is about the size cap; keep every object cacheable.
const CAP: usize = 1024 * 1024;

/// Two coherent nodes over `origin`, both indexing its bucket, both applying each
/// other's writes. `ids` name the gossip nodes — unique per test, so parallel tests
/// never share an identity. Built after the origin's fixtures are seeded, so both
/// warm-up syncs see them.
async fn two_nodes(origin: &Origin, ids: (&str, &str)) -> (CachingProxy, CachingProxy) {
    let bucket = origin.bucket();
    let client = origin.counted_client();
    let (sync_a, sync_b) = gossip_pair(ids.0, ids.1).await;

    let mut nodes = Vec::new();
    for sync in [sync_a, sync_b] {
        let proxy = proxy_over(&client, CAP, Some(sync));
        proxy.start_coherence(&[bucket.to_owned()]);
        proxy.spawn_background_sync(vec![bucket.to_owned()]);
        nodes.push(proxy);
    }
    let (node_b, node_a) = (nodes.pop().expect("b"), nodes.pop().expect("a"));
    wait_for_index(&node_a, origin, bucket).await;
    wait_for_index(&node_b, origin, bucket).await;
    (node_a, node_b)
}

/// Wait until the two nodes have actually found each other, by the only signal that
/// matters: a write through A that node B already knows about by the time the write
/// returns. Until membership is established there is no peer to wait for an ack from,
/// so A's write returns without B having applied anything — a startup window, not the
/// steady-state semantics the tests below assert.
async fn settle_cluster(
    origin: &Origin,
    node_a: &CachingProxy,
    node_b: &CachingProxy,
    bucket: &str,
) {
    let mut probe = 0;
    eventually!("the gossip cluster to establish", {
        probe += 1;
        let key = format!("probe-{probe}");
        put(node_a, bucket, &key, b"probe").await;
        let seen = list(node_b, bucket).await.contains(&key);
        delete(node_a, bucket, &key).await;
        seen
    });
    // The probes were real writes; both indexes have to converge back off them before a
    // test can assert on exact key sets.
    let cleared = |keys: Vec<String>| keys.iter().all(|key| !key.starts_with("probe-"));
    eventually!("the probe keys to clear from both indexes", {
        cleared(list(node_a, bucket).await) && cleared(list(node_b, bucket).await)
    });
    // A node only answers the index-authoritative 404 once its view of the cluster has
    // held still for the failure detector's whole window — inside it a peer could be
    // holding a write this node has not seen, and a 404 for a key that exists is a lie
    // no retry fixes. Steady state includes that window having elapsed, and the only
    // signal for it a test should use is the black-box one: a 404 that costs nothing.
    eventually!(
        "the index-authoritative 404 to be trustworthy on both nodes",
        {
            let heads = origin.ops.head();
            let _ = head(node_a, bucket, "never-written").await;
            let _ = head(node_b, bucket, "never-written").await;
            origin.ops.head() == heads
        }
    );
}

/// A write on A is in B's index by the time the write returns — no polling: strong mode
/// holds the response until every alive peer has applied the event. B then answers LIST
/// for a key it has never seen at the origin, carrying the writer's own `ETag`, and one
/// forwarded HEAD completes the entry so every HEAD after it is local too.
#[tokio::test]
async fn a_write_on_a_folds_into_bs_index() {
    let origin = Origin::start("coherence-fold").await;
    let bucket = origin.bucket();
    let (node_a, node_b) = two_nodes(&origin, ("fold-a", "fold-b")).await;
    settle_cluster(&origin, &node_a, &node_b, bucket).await;
    let (lists, heads) = (origin.ops.list(), origin.ops.head());

    put(&node_a, bucket, "shared", b"from-a").await;

    assert_eq!(
        list(&node_b, bucket).await,
        ["shared"],
        "B's index has the key the moment A's write returned"
    );
    // The v2 feed envelope carries the origin's ETag, so B reports it without ever
    // asking the origin — where v1 advertised existence and size and nothing else.
    let (size, etag) = list_entry(&node_b, bucket, "shared")
        .await
        .expect("B lists the key");
    assert_eq!(size, 6);
    assert_eq!(
        etag,
        origin.etag("shared").await,
        "the feed carried the writer's ETag to the peer"
    );
    assert_eq!(
        origin.ops.list(),
        lists,
        "B answered LIST from its own index"
    );

    // A HEAD is the one answer the feed cannot supply: no user metadata rides it, and a
    // HEAD that omitted `x-amz-meta-*` would differ from the origin's. B forwards the
    // first one, completes the entry from the answer, and is local from then on.
    let out = head(&node_b, bucket, "shared")
        .await
        .expect("B can HEAD it too");
    assert_eq!(out.content_length, Some(6));
    assert_eq!(out.e_tag.as_ref().map(ETag::value), etag.as_deref());
    assert_eq!(
        origin.ops.head(),
        heads + 1,
        "one forwarded HEAD completed the peer-folded entry"
    );

    let heads = origin.ops.head();
    let again = head(&node_b, bucket, "shared")
        .await
        .expect("and B still has it");
    assert_eq!(again.content_length, Some(6));
    assert_eq!(again.e_tag, out.e_tag);
    assert_eq!(
        origin.ops.head(),
        heads,
        "the completed entry answers every HEAD after it"
    );
}

/// The invalidation half, and the asymmetry that makes it right: B has a body cached, A
/// overwrites it, and B's next read is the new bytes — refetched from the origin, because
/// its stale copy was dropped. The *writer* is the one node that does not have to refetch:
/// A wrote those bytes and kept them, so A's own read after the write is both fresh and
/// free. Peers are invalidated; the writer is refilled.
#[tokio::test]
async fn an_overwrite_on_a_invalidates_bs_cached_body() {
    let origin = Origin::start("coherence-invalidate").await;
    let bucket = origin.bucket();
    origin.seed("obj", b"version-1").await;
    let (node_a, node_b) = two_nodes(&origin, ("inval-a", "inval-b")).await;
    settle_cluster(&origin, &node_a, &node_b, bucket).await;

    assert_eq!(get(&node_b, bucket, "obj").await, "version-1");
    let fetched = origin.ops.get();
    assert_eq!(
        get(&node_b, bucket, "obj").await,
        "version-1",
        "B has it cached"
    );
    assert_eq!(origin.ops.get(), fetched, "B served it locally");

    put_typed(&node_a, bucket, "obj", b"version-2", "text/x-fixture").await;

    assert_eq!(
        get(&node_a, bucket, "obj").await,
        "version-2",
        "A serves what it just wrote — the writer's kept copy is fresh, not stale"
    );
    assert_eq!(
        origin.ops.get(),
        fetched,
        "and A did not pay the origin for bytes it had in hand"
    );

    assert_eq!(
        get(&node_b, bucket, "obj").await,
        "version-2",
        "B must not serve the copy A just overwrote"
    );
    assert_eq!(
        origin.ops.get(),
        fetched + 1,
        "B refetched rather than answering from a stale body"
    );
}

/// A delete on A unindexes the key on B: it leaves B's LIST, and B's HEAD answers the
/// authoritative 404 from its own index rather than asking the origin.
#[tokio::test]
async fn a_delete_on_a_removes_the_key_from_bs_index() {
    let origin = Origin::start("coherence-delete").await;
    let bucket = origin.bucket();
    let (node_a, node_b) = two_nodes(&origin, ("del-a", "del-b")).await;
    settle_cluster(&origin, &node_a, &node_b, bucket).await;

    put(&node_a, bucket, "doomed", b"briefly here").await;
    assert_eq!(list(&node_b, bucket).await, ["doomed"]);
    let heads = origin.ops.head();

    delete(&node_a, bucket, "doomed").await;

    assert!(
        list(&node_b, bucket).await.is_empty(),
        "the delete reached B's index"
    );
    let err = head(&node_b, bucket, "doomed")
        .await
        .expect_err("the key is gone");
    assert_eq!(*err.code(), S3ErrorCode::NoSuchKey);
    assert_eq!(err.status_code().map(|status| status.as_u16()), Some(404));
    assert_eq!(
        origin.ops.head(),
        heads,
        "the 404 came from B's index, not from the origin"
    );
}

/// Compare-and-set across nodes: the origin, not either cache, arbitrates. Two nodes
/// race to create the same key and exactly one wins — and the loser's node still ends up
/// serving the winner's bytes, because the winning write invalidated it there too.
#[tokio::test]
async fn a_contested_create_is_arbitrated_by_the_origin() {
    let origin = Origin::start("coherence-cas").await;
    let bucket = origin.bucket();
    let (node_a, node_b) = two_nodes(&origin, ("cas-a", "cas-b")).await;
    settle_cluster(&origin, &node_a, &node_b, bucket).await;

    let (from_a, from_b) = tokio::join!(
        put_conditional(
            &node_a,
            bucket,
            "contested",
            b"from-a",
            Some(ETagCondition::Any),
            None
        ),
        put_conditional(
            &node_b,
            bucket,
            "contested",
            b"from-b",
            Some(ETagCondition::Any),
            None
        ),
    );

    let winner = match (from_a.is_ok(), from_b.is_ok()) {
        (true, false) => b"from-a",
        (false, true) => b"from-b",
        (true, true) => panic!("both creates won: the origin did not arbitrate"),
        (false, false) => panic!("neither create won: {from_a:?} / {from_b:?}"),
    };
    for loser in [&from_a, &from_b] {
        if let Err(err) = loser {
            assert_eq!(err.status_code().map(|status| status.as_u16()), Some(412));
        }
    }
    assert_eq!(
        origin.stored("contested").await.as_deref(),
        Some(&winner[..])
    );
    assert_eq!(get(&node_a, bucket, "contested").await, &winner[..]);
    assert_eq!(
        get(&node_b, bucket, "contested").await,
        &winner[..],
        "both nodes serve the winner's bytes"
    );
}
