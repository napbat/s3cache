//! Two proxies, one real S3 origin (`MinIO`), real gossip between them: the cross-node
//! coherence claims checked as a client would experience them.
//!
//! Both nodes run the production wiring — their own index, their own body cache, a
//! [`WriteSync`](s3cache::sync::coherence::WriteSync) over loopback UDP seeded with the other, in
//! strong mode — and every request that reaches the origin is counted, so "node B knew
//! without asking" is a fact rather than a hope.

mod common;

use std::sync::Arc;

use common::{
    Origin, WarmDir, counter, delete, free_udp_port, get, get_with_read_token, gossip_node,
    gossip_pair, head, list, list_entry, proxy_over, proxy_over_with_metrics, put, put_conditional,
    put_typed, wait_for_index, warm_proxy_over,
};
use s3cache::cache::proxy::CachingProxy;
use s3cache::metrics::Metrics;
use s3s::dto::{ETag, ETagCondition};

/// Nothing here is about the size cap; keep every object cacheable.
const CAP: usize = 1024 * 1024;

/// Two coherent nodes over `origin`, both indexing its bucket, both applying each
/// other's writes. `ids` name the gossip nodes — unique per test, so parallel tests
/// never share an identity. Built after the origin's fixtures are seeded, so both
/// warm-up syncs see them.
async fn two_nodes(origin: &Origin, ids: (&str, &str)) -> (CachingProxy, CachingProxy) {
    let (node_a, node_b, _, _) = two_nodes_with_metrics(origin, ids).await;
    (node_a, node_b)
}

/// [`two_nodes`] with each node's counters retained by the caller. The proxy and
/// its coherence apply loop share the same `Arc`, exactly as the binary does.
async fn two_nodes_with_metrics(
    origin: &Origin,
    ids: (&str, &str),
) -> (CachingProxy, CachingProxy, Arc<Metrics>, Arc<Metrics>) {
    let bucket = origin.bucket();
    let client = origin.counted_client();
    let (sync_a, sync_b) = gossip_pair(ids.0, ids.1).await;

    let metrics_a = Arc::new(Metrics::default());
    let metrics_b = Arc::new(Metrics::default());
    let node_a = proxy_over_with_metrics(&client, CAP, Some(sync_a), &metrics_a);
    let node_b = proxy_over_with_metrics(&client, CAP, Some(sync_b), &metrics_b);
    for proxy in [&node_a, &node_b] {
        proxy.start_coherence(&[bucket.to_owned()]);
        proxy.spawn_background_sync(vec![bucket.to_owned()]);
    }
    wait_for_index(&node_a, origin, bucket).await;
    wait_for_index(&node_b, origin, bucket).await;
    (node_a, node_b, metrics_a, metrics_b)
}

/// Wait until the two nodes have actually found each other, by the only signal that
/// matters: a write through A that node B already knows about by the time the write
/// returns. Until membership is established there is no peer to wait for an ack from,
/// so A's write returns without B having applied anything — a startup window, not the
/// steady-state semantics the tests below assert.
async fn settle_cluster(node_a: &CachingProxy, node_b: &CachingProxy, bucket: &str) {
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
    settle_cluster(&node_a, &node_b, bucket).await;
    let (lists, heads) = (origin.ops.list(), origin.ops.head());

    put(&node_a, bucket, "shared", b"from-a").await;

    assert_eq!(
        list(&node_b, bucket).await,
        ["shared"],
        "B's index has the key the moment A's write returned"
    );
    // The feed envelope carries the origin's ETag, so B reports it without ever
    // asking the origin.
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

/// A replicated index can prove a faithful positive, but never absence: an object
/// created directly at the origin has no feed event. The first HEAD must therefore
/// forward, and its faithful answer may complete the local index for the next one.
#[tokio::test]
async fn a_replicated_index_miss_forwards_instead_of_returning_a_false_404() {
    let origin = Origin::start("coherence-origin-only").await;
    let bucket = origin.bucket();
    let (node_a, node_b) = two_nodes(&origin, ("origin-only-a", "origin-only-b")).await;
    settle_cluster(&node_a, &node_b, bucket).await;

    origin.seed("out-of-band", b"exists-at-origin").await;
    let heads = origin.ops.head();
    let first = head(&node_b, bucket, "out-of-band")
        .await
        .expect("replicated miss forwards to the origin");
    assert_eq!(first.content_length, Some(16));
    assert_eq!(
        origin.ops.head(),
        heads + 1,
        "the origin, not the replicated miss, decided existence"
    );

    let second = head(&node_b, bucket, "out-of-band")
        .await
        .expect("the observed faithful positive remains available");
    assert_eq!(second.content_length, first.content_length);
    assert_eq!(
        origin.ops.head(),
        heads + 1,
        "the forwarded positive completed the local index entry"
    );
}

/// An origin route is terminal for this request. In particular a whole-object GET must
/// not enter the cache's probe-then-gate fill path, which would re-probe and hand back a
/// stale trusted hot body after the read token already rejected local service.
#[tokio::test]
async fn an_origin_routed_whole_get_cannot_reprobe_a_stale_cached_body() {
    let origin = Origin::start("coherence-origin-route").await;
    let bucket = origin.bucket();
    origin.seed("body", b"version-1").await;
    let (node_a, node_b) = two_nodes(&origin, ("route-a", "route-b")).await;
    settle_cluster(&node_a, &node_b, bucket).await;

    assert_eq!(get(&node_b, bucket, "body").await, "version-1");
    let fetched = origin.ops.get();
    origin.seed("body", b"version-2").await;

    assert_eq!(
        get_with_read_token(&node_b, bucket, "body", "unknown-writer:1:1").await,
        "version-2",
        "the unsatisfied token routes around the stale cached body"
    );
    assert_eq!(
        origin.ops.get(),
        fetched + 1,
        "the whole-object origin route reached upstream exactly once"
    );
}

/// A write feed is one gossiped state entry. Its item-count capacity is deliberately
/// much larger than one UDP frame, so each acknowledged strong write retires the safe
/// prefix while retaining the current head anchor. Before acknowledged retirement, the
/// frame stayed at the transport envelope under sustained writes: B's applied counter
/// froze and every later strong write timed out.
#[tokio::test]
async fn sustained_writes_keep_the_coherence_feed_moving() {
    const WRITES: u64 = 640;

    let origin = Origin::start("coherence-sustained").await;
    let bucket = origin.bucket();
    let (node_a, node_b, metrics_a, metrics_b) =
        two_nodes_with_metrics(&origin, ("sustain-a", "sustain-b")).await;
    settle_cluster(&node_a, &node_b, bucket).await;

    let published = counter(&metrics_a, "feed_published");
    let applied = counter(&metrics_b, "feed_applied");
    let timeouts = counter(&metrics_a, "ack_timeouts");
    let lapses = counter(&metrics_a, "write_lease_lapses");
    let gaps = counter(&metrics_b, "feed_gaps");
    let suffix = "x".repeat(160);
    let mut last = String::new();

    for seq in 0..WRITES {
        last = format!(
            "fixtures/long-object-keys/tenant-0000000000000000/shard-0000000000000000/\
             {seq:016x}-{suffix}"
        );
        put(&node_a, bucket, &last, b"x").await;
        assert_eq!(
            counter(&metrics_a, "ack_timeouts"),
            timeouts,
            "the coherence feed arrested at sustained write {seq}"
        );
        assert_eq!(
            counter(&metrics_b, "feed_applied"),
            applied + seq + 1,
            "strong mode returned write {seq} before B applied it"
        );
    }

    assert_eq!(counter(&metrics_a, "feed_published"), published + WRITES);
    assert_eq!(
        counter(&metrics_a, "write_lease_lapses"),
        lapses,
        "a healthy peer must not need the lapse escape path"
    );
    assert_eq!(
        counter(&metrics_b, "feed_gaps"),
        gaps,
        "a peer acknowledged after every write, so it never fell behind the byte window"
    );

    let origin_lists = origin.ops.list();
    let keys = list(&node_b, bucket).await;
    assert_eq!(
        keys.len(),
        usize::try_from(WRITES).expect("the fixed regression count fits usize")
    );
    assert!(
        keys.contains(&last),
        "B retained the tail of the sustained run"
    );
    assert_eq!(
        origin.ops.list(),
        origin_lists,
        "B served the final bucket view from its continuously updated index"
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
    settle_cluster(&node_a, &node_b, bucket).await;

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

/// A delete on A unindexes the key on B: it leaves B's LIST, but replicated absence is
/// never authoritative, so B forwards one HEAD and the origin returns the 404.
#[tokio::test]
async fn a_delete_on_a_removes_the_key_from_bs_index() {
    let origin = Origin::start("coherence-delete").await;
    let bucket = origin.bucket();
    let (node_a, node_b) = two_nodes(&origin, ("del-a", "del-b")).await;
    settle_cluster(&node_a, &node_b, bucket).await;

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
    assert_eq!(err.status_code().map(|status| status.as_u16()), Some(404));
    assert_eq!(
        origin.ops.head(),
        heads + 1,
        "the replicated 404 was decided by the origin"
    );
}

/// Applying a peer overwrite updates the index and awaits only hot eviction before
/// acknowledging. Warm disk remains off the frontier: the changed copy is retained
/// suspect and rejected against the new index on demand, while unrelated entries keep
/// their proof and remain free to serve.
#[tokio::test]
async fn a_peer_overwrite_keeps_warm_disk_off_the_apply_frontier() {
    let origin = Origin::start("coherence-warm-apply").await;
    let bucket = origin.bucket();
    origin.seed("kept", b"never-touched").await;
    origin.seed("changed", b"version-1").await;
    let dir = WarmDir::new("warm-apply");
    let (sync_a, sync_b) = gossip_pair("warm-live-a", "warm-live-b").await;
    let metrics_a = Arc::new(Metrics::default());
    let metrics_b = Arc::new(Metrics::default());
    let client = origin.counted_client();
    let node_a = proxy_over_with_metrics(&client, CAP, Some(sync_a), &metrics_a);
    let node_b = warm_proxy_over(&client, CAP, Some(sync_b), &dir, &metrics_b);
    for proxy in [&node_a, &node_b] {
        proxy.start_coherence(&[bucket.to_owned()]);
        proxy.spawn_background_sync(vec![bucket.to_owned()]);
    }
    wait_for_index(&node_a, &origin, bucket).await;
    wait_for_index(&node_b, &origin, bucket).await;
    settle_cluster(&node_a, &node_b, bucket).await;

    assert_eq!(get(&node_b, bucket, "kept").await, "never-touched");
    assert_eq!(get(&node_b, bucket, "changed").await, "version-1");
    eventually!("both bodies to reach warm disk", dir.files() == 2);
    let fetched = origin.ops.get();
    let evictions = counter(&metrics_b, "body_revalidation_evictions");

    put_typed(&node_a, bucket, "changed", b"version-2", "text/x-fixture").await;
    assert_eq!(
        dir.files(),
        2,
        "the applied acknowledgement did not wait for or enqueue warm deletion"
    );
    assert_eq!(get(&node_b, bucket, "kept").await, "never-touched");
    assert_eq!(
        origin.ops.get(),
        fetched,
        "the unrelated trusted hot body remains locally serveable"
    );
    assert_eq!(get(&node_b, bucket, "changed").await, "version-2");
    assert_eq!(
        origin.ops.get(),
        fetched + 1,
        "only the contradicted warm body was refetched"
    );
    assert_eq!(
        counter(&metrics_b, "body_revalidation_evictions"),
        evictions + 1,
        "the retained warm copy was checked against the already-updated index"
    );
}

/// The **retention** claim, with the origin's counter as the witness: a node that comes
/// back onto its persisted warm tier pays the origin only for what actually changed while
/// it was gone.
///
/// A body off the disk tier is *suspect*, because the trust stamp is bookkeeping about a
/// copy in a live cache and does not survive the encoding — a decoded copy carries
/// generation `0`, which no live cache ever issues. Suspect is not stale, and that
/// distinction is the whole of what this test is about: each copy proves itself against
/// the LIST index this node re-read from the origin on the way up. `kept` proves itself —
/// same `ETag`, an mtime the index has not moved past — and is served off disk for
/// nothing.
///
/// `changed` is the other half: the re-read index contradicts the retained suspect warm
/// copy, so the read drops exactly that key and refetches it. Both replay and a full
/// gap recovery land on the same validation rule, which is why the assertion is the
/// counter and not the route.
///
/// One origin GET for the whole restart, then. A node that had thrown its tier away
/// instead would have paid two, and the counter is what tells the two apart.
///
/// The previous life is staged as the directory it left behind rather than as a node that
/// is shut down, because a node in this harness cannot be shut down: `start_coherence`
/// spawns an apply loop holding the resync closure, which holds the [`WriteSync`], which
/// owns the gossip node — so a dropped proxy keeps gossiping. The disk tier is all a
/// restart inherits either way.
#[tokio::test]
async fn a_restart_onto_a_warm_tier_pays_only_for_what_changed() {
    let origin = Origin::start("coherence-warm-restart").await;
    let bucket = origin.bucket();
    origin.seed("kept", b"never-touched").await;
    origin.seed("changed", b"version-1").await;
    let dir = WarmDir::new("warm-restart");

    // The previous life: it fills its disk tier from the origin and goes away.
    {
        let metrics = Arc::new(Metrics::default());
        let previous = warm_proxy_over(&origin.counted_client(), CAP, None, &dir, &metrics);
        previous.spawn_background_sync(vec![bucket.to_owned()]);
        wait_for_index(&previous, &origin, bucket).await;
        let fetched = origin.ops.get();
        assert_eq!(get(&previous, bucket, "kept").await, "never-touched");
        assert_eq!(get(&previous, bucket, "changed").await, "version-1");
        assert_eq!(
            origin.ops.get(),
            fetched + 2,
            "the first life filled both bodies from the origin"
        );
        // The disk fill is offloaded to its own threads, so wait for it rather than
        // assume it: with nothing on disk there is no retention left to prove.
        eventually!("both bodies to reach the disk tier", dir.files() == 2);
    }

    // The peer, and the write the restarted node is going to miss: A is up, and nothing
    // that could be invalidated is running when it overwrites `changed`.
    let (port_a, port_b) = (free_udp_port(), free_udp_port());
    let sync_a = gossip_node("warm-a", port_a, &[("warm-b", port_b)]).await;
    let node_a = proxy_over(&origin.counted_client(), CAP, Some(sync_a));
    node_a.start_coherence(&[bucket.to_owned()]);
    node_a.spawn_background_sync(vec![bucket.to_owned()]);
    wait_for_index(&node_a, &origin, bucket).await;
    put_typed(&node_a, bucket, "changed", b"version-2", "text/x-fixture").await;

    // The restart: the same disk, and nothing else the same — a cold hot tier, an empty
    // index, and a peer that has been writing.
    let metrics = Arc::new(Metrics::default());
    let sync_b = gossip_node("warm-b", port_b, &[("warm-a", port_a)]).await;
    let node_b = warm_proxy_over(&origin.counted_client(), CAP, Some(sync_b), &dir, &metrics);
    node_b.start_coherence(&[bucket.to_owned()]);
    node_b.spawn_background_sync(vec![bucket.to_owned()]);
    wait_for_index(&node_b, &origin, bucket).await;
    settle_cluster(&node_a, &node_b, bucket).await;

    let fetched = origin.ops.get();
    assert_eq!(
        get(&node_b, bucket, "kept").await,
        "never-touched",
        "the body outlived the process that cached it"
    );
    assert_eq!(
        origin.ops.get(),
        fetched,
        "and the origin was never asked for it: the copy proved itself against the index"
    );
    assert_eq!(counter(&metrics, "warm_hit"), 1, "served off the disk tier");
    assert_eq!(
        counter(&metrics, "body_revalidations"),
        1,
        "and proved before it was served, not trusted for having survived"
    );

    assert_eq!(
        get(&node_b, bucket, "changed").await,
        "version-2",
        "the copy this node kept is not what the peer left at the origin"
    );
    assert_eq!(
        origin.ops.get(),
        fetched + 1,
        "exactly one refetch, for the one key that changed"
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
    settle_cluster(&node_a, &node_b, bucket).await;

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
