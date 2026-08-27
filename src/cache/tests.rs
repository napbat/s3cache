use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use groupnet::consistency::{LeaseConfig, WriteFeed, advertised_head};
use groupnet::core::{Config, NodeId};
use groupnet::runtime::Node;
use groupnet::transport::mem::{MemTransport, Network};
use http::HeaderMap;
use s3s::dto::{ETag, GetObjectOutput, ListObjectsV2Input, Timestamp};
use s3s::{S3, S3ErrorCode, S3Request};

use crate::cache::proxy::{CacheConfig, CachingProxy, FullSyncOwner, ReadRoute, affirm_after};
use crate::index::{ObjEntry, apply_put, standard_class};
use crate::list_token;
use crate::metrics::Metrics;
use crate::sync::coherence::{Consistency, WriteSync};
use crate::tier::CachedObject;

/// The same tuning shape [`WriteSync::new`] ships, in miniature: `dead_timeout_ms`
/// tracks the lease duration, and the probe timings are brisk so the lease shell's
/// warm-up window is milliseconds rather than a second.
fn brisk() -> Config {
    Config {
        gossip_interval_ms: 10,
        probe_interval_ms: 20,
        probe_timeout_ms: 10,
        suspect_timeout_ms: 50,
        dead_timeout_ms: 300,
        anti_entropy_interval_ms: 25,
        ..Config::default()
    }
}

/// One leased node, alone in its group. A solo reader's granter roster is empty, so
/// its lease confirms vacuously — which is exactly why the shell's warm-up guard,
/// not the confirmation, is what a booting node has to get past.
fn solo(id: &str) -> (Node<MemTransport>, Arc<WriteSync>) {
    let net = Network::new();
    let me = NodeId::new(id);
    let node = Node::builder(me.clone(), net.endpoint(me.clone()))
        .config(brisk())
        .spawn();
    let group = node.join_group("s3cache");
    let sync = WriteSync::attach(
        group,
        me,
        Consistency::Strong,
        LeaseConfig::for_duration(Duration::from_millis(300)),
        None,
    );
    (node, Arc::new(sync))
}

/// A node told to warm nothing affirms as soon as its lease allows — and that is
/// safe rather than a shortcut: with empty tiers and a passthrough index there is no
/// local state for the licence to license.
#[tokio::test]
async fn the_boot_affirmation_is_immediate_with_no_buckets() {
    let (_node, sync) = solo("boot-none");
    assert!(!sync.may_serve_local(), "a booting node holds no licence");

    affirm_after(
        Vec::new(),
        Some(Arc::clone(&sync)),
        Some(sync.resync_gen()),
        None,
    )
    .await;

    assert!(
        sync.may_serve_local(),
        "and takes one as soon as the lease shell's warm-up guard releases"
    );
}

/// With a bucket, the licence waits for that bucket's warm-up to land. A node whose
/// index is still filling from the origin must not answer a LIST or positive local read
/// out of it.
#[tokio::test]
async fn the_boot_affirmation_waits_for_every_bucket_warmup() {
    let (_node, sync) = solo("boot-one");
    let (done, warmed) = tokio::sync::oneshot::channel::<()>();
    let warmup = tokio::spawn(async move {
        let _ = warmed.await;
    });

    let affirming = tokio::spawn(affirm_after(
        vec![warmup],
        Some(Arc::clone(&sync)),
        Some(sync.resync_gen()),
        None,
    ));
    // Comfortably past the warm-up guard (one detection window plus two
    // anti-entropy rounds, ~100ms here): the lease would take the affirmation by
    // now, and the only thing holding it back is the bucket.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        !sync.may_serve_local(),
        "an index still filling from the origin licenses nothing"
    );

    drop(done);
    affirming.await.expect("the affirmation task");
    assert!(
        sync.may_serve_local(),
        "and the warm-up landing is what puts the node in service"
    );
}

/// Retry and affirmation ownership is independent of the coherence generation: boot
/// warm-up can start after a gap has already claimed that same coherence generation.
/// Only the newest full-index recovery may keep retrying or affirm it.
#[tokio::test]
async fn only_the_newest_full_sync_may_retry_or_affirm() {
    let (_node, sync) = solo("full-sync-owner");
    let owner = FullSyncOwner::default();
    let stale = owner.claim();
    let current = owner.claim();
    assert!(!stale.is_current());
    assert!(current.is_current());

    affirm_after(
        Vec::new(),
        Some(Arc::clone(&sync)),
        Some(sync.resync_gen()),
        Some(stale),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        !sync.may_serve_local(),
        "a superseded full sync cannot affirm while its replacement is pending"
    );

    affirm_after(
        Vec::new(),
        Some(Arc::clone(&sync)),
        Some(sync.resync_gen()),
        Some(current),
    )
    .await;
    assert!(
        sync.may_serve_local(),
        "the newest full sync retains retry and affirmation ownership"
    );
}

// ---- the retention read path (`validated_get`) -------------------------------

/// A proxy over an origin it never dials. Every case below is decided from local
/// state; a row that reached the endpoint would fail on the connection, not pass.
fn proxy(sync: Option<Arc<WriteSync>>) -> CachingProxy {
    let conf = aws_sdk_s3::config::Builder::new()
        .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
        .region(aws_sdk_s3::config::Region::new("us-east-1"))
        .credentials_provider(aws_sdk_s3::config::Credentials::new(
            "unused",
            "unused",
            None,
            None,
            "s3cache-unit-tests",
        ))
        .endpoint_url("http://127.0.0.1:1")
        .force_path_style(true)
        .build();
    let client = aws_sdk_s3::Client::from_conf(conf);
    CachingProxy::new(
        s3s_aws::Proxy::from(client.clone()),
        client,
        CacheConfig {
            cache_bytes: 1024 * 1024,
            max_obj_bytes: 1024 * 1024,
        },
        None,
        sync,
        Arc::new(Metrics::default()),
    )
}

fn at(secs: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_secs(secs)
}

fn ck(key: &str) -> (String, String) {
    ("b".to_owned(), key.to_owned())
}

/// A cached body at version `etag`, filled when the origin said `modified`.
fn cached(etag: &str, modified: SystemTime) -> Arc<CachedObject> {
    let out = GetObjectOutput {
        content_length: Some(4),
        content_type: Some("text/x-fixture".to_owned()),
        e_tag: Some(ETag::Strong(etag.to_owned())),
        last_modified: Some(Timestamp::from(modified)),
        ..Default::default()
    };
    Arc::new(CachedObject::from_get(&out, Bytes::from_static(b"body")))
}

/// Index `key` at version `etag` (or with none), stamped `modified`.
fn index(proxy: &CachingProxy, key: &str, etag: Option<&str>, modified: SystemTime) {
    apply_put(
        &proxy.state,
        "b",
        key,
        ObjEntry {
            size: Some(4),
            last_modified: modified,
            etag: etag.map(|tag| ETag::Strong(tag.to_owned())),
            storage_class: standard_class(),
            content_type: Some("text/x-fixture".to_owned()),
            meta: Some(Box::default()),
        },
    );
}

/// Flip the bucket to synced — the state in which the index may arbitrate.
fn synced(proxy: &CachingProxy) {
    proxy.state.mark_bucket_synced("b");
}

fn request<T>(input: T) -> S3Request<T> {
    S3Request {
        input,
        method: http::Method::GET,
        uri: http::Uri::default(),
        headers: HeaderMap::new(),
        extensions: http::Extensions::new(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

#[tokio::test]
async fn malformed_and_mismatched_owned_list_tokens_are_invalid_arguments() {
    let proxy = proxy(None);
    synced(&proxy);

    let malformed = ListObjectsV2Input {
        bucket: "b".to_owned(),
        continuation_token: Some("s3cache:list-token:v1:not_base64!".to_owned()),
        ..Default::default()
    };
    let error = proxy
        .list_objects_v2(request(malformed))
        .await
        .expect_err("a malformed owned token is rejected before routing");
    assert_eq!(*error.code(), S3ErrorCode::InvalidArgument);

    let shape = ListObjectsV2Input {
        bucket: "b".to_owned(),
        prefix: Some("expected/".to_owned()),
        ..Default::default()
    };
    let token = list_token::encode(&shape, "expected/cursor");
    let mismatch = ListObjectsV2Input {
        bucket: "b".to_owned(),
        prefix: Some("changed/".to_owned()),
        continuation_token: Some(token),
        ..Default::default()
    };
    let error = proxy
        .list_objects_v2(request(mismatch))
        .await
        .expect_err("a token cannot be reused with another request shape");
    assert_eq!(*error.code(), S3ErrorCode::InvalidArgument);
}

fn counter(proxy: &CachingProxy, name: &str) -> u64 {
    let text = proxy.metrics().prometheus_text();
    let prefix = format!("s3cache_{name} ");
    text.lines()
        .find_map(|line| line.strip_prefix(&prefix))
        .and_then(|value| value.parse().ok())
        .unwrap_or_else(|| panic!("{name} is not exposed:\n{text}"))
}

/// A peer can advertise a head before its frame is usable locally. The read barrier
/// must fail closed rather than serve the best stale state this node currently has.
#[tokio::test]
async fn a_freshness_timeout_routes_to_origin_and_is_counted() {
    let net = Network::new();
    let a_id = NodeId::new("barrier-a");
    let b_id = NodeId::new("barrier-b");
    let a_node = Node::builder(a_id.clone(), net.endpoint(a_id.clone()))
        .seed(b_id.clone())
        .config(brisk())
        .spawn();
    let b_node = Node::builder(b_id.clone(), net.endpoint(b_id.clone()))
        .seed(a_id.clone())
        .config(brisk())
        .spawn();
    let a_group = a_node.join_group("s3cache");
    let b_group = b_node.join_group("s3cache");
    let sync = Arc::new(WriteSync::attach(
        b_group.clone(),
        b_id,
        Consistency::Bounded,
        LeaseConfig::for_duration(Duration::from_millis(300)),
        None,
    ));
    let proxy = proxy(Some(sync));
    proxy.start_coherence(&[]);

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if b_group.members().contains(&a_id) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the peers converge");

    // This is a valid Groupnet frame whose payload is not an IndexEvent. The peer
    // cursor observes it, but s3cache cannot apply it and therefore cannot advance its
    // frontier to the advertised head.
    let feed = WriteFeed::new(
        a_group,
        NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN),
        |_value: &u8| vec![0],
    );
    let token = feed.publish(&1).await;
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if advertised_head(&b_group, &a_id) == Some(token) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the unusable head is advertised");

    assert!(matches!(
        proxy.read_barrier(&HeaderMap::new()).await,
        ReadRoute::Origin
    ));
    assert_eq!(counter(&proxy, "unhealthy_bypasses"), 1);
}

/// Single node: no feed, so this proxy is the only writer and its own tiers cannot
/// have missed anything — a copy is served with no index consulted and nothing
/// dropped, which is the property the warm tier's restart survival rests on.
#[tokio::test]
async fn a_single_node_serves_a_suspect_copy_unproved() {
    let proxy = proxy(None);
    proxy
        .obj_cache
        .insert(ck("k"), cached("v1", at(1_700_000_000)))
        .await;
    // Unsynced bucket, no entry: with a peer, this is the drop case.
    assert!(
        proxy.validated_get(&ck("k")).await.is_some(),
        "the sole writer's own copy is never suspect"
    );
    assert!(
        proxy.obj_cache.get(&ck("k")).await.is_some(),
        "and it is still there afterwards"
    );
}

/// The steady state: a copy stamped under the current generation is served on one
/// atomic load, without the index being asked anything.
#[tokio::test]
async fn a_proved_copy_is_served_without_consulting_the_index() {
    let (_node, sync) = solo("proved");
    let proxy = proxy(Some(sync));
    let obj = cached("v1", at(1_700_000_000));
    obj.mark_trusted(proxy.obj_cache.suspect_gen());
    proxy.obj_cache.insert(ck("k"), obj).await;

    // The bucket is unsynced and holds no entry — the state that drops a *suspect*
    // copy — so serving here can only be the stamp doing it.
    assert!(proxy.validated_get(&ck("k")).await.is_some());
    assert_eq!(counter(&proxy, "body_revalidations"), 0);
    assert_eq!(counter(&proxy, "body_revalidation_evictions"), 0);
}

/// A suspect copy the index confirms is served — and stamped, so it is proved once
/// and not once per read. The second call must not touch the index again.
#[tokio::test]
async fn a_suspect_copy_the_index_confirms_is_proved_exactly_once() {
    let (_node, sync) = solo("confirm");
    let proxy = proxy(Some(sync));
    let filled = at(1_700_000_000);
    proxy.obj_cache.insert(ck("k"), cached("v1", filled)).await;
    // The write-fill shape: the entry is stamped a moment after the body it describes.
    index(&proxy, "k", Some("v1"), filled + Duration::from_micros(120));
    synced(&proxy);

    assert!(proxy.validated_get(&ck("k")).await.is_some());
    assert_eq!(counter(&proxy, "body_revalidations"), 1);
    assert!(proxy.validated_get(&ck("k")).await.is_some());
    assert_eq!(
        counter(&proxy, "body_revalidations"),
        1,
        "the stamp put the second read back on the fast path"
    );
    assert_eq!(counter(&proxy, "body_revalidation_evictions"), 0);
}

/// Every way the index can contradict a copy, and the one answer to all of them:
/// drop it and let the origin serve the read.
#[tokio::test]
async fn a_suspect_copy_the_index_contradicts_is_dropped() {
    let (_node, sync) = solo("contradict");
    let proxy = proxy(Some(sync));
    let filled = at(1_700_000_000);
    synced(&proxy);

    // An overwrite this node missed: same key, a version it is not holding.
    proxy
        .obj_cache
        .insert(ck("rewritten"), cached("v1", filled))
        .await;
    index(&proxy, "rewritten", Some("v2"), filled);
    // A DELETE this node missed: on a synced bucket, absent means gone.
    proxy
        .obj_cache
        .insert(ck("deleted"), cached("v1", filled))
        .await;
    // Nothing to compare with: a skeletal entry proves the key exists and nothing
    // about which version of it.
    proxy
        .obj_cache
        .insert(ck("etagless"), cached("v1", filled))
        .await;
    index(&proxy, "etagless", None, filled);

    for key in ["rewritten", "deleted", "etagless"] {
        assert!(
            proxy.validated_get(&ck(key)).await.is_none(),
            "{key} must not be served"
        );
        assert!(
            proxy.obj_cache.get(&ck(key)).await.is_none(),
            "{key} must be gone from the tiers, so the refill cannot re-probe it"
        );
    }
    assert_eq!(counter(&proxy, "body_revalidation_evictions"), 3);
    assert_eq!(counter(&proxy, "body_revalidations"), 0);
}

/// The corner the mtime clause exists for: a rewrite storing byte-identical content
/// keeps the `ETag`, so only the moved mtime separates the new object from a copy of
/// the old one. Asserted against the fill it must *not* break — a write fill, whose
/// entry is stamped microseconds after its body.
#[tokio::test]
async fn a_byte_identical_rewrite_is_caught_by_the_mtime_and_a_fresh_fill_is_not() {
    let (_node, sync) = solo("rewrite");
    let proxy = proxy(Some(sync));
    let filled = at(1_700_000_000);
    synced(&proxy);

    proxy
        .obj_cache
        .insert(ck("rewritten"), cached("same", filled))
        .await;
    index(
        &proxy,
        "rewritten",
        Some("same"),
        filled + Duration::from_secs(30),
    );
    proxy
        .obj_cache
        .insert(ck("fresh"), cached("same", filled))
        .await;
    index(
        &proxy,
        "fresh",
        Some("same"),
        filled + Duration::from_micros(120),
    );

    assert!(
        proxy.validated_get(&ck("rewritten")).await.is_none(),
        "identical bytes, a newer object: the ETag agrees and the mtime does not"
    );
    assert!(
        proxy.validated_get(&ck("fresh")).await.is_some(),
        "and the stamp order of a real write fill must still validate"
    );
    assert_eq!(counter(&proxy, "body_revalidation_evictions"), 1);
    assert_eq!(counter(&proxy, "body_revalidations"), 1);
}

/// A bucket whose index has not finished warming has nothing to arbitrate with, so a
/// suspect copy is dropped rather than served on trust. Same outcome a flush would
/// have produced for the key — reached one key at a time, and only for the keys that
/// are actually read.
#[tokio::test]
async fn an_unsynced_bucket_arbitrates_nothing_and_drops_the_copy() {
    let (_node, sync) = solo("unsynced");
    let proxy = proxy(Some(sync));
    let filled = at(1_700_000_000);
    proxy.obj_cache.insert(ck("k"), cached("v1", filled)).await;
    // The entry is even *there* and even matches — it just cannot be trusted yet,
    // because the bucket's warm-up LIST has not landed.
    index(&proxy, "k", Some("v1"), filled);

    assert!(proxy.validated_get(&ck("k")).await.is_none());
    assert!(
        proxy.obj_cache.get(&ck("k")).await.is_none(),
        "dropped, not merely refused"
    );
    assert_eq!(
        counter(&proxy, "body_revalidation_evictions"),
        0,
        "nothing was contradicted — there was nothing to contradict it with"
    );
}
