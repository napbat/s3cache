use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use groupnet::consistency::{LeaseConfig, WriteFeed, advertised_head};
use groupnet::core::{Config, NodeId};
use groupnet::runtime::Node;
use groupnet::transport::mem::{MemTransport, Network};
use http::HeaderMap;
use s3s::dto::{ETag, GetObjectInput, GetObjectOutput, ListObjectsV2Input, Timestamp};
use s3s::{S3, S3ErrorCode, S3Request};

use crate::cache::proxy::{CacheConfig, CachingProxy, FullSyncOwner, ObservedObject, ReadRoute};
use crate::index::{
    IndexedHead, ObjEntry, ObjMeta, apply_put, head_object_from_index, standard_class,
};
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

#[test]
fn only_the_newest_single_node_warmup_may_retry() {
    let owner = FullSyncOwner::default();
    let stale = owner.claim();
    let current = owner.claim();
    assert!(!stale.is_current());
    assert!(current.is_current());
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
        .retry_config(aws_sdk_s3::config::retry::RetryConfig::standard().with_max_attempts(1))
        .build();
    let client = aws_sdk_s3::Client::from_conf(conf);
    CachingProxy::new(
        &client,
        CacheConfig {
            cache_bytes: 1024 * 1024,
            max_obj_bytes: 1024 * 1024,
        },
        None,
        sync,
        Arc::new(Metrics::default()),
    )
}

/// Pause exactly after a local body or index answer was computed, before the
/// final serving-permission check. The two barriers make revocation ordered
/// without relying on scheduler timing.
async fn revoke_before_local_return(
    proxy: &CachingProxy,
    sync: &WriteSync,
    read: impl std::future::Future<Output = bool> + Send + 'static,
) {
    let pause = Arc::new(tokio::sync::Barrier::new(2));
    *proxy.read_return_pause.lock().unwrap() = Some(pause.clone());
    let response = tokio::spawn(read);
    tokio::time::timeout(Duration::from_secs(3), pause.wait())
        .await
        .expect("read reached its local-return decision");
    sync.require_lease_resync();
    assert!(!sync.may_serve_local());
    pause.wait().await;
    assert!(
        tokio::time::timeout(Duration::from_secs(5), response)
            .await
            .expect("origin fallback finishes")
            .expect("read task finishes"),
        "a revoked local answer must route to the origin"
    );
}

#[tokio::test]
async fn a_lease_revoked_after_local_body_lookup_forces_origin_get() {
    let (_node, sync) = solo("return-fence-get");
    let proxy = proxy(Some(sync.clone()));
    let body = cached("v1", at(1_700_000_000));
    body.mark_trusted(proxy.obj_cache.suspect_gen());
    proxy.obj_cache.insert(ck("k"), body).await;
    sync.start_apply(
        proxy.obj_cache.local(),
        proxy.state.clone(),
        proxy.metrics.clone(),
        Arc::new(|_: &str, _: &str, _: u64| {}),
    );
    tokio::time::timeout(Duration::from_secs(3), async {
        while !sync.may_serve_local() {
            let _ = sync.affirm_lease_now();
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("solo lease becomes valid");
    let pending = proxy.clone();
    revoke_before_local_return(&proxy, &sync, async move {
        pending
            .get_object(request(GetObjectInput {
                bucket: "b".to_owned(),
                key: "k".to_owned(),
                ..Default::default()
            }))
            .await
            .is_err()
    })
    .await;
}

#[tokio::test]
async fn a_lease_revoked_after_index_answer_forces_origin_list() {
    let (_node, sync) = solo("return-fence-list");
    let proxy = proxy(Some(sync.clone()));
    index(&proxy, "k", Some("v1"), at(1_700_000_000));
    synced(&proxy);
    sync.start_apply(
        proxy.obj_cache.local(),
        proxy.state.clone(),
        proxy.metrics.clone(),
        Arc::new(|_: &str, _: &str, _: u64| {}),
    );
    tokio::time::timeout(Duration::from_secs(3), async {
        while !sync.may_serve_local() {
            let _ = sync.affirm_lease_now();
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("solo lease becomes valid");
    let pending = proxy.clone();
    revoke_before_local_return(&proxy, &sync, async move {
        pending
            .list_objects_v2(request(ListObjectsV2Input {
                bucket: "b".to_owned(),
                ..Default::default()
            }))
            .await
            .is_err()
    })
    .await;
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

/// A read during bucket warm-up can discover an old object before LIST reaches it.
/// Its index clock must be the origin's mtime so HEAD stays faithful and a warm
/// body of that same version can pass revalidation after warm-up completes.
#[tokio::test]
async fn an_unknown_read_observation_keeps_the_origin_mtime() {
    let (_node, sync) = solo("observed-origin-mtime");
    let proxy = proxy(Some(sync));
    let modified = at(1_700_000_000);
    let observed = ObservedObject {
        size: Some(4),
        last_modified: Some(Timestamp::from(modified)),
        etag: Some(ETag::Strong("v1".to_owned())),
        content_type: Some("text/x-fixture".to_owned()),
        storage_class: standard_class(),
        meta: ObjMeta::default(),
    };
    proxy.observe("b", "old", &observed);

    let indexed_head = {
        let state = proxy.state.read().expect("index lock");
        head_object_from_index(state.get("b").map(|bucket| &bucket.keys), "old")
    };
    let IndexedHead::Faithful(head) = indexed_head else {
        panic!("the read observation must supply a faithful HEAD");
    };
    assert_eq!(head.last_modified, observed.last_modified);
    assert_eq!(head.content_length, observed.size);
    assert_eq!(head.e_tag, observed.etag);

    proxy
        .obj_cache
        .insert(ck("old"), cached("v1", modified))
        .await;
    synced(&proxy);
    assert!(proxy.validated_get(&ck("old")).await.is_some());
    assert_eq!(counter(&proxy, "body_revalidations"), 1);
    assert_eq!(counter(&proxy, "body_revalidation_timestamp_mismatch"), 0);
    assert_eq!(counter(&proxy, "body_revalidation_evictions"), 0);
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
    assert_eq!(counter(&proxy, "read_freshness_bypasses"), 1);
    assert_eq!(counter(&proxy, "read_licence_bypasses"), 0);
}

#[tokio::test]
async fn an_unlicensed_read_records_its_bypass_reason() {
    let (_node, sync) = solo("unlicensed-read");
    let proxy = proxy(Some(sync));
    assert!(matches!(
        proxy.read_barrier(&HeaderMap::new()).await,
        ReadRoute::Origin
    ));
    assert_eq!(counter(&proxy, "unhealthy_bypasses"), 1);
    assert_eq!(counter(&proxy, "read_licence_bypasses"), 1);
    assert_eq!(counter(&proxy, "read_freshness_bypasses"), 0);
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

#[tokio::test]
async fn peer_put_cannot_complete_new_index_metadata_from_an_older_get() {
    let proxy = Arc::new(proxy(None));
    let key = ck("racing");
    let old = cached("old", at(1_700_000_000));
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let reader = {
        let proxy = Arc::clone(&proxy);
        let key = key.clone();
        tokio::spawn(async move {
            proxy
                .obj_cache
                .get_or_fetch_with_commit(
                    &key,
                    async move {
                        let _ = started_tx.send(());
                        let _ = release_rx.await;
                        Ok::<_, String>((old, ()))
                    },
                    |obj| async move { Some(obj) },
                    || true,
                    |obj, ()| {
                        proxy.observe(
                            "b",
                            "racing",
                            &ObservedObject {
                                size: Some(4),
                                last_modified: obj.last_modified().cloned(),
                                etag: obj.e_tag().cloned(),
                                content_type: Some("old/type".to_owned()),
                                storage_class: standard_class(),
                                meta: ObjMeta::default(),
                            },
                        );
                    },
                )
                .await
        })
    };
    started_rx.await.expect("old origin GET started");
    let local = proxy.obj_cache.local();
    let mutation = local.fence_mutation(&key).await;
    apply_put(
        &proxy.state,
        "b",
        "racing",
        ObjEntry {
            size: Some(4),
            last_modified: at(1_700_000_001),
            etag: Some(ETag::Strong("new".to_owned())),
            storage_class: standard_class(),
            content_type: None,
            meta: None,
        },
    );
    drop(mutation);
    local.invalidate_hot(&key).await;
    release_tx.send(()).expect("finish old GET");
    assert!(reader.await.expect("GET task").is_ok());
    assert!(proxy.obj_cache.get(&key).await.is_none());
    let index = proxy.state.read().unwrap();
    let entry = &index.get("b").expect("bucket").keys["racing"];
    assert_eq!(entry.etag, Some(ETag::Strong("new".to_owned())));
    assert!(
        entry.meta.is_none(),
        "old GET metadata must not complete the new row"
    );
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
    assert_eq!(counter(&proxy, "body_revalidation_etag_mismatch"), 1);
    assert_eq!(counter(&proxy, "body_revalidation_index_absent"), 1);
    assert_eq!(counter(&proxy, "body_revalidation_missing_identity"), 1);
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
    assert_eq!(counter(&proxy, "body_revalidation_timestamp_mismatch"), 1);
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
