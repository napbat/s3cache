//! Startup tests with a real S3 origin and a blocked background index scan.
//!
//! A strong node may serve a body it fetched after its coherence lease became valid.
//! A bucket with an incomplete index must still forward LIST requests to the origin.

mod common;

use std::sync::Arc;

use common::{
    Origin, WarmDir, counter, delete, free_udp_port, get, gossip_node, gossip_pair, list,
    proxy_over, proxy_over_with_metrics, put_typed, request, warm_proxy_over,
};
use s3cache::cache::proxy::CachingProxy;
use s3cache::metrics::Metrics;
use s3s::S3;
use s3s::dto::GetObjectInput;

const CAP: usize = 1024 * 1024;

/// Wait for a post-lease origin fill and a local hit. A pre-lease read increments
/// the licence-bypass counter and does not satisfy this condition.
async fn wait_for_early_hit(
    proxy: &CachingProxy,
    origin: &Origin,
    metrics: &Metrics,
    bucket: &str,
    key: &str,
    expected: &[u8],
) -> u64 {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        let bypasses = counter(metrics, "read_licence_bypasses");
        let gets = origin.ops.get();
        assert_eq!(get(proxy, bucket, key).await.as_ref(), expected);
        if counter(metrics, "read_licence_bypasses") == bypasses && origin.ops.get() == gets + 1 {
            assert_eq!(get(proxy, bucket, key).await.as_ref(), expected);
            if origin.ops.get() == gets + 1 {
                return gets + 1;
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the boot lease did not permit a cache hit before the index scan completed"
        );
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

/// Hold one actual origin LIST response. This keeps the index scan incomplete while
/// both nodes start, read, and mutate objects through the real S3 origin.
#[tokio::test]
async fn strong_boot_serves_fresh_body_before_the_index_finishes() {
    let origin = Origin::start("startup-early-body").await;
    let bucket = origin.bucket();
    origin.seed("body", b"version-1").await;
    origin.seed("doomed", b"soon-deleted").await;

    let (sync_a, sync_b) = gossip_pair("startup-body-a", "startup-body-b").await;
    let client = origin.counted_client();
    let node_a = proxy_over(&client, CAP, Some(sync_a));
    let metrics_b = Arc::new(Metrics::default());
    let node_b = proxy_over_with_metrics(&client, CAP, Some(sync_b), &metrics_b);
    for node in [&node_a, &node_b] {
        node.start_coherence(&[bucket.to_owned()]);
    }
    origin.pause_next_list();
    node_b.spawn_background_sync(vec![bucket.to_owned()]);
    origin.wait_for_paused_list().await;
    node_a.spawn_background_sync(vec![bucket.to_owned()]);

    let lists = origin.ops.list();
    let passthroughs = counter(&metrics_b, "list_passthrough");
    assert_eq!(list(&node_b, bucket).await, ["body", "doomed"]);
    assert_eq!(
        counter(&metrics_b, "list_passthrough"),
        passthroughs + 1,
        "the blocked index cannot answer LIST"
    );
    assert!(origin.ops.list() > lists, "the LIST reached the origin");

    let _ = wait_for_early_hit(&node_b, &origin, &metrics_b, bucket, "body", b"version-1").await;

    // The origin has answered this GET, but the cache has not received the
    // response. A peer delete must fence the pending body before it can commit.
    origin.pause_next_get();
    let pending_node = node_b.clone();
    let pending_bucket = bucket.to_owned();
    let pending = tokio::spawn(async move { get(&pending_node, &pending_bucket, "doomed").await });
    origin.wait_for_paused_get().await;
    delete(&node_a, bucket, "doomed").await;
    origin.release_paused_get();
    assert_eq!(
        pending.await.expect("the in-flight GET completes"),
        "soon-deleted"
    );
    let stale = node_b
        .get_object(request(GetObjectInput {
            bucket: bucket.to_owned(),
            key: "doomed".into(),
            ..Default::default()
        }))
        .await
        .expect_err("the deleted body must not be cached by the late GET");
    assert_eq!(stale.status_code().map(|status| status.as_u16()), Some(404));

    let gets = origin.ops.get();
    put_typed(&node_a, bucket, "body", b"version-2", "text/x-fixture").await;
    assert_eq!(get(&node_b, bucket, "body").await, "version-2");
    assert_eq!(
        origin.ops.get(),
        gets + 1,
        "the peer overwrite invalidates the early body"
    );
    assert_eq!(get(&node_b, bucket, "body").await, "version-2");
    assert_eq!(origin.ops.get(), gets + 1, "the new body remains cached");

    delete(&node_a, bucket, "body").await;
    let error = node_b
        .get_object(request(GetObjectInput {
            bucket: bucket.to_owned(),
            key: "body".into(),
            ..Default::default()
        }))
        .await
        .expect_err("the peer delete must invalidate the body");
    assert_eq!(error.status_code().map(|status| status.as_u16()), Some(404));

    origin.release_paused_list();
}

/// A disk body from a prior process has no proof in the current lease epoch. The
/// new node must fetch the current origin body while its LIST index is incomplete.
#[tokio::test]
async fn strong_boot_does_not_serve_an_old_disk_body_without_proof() {
    let origin = Origin::start("startup-warm-proof").await;
    let bucket = origin.bucket();
    let dir = WarmDir::new("startup-warm-proof");
    origin.seed("body", b"version-1").await;

    {
        let metrics = Arc::new(Metrics::default());
        let previous = warm_proxy_over(&origin.counted_client(), CAP, None, &dir, &metrics);
        assert_eq!(get(&previous, bucket, "body").await, "version-1");
        eventually!("the old body to reach the disk tier", dir.files() == 1);
    }
    // A peer stays online while this node is absent. Its write is in the feed
    // before the restarted node binds its gossip socket.
    let (port_a, port_b) = (free_udp_port(), free_udp_port());
    let sync_a = gossip_node("startup-warm-a", port_a, &[("startup-warm-b", port_b)]).await;
    let client = origin.counted_client();
    let node_a = proxy_over(&client, CAP, Some(sync_a));
    node_a.start_coherence(&[bucket.to_owned()]);
    put_typed(&node_a, bucket, "body", b"version-2", "text/x-fixture").await;

    let sync_b = gossip_node("startup-warm-b", port_b, &[("startup-warm-a", port_a)]).await;
    let metrics_b = Arc::new(Metrics::default());
    let node_b = warm_proxy_over(&client, CAP, Some(sync_b), &dir, &metrics_b);
    node_b.start_coherence(&[bucket.to_owned()]);
    origin.pause_next_list();
    node_b.spawn_background_sync(vec![bucket.to_owned()]);
    origin.wait_for_paused_list().await;

    let _ = wait_for_early_hit(&node_b, &origin, &metrics_b, bucket, "body", b"version-2").await;
    let passthroughs = counter(&metrics_b, "list_passthrough");
    assert_eq!(list(&node_b, bucket).await, ["body"]);
    assert_eq!(counter(&metrics_b, "list_passthrough"), passthroughs + 1);

    origin.release_paused_list();
}
